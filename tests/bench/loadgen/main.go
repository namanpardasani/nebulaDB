// Command loadgen is a closed-loop RESP load generator: N connections each send
// one command, wait for the reply, record its latency, and repeat. It works
// against NebulaDB and Redis alike.
//
// Latency is exact (every operation is recorded and sorted), not bucketed. It is
// closed-loop, so it does not correct for coordinated omission: when the server
// stalls, the client also stops issuing requests. That makes tail latency
// optimistic under saturation, and it is the same for every target.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/tests/bench/benchutil"
)

type conn struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func dial(addr string) *conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return &conn{c, resp.NewReader(c), resp.NewWriter(c)}
}

func (c *conn) do(args ...[]byte) (resp.Value, error) {
	c.w.WriteCommand(args...)
	if err := c.w.Flush(); err != nil {
		return resp.Value{}, err
	}
	return c.r.ReadValue()
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	target := flag.String("target", "unnamed", "label for the result")
	workload := flag.String("workload", "mixed", "write | read | mixed | large | scan | preload")
	conns := flag.Int("conns", 16, "concurrent connections")
	dur := flag.Duration("duration", 8*time.Second, "measured duration")
	warm := flag.Duration("warmup", 2*time.Second, "warm-up (not measured)")
	keys := flag.Int("keys", 100000, "key space size")
	valSize := flag.Int("value-size", 100, "value size in bytes")
	bigSize := flag.Int("large-size", 16384, "value size for the large workload")
	out := flag.String("out", "", "append the JSON result to this file")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	val := make([]byte, *valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	big := make([]byte, *bigSize)
	for i := range big {
		big[i] = byte('A' + i%26)
	}
	keyOf := func(i int) []byte { return []byte("key:" + strconv.Itoa(i)) }

	if *workload == "preload" {
		preload(*addr, *keys, val)
		return
	}

	var stop atomic.Bool
	var measuring atomic.Bool
	var errCount atomic.Uint64
	lats := make([][]time.Duration, *conns)
	var wg sync.WaitGroup
	for ci := 0; ci < *conns; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			c := dial(*addr)
			defer c.c.Close()
			rng := rand.New(rand.NewSource(*seed*1000 + int64(ci)))
			local := make([]time.Duration, 0, 1<<16)
			cursor := []byte("0")
			for !stop.Load() {
				k := keyOf(rng.Intn(*keys))
				var args [][]byte
				switch *workload {
				case "write":
					args = [][]byte{[]byte("SET"), k, val}
				case "read": // read-heavy: 95% GET, 5% SET
					if rng.Intn(100) < 95 {
						args = [][]byte{[]byte("GET"), k}
					} else {
						args = [][]byte{[]byte("SET"), k, val}
					}
				case "mixed":
					if rng.Intn(2) == 0 {
						args = [][]byte{[]byte("GET"), k}
					} else {
						args = [][]byte{[]byte("SET"), k, val}
					}
				case "large":
					args = [][]byte{[]byte("SET"), []byte("big:" + strconv.Itoa(rng.Intn(2000))), big}
				case "scan":
					args = [][]byte{[]byte("SCAN"), cursor, []byte("COUNT"), []byte("100")}
				}
				t0 := time.Now()
				v, err := c.do(args...)
				d := time.Since(t0)
				if err != nil || v.Kind == resp.KindError {
					errCount.Add(1)
					if err != nil {
						return
					}
					continue
				}
				if *workload == "scan" && v.Kind == resp.KindArray && len(v.Elems) == 2 {
					cursor = v.Elems[0].Str
				}
				if measuring.Load() {
					local = append(local, d)
				}
			}
			lats[ci] = local
		}(ci)
	}
	time.Sleep(*warm)
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(*dur)
	measuring.Store(false)
	elapsed := time.Since(t0)
	stop.Store(true)
	wg.Wait()

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	r := benchutil.Result{Suite: "network", Target: *target, Workload: *workload, Conns: *conns, Errors: errCount.Load(),
		Extra: map[string]any{"keys": *keys, "value_size": *valSize, "large_size": *bigSize}}
	benchutil.Summarize(&r, all, elapsed)
	fmt.Printf("%-24s %-6s conns=%-3d %10.0f ops/s  mean %7.0fus  p50 %7.0fus  p99 %8.0fus  p99.9 %9.0fus  errors=%d\n",
		r.Target, r.Workload, r.Conns, r.OpsPerSec, r.MeanUs, r.P50Us, r.P99Us, r.P999Us, r.Errors)
	if *out != "" {
		if err := benchutil.Append(*out, r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

// preload fills the key space with pipelined SETs on several connections
// (unmeasured), so read workloads find data.
func preload(addr string, keys int, val []byte) {
	const workers, depth = 8, 128
	var wg sync.WaitGroup
	per := (keys + workers - 1) / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := dial(addr)
			defer c.c.Close()
			lo, hi := w*per, (w+1)*per
			if hi > keys {
				hi = keys
			}
			for i := lo; i < hi; i += depth {
				end := i + depth
				if end > hi {
					end = hi
				}
				for j := i; j < end; j++ {
					c.w.WriteCommand([]byte("SET"), []byte("key:"+strconv.Itoa(j)), val)
				}
				c.w.Flush()
				for j := i; j < end; j++ {
					if _, err := c.r.ReadValue(); err != nil {
						fmt.Fprintln(os.Stderr, "preload:", err)
						os.Exit(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
}
