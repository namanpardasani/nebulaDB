package model

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/internal/server"
	"github.com/naman/nebuladb/internal/wal"
)

func startStress(t *testing.T) (addr string, db *engine.DB) {
	t.Helper()
	db, _, err := engine.Open(engine.Options{
		Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true,
		MemtableSize: 16 << 10, BlockSize: 1024, BloomBits: 10,
		Compaction: engine.CompactionOptions{L0CompactTrigger: 2, L0SlowdownTrigger: 8, L0StopTrigger: 16, LevelSizeRatio: 4, L1MaxBytes: 64 << 10, TargetFileBytes: 16 << 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(db, server.Options{MaxConns: 512})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		db.Close()
	})
	return ln.Addr().String(), db
}

type conn struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func connect(t testing.TB, addr string) *conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return nil
	}
	return &conn{c, resp.NewReader(c), resp.NewWriter(c)}
}

func (c *conn) do(args ...string) (resp.Value, error) {
	c.c.SetDeadline(time.Now().Add(20 * time.Second))
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	c.w.WriteCommand(bs...)
	if err := c.w.Flush(); err != nil {
		return resp.Value{}, err
	}
	return c.r.ReadValue()
}

// TestConcurrentClientsInvariants runs many clients against one database that
// is continuously flushing and compacting, and checks invariants no interleaving
// may violate. Run with -race.
func TestConcurrentClientsInvariants(t *testing.T) {
	addr, db := startStress(t)
	dur := 3 * time.Second
	if testing.Short() {
		dur = time.Second
	}
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	var failed atomic.Bool
	failf := func(f string, a ...any) {
		if failed.CompareAndSwap(false, true) {
			t.Errorf(f, a...)
		}
	}

	// 1. Shared counters: every acknowledged INCR must be counted exactly once.
	const incrClients, nCounters = 12, 4
	var incrDone [nCounters]atomic.Int64
	for i := 0; i < incrClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := connect(t, addr)
			if c == nil {
				return
			}
			defer c.c.Close()
			for n := 0; time.Now().Before(deadline); n++ {
				k := n % nCounters
				v, err := c.do("INCR", fmt.Sprintf("ctr%d", k))
				if err != nil || v.Kind != resp.KindInt {
					failf("INCR: %v %v", err, v)
					return
				}
				incrDone[k].Add(1)
			}
		}(i)
	}

	// 2. Monotonic registers: one writer per key; readers must never see a value
	// older than what the writer had already been acknowledged before the read
	// began, and never go backwards.
	const regs = 6
	var acked [regs]atomic.Int64
	for r := 0; r < regs; r++ {
		wg.Add(1)
		go func(r int) { // writer
			defer wg.Done()
			c := connect(t, addr)
			if c == nil {
				return
			}
			defer c.c.Close()
			key := fmt.Sprintf("reg%d", r)
			for n := int64(1); time.Now().Before(deadline); n++ {
				if _, err := c.do("SET", key, strconv.FormatInt(n, 10)); err != nil {
					failf("SET: %v", err)
					return
				}
				acked[r].Store(n)
			}
		}(r)
		for rd := 0; rd < 2; rd++ {
			wg.Add(1)
			go func(r int) { // readers
				defer wg.Done()
				c := connect(t, addr)
				if c == nil {
					return
				}
				defer c.c.Close()
				key := fmt.Sprintf("reg%d", r)
				var prev int64
				for time.Now().Before(deadline) {
					floor := acked[r].Load()
					v, err := c.do("GET", key)
					if err != nil {
						failf("GET: %v", err)
						return
					}
					var got int64
					if !v.Nil {
						got, _ = strconv.ParseInt(string(v.Str), 10, 64)
					}
					if got < floor {
						failf("stale read of %s: saw %d after the writer had been acknowledged %d", key, got, floor)
						return
					}
					if got < prev {
						failf("time went backwards on %s: %d after %d", key, got, prev)
						return
					}
					prev = got
				}
			}(r)
		}
	}

	// 3. Atomic multi-key writes: MGET (one snapshot) must never see a torn MSET.
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := connect(t, addr)
		if c == nil {
			return
		}
		defer c.c.Close()
		for n := 0; time.Now().Before(deadline); n++ {
			s := strconv.Itoa(n)
			if _, err := c.do("MSET", "atomA", s, "atomB", s, "atomC", s); err != nil {
				failf("MSET: %v", err)
				return
			}
		}
	}()
	for rd := 0; rd < 3; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := connect(t, addr)
			if c == nil {
				return
			}
			defer c.c.Close()
			for time.Now().Before(deadline) {
				v, err := c.do("MGET", "atomA", "atomB", "atomC")
				if err != nil || v.Kind != resp.KindArray || len(v.Elems) != 3 {
					failf("MGET: %v %v", err, v)
					return
				}
				a, b, cc := render(v.Elems[0]), render(v.Elems[1]), render(v.Elems[2])
				if a != b || b != cc {
					failf("torn MSET observed by MGET: %s %s %s", a, b, cc)
					return
				}
			}
		}()
	}

	// 4. SCAN under churn: sorted, no duplicates, and every key that was never
	// modified (the 'stable:*' set) is always present.
	const stable = 120
	setup := connect(t, addr)
	for i := 0; i < stable; i++ {
		setup.do("SET", fmt.Sprintf("stable:%03d", i), "x")
	}
	setup.c.Close()
	for sc := 0; sc < 2; sc++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := connect(t, addr)
			if c == nil {
				return
			}
			defer c.c.Close()
			for time.Now().Before(deadline) {
				seen := map[string]bool{}
				cursor, prev := "0", ""
				for {
					v, err := c.do("SCAN", cursor, "MATCH", "stable:*", "COUNT", "25")
					if err != nil || v.Kind != resp.KindArray {
						failf("SCAN: %v", err)
						return
					}
					cursor = string(v.Elems[0].Str)
					for _, e := range v.Elems[1].Elems {
						k := string(e.Str)
						if seen[k] || k <= prev {
							failf("SCAN not strictly ascending/unique at %q (prev %q)", k, prev)
							return
						}
						seen[k], prev = true, k
					}
					if cursor == "0" {
						break
					}
				}
				if len(seen) != stable {
					failf("SCAN saw %d of %d untouched keys while the tree was compacting", len(seen), stable)
					return
				}
			}
		}()
	}

	// 5. Contended SET/DEL/EXISTS on a small key space plus connection churn and
	// pipelined bursts, to shake out races in the server/engine paths.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				c := connect(t, addr)
				if c == nil {
					return
				}
				for n := 0; n < 50; n++ {
					c.w.WriteCommand([]byte("SET"), []byte(fmt.Sprintf("hot%d", n%5)), []byte(fmt.Sprintf("g%d-%d", g, n)))
					c.w.WriteCommand([]byte("DEL"), []byte(fmt.Sprintf("hot%d", (n+1)%5)))
					c.w.WriteCommand([]byte("EXISTS"), []byte("hot0"), []byte("hot1"))
				}
				if err := c.w.Flush(); err != nil {
					failf("burst write: %v", err)
					return
				}
				for i := 0; i < 150; i++ {
					if _, err := c.r.ReadValue(); err != nil {
						failf("burst read %d: %v", i, err)
						return
					}
				}
				c.c.Close() // churn
			}
		}(g)
	}

	wg.Wait()
	if failed.Load() {
		return
	}
	// Final accounting: counters are exact.
	c := connect(t, addr)
	defer c.c.Close()
	var keys []string
	for k := 0; k < nCounters; k++ {
		v, _ := c.do("GET", fmt.Sprintf("ctr%d", k))
		got, _ := strconv.ParseInt(string(v.Str), 10, 64)
		if got != incrDone[k].Load() {
			t.Errorf("ctr%d = %d but %d increments were acknowledged: lost or duplicated update", k, got, incrDone[k].Load())
		}
		keys = append(keys, strconv.FormatInt(got, 10))
	}
	sort.Strings(keys)
	st := map[string]string{}
	for _, s := range db.Stats() {
		st[s.Name] = s.Value
	}
	t.Logf("stress: counters %v; flushes=%s compactions=%s l0=%s wal_max_group=%s write_slowdowns=%s",
		keys, st["flushes"], st["compactions"], st["l0_files"], st["wal_max_group"], st["write_slowdowns"])
	if st["flushes"] == "0" || st["compactions"] == "0" {
		t.Errorf("the stress run did not exercise flush/compaction (flushes=%s compactions=%s)", st["flushes"], st["compactions"])
	}
}
