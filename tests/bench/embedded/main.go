// Command embedded compares NebulaDB, Pebble and BadgerDB used as in-process
// libraries on identical workloads. Each invocation measures ONE engine in its
// own process, so the engines never share a heap or page-cache warm-up.
//
// Workloads: write (random-key puts), read (random gets, all hits), mixed
// (50/50), scan (key-only range reads of 100 keys), amp (write/space
// amplification: N overwrites, wait for background work to settle, compare
// bytes the process wrote to disk and the directory size with the logical data).
//
// Durability: -sync=true makes every write durable before it returns
// (NebulaDB fsync=always, Pebble pebble.Sync, Badger SyncWrites); -sync=false
// leaves flushing to the OS.
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/dgraph-io/badger/v4"

	"github.com/naman/nebuladb/pkg/nebuladb"
	"github.com/naman/nebuladb/tests/bench/benchutil"
)

// store is the minimal common surface the workloads need.
type store interface {
	Put(k, v []byte) error
	Get(k []byte) ([]byte, bool, error)
	ScanKeys(start []byte, n int) (int, error)
	Settle() error // wait for background flush/compaction to go idle
	Stats() map[string]any
	Close() error
}

const cacheBytes, memtableBytes = 32 << 20, 4 << 20

// ---------------------------------------------------------------- NebulaDB

type nebula struct {
	db *nebuladb.DB
}

func openNebula(dir string, sync bool, style string) (store, error) {
	o := &nebuladb.Options{MemtableSize: memtableBytes, BlockCacheBytes: cacheBytes, CompactionStyle: style}
	if !sync {
		o.Sync = nebuladb.SyncNever
	}
	db, err := nebuladb.Open(dir, o)
	return &nebula{db}, err
}
func (n *nebula) Put(k, v []byte) error { return n.db.Set(k, v) }
func (n *nebula) Get(k []byte) ([]byte, bool, error) {
	v, err := n.db.Get(k)
	if errors.Is(err, nebuladb.ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}
func (n *nebula) ScanKeys(start []byte, cnt int) (int, error) {
	ks, _, err := n.db.Keys(start, cnt)
	return len(ks), err
}
func (n *nebula) Settle() error {
	if err := n.db.WaitIdle(); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond) // let a triggered compaction start, then wait it out
	for i := 0; i < 600; i++ {
		s := n.db.Stats()
		c1 := s["compactions"]
		time.Sleep(250 * time.Millisecond)
		if n.db.Stats()["compactions"] == c1 {
			return nil
		}
	}
	return nil
}
func (n *nebula) Stats() map[string]any {
	m := map[string]any{}
	for k, v := range n.db.Stats() {
		m[k] = v
	}
	return m
}
func (n *nebula) Close() error { return n.db.Close() }

// ---------------------------------------------------------------- Pebble

type pebbleStore struct {
	db   *pebble.DB
	sync bool
	wo   *pebble.WriteOptions
}

func openPebble(dir string, sync bool) (store, error) {
	c := pebble.NewCache(cacheBytes)
	defer c.Unref()
	db, err := pebble.Open(dir, &pebble.Options{Cache: c, MemTableSize: memtableBytes})
	wo := pebble.NoSync
	if sync {
		wo = pebble.Sync
	}
	return &pebbleStore{db, sync, wo}, err
}
func (p *pebbleStore) Put(k, v []byte) error { return p.db.Set(k, v, p.wo) }
func (p *pebbleStore) Get(k []byte) ([]byte, bool, error) {
	v, closer, err := p.db.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, true, nil
}
func (p *pebbleStore) ScanKeys(start []byte, cnt int) (int, error) {
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: start})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for ok := it.First(); ok && n < cnt; ok = it.Next() {
		_ = it.Key()
		n++
	}
	return n, it.Error()
}
func (p *pebbleStore) Settle() error {
	if err := p.db.Flush(); err != nil {
		return err
	}
	for i := 0; i < 600; i++ {
		m := p.db.Metrics()
		if m.Compact.NumInProgress == 0 && m.Compact.EstimatedDebt < 1<<20 {
			time.Sleep(300 * time.Millisecond)
			m2 := p.db.Metrics()
			if m2.Compact.NumInProgress == 0 {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return nil
}
func (p *pebbleStore) Stats() map[string]any {
	m := p.db.Metrics()
	return map[string]any{"pebble_compactions": m.Compact.Count, "pebble_flushes": m.Flush.Count}
}
func (p *pebbleStore) Close() error { return p.db.Close() }

// ---------------------------------------------------------------- Badger

type badgerStore struct{ db *badger.DB }

func openBadger(dir string, sync bool) (store, error) {
	o := badger.DefaultOptions(dir).WithSyncWrites(sync).WithLogger(nil).
		WithBlockCacheSize(cacheBytes).WithIndexCacheSize(cacheBytes).WithMemTableSize(memtableBytes)
	db, err := badger.Open(o)
	return &badgerStore{db}, err
}
func (b *badgerStore) Put(k, v []byte) error {
	return b.db.Update(func(txn *badger.Txn) error { return txn.Set(k, v) })
}
func (b *badgerStore) Get(k []byte) ([]byte, bool, error) {
	var out []byte
	err := b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(k)
		if err != nil {
			return err
		}
		out, err = it.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	return out, err == nil, err
}
func (b *badgerStore) ScanKeys(start []byte, cnt int) (int, error) {
	n := 0
	err := b.db.View(func(txn *badger.Txn) error {
		o := badger.DefaultIteratorOptions
		o.PrefetchValues = false
		it := txn.NewIterator(o)
		defer it.Close()
		for it.Seek(start); it.Valid() && n < cnt; it.Next() {
			_ = it.Item().Key()
			n++
		}
		return nil
	})
	return n, err
}
func (b *badgerStore) Settle() error {
	b.db.Sync()
	time.Sleep(2 * time.Second) // Badger exposes no "compactions idle" signal; give its compactors time
	return nil
}
func (b *badgerStore) Stats() map[string]any {
	lsm, vlog := b.db.Size()
	return map[string]any{"badger_lsm_bytes": lsm, "badger_vlog_bytes": vlog}
}
func (b *badgerStore) Close() error { return b.db.Close() }

// ---------------------------------------------------------------- workloads

func keyOf(i int) []byte {
	k := make([]byte, 12)
	copy(k, "key:")
	binary.BigEndian.PutUint64(k[4:], uint64(i))
	return k
}

func procWriteBytes() uint64 {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "write_bytes:") {
			v, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(sc.Text(), "write_bytes:")), 10, 64)
			return v
		}
	}
	return 0
}

func dirBytes(dir string) (n int64) {
	filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			n += fi.Size()
		}
		return nil
	})
	return
}

func main() {
	eng := flag.String("engine", "nebuladb", "nebuladb | pebble | badger")
	style := flag.String("style", "leveled", "NebulaDB compaction style: leveled | size-tiered")
	doSync := flag.Bool("sync", false, "make every write durable before it returns")
	workload := flag.String("workload", "mixed", "write | read | mixed | scan | amp")
	threads := flag.Int("threads", 16, "concurrent goroutines")
	keys := flag.Int("keys", 100000, "key space")
	valSize := flag.Int("value-size", 100, "value size")
	dur := flag.Duration("duration", 8*time.Second, "measured duration")
	warm := flag.Duration("warmup", 2*time.Second, "warm-up")
	ops := flag.Int("ops", 500000, "overwrites for the amp workload")
	dir := flag.String("dir", "", "data directory (default: temp under ./bin)")
	out := flag.String("out", "", "append JSON result here")
	label := flag.String("label", "", "target label (default engine[-sync])")
	flag.Parse()

	if *dir == "" {
		d, _ := os.MkdirTemp("../../bin", "embedded-*")
		*dir = d
	}
	defer os.RemoveAll(*dir)
	var s store
	var err error
	switch *eng {
	case "nebuladb":
		s, err = openNebula(*dir, *doSync, *style)
	case "pebble":
		s, err = openPebble(*dir, *doSync)
	case "badger":
		s, err = openBadger(*dir, *doSync)
	default:
		err = fmt.Errorf("unknown engine %q", *eng)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer s.Close()
	name := *label
	if name == "" {
		name = *eng
		if *eng == "nebuladb" && *style != "leveled" {
			name += "-" + *style
		}
		if *doSync {
			name += "-sync"
		}
	}
	val := make([]byte, *valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	if *workload == "amp" {
		runAmp(s, name, *dir, *keys, *ops, val, *eng, *style, *out)
		return
	}

	// Preload (unmeasured) with a relaxed write path would hide differences in
	// on-disk layout, so use the same engine, just do it concurrently.
	{
		var wg sync.WaitGroup
		per := (*keys + 7) / 8
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := w * per; i < (w+1)*per && i < *keys; i++ {
					if err := s.Put(keyOf(i), val); err != nil {
						fmt.Fprintln(os.Stderr, "preload:", err)
						os.Exit(1)
					}
				}
			}(w)
		}
		wg.Wait()
		s.Settle()
	}

	var stop, measuring atomic.Bool
	var errs atomic.Uint64
	lats := make([][]time.Duration, *threads)
	var wg sync.WaitGroup
	for t := 0; t < *threads; t++ {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(t) + 7))
			local := make([]time.Duration, 0, 1<<16)
			for !stop.Load() {
				k := keyOf(rng.Intn(*keys))
				t0 := time.Now()
				var err error
				switch *workload {
				case "write":
					err = s.Put(k, val)
				case "read":
					var ok bool
					_, ok, err = s.Get(k)
					if err == nil && !ok {
						err = errors.New("missing key")
					}
				case "mixed":
					if rng.Intn(2) == 0 {
						_, _, err = s.Get(k)
					} else {
						err = s.Put(k, val)
					}
				case "scan":
					_, err = s.ScanKeys(k, 100)
				}
				d := time.Since(t0)
				if err != nil {
					errs.Add(1)
					continue
				}
				if measuring.Load() {
					local = append(local, d)
				}
			}
			lats[t] = local
		}(t)
	}
	time.Sleep(*warm)
	measuring.Store(true)
	w0, t0 := procWriteBytes(), time.Now()
	time.Sleep(*dur)
	measuring.Store(false)
	elapsed, w1 := time.Since(t0), procWriteBytes()
	stop.Store(true)
	wg.Wait()
	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	r := benchutil.Result{Suite: "embedded", Target: name, Workload: *workload, Conns: *threads, Errors: errs.Load(),
		Extra: map[string]any{"engine": *eng, "sync": *doSync, "keys": *keys, "value_size": *valSize, "disk_write_bytes_per_sec": float64(w1-w0) / elapsed.Seconds()}}
	benchutil.Summarize(&r, all, elapsed)
	fmt.Printf("%-22s %-6s threads=%-3d %10.0f ops/s  mean %7.0fus  p50 %7.0fus  p99 %8.0fus  p99.9 %9.0fus  errors=%d\n",
		r.Target, r.Workload, r.Conns, r.OpsPerSec, r.MeanUs, r.P50Us, r.P99Us, r.P999Us, r.Errors)
	if *out != "" {
		benchutil.Append(*out, r)
	}
}

// runAmp measures amplification. The workload: write every key once (so the data
// set exists), then `ops` random overwrites. Afterwards background work is
// allowed to settle and we compare what the process wrote to disk and what is
// stored against the logical data.
func runAmp(s store, name, dir string, keys, ops int, val []byte, eng, style, out string) {
	w0 := procWriteBytes()
	t0 := time.Now()
	rng := rand.New(rand.NewSource(3))
	var user int64
	for i := 0; i < keys; i++ {
		s.Put(keyOf(i), val)
		user += int64(12 + len(val))
	}
	for i := 0; i < ops; i++ {
		s.Put(keyOf(rng.Intn(keys)), val)
		user += int64(12 + len(val))
	}
	s.Settle()
	elapsed := time.Since(t0)
	written := procWriteBytes() - w0
	live := int64(keys) * int64(12+len(val))
	size := dirBytes(dir)
	extra := map[string]any{
		"engine": eng, "user_bytes": user, "live_bytes": live, "disk_write_bytes": written, "dir_bytes": size,
		"write_amp_os": float64(written) / float64(user), "space_amp": float64(size) / float64(live), "seconds_total": elapsed.Seconds(),
		"keys": keys, "overwrites": ops,
	}
	for k, v := range s.Stats() {
		extra[k] = v
	}
	// Read amplification (NebulaDB only: it counts block reads): random gets
	// with a tiny cache effect ignored, report blocks read per get.
	if n, ok := s.(*nebula); ok {
		before := n.db.Stats()
		const gets = 20000
		for i := 0; i < gets; i++ {
			n.Get(keyOf(rng.Intn(keys)))
		}
		after := n.db.Stats()
		br := func(m map[string]string) float64 { f, _ := strconv.ParseFloat(m["block_reads"], 64); return f }
		hits := func(m map[string]string) float64 { f, _ := strconv.ParseFloat(m["block_cache_hits"], 64); return f }
		misses := func(m map[string]string) float64 { f, _ := strconv.ParseFloat(m["block_cache_misses"], 64); return f }
		extra["read_amp_blocks_per_get"] = (hits(after) + misses(after) - hits(before) - misses(before)) / gets
		extra["read_amp_disk_blocks_per_get"] = (br(after) - br(before)) / gets
		extra["table_files"] = after["table_files"]
	}
	r := benchutil.Result{Suite: "amp", Target: name, Workload: "amp", Extra: extra}
	benchutil.Summarize(&r, nil, elapsed)
	fmt.Printf("%-24s write_amp(os)=%.2f space_amp=%.2f  disk_written=%.1fMiB dir=%.1fMiB live=%.1fMiB  %.1fs\n",
		name, extra["write_amp_os"], extra["space_amp"], float64(written)/1048576, float64(size)/1048576, float64(live)/1048576, elapsed.Seconds())
	if rb, ok := extra["read_amp_blocks_per_get"]; ok {
		fmt.Printf("%-24s read_amp: %.2f block lookups per get (%.3f disk block reads per get), tables=%v\n", "", rb, extra["read_amp_disk_blocks_per_get"], extra["table_files"])
	}
	if out != "" {
		benchutil.Append(out, r)
	}
}
