package engine_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/engine/enginetest"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

func dbOpts(fs vfs.FS, dir string, clk *enginetest.Clock, memSize int64, p wal.SyncPolicy) engine.Options {
	return engine.Options{FS: fs, Dir: dir, Sync: p, GroupCommit: true, MemtableSize: memSize, Now: clk.Now}
}

func openDB(t testing.TB, o engine.Options) *engine.DB {
	t.Helper()
	d, _, err := engine.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func stat(d *engine.DB, name string) string {
	for _, s := range d.Stats() {
		if s.Name == name {
			return s.Value
		}
	}
	return ""
}

func TestDBConformance(t *testing.T) {
	enginetest.Run(t, func(t *testing.T, now func() int64) engine.Engine {
		d, _, err := engine.Open(engine.Options{Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

// Same suite with a tiny memtable so nearly every operation straddles
// memtable rotations: reads must merge the active and immutable memtables.
func TestDBConformanceTinyMemtable(t *testing.T) {
	enginetest.Run(t, func(t *testing.T, now func() int64) engine.Engine {
		d, _, err := engine.Open(engine.Options{Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true, Now: now, MemtableSize: 1024})
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

func TestRotationAndShadowing(t *testing.T) {
	clk := enginetest.NewClock()
	d := openDB(t, dbOpts(vfs.Default, t.TempDir(), clk, 2048, wal.SyncNever))
	defer d.Close()
	v := func(s string) []byte { return []byte(s) }

	d.Set(v("victim"), v("old"), engine.SetOptions{})
	d.Set(v("overwritten"), v("v1"), engine.SetOptions{})
	d.Set(v("ttl"), v("x"), engine.SetOptions{ExpireAtMs: clk.Now() + 100})
	// Fill until at least 3 memtables were sealed (and, in the background,
	// flushed to SSTables): the old values now live on disk.
	for i := 0; stat(d, "memtable_rotations") < "3"; i++ {
		d.Set(v(fmt.Sprintf("fill%05d", i)), make([]byte, 100), engine.SetOptions{})
		if i > 10000 {
			t.Fatal("memtable never rotated")
		}
	}
	if err := d.WaitForFlush(); err != nil {
		t.Fatal(err)
	}
	if stat(d, "table_files") == "0" {
		t.Fatal("expected SSTables on disk")
	}
	// Tombstone / overwrite / expiry in the NEW memtable must hide data that
	// lives in OLD SSTables.
	d.Del([][]byte{v("victim")})
	d.Set(v("overwritten"), v("v2"), engine.SetOptions{})
	clk.Advance(100)
	if _, ok, _ := d.Get(v("victim")); ok {
		t.Fatal("tombstone in new memtable failed to hide value in old one")
	}
	if got, _, _ := d.Get(v("overwritten")); string(got) != "v2" {
		t.Fatalf("got %q", got)
	}
	if _, ok, _ := d.Get(v("ttl")); ok {
		t.Fatal("expired key visible")
	}
	// Scan merges all memtables, dedupes versions, hides dead keys.
	keys, _, _ := d.Scan(nil, 100000)
	seen := map[string]int{}
	for _, k := range keys {
		seen[string(k)]++
	}
	if seen["victim"] != 0 || seen["ttl"] != 0 || seen["overwritten"] != 1 {
		t.Fatalf("scan: victim=%d ttl=%d overwritten=%d", seen["victim"], seen["ttl"], seen["overwritten"])
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %s returned %d times", k, n)
		}
	}
	for i := 1; i < len(keys); i++ {
		if string(keys[i-1]) >= string(keys[i]) {
			t.Fatal("scan not sorted")
		}
	}
}

func TestRestartAfterRotation(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	o := dbOpts(vfs.Default, dir, clk, 4096, wal.SyncAlways)
	d := openDB(t, o)
	want := map[string]string{}
	for i := 0; i < 500; i++ {
		k, v := fmt.Sprintf("k%03d", i%120), fmt.Sprintf("v%d", i)
		d.Set([]byte(k), []byte(v), engine.SetOptions{})
		want[k] = v
		if i%9 == 0 {
			d.Del([][]byte{[]byte(k)})
			delete(want, k)
		}
	}
	if stat(d, "memtable_rotations") == "0" {
		t.Fatal("test did not rotate")
	}
	d.Close()
	d2, info, err := engine.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if stat(d2, "table_files") == "0" {
		t.Fatalf("expected SSTables to be recovered from the MANIFEST; wal segments replayed: %v", info.Segments)
	}
	for i := 0; i < 120; i++ {
		k := fmt.Sprintf("k%03d", i)
		got, ok, _ := d2.Get([]byte(k))
		if w, has := want[k]; has != ok || (ok && string(got) != w) {
			t.Fatalf("%s: got %q ok=%v want %q has=%v", k, got, ok, w, has)
		}
	}
	// Sequence numbers continue (no reuse that could mis-order versions).
	d2.Set([]byte("k001"), []byte("after"), engine.SetOptions{})
	d2.Close()
	d3 := openDB(t, o)
	defer d3.Close()
	if got, _, _ := d3.Get([]byte("k001")); string(got) != "after" {
		t.Fatalf("post-restart write shadowed by older version: %q", got)
	}
}

// A reader must never observe half of an MSET: batch visibility is atomic
// because the visible sequence number only advances after the whole batch has
// been inserted. MGet reads all keys from one snapshot, so a, b and c (always
// written together with the same value) must always match.
func TestBatchAtomicVisibility(t *testing.T) {
	d := openDB(t, engine.Options{Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true, MemtableSize: 64 << 10})
	defer d.Close()
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	const writes = 3000
	var wg sync.WaitGroup
	var stop atomic.Bool
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				vals, found, err := d.MGet(keys)
				if err != nil {
					t.Error(err)
					return
				}
				if found[0] != found[1] || found[1] != found[2] ||
					string(vals[0]) != string(vals[1]) || string(vals[1]) != string(vals[2]) {
					t.Errorf("torn batch observed: %q %q %q", vals[0], vals[1], vals[2])
					return
				}
			}
		}()
	}
	for i := 0; i < writes; i++ {
		s := []byte(strconv.Itoa(i))
		if err := d.MSet([][]byte{keys[0], s, keys[1], s, keys[2], s}); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestModelAgainstOracle(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		runModel(t, seed, 1500)
	}
}

// runModel applies random operations (including restarts) to the DB and to a
// trivially-correct map oracle, comparing observable results. Seeded so any
// failure is reproducible with the same seed.
func runModel(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	clk := enginetest.NewClock()
	dir := t.TempDir()
	o := dbOpts(vfs.Default, dir, clk, 2048, wal.SyncNever)
	d := openDB(t, o)
	type ent struct {
		v   string
		exp int64
	}
	oracle := map[string]ent{}
	live := func(k string) (ent, bool) {
		e, ok := oracle[k]
		if ok && e.exp != 0 && e.exp <= clk.Now() {
			return e, false
		}
		return e, ok
	}
	key := func() string { return fmt.Sprintf("k%02d", rng.Intn(40)) }
	fail := func(step int, format string, a ...any) {
		t.Helper()
		t.Fatalf("seed=%d step=%d: %s", seed, step, fmt.Sprintf(format, a...))
	}
	for step := 0; step < steps; step++ {
		k := key()
		switch op := rng.Intn(12); op {
		case 0, 1, 2: // SET
			v := fmt.Sprint("v", rng.Intn(1000))
			exp := int64(0)
			if rng.Intn(4) == 0 {
				exp = clk.Now() + int64(1+rng.Intn(500))
			}
			d.Set([]byte(k), []byte(v), engine.SetOptions{ExpireAtMs: exp})
			oracle[k] = ent{v, exp}
		case 3: // DEL
			n, _ := d.Del([][]byte{[]byte(k)})
			_, was := live(k)
			if (n == 1) != was {
				fail(step, "DEL %s returned %d, oracle live=%v", k, n, was)
			}
			delete(oracle, k)
		case 4: // INCR
			e, ok := live(k)
			cur, isInt := int64(0), true
			if ok {
				c, err := strconv.ParseInt(e.v, 10, 64)
				cur, isInt = c, err == nil
			}
			got, err := d.IncrBy([]byte(k), 3)
			if !isInt {
				if err == nil {
					fail(step, "INCR on non-integer succeeded")
				}
			} else {
				if err != nil || got != cur+3 {
					fail(step, "INCR %s got %d err %v want %d", k, got, err, cur+3)
				}
				oracle[k] = ent{strconv.FormatInt(cur+3, 10), e.exp}
				if !ok {
					oracle[k] = ent{strconv.FormatInt(cur+3, 10), 0}
				}
			}
		case 5: // EXPIRE
			ok, _ := d.SetExpiry([]byte(k), clk.Now()+int64(1+rng.Intn(300)))
			e, was := live(k)
			if ok != was {
				fail(step, "SetExpiry %s ok=%v oracle=%v", k, ok, was)
			}
			if was {
				tt, _ := d.TTL([]byte(k))
				e.exp = clk.Now() + tt
				oracle[k] = e
			}
		case 6: // PERSIST
			ok, _ := d.Persist([]byte(k))
			e, was := live(k)
			if ok != (was && e.exp != 0) {
				fail(step, "Persist %s", k)
			}
			if ok {
				e.exp = 0
				oracle[k] = e
			}
		case 7: // advance time
			clk.Advance(int64(rng.Intn(200)))
		case 8: // MSET
			a, b := key(), key()
			d.MSet([][]byte{[]byte(a), []byte("m1"), []byte(b), []byte("m2")})
			oracle[a] = ent{"m1", 0}
			oracle[b] = ent{"m2", 0}
		case 9: // restart (clean) — WAL replay must reproduce the exact state
			d.Close()
			d = openDB(t, o)
		case 10: // SCAN everything and compare
			keys, _, _ := d.Scan(nil, 1000)
			var want []string
			for kk := range oracle {
				if _, ok := live(kk); ok {
					want = append(want, kk)
				}
			}
			if len(keys) != len(want) {
				fail(step, "scan returned %d keys, oracle has %d", len(keys), len(want))
			}
		default: // GET
		}
		// After every step the touched key must agree with the oracle.
		got, ok, _ := d.Get([]byte(k))
		e, want := live(k)
		if ok != want || (ok && string(got) != e.v) {
			fail(step, "GET %s = %q ok=%v, oracle %q ok=%v", k, got, ok, e.v, want)
		}
	}
	d.Close()
}

// Power loss with concurrent writers, small memtables (so WAL segments rotate
// mid-run) and fsync=always: every acknowledged write and increment survives.
func TestDBCrashLosesNoAckedWrite(t *testing.T) {
	for _, mode := range []vfs.CrashMode{vfs.CrashDropUnsynced, vfs.CrashTornTail} {
		for round := 0; round < 6; round++ {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			clk := enginetest.NewClock()
			o := dbOpts(fs, dir, clk, 2048, wal.SyncAlways)
			d := openDB(t, o)
			var mu sync.Mutex
			acked := map[string]string{}
			var incr int64
			var wg sync.WaitGroup
			for g := 0; g < 6; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 40; i++ {
						k, v := fmt.Sprintf("g%d-k%d", g, i%5), fmt.Sprintf("v%d-%d", g, i)
						if ok, err := d.Set([]byte(k), []byte(v), engine.SetOptions{}); err == nil && ok {
							mu.Lock()
							acked[k] = v
							mu.Unlock()
						}
						if _, err := d.IncrBy([]byte("ctr"), 1); err == nil {
							mu.Lock()
							incr++
							mu.Unlock()
						}
					}
				}()
			}
			wg.Wait()
			d.WaitForFlush()
			if stat(d, "memtable_rotations") == "0" || stat(d, "flushes") == "0" {
				t.Fatal("expected rotations and flushes during the run")
			}
			fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Count: -1})
			d.Set([]byte("inflight"), []byte("x"), engine.SetOptions{}) // fails: never acked
			fs.Crash(mode)
			fs.ClearRules()

			d2 := openDB(t, o)
			for k, want := range acked {
				got, ok, _ := d2.Get([]byte(k))
				if !ok || string(got) != want {
					t.Fatalf("mode=%d round=%d: %s=%q ok=%v want %q", mode, round, k, got, ok, want)
				}
			}
			if v, _, _ := d2.Get([]byte("ctr")); string(v) != fmt.Sprint(incr) {
				t.Fatalf("counter %s != acked %d", v, incr)
			}
			d2.Close()
		}
	}
}
