package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

type clock struct{ ms atomic.Int64 }

func newClock() *clock       { c := &clock{}; c.ms.Store(1_000_000); return c }
func (c *clock) Now() int64  { return c.ms.Load() }
func (c *clock) Add(d int64) { c.ms.Add(d) }

func smallOpts(dir string, clk *clock) Options {
	return Options{
		Dir: dir, Sync: wal.SyncNever, GroupCommit: true, Now: clk.Now,
		MemtableSize: 4 << 10, BlockSize: 512, BloomBits: 10,
		Compaction: CompactionOptions{L0CompactTrigger: 2, L0SlowdownTrigger: 6, L0StopTrigger: 10, LevelSizeRatio: 4, L1MaxBytes: 16 << 10, TargetFileBytes: 6 << 10},
	}
}

func mustOpen(t testing.TB, o Options) *DB {
	t.Helper()
	d, _, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func b(s string) []byte { return []byte(s) }

// waitIdle blocks until no flush or compaction is pending.
func waitIdle(t testing.TB, d *DB) {
	t.Helper()
	if err := d.WaitForFlush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		d.compactMu.Lock()
		d.mu.RLock()
		cur := d.cur
		cur.ref()
		d.mu.RUnlock()
		pending := d.pickCompaction(cur) != nil
		cur.unref()
		d.compactMu.Unlock()
		if !pending {
			return
		}
		d.kickCompaction()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("compaction never became idle")
}

// checkInvariants verifies the structural rules of the tree.
func checkInvariants(t testing.TB, d *DB) {
	t.Helper()
	d.mu.RLock()
	cur := d.cur
	d.mu.RUnlock()
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		fs := cur.levels[lvl]
		for i := 1; i < len(fs); i++ {
			if bytes.Compare(fs[i-1].meta.Largest, fs[i].meta.Smallest) >= 0 {
				t.Fatalf("level %d files %d and %d overlap", lvl, fs[i-1].meta.Num, fs[i].meta.Num)
			}
		}
	}
	// Version in memory must equal the MANIFEST on disk.
	_, mv, _, err := manifest.Dump(d.opts.FS, d.opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for lvl := range cur.levels {
		if len(cur.levels[lvl]) != len(mv.Levels[lvl]) {
			t.Fatalf("level %d: %d files in memory, %d in MANIFEST", lvl, len(cur.levels[lvl]), len(mv.Levels[lvl]))
		}
	}
	// Every .sst on disk is live (obsolete files may linger only while pinned).
	live := map[string]bool{}
	for _, l := range cur.levels {
		for _, f := range l {
			live[fmt.Sprintf("%06d.sst", f.meta.Num)] = true
		}
	}
	names, _ := d.opts.FS.List(d.opts.Dir)
	for _, n := range names {
		if strings.HasSuffix(n, ".sst") && !live[n] {
			t.Fatalf("stray table %s on disk (not in the live version)", n)
		}
	}
}

// Randomised workload under constant compaction, compared against an oracle.
func TestLeveledCompactionModel(t *testing.T) {
	for _, style := range []string{StyleLeveled, StyleSizeTiered} {
		t.Run(style, func(t *testing.T) {
			dir := t.TempDir()
			clk := newClock()
			o := smallOpts(dir, clk)
			o.Compaction.Style = style
			d := mustOpen(t, o)
			rng := rand.New(rand.NewSource(7))
			type ent struct {
				v   string
				exp int64
			}
			oracle := map[string]ent{}
			liveOf := func(k string) (ent, bool) {
				e, ok := oracle[k]
				return e, ok && (e.exp == 0 || e.exp > clk.Now())
			}
			verify := func(stage string) {
				t.Helper()
				for i := 0; i < 600; i++ {
					k := fmt.Sprintf("key%04d", i)
					got, ok, err := d.Get(b(k))
					want, wok := liveOf(k)
					if err != nil || ok != wok || (ok && string(got) != want.v) {
						t.Fatalf("%s: %s got %q ok=%v err=%v; want %q ok=%v", stage, k, got, ok, err, want.v, wok)
					}
				}
				keys, _, _ := d.Scan(nil, 100000)
				n := 0
				for k := range oracle {
					if _, ok := liveOf(k); ok {
						n++
					}
				}
				if len(keys) != n {
					t.Fatalf("%s: scan returned %d keys, oracle has %d", stage, len(keys), n)
				}
			}
			for step := 0; step < 12000; step++ {
				k := fmt.Sprintf("key%04d", rng.Intn(600))
				switch r := rng.Intn(10); {
				case r < 6:
					v := fmt.Sprintf("v%d-%d", step, rng.Intn(1000))
					var exp int64
					if rng.Intn(8) == 0 {
						exp = clk.Now() + int64(50+rng.Intn(500))
					}
					d.Set(b(k), b(v), SetOptions{ExpireAtMs: exp})
					oracle[k] = ent{v, exp}
				case r < 8:
					d.Del([][]byte{b(k)})
					delete(oracle, k)
				case r == 8:
					clk.Add(int64(rng.Intn(120)))
				default:
					want, wok := liveOf(k)
					got, ok, _ := d.Get(b(k))
					if ok != wok || (ok && string(got) != want.v) {
						t.Fatalf("step %d: %s got %q ok=%v want %q ok=%v", step, k, got, ok, want.v, wok)
					}
				}
				if step%3000 == 2999 {
					verify(fmt.Sprintf("step %d", step))
				}
			}
			waitIdle(t, d)
			verify("idle")
			checkInvariants(t, d)
			stats := map[string]string{}
			for _, s := range d.Stats() {
				stats[s.Name] = s.Value
			}
			if stats["compactions"] == "0" {
				t.Fatal("no compaction ever ran")
			}
			t.Logf("%s: compactions=%s tables=%s l0=%s write_amp=%s dropped=%s", style, stats["compactions"], stats["table_files"], stats["l0_files"], stats["write_amplification"], stats["compaction_entries_dropped"])
			d.Close()

			d2 := mustOpen(t, o)
			defer d2.Close()
			d = d2
			verify("after restart")
			// A full compaction must preserve everything and shrink to live data.
			if err := d2.CompactAll(); err != nil {
				t.Fatal(err)
			}
			verify("after CompactAll")
			checkInvariants(t, d2)
		})
	}
}

// tableEntries returns every internal entry in the live tables of a level.
func tableEntries(t testing.TB, d *DB, level int) []iterator.Entry {
	t.Helper()
	d.mu.RLock()
	cur := d.cur
	d.mu.RUnlock()
	var out []iterator.Entry
	for _, f := range cur.levels[level] {
		it := f.r.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			e := it.Entry()
			e.Key = append([]byte(nil), e.Key...)
			e.Value = append([]byte(nil), e.Value...)
			out = append(out, e)
		}
		if it.Err() != nil {
			t.Fatal(it.Err())
		}
	}
	return out
}

func hasEntry(es []iterator.Entry, key string, kind iterator.Kind) bool {
	for _, e := range es {
		if string(e.Key) == key && e.Kind == kind {
			return true
		}
	}
	return false
}

// compactLevel merges ALL files of `level` (plus overlapping files of level+1)
// into level+1, regardless of size triggers.
func compactLevel(t testing.TB, d *DB, level int) {
	t.Helper()
	d.compactMu.Lock()
	defer d.compactMu.Unlock()
	d.mu.RLock()
	base := d.cur
	base.ref()
	d.mu.RUnlock()
	defer base.unref()
	c := &compaction{base: base, level: level, outLevel: level + 1, reason: "test"}
	c.inputs = append(c.inputs, base.levels[level]...)
	lo, hi := keyRange(c.inputs)
	c.inputs = append(c.inputs, overlapping(base.levels[level+1], lo, hi)...)
	c.finish()
	if err := d.runCompaction(c); err != nil {
		t.Fatal(err)
	}
}

// A tombstone must survive a compaction while older data for its key still
// exists in a deeper level (dropping it would resurrect the old value), and may
// be dropped only when nothing older remains below.
func TestTombstoneDroppedOnlyWhenSafe(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	o := smallOpts(dir, clk)
	o.Compaction.Disabled = true
	d := mustOpen(t, o)
	defer d.Close()

	d.Set(b("victim"), b("old-value"), SetOptions{})
	d.Set(b("other"), b("x"), SetOptions{})
	d.FlushMemtable()
	compactLevel(t, d, 0) // -> L1
	compactLevel(t, d, 1) // -> L2: the old value now lives two levels down
	if !hasEntry(tableEntries(t, d, 2), "victim", iterator.KindPut) {
		t.Fatal("setup: value should be in L2")
	}

	d.Del([][]byte{b("victim")})
	d.FlushMemtable()
	compactLevel(t, d, 0) // tombstone (L0) merges into L1; L2 still holds the old value
	if !hasEntry(tableEntries(t, d, 1), "victim", iterator.KindDelete) {
		t.Fatal("tombstone was dropped while older data exists below: the key would resurrect")
	}
	if _, ok, _ := d.Get(b("victim")); ok {
		t.Fatal("deleted key visible after L0->L1 compaction")
	}

	compactLevel(t, d, 1) // tombstone meets the old value: both disappear
	for lvl := 0; lvl < manifest.NumLevels; lvl++ {
		for _, e := range tableEntries(t, d, lvl) {
			if string(e.Key) == "victim" {
				t.Fatalf("level %d still holds %v for the deleted key after the tombstone reached the data", lvl, e.Kind)
			}
		}
	}
	if _, ok, _ := d.Get(b("victim")); ok {
		t.Fatal("deleted key resurrected")
	}
	if v, ok, _ := d.Get(b("other")); !ok || string(v) != "x" {
		t.Fatal("unrelated key lost")
	}
}

// An expired value that still shadows older data is rewritten as a tombstone
// (value bytes reclaimed, shadowing preserved); with nothing below it vanishes.
func TestExpiredEntriesCompactAway(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	o := smallOpts(dir, clk)
	o.Compaction.Disabled = true
	d := mustOpen(t, o)
	defer d.Close()

	d.Set(b("k"), b("v-old"), SetOptions{})
	d.FlushMemtable()
	compactLevel(t, d, 0)
	compactLevel(t, d, 1) // v-old in L2
	d.Set(b("k"), b("v-ttl"), SetOptions{ExpireAtMs: clk.Now() + 100})
	d.FlushMemtable()
	clk.Add(200) // v-ttl expires; v-old must NOT come back
	if _, ok, _ := d.Get(b("k")); ok {
		t.Fatal("expired key visible (or old value resurrected)")
	}
	compactLevel(t, d, 0)
	es := tableEntries(t, d, 1)
	if !hasEntry(es, "k", iterator.KindDelete) || hasEntry(es, "k", iterator.KindPut) {
		t.Fatalf("expired put should become a tombstone while older data lies below: %+v", es)
	}
	if _, ok, _ := d.Get(b("k")); ok {
		t.Fatal("old value resurrected after compaction")
	}
	compactLevel(t, d, 1)
	for lvl := 0; lvl < manifest.NumLevels; lvl++ {
		if len(tableEntries(t, d, lvl)) != 0 {
			t.Fatalf("level %d should be empty after everything expired/deleted: %+v", lvl, tableEntries(t, d, lvl))
		}
	}
}

// Snapshots and iterators pin files: compaction may replace them, but they stay
// readable (with the old data) until released, and are then deleted.
func TestSnapshotPinsFilesAcrossCompaction(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	o := smallOpts(dir, clk)
	o.Compaction.Disabled = true
	d := mustOpen(t, o)
	defer d.Close()
	for i := 0; i < 200; i++ {
		d.Set(b(fmt.Sprintf("k%03d", i)), b("v1"), SetOptions{})
	}
	d.FlushMemtable()

	snap, err := d.NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	it := snap.NewIterator()
	it.SeekToFirst()

	// Change the world: overwrite, delete, add; push everything through compaction.
	for i := 0; i < 200; i++ {
		d.Set(b(fmt.Sprintf("k%03d", i)), b("v2"), SetOptions{})
	}
	for i := 0; i < 50; i++ {
		d.Del([][]byte{b(fmt.Sprintf("k%03d", i))})
	}
	d.Set(b("newkey"), b("n"), SetOptions{})
	if err := d.CompactAll(); err != nil {
		t.Fatal(err)
	}
	sstsBefore := countExt(t, dir, ".sst")

	// The snapshot still sees v1 for everything, and no 'newkey'.
	for i := 0; i < 200; i++ {
		v, ok, err := snap.Get(b(fmt.Sprintf("k%03d", i)))
		if err != nil || !ok || string(v) != "v1" {
			t.Fatalf("snapshot k%03d = %q ok=%v err=%v", i, v, ok, err)
		}
	}
	if _, ok, _ := snap.Get(b("newkey")); ok {
		t.Fatal("snapshot sees a later write")
	}
	n := 0
	for ; it.Valid(); it.Next() {
		if string(it.Value()) != "v1" {
			t.Fatalf("iterator saw %q for %s", it.Value(), it.Key())
		}
		n++
	}
	if it.Err() != nil || n != 200 {
		t.Fatalf("iterator: n=%d err=%v", n, it.Err())
	}
	// ... while the live database sees the new state.
	if v, _, _ := d.Get(b("k100")); string(v) != "v2" {
		t.Fatalf("live read: %q", v)
	}
	if _, ok, _ := d.Get(b("k010")); ok {
		t.Fatal("live read sees deleted key")
	}

	it.Close()
	snap.Close()
	sstsAfter := countExt(t, dir, ".sst")
	t.Logf("sstable files on disk: %d while pinned, %d after release", sstsBefore, sstsAfter)
	if sstsAfter >= sstsBefore {
		t.Fatalf("obsolete tables were not deleted after the snapshot was released (%d -> %d)", sstsBefore, sstsAfter)
	}
	checkInvariants(t, d) // no stray tables remain
}

func countExt(t testing.TB, dir, ext string) int {
	t.Helper()
	names, _ := vfs.Default.List(dir)
	n := 0
	for _, x := range names {
		if strings.HasSuffix(x, ext) {
			n++
		}
	}
	return n
}

// L0 backpressure: past the slowdown trigger writes are delayed; at the stop
// trigger they block and then fail; draining L0 lets them resume.
func TestL0BackpressureSlowdownStopRecovery(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	o := smallOpts(dir, clk)
	o.Compaction.Disabled = true // nothing drains L0 until we say so
	o.Compaction.L0SlowdownTrigger = 3
	o.Compaction.L0StopTrigger = 5
	o.StallTimeout = 300 * time.Millisecond
	d := mustOpen(t, o)
	defer d.Close()

	var stallErr error
	i := 0
	for ; i < 20000 && stallErr == nil; i++ {
		_, stallErr = d.Set(b(fmt.Sprintf("k%06d", i)), make([]byte, 100), SetOptions{})
	}
	if stallErr != ErrWriteStall {
		t.Fatalf("want ErrWriteStall, got %v after %d writes", stallErr, i)
	}
	st := map[string]string{}
	for _, s := range d.Stats() {
		st[s.Name] = s.Value
	}
	t.Logf("stopped at l0_files=%s after %d writes; slowdowns=%s stalls=%s stall_seconds=%s",
		st["l0_files"], i, st["write_slowdowns"], st["write_stalls"], st["write_stall_seconds"])
	if st["write_slowdowns"] == "0" {
		t.Fatal("writers were never slowed before the stop")
	}
	if st["write_stalls"] == "0" {
		t.Fatal("stall not recorded")
	}
	// Drain L0 and resume.
	if err := d.CompactAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Set(b("after"), b("ok"), SetOptions{}); err != nil {
		t.Fatalf("writes did not resume after compaction: %v", err)
	}
}

// Failures at every stage of a compaction leave the tree consistent, leak no
// files, and a retry succeeds.
func TestCompactionFailureStages(t *testing.T) {
	stages := []struct {
		name string
		rule vfs.Rule
	}{
		{"create output", vfs.Rule{Op: vfs.OpCreate, Match: ".sst.tmp", Count: -1}},
		{"write output", vfs.Rule{Op: vfs.OpWrite, Match: ".sst.tmp", Count: -1, Partial: 30}},
		{"fsync output", vfs.Rule{Op: vfs.OpSync, Match: ".sst.tmp", Count: -1}},
		{"rename output", vfs.Rule{Op: vfs.OpRename, Match: ".sst", Count: -1}},
		{"manifest write", vfs.Rule{Op: vfs.OpWrite, Match: "MANIFEST", Count: -1}},
	}
	for _, st := range stages {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			clk := newClock()
			o := smallOpts(dir, clk)
			o.FS = fs
			o.Compaction.Disabled = true
			d := mustOpen(t, o)
			want := map[string]string{}
			for i := 0; i < 400; i++ {
				k, v := fmt.Sprintf("k%04d", i%150), fmt.Sprintf("v%d", i)
				d.Set(b(k), b(v), SetOptions{})
				want[k] = v
			}
			d.FlushMemtable()
			before := d.cur.numFiles()
			fs.AddRule(st.rule)
			if err := d.CompactAll(); err == nil {
				t.Fatal("compaction should have failed")
			}
			fs.ClearRules()
			if d.cur.numFiles() != before {
				t.Fatalf("failed compaction changed the version: %d -> %d files", before, d.cur.numFiles())
			}
			for k, v := range want {
				if got, ok, _ := d.Get(b(k)); !ok || string(got) != v {
					t.Fatalf("%s lost after failed compaction", k)
				}
			}
			if tmp := countExt(t, dir, ".tmp"); tmp != 0 {
				t.Fatalf("%d temp files leaked", tmp)
			}
			checkInvariants(t, d) // no stray output tables
			if err := d.CompactAll(); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			for k, v := range want {
				if got, ok, _ := d.Get(b(k)); !ok || string(got) != v {
					t.Fatalf("%s lost after retry", k)
				}
			}
			d.Close()
		})
	}
}

func TestCompactionRateLimit(t *testing.T) {
	run := func(rate int64) time.Duration {
		dir := t.TempDir()
		clk := newClock()
		o := smallOpts(dir, clk)
		o.Compaction.Disabled = true
		o.Compaction.L0SlowdownTrigger, o.Compaction.L0StopTrigger = 1000, 2000 // no backpressure here
		o.Compaction.RateBytesPerSec = rate
		d := mustOpen(t, o)
		defer d.Close()
		for i := 0; i < 3000; i++ {
			d.Set(b(fmt.Sprintf("k%05d", i)), make([]byte, 100), SetOptions{})
		}
		d.FlushMemtable()
		t0 := time.Now()
		if err := d.CompactAll(); err != nil {
			t.Fatal(err)
		}
		return time.Since(t0)
	}
	fast := run(0)
	slow := run(1 << 20) // 1 MiB/s over ~350 KB of data
	t.Logf("compaction took %v unthrottled, %v at 1 MiB/s", fast, slow)
	if slow < 250*time.Millisecond || slow < fast*3 {
		t.Fatalf("throttle ineffective: %v vs %v", slow, fast)
	}
}

// Power loss while writers, flushes AND compactions run: nothing acknowledged is lost.
func TestPowerLossDuringCompaction(t *testing.T) {
	for _, style := range []string{StyleLeveled, StyleSizeTiered} {
		for round := 0; round < 12; round++ {
			mode := vfs.CrashDropUnsynced
			if round%2 == 1 {
				mode = vfs.CrashTornTail
			}
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			clk := newClock()
			o := smallOpts(dir, clk)
			o.FS = fs
			o.Sync = wal.SyncAlways
			o.MemtableSize = 2 << 10
			o.Compaction.Style = style
			d := mustOpen(t, o)

			var mu sync.Mutex
			acked := map[string]int{}
			var stop atomic.Bool
			var wg sync.WaitGroup
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; !stop.Load(); i++ {
						k := fmt.Sprintf("g%d-k%03d", g, i%40)
						if ok, err := d.Set(b(k), b(fmt.Sprint(i)), SetOptions{}); err == nil && ok {
							mu.Lock()
							acked[k] = i
							mu.Unlock()
						} else if err != nil {
							return
						}
					}
				}()
			}
			rng := rand.New(rand.NewSource(int64(round) * 31))
			time.Sleep(time.Duration(30+rng.Intn(150)) * time.Millisecond)
			fs.Crash(mode)
			stop.Store(true)
			wg.Wait()
			compactions := d.compactions.Load()
			d.Close()

			d2, _, err := Open(o)
			if err != nil {
				t.Fatalf("%s round %d: recovery failed: %v", style, round, err)
			}
			mu.Lock()
			for k, want := range acked {
				got, ok, err := d2.Get(b(k))
				var gi int
				fmt.Sscanf(string(got), "%d", &gi)
				if err != nil || !ok || gi < want { // an in-flight later write may legitimately persist
					mu.Unlock()
					t.Fatalf("%s round %d mode %d: acked %s=%d lost (got %q ok=%v err=%v)", style, round, mode, k, want, got, ok, err)
				}
			}
			mu.Unlock()
			if round == 0 {
				t.Logf("%s round 0: %d compactions had completed before the crash", style, compactions)
			}
			d2.Close()
		}
	}
}

func BenchmarkCompactAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		o := smallOpts(dir, newClock())
		o.Compaction.Disabled = true
		o.MemtableSize = 64 << 10
		d, _, _ := Open(o)
		for j := 0; j < 20000; j++ {
			d.Set([]byte(fmt.Sprintf("key%06d", j%7000)), make([]byte, 100), SetOptions{})
		}
		d.FlushMemtable()
		b.StartTimer()
		d.CompactAll()
		b.StopTimer()
		d.Close()
	}
}

var _ = os.Remove
