package engine_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/engine/enginetest"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

func listExt(t *testing.T, dir, ext string) []string {
	t.Helper()
	names, _ := vfs.Default.List(dir)
	var out []string
	for _, n := range names {
		if strings.HasSuffix(n, ext) {
			out = append(out, n)
		}
	}
	return out
}

func fillKeys(t *testing.T, d *engine.DB, n int, valSize int) map[string]string {
	t.Helper()
	want := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		v := fmt.Sprintf("%0*d", valSize, i)
		if _, err := d.Set([]byte(k), []byte(v), engine.SetOptions{}); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	return want
}

func checkAll(t *testing.T, d *engine.DB, want map[string]string) {
	t.Helper()
	for k, v := range want {
		got, ok, err := d.Get([]byte(k))
		if err != nil || !ok || string(got) != v {
			t.Fatalf("%s: got %q ok=%v err=%v want %q", k, got, ok, err, v)
		}
	}
}

func TestFlushCreatesTablesTrimsWALAndRecovers(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	o := dbOpts(vfs.Default, dir, clk, 16<<10, wal.SyncAlways)
	o.BloomBits = 10
	d := openDB(t, o)
	want := fillKeys(t, d, 2000, 100)
	if err := d.WaitForFlush(); err != nil {
		t.Fatal(err)
	}
	ssts := listExt(t, dir, ".sst")
	if len(ssts) == 0 || stat(d, "flushes") == "0" {
		t.Fatalf("no tables written: %v", ssts)
	}
	// WAL must have been trimmed: only segments at/after the boundary remain.
	if wals := listExt(t, dir, ".wal"); len(wals) > 3 {
		t.Fatalf("flushed WAL segments were not deleted: %v", wals)
	}
	checkAll(t, d, want)
	d.Close()

	d2 := openDB(t, o)
	defer d2.Close()
	if got := listExt(t, dir, ".sst"); len(got) != len(ssts) {
		t.Fatalf("tables changed across clean restart: %v -> %v", ssts, got)
	}
	checkAll(t, d2, want)
	// Overwrites and deletes after restart shadow the on-disk versions.
	d2.Set([]byte("key000005"), []byte("new"), engine.SetOptions{})
	d2.Del([][]byte{[]byte("key000006")})
	if v, _, _ := d2.Get([]byte("key000005")); string(v) != "new" {
		t.Fatal("overwrite not visible over SSTable data")
	}
	if _, ok, _ := d2.Get([]byte("key000006")); ok {
		t.Fatal("tombstone not hiding SSTable data")
	}
	keys, _, _ := d2.Scan(nil, 100000)
	if len(keys) != 1999 {
		t.Fatalf("scan returned %d keys, want 1999", len(keys))
	}
}

func TestOrphansRemovedAndMissingTableRefused(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	o := dbOpts(vfs.Default, dir, clk, 8<<10, wal.SyncNever)
	d := openDB(t, o)
	fillKeys(t, d, 800, 60)
	d.WaitForFlush()
	d.Close()

	os.WriteFile(filepath.Join(dir, "999998.sst"), []byte("not in manifest"), 0o644)
	os.WriteFile(filepath.Join(dir, "999999.sst.tmp"), []byte("half written"), 0o644)
	d2 := openDB(t, o)
	d2.Close()
	for _, n := range []string{"999998.sst", "999999.sst.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Fatalf("orphan %s not removed", n)
		}
	}
	// A live table that vanished is data loss: refuse to start, loudly.
	ssts := listExt(t, dir, ".sst")
	os.Remove(filepath.Join(dir, ssts[0]))
	if _, _, err := engine.Open(o); err == nil || !strings.Contains(err.Error(), ssts[0]) {
		t.Fatalf("want error naming %s, got %v", ssts[0], err)
	}
}

func TestCorruptTableNeverReturnsWrongData(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	o := dbOpts(vfs.Default, dir, clk, 8<<10, wal.SyncNever)
	d := openDB(t, o)
	want := fillKeys(t, d, 600, 60)
	d.WaitForFlush()
	d.Close()
	// Corrupt a byte in the middle of every table's data region.
	for _, n := range listExt(t, dir, ".sst") {
		p := filepath.Join(dir, n)
		b, _ := os.ReadFile(p)
		b[len(b)/3] ^= 0x55
		os.WriteFile(p, b, 0o644)
	}
	d2, _, err := engine.Open(o)
	if err != nil {
		return // detected at open (index/footer damage): also a fail-safe outcome
	}
	defer d2.Close()
	sawErr := false
	for k, v := range want {
		got, ok, err := d2.Get([]byte(k))
		if err != nil {
			sawErr = true
			continue
		}
		if ok && string(got) != v {
			t.Fatalf("SILENT CORRUPTION: %s = %q, want %q", k, got, v)
		}
	}
	if !sawErr {
		t.Fatal("damaged blocks were never reported")
	}
}

// Every stage of a flush can fail (ENOSPC, EIO...). Nothing acknowledged may be
// lost, the failure must not wedge the DB, and once the fault clears the flush
// must complete by itself.
func TestFlushFailureStagesAreSafe(t *testing.T) {
	stages := []struct {
		name string
		rule vfs.Rule
	}{
		{"create tmp", vfs.Rule{Op: vfs.OpCreate, Match: ".sst.tmp", Count: -1}},
		{"write table", vfs.Rule{Op: vfs.OpWrite, Match: ".sst.tmp", Count: -1, Partial: 50}},
		{"fsync table", vfs.Rule{Op: vfs.OpSync, Match: ".sst.tmp", Count: -1}},
		{"rename table", vfs.Rule{Op: vfs.OpRename, Match: ".sst", Count: -1}},
		{"write manifest", vfs.Rule{Op: vfs.OpWrite, Match: "MANIFEST", Count: -1}},
	}
	for _, st := range stages {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			clk := enginetest.NewClock()
			o := dbOpts(fs, dir, clk, 8<<10, wal.SyncNever)
			o.MaxImmutables = 100 // do not stall; we want to observe the backlog
			d := openDB(t, o)
			fs.AddRule(st.rule)
			want := fillKeys(t, d, 600, 60)
			time.Sleep(700 * time.Millisecond) // let the flusher fail and retry a couple of times
			if stat(d, "flush_errors") == "0" {
				t.Fatalf("fault never hit the flush path")
			}
			checkAll(t, d, want) // still readable from immutables + WAL
			fs.ClearRules()
			if err := d.WaitForFlush(); err != nil {
				t.Fatal(err) // the flusher must recover by itself
			}
			checkAll(t, d, want)
			d.Close()
			d2 := openDB(t, o)
			defer d2.Close()
			checkAll(t, d2, want)
			if tmp := listExt(t, dir, ".tmp"); len(tmp) != 0 {
				t.Fatalf("temp files left: %v", tmp)
			}
		})
	}
}

// Backpressure: with flushing blocked, writers stall instead of growing memory
// without bound, then fail with ErrWriteStall after the timeout, and resume
// once flushing works again.
func TestWriteStallAndRecovery(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewFaulty(vfs.Default)
	clk := enginetest.NewClock()
	o := dbOpts(fs, dir, clk, 4<<10, wal.SyncNever)
	o.MaxImmutables = 2
	o.StallTimeout = 300 * time.Millisecond
	d := openDB(t, o)
	defer d.Close()
	fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".sst.tmp", Count: -1})
	var stalled error
	t0 := time.Now()
	for i := 0; i < 5000 && stalled == nil; i++ {
		_, stalled = d.Set([]byte(fmt.Sprintf("k%05d", i)), make([]byte, 200), engine.SetOptions{})
	}
	if stalled != engine.ErrWriteStall {
		t.Fatalf("want ErrWriteStall, got %v", stalled)
	}
	if time.Since(t0) < 250*time.Millisecond {
		t.Fatal("writer was not delayed before failing")
	}
	if stat(d, "write_stalls") == "0" {
		t.Fatal("stall not counted")
	}
	fs.ClearRules()
	d.WaitForFlush()
	if _, err := d.Set([]byte("after"), []byte("ok"), engine.SetOptions{}); err != nil {
		t.Fatalf("writes did not resume: %v", err)
	}
}

// Random-moment power loss while writers and the background flusher run:
// whatever was acknowledged must survive, whatever step the flush was in.
func TestPowerLossDuringFlushLosesNothingAcked(t *testing.T) {
	rounds := 25
	for round := 0; round < rounds; round++ {
		mode := vfs.CrashDropUnsynced
		if round%2 == 1 {
			mode = vfs.CrashTornTail
		}
		dir := t.TempDir()
		fs := vfs.NewFaulty(vfs.Default)
		clk := enginetest.NewClock()
		o := dbOpts(fs, dir, clk, 6<<10, wal.SyncAlways)
		o.BloomBits = 10
		d := openDB(t, o)

		var mu sync.Mutex
		acked := map[string]string{}
		var stop atomic.Bool
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; !stop.Load(); i++ {
					k, v := fmt.Sprintf("g%d-k%03d", g, i%50), fmt.Sprintf("v%d-%d", g, i)
					if ok, err := d.Set([]byte(k), []byte(v), engine.SetOptions{}); err == nil && ok {
						mu.Lock()
						acked[k] = v
						mu.Unlock()
					} else if err != nil {
						return
					}
				}
			}()
		}
		rng := rand.New(rand.NewSource(int64(round)))
		time.Sleep(time.Duration(5+rng.Intn(80)) * time.Millisecond)
		// The moment of power loss. Anything still running is a "zombie".
		fs.Crash(mode)
		stop.Store(true)
		wg.Wait()
		d.Close()

		d2, _, err := engine.Open(o)
		if err != nil {
			t.Fatalf("round %d mode %d: recovery failed: %v", round, mode, err)
		}
		mu.Lock()
		for k, want := range acked {
			got, ok, err := d2.Get([]byte(k))
			// Each key is written by one goroutine with an increasing counter in
			// the value ("v<g>-<i>"). The recovered value must be the last
			// acknowledged write, or a LATER one that was in flight at the crash
			// (written, fsync not yet acknowledged, partially persisted in the
			// torn tail): that is allowed. An older value, or no value, is data loss.
			if err != nil || !ok || counterOf(string(got)) < counterOf(want) {
				mu.Unlock()
				t.Fatalf("round %d mode %d: acked %s=%q lost (got %q ok=%v err=%v)", round, mode, k, want, got, ok, err)
			}
		}
		n := len(acked)
		mu.Unlock()
		if round == 0 {
			t.Logf("round 0: %d keys acked, tables after recovery: %s", n, stat(d2, "table_files"))
		}
		d2.Close()
	}
}

// Regression test for a durability bug found by TestPowerLossDuringFlush...:
// rotating the WAL/memtable in the middle of a group commit put a batch whose
// record lives in the OLD segment into the NEW memtable; flushing the old
// memtable then deleted that segment, so the batch existed only in RAM. With a
// tiny memtable and many concurrent writers (big groups) every acknowledged
// write must still be recoverable from disk after a restart that replays only
// what is on disk.
func TestRotationAtGroupBoundaryOnly(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	// A slow fsync makes writers pile up so groups really contain many batches.
	fs := vfs.NewFaulty(vfs.Default)
	fs.SetSyncDelay(time.Millisecond)
	o := dbOpts(fs, dir, clk, 512, wal.SyncAlways)
	d := openDB(t, o)
	var mu sync.Mutex
	want := map[string]string{}
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				k, v := fmt.Sprintf("g%02d-%03d", g, i), fmt.Sprintf("val-%d-%d", g, i)
				if _, err := d.Set([]byte(k), []byte(v), engine.SetOptions{}); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				want[k] = v
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := d.WaitForFlush(); err != nil {
		t.Fatal(err)
	}
	if stat(d, "wal_max_group") == "1" {
		t.Fatal("no multi-batch group formed: this test would not exercise the bug")
	}
	if n := stat(d, "rotation_errors"); n != "0" {
		t.Fatalf("rotation invariant violated %s times: rotated mid-group", n)
	}
	// Do not trust memory: reopen from disk only.
	d.Close()
	d2 := openDB(t, o)
	defer d2.Close()
	checkAll(t, d2, want)
}

// counterOf extracts i from "v<g>-<i>".
func counterOf(v string) int {
	i := strings.LastIndexByte(v, '-')
	n := 0
	fmt.Sscanf(v[i+1:], "%d", &n)
	return n
}
