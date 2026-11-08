// Package fault holds the fault-injection tests: they run the real engine on a
// filesystem (vfs.Faulty) that can fail on command and then check three things
// for every fault class:
//
//  1. the engine fails SAFELY: it returns errors instead of acknowledging
//     writes it cannot make durable, and never panics or deadlocks;
//  2. it never silently returns bad data: every read is either the correct value
//     or an error;
//  3. no acknowledged write is lost across a crash that follows the fault.
package fault

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

type clock struct{ ms atomic.Int64 }

func (c *clock) Now() int64 { return c.ms.Load() }

func opts(fs vfs.FS, dir string) engine.Options {
	c := &clock{}
	c.ms.Store(1_000_000)
	return engine.Options{
		FS: fs, Dir: dir, Sync: wal.SyncAlways, GroupCommit: true, Now: c.Now,
		MemtableSize: 4 << 10, BlockSize: 512, BloomBits: 10, StallTimeout: 2 * time.Second,
		Compaction: engine.CompactionOptions{L0CompactTrigger: 2, L0SlowdownTrigger: 6, L0StopTrigger: 10, LevelSizeRatio: 4, L1MaxBytes: 16 << 10, TargetFileBytes: 6 << 10},
	}
}

func open(t testing.TB, o engine.Options) *engine.DB {
	t.Helper()
	d, _, err := engine.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return d
}

func b(s string) []byte { return []byte(s) }

func get(t testing.TB, d *engine.DB, k string) (string, bool) {
	t.Helper()
	v, ok, err := d.Get(b(k))
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	return string(v), ok
}

func stat(d *engine.DB, name string) string {
	for _, s := range d.Stats() {
		if s.Name == name {
			return s.Value
		}
	}
	return ""
}

// ------------------------------------------------------------------ ENOSPC

// Disk fills up at a different point in every run. Writes must start failing
// with ENOSPC (never be silently dropped), everything acknowledged must survive
// a restart, and the database must work again once space is available.
func TestDiskFullSweep(t *testing.T) {
	for limit := int64(2 << 10); limit <= 160<<10; limit += 6 << 10 {
		t.Run(fmt.Sprintf("limit=%dK", limit>>10), func(t *testing.T) {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			o := opts(fs, dir)
			o.StallTimeout = 100 * time.Millisecond
			d := open(t, o)
			fs.SetDiskLimit(limit)
			acked := map[string]string{}
			var firstErr, lastErr error
			for i := 0; i < 600; i++ {
				if errors.Is(lastErr, engine.ErrWriteStall) {
					break // flushing cannot make progress on a full disk: writers are being refused
				}
				k, v := fmt.Sprintf("k%03d", i%90), fmt.Sprintf("v%d-%s", i, strings.Repeat("p", 40))
				ok, err := d.Set(b(k), b(v), engine.SetOptions{})
				lastErr = err
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if ok {
					acked[k] = v
				}
			}
			if firstErr != nil && !errors.Is(firstErr, syscall.ENOSPC) && !errors.Is(firstErr, engine.ErrWriteStall) {
				t.Fatalf("a full disk must surface as ENOSPC or a write stall, got %v", firstErr)
			}
			// Everything acknowledged is readable now...
			for k, v := range acked {
				if got, ok := get(t, d, k); !ok || got != v {
					t.Fatalf("live: %s = %q ok=%v, acked %q", k, got, ok, v)
				}
			}
			// ...free some space, and the engine recovers by itself.
			fs.SetDiskLimit(-1)
			// Recovery is automatic but not instant: the flusher retries on a
			// 500 ms timer. Writes must resume within a few seconds, no restart.
			var rerr error
			for try := 0; try < 40; try++ {
				if _, rerr = d.Set(b("after-recovery"), b("ok"), engine.SetOptions{}); rerr == nil {
					break
				}
				time.Sleep(150 * time.Millisecond)
			}
			if rerr != nil {
				t.Fatalf("writes did not resume within 6s after space was freed: %v", rerr)
			}
			acked["after-recovery"] = "ok"
			if err := d.WaitForFlush(); err != nil {
				t.Fatal(err)
			}
			d.Close()
			// ...and across a restart.
			d2 := open(t, o)
			defer d2.Close()
			for k, v := range acked {
				if got, ok := get(t, d2, k); !ok || got != v {
					t.Fatalf("after restart: %s = %q ok=%v, acked %q", k, got, ok, v)
				}
			}
		})
	}
}

// ------------------------------------------------------------------ fsync

func TestFsyncFailureByFileClass(t *testing.T) {
	t.Run("WAL: writes are refused, reads keep working, restart recovers", func(t *testing.T) {
		dir := t.TempDir()
		fs := vfs.NewFaulty(vfs.Default)
		o := opts(fs, dir)
		d := open(t, o)
		for i := 0; i < 50; i++ {
			d.Set(b(fmt.Sprintf("a%02d", i)), b("acked"), engine.SetOptions{})
		}
		fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Count: -1})
		if _, err := d.Set(b("unacked"), b("x"), engine.SetOptions{}); err == nil {
			t.Fatal("a write whose fsync failed must not be acknowledged")
		}
		fs.ClearRules() // the fault was transient, but the WAL must stay failed (fsyncgate)
		if _, err := d.Set(b("later"), b("y"), engine.SetOptions{}); err == nil {
			t.Fatal("WAL must stay read-only after an fsync failure even if later fsyncs would succeed")
		}
		if stat(d, "wal_failed") == "" {
			t.Fatal("the failure must be visible in INFO")
		}
		if v, ok := get(t, d, "a10"); !ok || v != "acked" {
			t.Fatal("reads must keep working while the WAL is failed")
		}
		d.Close()
		d2 := open(t, o)
		defer d2.Close()
		for i := 0; i < 50; i++ {
			if v, ok := get(t, d2, fmt.Sprintf("a%02d", i)); !ok || v != "acked" {
				t.Fatalf("a%02d lost", i)
			}
		}
		if _, ok := get(t, d2, "later"); ok {
			t.Fatal("a refused write appeared after restart")
		}
		if _, err := d2.Set(b("fresh"), b("z"), engine.SetOptions{}); err != nil {
			t.Fatalf("restart must clear the failed state: %v", err)
		}
	})
	for _, c := range []struct{ name, match string }{{"SSTable", ".sst.tmp"}, {"MANIFEST", "MANIFEST-"}} {
		t.Run(c.name+": flush keeps retrying, nothing lost", func(t *testing.T) {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			o := opts(fs, dir)
			o.MaxImmutables = 100
			o.Compaction.Disabled = true
			d := open(t, o)
			fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: c.match, Count: -1})
			want := map[string]string{}
			for i := 0; i < 300; i++ {
				k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)
				if _, err := d.Set(b(k), b(v), engine.SetOptions{}); err != nil {
					t.Fatalf("WAL writes are unaffected and must succeed: %v", err)
				}
				want[k] = v
			}
			time.Sleep(600 * time.Millisecond)
			if stat(d, "flush_errors") == "0" {
				t.Fatal("flush failure not recorded")
			}
			for k, v := range want {
				if got, ok := get(t, d, k); !ok || got != v {
					t.Fatalf("%s unreadable while flush is failing", k)
				}
			}
			fs.ClearRules()
			if c.name == "MANIFEST" {
				// An fsync failure on the manifest poisons it until restart.
				d.Close()
				d = open(t, o)
			} else if err := d.WaitForFlush(); err != nil {
				t.Fatal(err)
			}
			d.Close()
			d2 := open(t, o)
			defer d2.Close()
			for k, v := range want {
				if got, ok := get(t, d2, k); !ok || got != v {
					t.Fatalf("%s lost after restart", k)
				}
			}
		})
	}
	t.Run("directory fsync failure during flush", func(t *testing.T) {
		dir := t.TempDir()
		fs := vfs.NewFaulty(vfs.Default)
		o := opts(fs, dir)
		o.Compaction.Disabled = true
		d := open(t, o)
		fs.AddRule(vfs.Rule{Op: vfs.OpSyncDir, Count: 3})
		want := map[string]string{}
		for i := 0; i < 200; i++ {
			k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)
			if _, err := d.Set(b(k), b(v), engine.SetOptions{}); err == nil {
				want[k] = v
			}
		}
		fs.ClearRules()
		d.WaitForFlush()
		for k, v := range want {
			if got, ok := get(t, d, k); !ok || got != v {
				t.Fatalf("%s lost", k)
			}
		}
		d.Close()
		d2 := open(t, o)
		defer d2.Close()
		for k, v := range want {
			if got, ok := get(t, d2, k); !ok || got != v {
				t.Fatalf("%s lost after restart", k)
			}
		}
	})
}

// ------------------------------------------------------------------ torn / short writes

// A WAL append that writes only the first k bytes of a record (for every k) is
// rolled back, reported as an error, never acknowledged, and the log keeps
// working with no torn tail left behind.
func TestShortWriteAtEveryLength(t *testing.T) {
	for k := 0; k <= 70; k += 1 {
		dir := t.TempDir()
		fs := vfs.NewFaulty(vfs.Default)
		o := opts(fs, dir)
		d := open(t, o)
		d.Set(b("before"), b("1"), engine.SetOptions{})
		fs.AddRule(vfs.Rule{Op: vfs.OpWrite, Match: ".wal", Partial: k})
		if _, err := d.Set(b("torn"), b("value-value-value"), engine.SetOptions{}); err == nil {
			t.Fatalf("k=%d: short write was acknowledged", k)
		}
		if _, err := d.Set(b("after"), b("2"), engine.SetOptions{}); err != nil {
			t.Fatalf("k=%d: log must recover from a repaired short write: %v", k, err)
		}
		d.Close()
		d2, info, err := engine.Open(o)
		if err != nil {
			t.Fatalf("k=%d: %v", k, err)
		}
		if info.TornTail {
			t.Fatalf("k=%d: the rolled-back partial record left a torn tail", k)
		}
		if _, ok := get(t, d2, "torn"); ok {
			t.Fatalf("k=%d: unacknowledged write appeared", k)
		}
		for _, kv := range [][2]string{{"before", "1"}, {"after", "2"}} {
			if v, ok := get(t, d2, kv[0]); !ok || v != kv[1] {
				t.Fatalf("k=%d: %s lost", k, kv[0])
			}
		}
		d2.Close()
	}
}

// ------------------------------------------------------------------ read errors and bit flips

func buildDB(t *testing.T) (string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	o := opts(vfs.Default, dir)
	d := open(t, o)
	want := map[string]string{}
	for i := 0; i < 700; i++ {
		k, v := fmt.Sprintf("key%04d", i%350), fmt.Sprintf("val-%d-%s", i, strings.Repeat("z", 30))
		d.Set(b(k), b(v), engine.SetOptions{})
		want[k] = v
	}
	for i := 0; i < 350; i += 9 {
		k := fmt.Sprintf("key%04d", i)
		d.Del([][]byte{b(k)})
		delete(want, k)
	}
	d.WaitForFlush()
	time.Sleep(150 * time.Millisecond) // let compactions settle
	d.Close()
	return dir, want
}

// Flip one bit at a random position of a random SSTable or MANIFEST file (on
// the read path, so the files on disk stay intact for the next trial). The
// database must either refuse to open or answer every query with the correct
// value or an error. Reporting "not found" for a key that exists, or returning a
// different value, is silent corruption and fails the test.
func TestBitFlipsNeverCauseSilentCorruption(t *testing.T) {
	dir, want := buildDB(t)
	var targets []string
	var sizes []int64
	names, _ := vfs.Default.List(dir)
	for _, n := range names {
		if strings.HasSuffix(n, ".sst") || strings.HasPrefix(n, "MANIFEST-") || n == "CURRENT" {
			fi, _ := os.Stat(filepath.Join(dir, n))
			targets = append(targets, n)
			sizes = append(sizes, fi.Size())
		}
	}
	if len(targets) < 3 {
		t.Fatalf("setup produced too few files: %v", targets)
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rng := rand.New(rand.NewSource(99))
	var openRefused, detectedOnRead, benign int
	const trials = 500
	for trial := 0; trial < trials; trial++ {
		fs := vfs.NewFaulty(vfs.Default)
		ti := rng.Intn(len(targets))
		off := rng.Int63n(sizes[ti])
		mask := byte(1) << uint(rng.Intn(8))
		fs.FlipOnRead(targets[ti], off, mask)
		o := opts(fs, dir)
		o.Compaction.Disabled = true
		d, _, err := engine.Open(o)
		if err != nil {
			openRefused++ // detected at open: fail-safe
			continue
		}
		bad := false
		for _, k := range keys {
			v, ok, err := d.Get(b(k))
			switch {
			case err != nil:
				bad = true // detected on read: fail-safe
			case !ok:
				t.Fatalf("trial %d (%s byte %d bit %02x): key %s silently reported NOT FOUND (data loss)", trial, targets[ti], off, mask, k)
			case string(v) != want[k]:
				t.Fatalf("trial %d (%s byte %d bit %02x): key %s silently returned WRONG DATA %q", trial, targets[ti], off, mask, k, v)
			}
		}
		// Scan must also be all-or-error.
		got, _, serr := d.Scan(nil, 100000)
		if serr != nil {
			bad = true
		} else if len(got) != len(want) {
			t.Fatalf("trial %d (%s byte %d bit %02x): SCAN returned %d keys, expected %d, without an error", trial, targets[ti], off, mask, len(got), len(want))
		}
		if bad {
			detectedOnRead++
		} else {
			benign++ // the flipped byte was not on any path these queries read
		}
		d.Close()
	}
	t.Logf("%d single-bit-flip trials: %d refused at open, %d detected on read, %d never touched by the queries; 0 silent corruptions",
		trials, openRefused, detectedOnRead, benign)
	if openRefused+detectedOnRead == 0 {
		t.Fatal("no flip was ever detected: the injector is not working")
	}
}

func TestSustainedReadErrorsFailSafe(t *testing.T) {
	dir, want := buildDB(t)
	fs := vfs.NewFaulty(vfs.Default)
	o := opts(fs, dir)
	o.Compaction.Disabled = true
	d := open(t, o)
	defer d.Close()
	fs.AddRule(vfs.Rule{Op: vfs.OpRead, Match: ".sst", Count: -1, Err: syscall.EIO})
	errs, wrong := 0, 0
	for k, v := range want {
		got, ok, err := d.Get(b(k))
		if err != nil {
			errs++
		} else if !ok || string(got) != v {
			wrong++
		}
	}
	if wrong != 0 {
		t.Fatalf("%d reads returned wrong/missing data instead of an error during EIO", wrong)
	}
	fs.ClearRules()
	if errs == 0 {
		t.Fatal("EIO was never surfaced")
	}
}

// ------------------------------------------------------------------ chaos

type uncertain struct{ values []string } // "" means a delete

// TestChaos mixes random faults (ENOSPC, fsync failures, short writes, rename
// failures), power losses and restarts into a live workload and checks an exact
// model after every restart. A write that returned an error is "uncertain":
// afterwards the key may hold either the previous value or that write's value.
func TestChaos(t *testing.T) {
	seeds := 30
	if testing.Short() {
		seeds = 6
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { runChaos(t, seed, 700) })
	}
}

func runChaos(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	fs := vfs.NewFaulty(vfs.Default)
	o := opts(fs, dir)
	o.StallTimeout = 300 * time.Millisecond
	d := open(t, o)
	defer func() { d.Close() }()

	acked := map[string]string{} // key -> last acknowledged value ("" absent)
	unsure := map[string]*uncertain{}
	keyOf := func() string { return fmt.Sprintf("k%02d", rng.Intn(60)) }
	consecutiveErrs := 0
	history := map[string][]string{} // per-key event log, printed when a check fails
	note := func(k, f string, a ...any) { history[k] = append(history[k], fmt.Sprintf(f, a...)) }
	global := []string{}
	gnote := func(f string, a ...any) { global = append(global, fmt.Sprintf(f, a...)) }
	fail := func(k, format string, a ...any) {
		t.Helper()
		t.Fatalf("seed %d: %s\n  history of %s:\n    %s\n  global events:\n    %s", seed, fmt.Sprintf(format, a...), k,
			strings.Join(history[k], "\n    "), strings.Join(global, "\n    "))
	}

	// verify checks every key. adopt says whether an observation becomes the new
	// baseline for keys that were uncertain: only true after a power loss, because
	// what we see after a plain process restart may still live only in the OS page
	// cache (e.g. an fsync-failed record) and a LATER power loss can still revert it.
	verify := func(stage string, adopt bool) {
		t.Helper()
		for i := 0; i < 60; i++ {
			k := fmt.Sprintf("k%02d", i)
			v, ok, err := d.Get(b(k))
			if err != nil {
				t.Fatalf("seed %d %s: read %s failed with %v", seed, stage, k, err)
			}
			cur := ""
			if ok {
				cur = string(v)
			}
			allowed := map[string]bool{acked[k]: true}
			if u := unsure[k]; u != nil {
				for _, x := range u.values {
					allowed[x] = true
				}
			}
			if !allowed[cur] {
				fail(k, "%s: key %s holds %q; acknowledged %q, uncertain %v", stage, k, cur, acked[k], unsure[k])
			}
			note(k, "%s: observed %q", stage, cur)
			if adopt || unsure[k] == nil {
				acked[k] = cur
				delete(unsure, k)
			}
		}
	}
	restart := func(crash bool, mode vfs.CrashMode) {
		gnote("step restart crash=%v mode=%d flushes=%s compactions=%s tables=%s wal_failed=%q", crash, mode, stat(d, "flushes"), stat(d, "compactions"), stat(d, "table_files"), stat(d, "wal_failed"))
		if crash {
			fs.Crash(mode)
		}
		fs.ClearRules()
		fs.SetDiskLimit(-1)
		d.Close()
		d = open(t, o)
		consecutiveErrs = 0
		verify(fmt.Sprintf("after restart(crash=%v)", crash), crash)
	}

	for step := 0; step < steps; step++ {
		switch r := rng.Intn(100); {
		case r < 55: // SET
			k, v := keyOf(), fmt.Sprintf("s%d-v%d", seed, step)
			ok, err := d.Set(b(k), b(v), engine.SetOptions{})
			note(k, "step %d SET %s -> ok=%v err=%v", step, v, ok, err)
			switch {
			case err != nil:
				consecutiveErrs++
				u := unsure[k]
				if u == nil {
					u = &uncertain{}
					unsure[k] = u
				}
				u.values = append(u.values, v)
			case ok:
				consecutiveErrs = 0
				acked[k] = v
				delete(unsure, k) // a later acknowledged write supersedes earlier uncertain ones
			}
		case r < 67: // DEL
			k := keyOf()
			n, err := d.Del([][]byte{b(k)})
			note(k, "step %d DEL -> n=%d err=%v", step, n, err)
			if err != nil {
				consecutiveErrs++
				u := unsure[k]
				if u == nil {
					u = &uncertain{}
					unsure[k] = u
				}
				u.values = append(u.values, "")
			} else {
				consecutiveErrs = 0
				acked[k] = ""
				delete(unsure, k)
			}
		case r < 80: // read-your-writes
			k := keyOf()
			v, ok, err := d.Get(b(k))
			if err == nil && unsure[k] == nil {
				cur := ""
				if ok {
					cur = string(v)
				}
				if cur != acked[k] {
					t.Fatalf("seed %d step %d: live read of %s = %q, acknowledged %q", seed, step, k, cur, acked[k])
				}
			}
		case r < 88: // arm a fault
			gnote("step %d arming fault", step)
			switch rng.Intn(6) {
			case 0:
				fs.SetDiskLimit(int64(1+rng.Intn(60)) << 10)
			case 1:
				fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Skip: rng.Intn(5), Count: 1 + rng.Intn(3)})
			case 2:
				fs.AddRule(vfs.Rule{Op: vfs.OpWrite, Match: ".wal", Skip: rng.Intn(5), Count: 1, Partial: rng.Intn(40)})
			case 3:
				fs.AddRule(vfs.Rule{Op: vfs.OpRename, Match: ".sst", Count: 1 + rng.Intn(3)})
			case 4:
				fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".sst.tmp", Count: 1 + rng.Intn(3)})
			case 5:
				fs.AddRule(vfs.Rule{Op: vfs.OpWrite, Match: "MANIFEST-", Count: 1, Partial: rng.Intn(20)})
			}
		case r < 92: // fault clears
			fs.ClearRules()
			fs.SetDiskLimit(-1)
		case r < 96: // power loss
			restart(true, []vfs.CrashMode{vfs.CrashDropUnsynced, vfs.CrashTornTail}[rng.Intn(2)])
		default: // clean restart
			restart(false, 0)
		}
		if consecutiveErrs > 25 { // a poisoned WAL/manifest only heals by restarting
			restart(rng.Intn(2) == 0, vfs.CrashTornTail)
		}
	}
	restart(true, vfs.CrashTornTail)
	// Finally the database must be healthy: writable, flushable, compactable.
	if _, err := d.Set(b("final"), b("ok"), engine.SetOptions{}); err != nil {
		t.Fatalf("seed %d: database unusable after chaos: %v", seed, err)
	}
	if err := d.CompactAll(); err != nil {
		t.Fatalf("seed %d: compaction failed after chaos: %v", seed, err)
	}
	verify("final", true)
}
