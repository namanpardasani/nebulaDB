package engine_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/engine/enginetest"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

func walOpts(fs vfs.FS, dir string, p wal.SyncPolicy) wal.Options {
	return wal.Options{FS: fs, Dir: dir, Policy: p, GroupCommit: true}
}

func open(t *testing.T, fs vfs.FS, dir string, clk *enginetest.Clock, p wal.SyncPolicy) *engine.Mem {
	t.Helper()
	m, _, err := engine.OpenMem(engine.MemOptions{Now: clk.Now, WAL: walOpts(fs, dir, p)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// The durable engine must satisfy exactly the same contract as the volatile one.
func TestDurableMemConformance(t *testing.T) {
	enginetest.Run(t, func(t *testing.T, now func() int64) engine.Engine {
		m, _, err := engine.OpenMem(engine.MemOptions{Now: now, WAL: walOpts(vfs.Default, t.TempDir(), wal.SyncNever)})
		if err != nil {
			t.Fatal(err)
		}
		return m
	})
}

func TestRestartRecoversState(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	m := open(t, vfs.Default, dir, clk, wal.SyncAlways)
	m.Set([]byte("a"), []byte("1"), engine.SetOptions{})
	m.Set([]byte("ttl"), []byte("x"), engine.SetOptions{ExpireAtMs: clk.Now() + 5000})
	m.MSet([][]byte{[]byte("m1"), []byte("v1"), []byte("m2"), []byte("v2")})
	m.IncrBy([]byte("n"), 41)
	m.IncrBy([]byte("n"), 1)
	m.Set([]byte("gone"), []byte("x"), engine.SetOptions{})
	m.Del([][]byte{[]byte("gone")})
	m.SetExpiry([]byte("a"), clk.Now()+100)
	m.Persist([]byte("a"))
	m.Close()

	m2 := open(t, vfs.Default, dir, clk, wal.SyncAlways)
	defer m2.Close()
	check := func(k, want string) {
		t.Helper()
		v, ok, _ := m2.Get([]byte(k))
		if want == "" && ok || want != "" && string(v) != want {
			t.Fatalf("%s: got %q ok=%v want %q", k, v, ok, want)
		}
	}
	check("a", "1")
	check("m1", "v1")
	check("m2", "v2")
	check("n", "42")
	check("gone", "")
	if ms, _ := m2.TTL([]byte("a")); ms != -1 {
		t.Fatalf("Persist lost across restart: ttl=%d", ms)
	}
	if ms, _ := m2.TTL([]byte("ttl")); ms != 5000 {
		t.Fatalf("TTL lost across restart: %d", ms)
	}
	clk.Advance(5000)
	check("ttl", "")
}

func TestFlushIsAtomicAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	clk := enginetest.NewClock()
	m := open(t, vfs.Default, dir, clk, wal.SyncAlways)
	for i := 0; i < 50; i++ {
		m.Set([]byte(fmt.Sprint("k", i)), []byte("v"), engine.SetOptions{})
	}
	m.Flush()
	m.Set([]byte("new"), []byte("v"), engine.SetOptions{})
	m.Close()
	m2 := open(t, vfs.Default, dir, clk, wal.SyncAlways)
	defer m2.Close()
	keys, _, _ := m2.Scan(nil, 100)
	if len(keys) != 1 || string(keys[0]) != "new" {
		t.Fatalf("got %q", keys)
	}
}

// Many concurrent writers, power loss, restart: every acknowledged write must
// be present with the right value, and INCR must not have lost updates.
func TestCrashLosesNoAckedWrite(t *testing.T) {
	for _, mode := range []vfs.CrashMode{vfs.CrashDropUnsynced, vfs.CrashTornTail} {
		for round := 0; round < 10; round++ {
			dir := t.TempDir()
			fs := vfs.NewFaulty(vfs.Default)
			clk := enginetest.NewClock()
			m := open(t, fs, dir, clk, wal.SyncAlways)
			var mu sync.Mutex
			acked := map[string]string{}
			var incrAcked int64
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 40; i++ {
						k, v := fmt.Sprintf("g%d-k%d", g, i%7), fmt.Sprintf("v%d-%d", g, i)
						if ok, err := m.Set([]byte(k), []byte(v), engine.SetOptions{}); err == nil && ok {
							mu.Lock()
							acked[k] = v // later acked writes to the same key supersede
							mu.Unlock()
						}
						if _, err := m.IncrBy([]byte("ctr"), 1); err == nil {
							mu.Lock()
							incrAcked++
							mu.Unlock()
						}
					}
				}()
			}
			wg.Wait()
			// Unsynced write in flight at the moment of the crash.
			fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Count: -1})
			m.Set([]byte("inflight"), []byte("x"), engine.SetOptions{})
			fs.Crash(mode)
			fs.ClearRules()

			m2 := open(t, fs, dir, clk, wal.SyncAlways)
			for k, want := range acked {
				got, ok, _ := m2.Get([]byte(k))
				if !ok || string(got) != want {
					t.Fatalf("mode=%d round=%d: key %s = %q ok=%v, want %q", mode, round, k, got, ok, want)
				}
			}
			if v, _, _ := m2.Get([]byte("ctr")); string(v) != fmt.Sprint(incrAcked) {
				t.Fatalf("counter %s != acked increments %d", v, incrAcked)
			}
			m2.Close()
		}
	}
}

// Group commit through the engine: concurrent writers on different keys must
// share fsyncs.
func TestEngineGroupCommit(t *testing.T) {
	fs := vfs.NewFaulty(vfs.Default)
	fs.SetSyncDelay(2_000_000) // 2ms
	clk := enginetest.NewClock()
	m := open(t, fs, t.TempDir(), clk, wal.SyncAlways)
	defer m.Close()
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				m.Set([]byte(fmt.Sprintf("g%d-%d", g, i)), []byte("v"), engine.SetOptions{})
			}
		}()
	}
	wg.Wait()
	syncs := fs.SyncCount()
	if syncs >= 160 {
		t.Fatalf("%d fsyncs for 320 writes: key striping must not serialise writers", syncs)
	}
	t.Logf("320 writes, %d fsyncs", syncs)
}
