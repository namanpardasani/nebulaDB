package manifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/naman/nebuladb/internal/vfs"
)

func fm(num uint64, level int, lo, hi string) FileMeta {
	return FileMeta{Num: num, Level: level, Size: 100 + int64(num), Entries: 10, Deletes: 1, MinSeq: num, MaxSeq: num + 5, Smallest: []byte(lo), Largest: []byte(hi)}
}

func add(files ...FileMeta) *Edit { return &Edit{Added: files} }

func open(t testing.TB, fs vfs.FS, dir string) (*Manifest, Info) {
	t.Helper()
	m, info, err := Open(fs, dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return m, info
}

func TestEditEncodeDecodeRoundTrip(t *testing.T) {
	e := Edit{
		HasLogNumber: true, LogNumber: 7, HasNextFileNum: true, NextFileNum: 99, HasLastSeq: true, LastSeq: 12345,
		Added:   []FileMeta{fm(5, 0, "a", "m"), fm(6, 3, "", "\x00\xff")},
		Deleted: []DeletedFile{{1, 2}, {0, 3}},
	}
	got, err := DecodeEdit(e.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, e) {
		t.Fatalf("got %+v\nwant %+v", got, e)
	}
	for _, bad := range [][]byte{{99}, {tagAddFile, 1}, {tagAddFile, 9, 1, 1, 1, 1, 1, 1, 0, 0}, {tagDeleteFile, 0}, {tagLogNumber}} {
		if _, err := DecodeEdit(bad); err == nil {
			t.Errorf("accepted malformed edit %v", bad)
		}
	}
}

func TestFreshOpenAndReopen(t *testing.T) {
	dir := t.TempDir()
	m, info := open(t, vfs.Default, dir)
	if !info.Fresh || info.ManifestNum != 1 {
		t.Fatalf("%+v", info)
	}
	n1, n2 := m.NewFileNumber(), m.NewFileNumber()
	if n1 != 2 || n2 != 3 {
		t.Fatalf("file numbers %d %d", n1, n2)
	}
	e := add(fm(n1, 0, "a", "f"), fm(n2, 0, "c", "k"))
	e.HasLogNumber, e.LogNumber, e.HasLastSeq, e.LastSeq = true, 4, true, 77
	if err := m.LogAndApply(e); err != nil {
		t.Fatal(err)
	}
	if err := m.LogAndApply(&Edit{Added: []FileMeta{fm(10, 1, "a", "z")}, Deleted: []DeletedFile{{0, n1}, {0, n2}}}); err != nil {
		t.Fatal(err)
	}
	want := m.Current()
	m.Close()

	m2, info := open(t, vfs.Default, dir)
	defer m2.Close()
	if info.Fresh || info.Edits != 3 { // initial snapshot + 2 edits
		t.Fatalf("%+v", info)
	}
	got := m2.Current()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state changed across reopen:\n got %+v\nwant %+v", got, want)
	}
	if len(got.Levels[0]) != 0 || len(got.Levels[1]) != 1 || got.LogNumber != 4 || got.LastSeq != 77 {
		t.Fatalf("unexpected state %+v", got)
	}
	// File numbers keep increasing after restart (never reused).
	if n := m2.NewFileNumber(); n <= 10 {
		t.Fatalf("reused file number %d", n)
	}
}

func TestInvalidEditsRejectedWithoutWriting(t *testing.T) {
	dir := t.TempDir()
	m, _ := open(t, vfs.Default, dir)
	defer m.Close()
	m.LogAndApply(add(fm(5, 1, "a", "f")))
	size := m.Size()
	bad := []*Edit{
		{Deleted: []DeletedFile{{0, 999}}},             // deleting a file that does not exist
		add(fm(5, 2, "x", "z")),                        // duplicate file number
		add(fm(6, 1, "e", "k")),                        // overlaps L1 [a,f]
		{HasLogNumber: true, LogNumber: 0, Added: nil}, // fine (not backwards: 0 == 0)
	}
	for i, e := range bad[:3] {
		if err := m.LogAndApply(e); err == nil {
			t.Errorf("edit %d accepted", i)
		}
	}
	if m.Size() != size {
		t.Fatal("rejected edits must not touch the file")
	}
	if len(m.Current().Levels[1]) != 1 {
		t.Fatal("rejected edit changed state")
	}
	m.LogAndApply(&Edit{HasLogNumber: true, LogNumber: 9})
	if err := m.LogAndApply(&Edit{HasLogNumber: true, LogNumber: 3}); err == nil {
		t.Fatal("log number moving backwards must be rejected")
	}
}

func TestTornTailEveryOffset(t *testing.T) {
	dir := t.TempDir()
	m, _ := open(t, vfs.Default, dir)
	m.LogAndApply(add(fm(2, 0, "a", "b")))
	m.LogAndApply(add(fm(3, 0, "c", "d")))
	sizeBefore := m.Size()
	m.LogAndApply(add(fm(4, 0, "e", "f")))
	sizeFull := m.Size()
	name := filepath.Join(dir, fmt.Sprintf("MANIFEST-%06d", m.Number()))
	m.Close()
	full, _ := os.ReadFile(name)
	for cut := sizeBefore; cut <= sizeFull; cut++ {
		os.WriteFile(name, full[:cut], 0o644)
		m2, info, err := Open(vfs.Default, dir, Options{})
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		wantFiles := 2
		if cut == sizeFull {
			wantFiles = 3
		}
		if n := len(m2.Current().Levels[0]); n != wantFiles {
			t.Fatalf("cut %d: %d files, want %d", cut, n, wantFiles)
		}
		if torn := cut != sizeBefore && cut != sizeFull; info.TornTail != torn {
			t.Fatalf("cut %d: TornTail=%v want %v", cut, info.TornTail, torn)
		}
		// Appending after a repaired tail works and persists.
		if err := m2.LogAndApply(add(fm(50, 1, "x", "y"))); err != nil {
			t.Fatal(err)
		}
		m2.Close()
		m3, _ := open(t, vfs.Default, dir)
		if len(m3.Current().Levels[1]) != 1 {
			t.Fatalf("cut %d: edit appended after tail repair was lost", cut)
		}
		m3.Close()
		os.WriteFile(name, full, 0o644) // restore for the next iteration
	}
}

func TestMidFileCorruptionRefused(t *testing.T) {
	dir := t.TempDir()
	m, _ := open(t, vfs.Default, dir)
	for i := uint64(2); i < 8; i++ {
		m.LogAndApply(add(fm(i, 0, fmt.Sprint("k", i), fmt.Sprint("k", i, "z"))))
	}
	name := filepath.Join(dir, fmt.Sprintf("MANIFEST-%06d", m.Number()))
	m.Close()
	data, _ := os.ReadFile(name)
	bad := append([]byte(nil), data...)
	bad[len(bad)/2] ^= 0x40
	os.WriteFile(name, bad, 0o644)
	_, _, err := Open(vfs.Default, dir, Options{})
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError, got %v", err)
	}
}

func TestMissingCurrentWithTables(t *testing.T) {
	dir := t.TempDir()
	m, _ := open(t, vfs.Default, dir)
	m.Close()
	os.WriteFile(filepath.Join(dir, "000002.sst"), []byte("x"), 0o644)
	os.Remove(filepath.Join(dir, "CURRENT"))
	if _, _, err := Open(vfs.Default, dir, Options{}); !errors.Is(err, ErrMissingCurrent) {
		t.Fatalf("got %v", err)
	}
	os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("garbage"), 0o644)
	if _, _, err := Open(vfs.Default, dir, Options{}); err == nil {
		t.Fatal("malformed CURRENT accepted")
	}
}

func TestRewriteCompactsAndSwitches(t *testing.T) {
	dir := t.TempDir()
	m, _, err := Open(vfs.Default, dir, Options{RewriteThreshold: 400})
	if err != nil {
		t.Fatal(err)
	}
	startNum := m.Number()
	// Churn: add and remove a file repeatedly so the log grows but the state stays tiny.
	for i := 0; i < 40; i++ {
		n := m.NewFileNumber()
		if err := m.LogAndApply(add(fm(n, 0, "a", "z"))); err != nil {
			t.Fatal(err)
		}
		if err := m.LogAndApply(&Edit{Deleted: []DeletedFile{{0, n}}}); err != nil {
			t.Fatal(err)
		}
	}
	if m.Number() == startNum {
		t.Fatal("manifest was never rewritten")
	}
	want := m.Current()
	m.Close()
	names, _ := vfs.Default.List(dir)
	manifests := 0
	for _, n := range names {
		if _, ok := parseManifestName(n); ok {
			manifests++
		}
	}
	if manifests != 1 {
		t.Fatalf("stale manifests left behind: %v", names)
	}
	m2, info := open(t, vfs.Default, dir)
	defer m2.Close()
	if !reflect.DeepEqual(m2.Current(), want) {
		t.Fatal("state differs after rewrite+reopen")
	}
	// 80 edits were logged; without rewrites a restart would replay all of them.
	if info.Edits >= 20 {
		t.Fatalf("manifest should have been compacted by rewrites, replayed %d edits", info.Edits)
	}
}

// Power loss at any moment: everything LogAndApply acknowledged survives.
func TestCrashKeepsAckedEdits(t *testing.T) {
	for _, mode := range []vfs.CrashMode{vfs.CrashDropUnsynced, vfs.CrashTornTail} {
		dir := t.TempDir()
		fs := vfs.NewFaulty(vfs.Default)
		m, _, _ := Open(fs, dir, Options{RewriteThreshold: 600}) // forces rewrites mid-run
		var acked []uint64
		for i := 0; i < 60; i++ {
			n := m.NewFileNumber()
			if err := m.LogAndApply(add(fm(n, 0, "a", "z"))); err == nil {
				acked = append(acked, n)
			}
			if i%3 == 2 && len(acked) > 2 {
				victim := acked[0]
				if err := m.LogAndApply(&Edit{Deleted: []DeletedFile{{0, victim}}}); err == nil {
					acked = acked[1:]
				}
			}
		}
		fs.Crash(mode)
		m2, _, err := Open(fs, dir, Options{})
		if err != nil {
			t.Fatalf("mode %d: reopen after crash: %v", mode, err)
		}
		got := map[uint64]bool{}
		for _, f := range m2.Current().Levels[0] {
			got[f.Num] = true
		}
		for _, n := range acked {
			if !got[n] {
				t.Fatalf("mode %d: acknowledged file %d lost", mode, n)
			}
		}
		if len(got) != len(acked) {
			t.Fatalf("mode %d: %d files after crash, %d acknowledged", mode, len(got), len(acked))
		}
		m2.Close()
	}
}

// A failure while switching CURRENT must poison the manifest instead of letting
// later edits go to a file that CURRENT might not point at.
func TestFailedCurrentSwapPoisons(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewFaulty(vfs.Default)
	m, _, _ := Open(fs, dir, Options{RewriteThreshold: 1 << 30})
	m.LogAndApply(add(fm(2, 0, "a", "b")))
	fs.AddRule(vfs.Rule{Op: vfs.OpRename, Match: "CURRENT", Count: -1})
	if err := m.Rewrite(); err == nil {
		t.Fatal("rewrite should fail")
	}
	if err := m.LogAndApply(add(fm(3, 0, "c", "d"))); err == nil {
		t.Fatal("manifest must refuse edits after a failed CURRENT switch")
	}
	fs.ClearRules()
	m.Close()
	m2, _ := open(t, fs, dir)
	defer m2.Close()
	if len(m2.Current().Levels[0]) != 1 {
		t.Fatal("recovery must see exactly the pre-failure state")
	}
}

// A failed append is rolled back (ENOSPC-style faults are recoverable); a
// failed fsync poisons the manifest.
func TestAppendFailureRollbackAndFsyncPoison(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewFaulty(vfs.Default)
	m, _, _ := Open(fs, dir, Options{})
	m.LogAndApply(add(fm(2, 0, "a", "b")))
	size := m.Size()
	fs.AddRule(vfs.Rule{Op: vfs.OpWrite, Match: "MANIFEST", Partial: 7})
	if err := m.LogAndApply(add(fm(3, 0, "c", "d"))); err == nil {
		t.Fatal("want error")
	}
	if len(m.Current().Levels[0]) != 1 {
		t.Fatal("failed edit changed in-memory state")
	}
	// The rule fired once; the retry works and the file has no garbage in it.
	if err := m.LogAndApply(add(fm(3, 0, "c", "d"))); err != nil {
		t.Fatalf("manifest should have recovered: %v", err)
	}
	if m.Size() <= size {
		t.Fatal("size not advanced")
	}
	m.Close()
	m2, info := open(t, fs, dir)
	if info.TornTail || len(m2.Current().Levels[0]) != 2 {
		t.Fatalf("%+v %d", info, len(m2.Current().Levels[0]))
	}
	fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: "MANIFEST", Count: -1})
	if err := m2.LogAndApply(add(fm(9, 0, "x", "y"))); err == nil {
		t.Fatal("want fsync error")
	}
	fs.ClearRules()
	if err := m2.LogAndApply(add(fm(10, 0, "p", "q"))); err == nil {
		t.Fatal("manifest must stay poisoned after an fsync failure")
	}
	m2.Close()
}

func FuzzDecodeEdit(f *testing.F) {
	e := Edit{HasLogNumber: true, LogNumber: 1, Added: []FileMeta{fm(2, 0, "a", "b")}, Deleted: []DeletedFile{{0, 1}}}
	f.Add(e.Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		if e, err := DecodeEdit(b); err == nil {
			// Whatever decodes must re-encode to something that decodes equal.
			e2, err := DecodeEdit(e.Encode())
			if err != nil || !reflect.DeepEqual(e, e2) {
				t.Fatalf("round trip failed: %v", err)
			}
		}
	})
}

func FuzzReplay(f *testing.F) {
	rec := appendRecord(nil, (&Edit{HasNextFileNum: true, NextFileNum: 5, Added: []FileMeta{fm(2, 0, "a", "b")}}).Encode())
	f.Add(rec)
	f.Add(append(rec, 1, 2, 3))
	f.Fuzz(func(t *testing.T, b []byte) { replay("fuzz", b, nil) })
}
