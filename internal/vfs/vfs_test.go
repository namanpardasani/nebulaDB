package vfs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func mustWrite(t *testing.T, f File, s string) {
	t.Helper()
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, fs FS, name string) string {
	t.Helper()
	b, err := ReadFile(fs, name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestOSWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "CURRENT")
	if err := WriteFileAtomic(Default, name, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(Default, name, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, Default, name); got != "two" {
		t.Fatalf("got %q", got)
	}
	names, _ := Default.List(dir)
	if len(names) != 1 {
		t.Fatalf("temp file left behind: %v", names)
	}
}

func TestFaultyRulesSkipCount(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	f, _ := fs.Create(filepath.Join(dir, "a.wal"))
	// Skip first sync, fail the next two, then recover.
	fs.AddRule(Rule{Op: OpSync, Match: ".wal", Skip: 1, Count: 2})
	var errs []bool
	for i := 0; i < 5; i++ {
		errs = append(errs, f.Sync() != nil)
	}
	want := []bool{false, true, true, false, false}
	for i := range want {
		if errs[i] != want[i] {
			t.Fatalf("sync %d: failed=%v want %v", i, errs[i], want[i])
		}
	}
}

func TestFaultyTornWrite(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	name := filepath.Join(dir, "x")
	f, _ := fs.Create(name)
	fs.AddRule(Rule{Op: OpWrite, Partial: 3})
	n, err := f.Write([]byte("abcdefgh"))
	if n != 3 || !errors.Is(err, ErrInjected) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := readAll(t, Default, name); got != "abc" {
		t.Fatalf("on disk %q", got)
	}
}

func TestFaultyENOSPC(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	f, _ := fs.Create(filepath.Join(dir, "x"))
	fs.SetDiskLimit(10)
	mustWrite(t, f, "12345678")
	n, err := f.Write([]byte("abcdef"))
	if n != 2 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// Freeing space (removing the file) lets writes proceed again.
	fs.SetDiskLimit(-1)
	mustWrite(t, f, "z")
}

func TestFaultyFlipOnRead(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	name := filepath.Join(dir, "data")
	f, _ := fs.Create(name)
	mustWrite(t, f, "hello")
	f.Close()
	fs.FlipOnRead("data", 1, 0x01)
	got := readAll(t, fs, name)
	if got == "hello" || len(got) != 5 || got[1] != 'e'^1 {
		t.Fatalf("flip not applied: %q", got)
	}
	if readAll(t, Default, name) != "hello" {
		t.Fatal("flip must not touch disk")
	}
}

func TestCrashDropsUnsyncedData(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	name := filepath.Join(dir, "wal")
	f, _ := fs.Create(name)
	mustWrite(t, f, "durable|")
	f.Sync()
	fs.SyncDir(dir)
	mustWrite(t, f, "volatile")
	if err := fs.Crash(CrashDropUnsynced); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, fs, name); got != "durable|" {
		t.Fatalf("after crash: %q", got)
	}
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrCrashed) {
		t.Fatalf("stale handle must fail, got %v", err)
	}
}

func TestCrashUndoesUnsyncedCreate(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	name := filepath.Join(dir, "sst")
	f, _ := fs.Create(name)
	mustWrite(t, f, "data")
	f.Sync() // file contents durable but directory entry is not
	fs.Crash(CrashDropUnsynced)
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("unsynced create must vanish, stat err=%v", err)
	}
}

func TestCrashRenameNeedsDirSync(t *testing.T) {
	dir := t.TempDir()
	cur := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(cur, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs := NewFaulty(Default)

	// Rename without SyncDir is rolled back: the old CURRENT reappears.
	tmp := cur + ".tmp"
	f, _ := fs.Create(tmp)
	mustWrite(t, f, "new")
	f.Sync()
	f.Close()
	fs.Rename(tmp, cur)
	fs.Crash(CrashDropUnsynced)
	if got := readAll(t, fs, cur); got != "old" {
		t.Fatalf("unsynced rename should roll back, got %q", got)
	}

	// The full atomic recipe survives.
	if err := WriteFileAtomic(fs, cur, []byte("new")); err != nil {
		t.Fatal(err)
	}
	fs.Crash(CrashDropUnsynced)
	if got := readAll(t, fs, cur); got != "new" {
		t.Fatalf("synced rename lost, got %q", got)
	}
}

func TestCrashTornTailKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	fs := NewFaulty(Default)
	name := filepath.Join(dir, "wal")
	f, _ := fs.Create(name)
	fs.SyncDir(dir)
	mustWrite(t, f, "AAAA")
	f.Sync()
	mustWrite(t, f, "BBBBBBBBBB")
	fs.Crash(CrashTornTail)
	got := readAll(t, fs, name)
	if !bytes.HasPrefix([]byte("AAAABBBBBBBBBB"), []byte(got)) || len(got) < 4 {
		t.Fatalf("torn tail must be a prefix >= synced part, got %q", got)
	}
}
