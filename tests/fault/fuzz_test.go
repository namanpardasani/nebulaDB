package fault

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

var (
	templateOnce sync.Once
	templateDir  string
)

// template builds (once) a small valid database with tables, a manifest and a
// WAL, which fuzz iterations copy and then damage.
func template(tb testing.TB) string {
	templateOnce.Do(func() {
		dir, err := os.MkdirTemp("", "nebula-fuzz-template-*")
		if err != nil {
			tb.Fatal(err)
		}
		o := opts(vfs.Default, dir)
		o.Compaction.Disabled = true
		d, _, err := engine.Open(o)
		if err != nil {
			tb.Fatal(err)
		}
		for i := 0; i < 150; i++ {
			d.Set(b(fmt.Sprintf("key%03d", i)), b(strings.Repeat("v", 30)), engine.SetOptions{})
		}
		d.WaitForFlush()
		d.Set(b("in-wal"), b("1"), engine.SetOptions{})
		d.Close()
		templateDir = dir
	})
	return templateDir
}

func copyDir(tb testing.TB, src, dst string) {
	ents, err := os.ReadDir(src)
	if err != nil {
		tb.Fatal(err)
	}
	for _, e := range ents {
		if e.Name() == "LOCK" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			tb.Fatal(err)
		}
	}
}

// openMustBeSafe opens dir. It must not panic. If it succeeds the database must
// be usable and reopening must give the same contents; if it fails the error must
// be a clean refusal.
func openMustBeSafe(t *testing.T, dir string) {
	o := opts(vfs.Default, dir)
	o.Compaction.Disabled = true
	d, _, err := engine.Open(o)
	if err != nil {
		return
	}
	first, _, err := d.Scan(nil, 100000)
	if err != nil {
		d.Close()
		return // a damaged table surfaced as a read error: fail-safe
	}
	if _, err := d.Set(b("fuzz-probe"), b("x"), engine.SetOptions{}); err != nil {
		d.Close()
		return
	}
	if v, ok, err := d.Get(b("fuzz-probe")); err != nil || !ok || string(v) != "x" {
		t.Fatalf("opened database does not work: %q %v %v", v, ok, err)
	}
	d.Close()
	d2, _, err := engine.Open(o)
	if err != nil {
		t.Fatalf("a database that opened once must reopen: %v", err)
	}
	second, _, err := d2.Scan(nil, 100000)
	d2.Close()
	if err == nil && len(second) != len(first)+1 {
		t.Fatalf("recovery is not idempotent: %d keys, then %d (+1 probe)", len(first), len(second))
	}
}

func FuzzOpenWithGarbageWAL(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("garbage garbage garbage garbage"))
	f.Add(make([]byte, 200))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		copyDir(t, template(t), dir)
		segs, _ := wal.ListSegments(vfs.Default, dir)
		path := wal.SegmentPath(dir, segs[len(segs)-1])
		orig, _ := os.ReadFile(path)
		// Half the time append garbage to a real log, half the time replace it.
		if len(data)%2 == 0 {
			data = append(orig, data...)
		}
		os.WriteFile(path, data, 0o644)
		openMustBeSafe(t, dir)
	})
}

func FuzzOpenWithGarbageManifest(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("MANIFEST-000001\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		copyDir(t, template(t), dir)
		names, _ := vfs.Default.List(dir)
		for _, n := range names {
			if strings.HasPrefix(n, "MANIFEST-") {
				orig, _ := os.ReadFile(filepath.Join(dir, n))
				if len(data)%2 == 0 {
					data = append(orig[:len(orig)/2], data...)
				}
				os.WriteFile(filepath.Join(dir, n), data, 0o644)
			}
			if n == "CURRENT" && len(data)%3 == 0 {
				os.WriteFile(filepath.Join(dir, n), data, 0o644)
			}
		}
		openMustBeSafe(t, dir)
	})
}

func FuzzOpenWithGarbageSSTable(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 100))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		copyDir(t, template(t), dir)
		names, _ := vfs.Default.List(dir)
		for _, n := range names {
			if strings.HasSuffix(n, ".sst") {
				orig, _ := os.ReadFile(filepath.Join(dir, n))
				if len(data)%2 == 0 && len(orig) > 0 { // splice garbage into a real table
					cut := len(data) % len(orig)
					data = append(append([]byte(nil), orig[:cut]...), data...)
				}
				os.WriteFile(filepath.Join(dir, n), data, 0o644)
				break
			}
		}
		openMustBeSafe(t, dir)
	})
}
