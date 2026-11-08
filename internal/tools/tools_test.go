package tools

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/wal"
)

// buildDB creates a database with several tables, overwrites, deletes and some
// unflushed data left in the WAL. It returns the directory and the expected
// live contents.
func buildDB(t *testing.T) (string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	db, _, err := engine.Open(engine.Options{
		Dir: dir, Sync: wal.SyncNever, MemtableSize: 8 << 10, BlockSize: 512, BloomBits: 10,
		Compaction: engine.CompactionOptions{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 600; i++ {
		k, v := fmt.Sprintf("key%04d", i), fmt.Sprintf("value-%04d-%s", i, strings.Repeat("x", 40))
		db.Set([]byte(k), []byte(v), engine.SetOptions{})
		want[k] = v
	}
	for i := 0; i < 600; i += 7 { // overwrites
		k, v := fmt.Sprintf("key%04d", i), fmt.Sprintf("NEW-%04d", i)
		db.Set([]byte(k), []byte(v), engine.SetOptions{})
		want[k] = v
	}
	for i := 0; i < 600; i += 11 { // deletes
		k := fmt.Sprintf("key%04d", i)
		db.Del([][]byte{[]byte(k)})
		delete(want, k)
	}
	db.WaitForFlush()
	// A little data stays only in the WAL.
	for i := 0; i < 5; i++ {
		k, v := fmt.Sprintf("tail%d", i), "in-wal"
		db.Set([]byte(k), []byte(v), engine.SetOptions{})
		want[k] = v
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, want
}

func readAll(t *testing.T, dir string) map[string]string {
	t.Helper()
	db, _, err := engine.Open(engine.Options{Dir: dir, Sync: wal.SyncNever, Compaction: engine.CompactionOptions{Disabled: true}})
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer db.Close()
	got := map[string]string{}
	var next []byte
	for {
		keys, n, err := db.Scan(next, 500)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			v, _, _ := db.Get(k)
			got[string(k)] = string(v)
		}
		if n == nil {
			return got
		}
		next = n
	}
}

func firstFile(t *testing.T, dir, ext string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*"+ext))
	sort.Strings(m)
	if len(m) == 0 {
		t.Fatalf("no %s files in %s", ext, dir)
	}
	return m[0]
}

func hashDir(t *testing.T, dir string) string {
	h := sha256.New()
	m, _ := filepath.Glob(filepath.Join(dir, "*"))
	sort.Strings(m)
	for _, p := range m {
		if strings.HasSuffix(p, "LOCK") {
			continue
		}
		b, _ := os.ReadFile(p)
		h.Write([]byte(p))
		h.Write(b)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestDumpsAndStats(t *testing.T) {
	dir, _ := buildDB(t)
	var b bytes.Buffer
	if err := Stats(&b, dir); err != nil || !strings.Contains(b.String(), "L0") || !strings.Contains(b.String(), "WAL segments") {
		t.Fatalf("stats:\n%s\n%v", b.String(), err)
	}
	b.Reset()
	if err := DumpManifest(&b, dir); err != nil || !strings.Contains(b.String(), "live version") || !strings.Contains(b.String(), "+ L0") {
		t.Fatalf("dump-manifest:\n%s\n%v", b.String(), err)
	}
	b.Reset()
	if err := DumpWAL(&b, dir, 0); err != nil || !strings.Contains(b.String(), `key="tail0"`) {
		t.Fatalf("dump-wal:\n%s\n%v", b.String(), err)
	}
	b.Reset()
	if err := DumpSSTable(&b, firstFile(t, dir, ".sst"), 5); err != nil || !strings.Contains(b.String(), "entries:") || !strings.Contains(b.String(), "PUT seq=") {
		t.Fatalf("dump-sstable:\n%s\n%v", b.String(), err)
	}
}

func TestVerifyHealthyAndDamaged(t *testing.T) {
	dir, _ := buildDB(t)
	var b bytes.Buffer
	rep, _ := Verify(&b, dir)
	if !rep.OK() || rep.Tables == 0 || rep.WALBatch == 0 {
		t.Fatalf("healthy database failed verification:\n%s", b.String())
	}

	// Flip a bit in the middle of a table's data region.
	sst := firstFile(t, dir, ".sst")
	data, _ := os.ReadFile(sst)
	data[len(data)/4] ^= 0x01
	os.WriteFile(sst, data, 0o644)
	b.Reset()
	rep, _ = Verify(&b, dir)
	if rep.OK() || !strings.Contains(b.String(), filepath.Base(sst)) || !strings.Contains(b.String(), "checksum") {
		t.Fatalf("corrupt table not reported:\n%s", b.String())
	}
	data[len(data)/4] ^= 0x01
	os.WriteFile(sst, data, 0o644)

	// Missing live table.
	os.Rename(sst, sst+".gone")
	b.Reset()
	rep, _ = Verify(&b, dir)
	if rep.OK() || !strings.Contains(b.String(), "missing") {
		t.Fatalf("missing table not reported:\n%s", b.String())
	}
	os.Rename(sst+".gone", sst)

	// A torn WAL tail is a warning, not a failure.
	seg := firstFile(t, dir, ".wal")
	f, _ := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{9, 9, 9})
	f.Close()
	b.Reset()
	rep, _ = Verify(&b, dir)
	if !rep.OK() || !strings.Contains(b.String(), "torn tail") {
		t.Fatalf("torn WAL tail should warn only:\n%s", b.String())
	}
	// Orphans and temp files are warnings.
	os.WriteFile(filepath.Join(dir, "999999.sst"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "000007.sst.tmp"), []byte("x"), 0o644)
	b.Reset()
	rep, _ = Verify(&b, dir)
	if !rep.OK() || !strings.Contains(b.String(), "orphan") || !strings.Contains(b.String(), "temp file") {
		t.Fatalf("orphans:\n%s", b.String())
	}
	// Garbage MANIFEST.
	os.Remove(filepath.Join(dir, "CURRENT"))
	os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("garbage"), 0o644)
	b.Reset()
	if rep, _ = Verify(&b, dir); rep.OK() {
		t.Fatalf("bad CURRENT not reported:\n%s", b.String())
	}
}

func TestRepairSalvagesDamagedDatabase(t *testing.T) {
	src, want := buildDB(t)

	// Damage one data block of one table (footer and index stay intact) and
	// scribble over the middle of the WAL.
	sst := firstFile(t, src, ".sst")
	data, _ := os.ReadFile(sst)
	for i := 100; i < 130; i++ {
		data[i] ^= 0xff
	}
	os.WriteFile(sst, data, 0o644)

	before := hashDir(t, src)
	dst := filepath.Join(t.TempDir(), "repaired")
	var out bytes.Buffer
	rep, err := Repair(&out, src, dst)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if hashDir(t, src) != before {
		t.Fatal("repair modified the source directory")
	}
	if rep.BlocksSkipped == 0 || !rep.UsedManifest {
		t.Fatalf("expected a skipped block: %+v\n%s", rep, out.String())
	}
	// The repaired database is healthy...
	var vb bytes.Buffer
	if vr, _ := Verify(&vb, dst); !vr.OK() {
		t.Fatalf("repaired database fails verification:\n%s", vb.String())
	}
	// ...and holds everything except what lived in the destroyed block.
	got := readAll(t, dst)
	lost := 0
	for k, v := range want {
		if g, ok := got[k]; !ok {
			lost++
		} else if g != v {
			// A key whose newest version sat in the destroyed block may fall back to an older version.
			lost++
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok && !strings.HasPrefix(k, "key") {
			t.Fatalf("repair invented key %q", k)
		}
	}
	t.Logf("repair: %d of %d expected keys missing/stale after destroying one data block (~%d%%)", lost, len(want), lost*100/len(want))
	if lost == 0 || lost > len(want)/2 {
		t.Fatalf("unexpected loss: %d of %d", lost, len(want))
	}
	if len(got) < len(want)/2 {
		t.Fatalf("recovered only %d keys", len(got))
	}

	// Non-empty destination is refused.
	if _, err := Repair(&out, src, dst); err == nil {
		t.Fatal("repair into a non-empty directory must fail")
	}
}

func TestRepairWithoutManifestAndWALCorruption(t *testing.T) {
	src, want := buildDB(t)
	os.Remove(filepath.Join(src, "CURRENT"))
	for _, m := range mustGlob(t, src, "MANIFEST-*") {
		os.Remove(m)
	}
	// Corrupt the middle of the WAL: salvage must resynchronise and still
	// recover the records after the damage.
	seg := firstFile(t, src, ".wal")
	data, _ := os.ReadFile(seg)
	if len(data) > 200 {
		for i := 20; i < 40; i++ {
			data[i] ^= 0xaa
		}
		os.WriteFile(seg, data, 0o644)
	}
	dst := filepath.Join(t.TempDir(), "out")
	var out bytes.Buffer
	rep, err := Repair(&out, src, dst)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if rep.UsedManifest || !strings.Contains(out.String(), "MANIFEST unreadable") {
		t.Fatalf("fallback mode not reported:\n%s", out.String())
	}
	got := readAll(t, dst)
	missing := 0
	for k := range want {
		if _, ok := got[k]; !ok {
			missing++
		}
	}
	// Without a MANIFEST, deleted keys may resurrect (documented), but live keys must survive.
	t.Logf("no-manifest repair: %d keys recovered, %d expected keys missing", len(got), missing)
	if missing > len(want)/10 {
		t.Fatalf("too many live keys lost: %d", missing)
	}
}

func mustGlob(t *testing.T, dir, pat string) []string {
	m, err := filepath.Glob(filepath.Join(dir, pat))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCompactRefusesRunningDatabaseThenWorks(t *testing.T) {
	dir, want := buildDB(t)
	db, _, err := engine.Open(engine.Options{Dir: dir, Sync: wal.SyncNever, Compaction: engine.CompactionOptions{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := Compact(&out, dir); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("compact must refuse a directory in use, got %v", err)
	}
	db.Close()
	if err := Compact(&out, dir); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "tables:") {
		t.Fatal(out.String())
	}
	got := readAll(t, dir)
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %q after compact, want %q", k, got[k], v)
		}
	}
	var vb bytes.Buffer
	if vr, _ := Verify(&vb, dir); !vr.OK() {
		t.Fatalf("verify after compact:\n%s", vb.String())
	}
}
