package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/vfs"
)

// memFile is an in-memory vfs.File for fuzzing the reader.
type memFile struct{ b []byte }

func (m *memFile) Read(p []byte) (int, error)     { return 0, io.EOF }
func (m *memFile) Write(p []byte) (int, error)    { return 0, errors.New("ro") }
func (m *memFile) Seek(int64, int) (int64, error) { return 0, nil }
func (m *memFile) Close() error                   { return nil }
func (m *memFile) Sync() error                    { return nil }
func (m *memFile) Truncate(int64) error           { return nil }
func (m *memFile) Name() string                   { return "mem" }
func (m *memFile) Stat() (os.FileInfo, error)     { return memInfo(len(m.b)), nil }
func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.b)) {
		return 0, io.EOF
	}
	n := copy(p, m.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type memInfo int

func (m memInfo) Name() string       { return "mem" }
func (m memInfo) Size() int64        { return int64(m) }
func (m memInfo) Mode() os.FileMode  { return 0 }
func (m memInfo) ModTime() time.Time { return time.Time{} }
func (m memInfo) IsDir() bool        { return false }
func (m memInfo) Sys() any           { return nil }

// genEntries makes a sorted run of entries with multiple versions per key,
// tombstones and TTLs.
func genEntries(rng *rand.Rand, nKeys int) []iterator.Entry {
	var es []iterator.Entry
	seq := uint64(1)
	for k := 0; k < nKeys; k++ {
		key := []byte(fmt.Sprintf("user:%06d:%s", k*3+rng.Intn(3), []string{"a", "bb", "ccc"}[rng.Intn(3)]))
		versions := 1 + rng.Intn(4)
		for v := 0; v < versions; v++ {
			e := iterator.Entry{Key: key, Seq: seq, Kind: iterator.KindPut}
			switch rng.Intn(6) {
			case 0:
				e.Kind = iterator.KindDelete
			case 1:
				e.ExpireAt = int64(1000 + rng.Intn(5000))
			}
			if e.Kind == iterator.KindPut {
				e.Value = make([]byte, rng.Intn(200))
				rng.Read(e.Value)
			}
			es = append(es, e)
			seq += uint64(1 + rng.Intn(3))
		}
	}
	sort.Slice(es, func(i, j int) bool { return iterator.Compare(es[i].Key, es[i].Seq, es[j].Key, es[j].Seq) < 0 })
	return es
}

func writeTable(t testing.TB, es []iterator.Entry, o WriterOptions) (string, Properties) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	f, err := vfs.Default.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, o)
	for _, e := range es {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	p, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	return path, p
}

func open(t testing.TB, path string, st *Stats) *Reader {
	t.Helper()
	r, err := Open(vfs.Default, path, ReaderOptions{FileNum: 1, Stats: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestRoundTripIteration(t *testing.T) {
	for _, bs := range []int{64, 256, 4096} { // 64B blocks => nearly one entry per block
		for _, ri := range []int{1, 4, 16} {
			rng := rand.New(rand.NewSource(int64(bs*100 + ri)))
			es := genEntries(rng, 400)
			path, props := writeTable(t, es, WriterOptions{BlockSize: bs, RestartInterval: ri, BloomBitsPerKey: 10})
			r := open(t, path, nil)
			if props.NumEntries != uint64(len(es)) || r.Props().NumEntries != props.NumEntries {
				t.Fatalf("props mismatch")
			}
			it := r.NewIterator()
			it.SeekToFirst()
			for i, want := range es {
				if !it.Valid() {
					t.Fatalf("bs=%d ri=%d: ended at %d/%d (err %v)", bs, ri, i, len(es), it.Err())
				}
				got := it.Entry()
				if !bytes.Equal(got.Key, want.Key) || got.Seq != want.Seq || got.Kind != want.Kind ||
					!bytes.Equal(got.Value, want.Value) || got.ExpireAt != want.ExpireAt {
					t.Fatalf("bs=%d ri=%d entry %d: got %+v want %+v", bs, ri, i, got, want)
				}
				it.Next()
			}
			if it.Valid() || it.Err() != nil {
				t.Fatalf("extra entries or error: %v", it.Err())
			}
			if err := r.Verify(); err != nil {
				t.Fatalf("verify: %v", err)
			}
			if string(props.Smallest) != string(es[0].Key) || string(props.Largest) != string(es[len(es)-1].Key) {
				t.Fatal("key range wrong")
			}
		}
	}
}

func TestSeekAndGetAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	es := genEntries(rng, 600)
	path, _ := writeTable(t, es, WriterOptions{BlockSize: 300, BloomBitsPerKey: 10})
	var st Stats
	r := open(t, path, &st)

	// Oracle: newest version <= snap by linear scan.
	oracle := func(key []byte, snap uint64) (iterator.Entry, bool) {
		for _, e := range es { // es is key asc, seq desc
			if bytes.Equal(e.Key, key) && e.Seq <= snap {
				return e, true
			}
		}
		return iterator.Entry{}, false
	}
	maxSeq := es[0].Seq
	for _, e := range es {
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	for i := 0; i < 3000; i++ {
		var key []byte
		if rng.Intn(3) == 0 {
			key = []byte(fmt.Sprintf("user:%06d:zz", rng.Intn(2000))) // mostly absent
		} else {
			key = es[rng.Intn(len(es))].Key
		}
		snap := uint64(rng.Int63n(int64(maxSeq) + 5))
		want, wok := oracle(key, snap)
		got, ok, err := r.Get(key, snap)
		if err != nil {
			t.Fatal(err)
		}
		if ok != wok || (ok && (got.Seq != want.Seq || got.Kind != want.Kind || !bytes.Equal(got.Value, want.Value))) {
			t.Fatalf("Get(%s, %d): got %+v ok=%v want %+v ok=%v", key, snap, got, ok, want, wok)
		}
	}
	// Seek lands on the newest version of the first key >= target.
	it := r.NewIterator()
	for i := 0; i < 1000; i++ {
		target := []byte(fmt.Sprintf("user:%06d", rng.Intn(1900)))
		it.Seek(target)
		idx := sort.Search(len(es), func(i int) bool { return bytes.Compare(es[i].Key, target) >= 0 })
		if idx == len(es) {
			if it.Valid() {
				t.Fatalf("seek %s should be past the end", target)
			}
			continue
		}
		if !it.Valid() || !bytes.Equal(it.Entry().Key, es[idx].Key) || it.Entry().Seq != es[idx].Seq {
			t.Fatalf("seek %s: got %+v want %+v", target, it.Entry(), es[idx])
		}
	}
	if st.BloomSkipped.Load() == 0 {
		t.Error("bloom filter never skipped an absent key")
	}
	t.Logf("bloom: checked=%d skipped=%d falsePositive=%d blockReads=%d",
		st.BloomChecked.Load(), st.BloomSkipped.Load(), st.BloomFalsePositive.Load(), st.BlockReads.Load())
}

func TestWriterRejectsMisuse(t *testing.T) {
	f, _ := vfs.Default.Create(filepath.Join(t.TempDir(), "x.sst"))
	w := NewWriter(f, WriterOptions{})
	w.Add(iterator.Entry{Key: []byte("b"), Seq: 5, Kind: iterator.KindPut})
	if err := w.Add(iterator.Entry{Key: []byte("a"), Seq: 6, Kind: iterator.KindPut}); err == nil {
		t.Fatal("out-of-order key accepted")
	}
	w2 := NewWriter(f, WriterOptions{})
	w2.Add(iterator.Entry{Key: []byte("b"), Seq: 5, Kind: iterator.KindPut})
	if err := w2.Add(iterator.Entry{Key: []byte("b"), Seq: 5, Kind: iterator.KindPut}); err == nil {
		t.Fatal("duplicate (key,seq) accepted")
	}
	if _, err := NewWriter(f, WriterOptions{}).Finish(); err == nil {
		t.Fatal("empty table accepted")
	}
}

func TestPrefixCompressionShrinksFile(t *testing.T) {
	var es []iterator.Entry
	for i := 0; i < 2000; i++ {
		es = append(es, iterator.Entry{Key: []byte(fmt.Sprintf("a/very/long/common/prefix/for/all/keys/%08d", i)), Seq: uint64(i + 1), Kind: iterator.KindPut, Value: []byte("v")})
	}
	_, p16 := writeTable(t, es, WriterOptions{RestartInterval: 16})
	_, p1 := writeTable(t, es, WriterOptions{RestartInterval: 1}) // no compression
	t.Logf("data size: restart=16 -> %d bytes, restart=1 (uncompressed) -> %d bytes", p16.DataSize, p1.DataSize)
	if p16.DataSize*2 > p1.DataSize {
		t.Fatalf("prefix compression ineffective: %d vs %d", p16.DataSize, p1.DataSize)
	}
}

func readAllFile(t testing.TB, path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Every strict prefix of a table must be rejected at Open: a crash mid-write
// can never produce a file that looks valid.
func TestTruncatedFileRejected(t *testing.T) {
	es := genEntries(rand.New(rand.NewSource(1)), 100)
	path, _ := writeTable(t, es, WriterOptions{BlockSize: 256, BloomBitsPerKey: 10})
	full := readAllFile(t, path)
	for n := 0; n < len(full); n++ {
		if _, err := openReader(&memFile{full[:n]}, "mem", ReaderOptions{}); err == nil {
			t.Fatalf("truncated to %d/%d bytes was accepted", n, len(full))
		}
	}
}

// Flip every single bit-position byte in the file: the damage must always be
// caught either at Open or by Verify, and never yield silently wrong data.
func TestBitFlipsAlwaysDetected(t *testing.T) {
	es := genEntries(rand.New(rand.NewSource(2)), 60)
	path, _ := writeTable(t, es, WriterOptions{BlockSize: 256, BloomBitsPerKey: 10})
	full := readAllFile(t, path)
	for off := 0; off < len(full); off++ {
		bad := append([]byte(nil), full...)
		bad[off] ^= 1 << uint(off%8)
		r, err := openReader(&memFile{bad}, "mem", ReaderOptions{})
		if err != nil {
			continue
		}
		if err := r.Verify(); err == nil {
			t.Fatalf("flip at byte %d/%d went undetected", off, len(full))
		}
	}
}

func FuzzOpenAndRead(f *testing.F) {
	es := genEntries(rand.New(rand.NewSource(3)), 30)
	path, _ := writeTable(f, es, WriterOptions{BlockSize: 200, BloomBitsPerKey: 10})
	good := readAllFile(f, path)
	f.Add(good)
	f.Add(good[:len(good)/2])
	f.Add(make([]byte, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := openReader(&memFile{data}, "mem", ReaderOptions{})
		if err != nil {
			return
		}
		// Must not panic whatever is inside.
		it := r.NewIterator()
		it.SeekToFirst()
		for n := 0; it.Valid() && n < 100000; n++ {
			it.Next()
		}
		it.Seek([]byte("user:000010"))
		r.Get([]byte("user:000010:a"), math.MaxUint64)
		r.Verify()
	})
}

func FuzzBlockIter(f *testing.F) {
	b := newBlockBuilder(4)
	for i := 0; i < 20; i++ {
		b.add([]byte(fmt.Sprintf("key%04d", i)), uint64(100-i), iterator.KindPut, 0, []byte("value"))
	}
	f.Add(b.finish())
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		it, err := newBlockIter(data)
		if err != nil {
			return
		}
		for it.first(); it.valid; it.step() {
		}
		it.seek([]byte("key0007"), 50)
	})
}

func BenchmarkGet(b *testing.B) {
	rng := rand.New(rand.NewSource(9))
	es := genEntries(rng, 50000)
	path, _ := writeTable(b, es, WriterOptions{BloomBitsPerKey: 10})
	r, _ := Open(vfs.Default, path, ReaderOptions{})
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Get(es[i%len(es)].Key, math.MaxUint64)
	}
}
