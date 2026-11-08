package memtable

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/naman/nebuladb/internal/iterator"
)

func TestGetVersions(t *testing.T) {
	m := New()
	m.Add(1, iterator.KindPut, []byte("k"), []byte("v1"), 0)
	m.Add(5, iterator.KindPut, []byte("k"), []byte("v5"), 99)
	m.Add(9, iterator.KindDelete, []byte("k"), nil, 0)
	m.Add(3, iterator.KindPut, []byte("other"), []byte("x"), 0)

	tests := []struct {
		snap uint64
		kind iterator.Kind
		val  string
		ok   bool
	}{
		{0, 0, "", false},
		{1, iterator.KindPut, "v1", true},
		{4, iterator.KindPut, "v1", true},
		{5, iterator.KindPut, "v5", true},
		{8, iterator.KindPut, "v5", true},
		{9, iterator.KindDelete, "", true},
		{1 << 62, iterator.KindDelete, "", true},
	}
	for _, tt := range tests {
		e, ok := m.Get([]byte("k"), tt.snap)
		if ok != tt.ok || (ok && (e.Kind != tt.kind || string(e.Value) != tt.val)) {
			t.Errorf("snap %d: got %+v ok=%v", tt.snap, e, ok)
		}
	}
	if e, _ := m.Get([]byte("k"), 5); e.ExpireAt != 99 {
		t.Error("expireAt lost")
	}
	if _, ok := m.Get([]byte("missing"), 100); ok {
		t.Error("found missing key")
	}
	// Prefix of an existing key is not a match.
	if _, ok := m.Get([]byte("ot"), 100); ok {
		t.Error("prefix matched")
	}
	min, max := m.SeqRange()
	if min != 1 || max != 9 || m.Len() != 4 || m.Tombstones() != 1 {
		t.Errorf("stats: %d %d len=%d tomb=%d", min, max, m.Len(), m.Tombstones())
	}
}

func TestCopiesInput(t *testing.T) {
	m := New()
	k, v := []byte("key"), []byte("val")
	m.Add(1, iterator.KindPut, k, v, 0)
	k[0], v[0] = 'X', 'X'
	e, ok := m.Get([]byte("key"), 1)
	if !ok || string(e.Value) != "val" {
		t.Fatal("memtable aliased caller buffers")
	}
}

func TestIteratorOrderAndSeek(t *testing.T) {
	m := New()
	rng := rand.New(rand.NewSource(7))
	type ver struct {
		k string
		s uint64
	}
	var all []ver
	seq := uint64(0)
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(300))
		seq++
		m.Add(seq, iterator.KindPut, []byte(k), []byte("v"), 0)
		all = append(all, ver{k, seq})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].k != all[j].k {
			return all[i].k < all[j].k
		}
		return all[i].s > all[j].s
	})
	it := m.NewIterator()
	it.SeekToFirst()
	for i, want := range all {
		if !it.Valid() {
			t.Fatalf("ended early at %d/%d", i, len(all))
		}
		e := it.Entry()
		if string(e.Key) != want.k || e.Seq != want.s {
			t.Fatalf("pos %d: got %s/%d want %s/%d", i, e.Key, e.Seq, want.k, want.s)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatal("extra entries")
	}
	// Seek lands on the NEWEST version of the first key >= target.
	it.Seek([]byte("k100"))
	e := it.Entry()
	var want ver
	for _, v := range all {
		if v.k >= "k100" {
			want = v
			break
		}
	}
	if string(e.Key) != want.k || e.Seq != want.s {
		t.Fatalf("seek: got %s/%d want %s/%d", e.Key, e.Seq, want.k, want.s)
	}
	it.Seek([]byte("zzz"))
	if it.Valid() {
		t.Fatal("seek past end must be invalid")
	}
	it.Seek(nil)
	if !it.Valid() || string(it.Entry().Key) != all[0].k {
		t.Fatal("seek(nil) should start at first")
	}
}

// One writer, many lock-free readers, under -race. Readers check structural
// invariants (sortedness) and that every key the writer has already finished
// inserting is found.
func TestConcurrentReadersOneWriter(t *testing.T) {
	m := New()
	const n = 20000
	var done atomic.Int64 // number of fully inserted keys
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				d := done.Load()
				if d > 0 {
					i := rng.Int63n(d)
					k := []byte(fmt.Sprintf("key%08d", i))
					if e, ok := m.Get(k, 1<<62); !ok || string(e.Value) != string(k) {
						t.Errorf("finished key %s not found", k)
						return
					}
				}
				// Short ordered scan.
				it := m.NewIterator()
				it.SeekToFirst()
				var prev []byte
				for c := 0; it.Valid() && c < 200; c++ {
					k := it.Entry().Key
					if prev != nil && string(prev) >= string(k) {
						t.Errorf("iteration out of order: %s then %s", prev, k)
						return
					}
					prev = append(prev[:0], k...)
					it.Next()
				}
			}
		}(r)
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%08d", i))
		m.Add(uint64(i+1), iterator.KindPut, k, k, 0)
		done.Store(int64(i + 1))
	}
	close(stop)
	wg.Wait()
	if m.Len() != n {
		t.Fatalf("len %d", m.Len())
	}
}

func TestSizeAccounting(t *testing.T) {
	m := New()
	before := m.ApproxSize()
	m.Add(1, iterator.KindPut, make([]byte, 100), make([]byte, 1000), 0)
	if d := m.ApproxSize() - before; d < 1100 || d > 1300 {
		t.Fatalf("size delta %d", d)
	}
	if m.Empty() {
		t.Fatal("not empty")
	}
}

func BenchmarkAdd(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%012d", rand.Int63()))
	}
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(uint64(i+1), iterator.KindPut, keys[i], []byte("value-value-value"), 0)
	}
}

func BenchmarkGet(b *testing.B) {
	const n = 100000
	m := New()
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%012d", rand.Int63()))
		m.Add(uint64(i+1), iterator.KindPut, keys[i], []byte("value"), 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(keys[i%n], 1<<62)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	const n = 100000
	m := New()
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%012d", rand.Int63()))
		m.Add(uint64(i+1), iterator.KindPut, keys[i], []byte("value"), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Intn(n)
		for pb.Next() {
			m.Get(keys[i%n], 1<<62)
			i++
		}
	})
}
