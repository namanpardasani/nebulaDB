package iterator

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// sliceIter is a trivial Iterator over a pre-sorted slice (test double).
type sliceIter struct {
	es  []Entry
	pos int
	err error
}

func (s *sliceIter) SeekToFirst() { s.pos = 0 }
func (s *sliceIter) Seek(key []byte) {
	s.pos = sort.Search(len(s.es), func(i int) bool { return string(s.es[i].Key) >= string(key) })
}
func (s *sliceIter) Next()        { s.pos++ }
func (s *sliceIter) Valid() bool  { return s.err == nil && s.pos < len(s.es) }
func (s *sliceIter) Entry() Entry { return s.es[s.pos] }
func (s *sliceIter) Err() error   { return s.err }
func (s *sliceIter) Close() error { return nil }

func sortEntries(es []Entry) {
	sort.Slice(es, func(i, j int) bool { return Compare(es[i].Key, es[i].Seq, es[j].Key, es[j].Seq) < 0 })
}

func put(k string, seq uint64, v string) Entry {
	return Entry{Key: []byte(k), Seq: seq, Kind: KindPut, Value: []byte(v)}
}
func del(k string, seq uint64) Entry { return Entry{Key: []byte(k), Seq: seq, Kind: KindDelete} }

func TestMergeOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	var all []Entry
	var children []Iterator
	seq := uint64(0)
	for c := 0; c < 7; c++ {
		var es []Entry
		for i := 0; i < 200; i++ {
			seq++
			es = append(es, put(fmt.Sprintf("k%03d", rng.Intn(150)), seq, "v"))
		}
		all = append(all, es...)
		sortEntries(es)
		children = append(children, &sliceIter{es: es})
	}
	sortEntries(all)
	m := NewMerge(children)
	m.SeekToFirst()
	for i, want := range all {
		if !m.Valid() {
			t.Fatalf("ended at %d/%d", i, len(all))
		}
		e := m.Entry()
		if string(e.Key) != string(want.Key) || e.Seq != want.Seq {
			t.Fatalf("pos %d: got %s/%d want %s/%d", i, e.Key, e.Seq, want.Key, want.Seq)
		}
		m.Next()
	}
	if m.Valid() {
		t.Fatal("extra")
	}
	m.Seek([]byte("k100"))
	if !m.Valid() || string(m.Entry().Key) < "k100" {
		t.Fatal("seek")
	}
}

func TestMergePropagatesError(t *testing.T) {
	bad := &sliceIter{es: []Entry{put("a", 1, "x"), put("b", 2, "x")}}
	m := NewMerge([]Iterator{bad, &sliceIter{es: []Entry{put("c", 3, "x")}}})
	m.SeekToFirst()
	bad.err = fmt.Errorf("disk on fire")
	m.Next()
	if m.Valid() || m.Err() == nil {
		t.Fatal("merge must surface child errors and stop")
	}
}

func TestViewSemantics(t *testing.T) {
	es := []Entry{
		put("a", 10, "a10"), put("a", 4, "a4"), // newest wins
		del("b", 8), put("b", 3, "b3"), // tombstone hides older put
		put("c", 2, "c2"),
		{Key: []byte("d"), Seq: 7, Kind: KindPut, Value: []byte("d7"), ExpireAt: 500}, // expired at now=1000...
		put("d", 1, "d1"),                      // ...and must NOT resurrect d1
		put("e", 20, "e20"), put("e", 6, "e6"), // snapshot sees e6
		put("f", 30, "f30"), // invisible at snap 10
	}
	sortEntries(es)
	collect := func(snap uint64, now int64) string {
		v := NewView(&sliceIter{es: es}, snap, now)
		v.SeekToFirst()
		out := ""
		for ; v.Valid(); v.Next() {
			out += fmt.Sprintf("%s=%s ", v.Key(), v.Value())
		}
		if v.Err() != nil {
			t.Fatal(v.Err())
		}
		return out
	}
	if got := collect(10, 1000); got != "a=a10 c=c2 e=e6 " {
		t.Errorf("snap10 now1000: %q", got)
	}
	if got := collect(10, 100); got != "a=a10 c=c2 d=d7 e=e6 " { // d not yet expired
		t.Errorf("snap10 now100: %q", got)
	}
	if got := collect(100, 1000); got != "a=a10 c=c2 e=e20 f=f30 " {
		t.Errorf("snap100: %q", got)
	}
	if got := collect(5, 100); got != "a=a4 b=b3 c=c2 d=d1 " { // before the tombstone and before d7
		t.Errorf("snap5: %q", got)
	}
	v := NewView(&sliceIter{es: es}, 10, 1000)
	v.Seek([]byte("b"))
	if !v.Valid() || string(v.Key()) != "c" {
		t.Errorf("seek over deleted key landed on %q", v.Key())
	}
}
