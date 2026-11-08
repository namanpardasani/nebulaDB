// Package iterator defines the internal-key entry type and the iterator
// interface shared by memtables, SSTables and the merging machinery.
//
// # Internal keys
//
// Every write gets a globally increasing sequence number. The LSM tree keeps
// *all versions* of a user key until compaction discards the old ones, ordered
//
//	user key ascending, then sequence number DESCENDING
//
// so the newest version of a key is encountered first. A delete is just another
// version with Kind == KindDelete (a tombstone) that shadows older versions.
// A put may carry an absolute expiry time; an expired put also shadows older
// versions (the key was overwritten, then timed out; it must not resurrect).
package iterator

import (
	"bytes"
	"container/heap"
)

// Kind is the type of a version.
type Kind uint8

const (
	KindPut    Kind = 1
	KindDelete Kind = 2 // tombstone
)

// Entry is one version of a key. Slices alias iterator-owned memory and are
// only valid until the iterator is moved (unless documented otherwise).
type Entry struct {
	Key      []byte
	Seq      uint64
	Kind     Kind
	Value    []byte
	ExpireAt int64 // unix ms; 0 = never
}

// Expired reports whether the entry is a put whose expiry has passed at now.
func (e *Entry) Expired(now int64) bool {
	return e.Kind == KindPut && e.ExpireAt != 0 && e.ExpireAt <= now
}

// Compare orders entries by user key ascending, then seq descending.
func Compare(aKey []byte, aSeq uint64, bKey []byte, bSeq uint64) int {
	if c := bytes.Compare(aKey, bKey); c != 0 {
		return c
	}
	switch {
	case aSeq > bSeq:
		return -1
	case aSeq < bSeq:
		return 1
	}
	return 0
}

// Iterator walks entries in internal-key order.
type Iterator interface {
	// SeekToFirst positions at the first entry.
	SeekToFirst()
	// Seek positions at the first entry whose user key is >= key (that is, the
	// newest version of the first such key).
	Seek(key []byte)
	// Next advances. Only valid when Valid().
	Next()
	// Valid reports whether the iterator is positioned at an entry.
	Valid() bool
	// Entry returns the current entry. Only valid when Valid().
	Entry() Entry
	// Err returns a sticky error (corruption, I/O); iteration stops on error.
	Err() error
	// Close releases resources (file references, cache pins).
	Close() error
}

// ---------------------------------------------------------------- Merge

// Merge is a k-way merge of child iterators, each already in internal-key
// order, using a binary min-heap. Cost per Next is O(log k).
//
// If two children yield the same (key, seq) — which a correct engine never
// produces — the child with the lower index wins and the other is skipped by
// the caller's dedupe, so put newer sources first.
type Merge struct {
	children []Iterator
	h        mergeHeap
	err      error
}

// NewMerge merges children. It takes ownership: Close closes them all.
func NewMerge(children []Iterator) *Merge {
	m := &Merge{children: children}
	m.h.m = m
	return m
}

type mergeHeap struct {
	m   *Merge
	idx []int // indices into m.children of valid children
}

func (h *mergeHeap) Len() int { return len(h.idx) }
func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.m.children[h.idx[i]].Entry(), h.m.children[h.idx[j]].Entry()
	if c := Compare(a.Key, a.Seq, b.Key, b.Seq); c != 0 {
		return c < 0
	}
	return h.idx[i] < h.idx[j]
}
func (h *mergeHeap) Swap(i, j int) { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *mergeHeap) Push(x any)    { h.idx = append(h.idx, x.(int)) }
func (h *mergeHeap) Pop() any {
	n := len(h.idx)
	x := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return x
}

func (m *Merge) rebuild() {
	m.h.idx = m.h.idx[:0]
	for i, c := range m.children {
		if err := c.Err(); err != nil && m.err == nil {
			m.err = err
		}
		if c.Valid() {
			m.h.idx = append(m.h.idx, i)
		}
	}
	heap.Init(&m.h)
}

func (m *Merge) SeekToFirst() {
	for _, c := range m.children {
		c.SeekToFirst()
	}
	m.rebuild()
}

func (m *Merge) Seek(key []byte) {
	for _, c := range m.children {
		c.Seek(key)
	}
	m.rebuild()
}

func (m *Merge) Valid() bool { return m.err == nil && len(m.h.idx) > 0 }

func (m *Merge) Entry() Entry { return m.children[m.h.idx[0]].Entry() }

func (m *Merge) Next() {
	top := m.children[m.h.idx[0]]
	top.Next()
	if err := top.Err(); err != nil && m.err == nil {
		m.err = err
	}
	if top.Valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

func (m *Merge) Err() error { return m.err }

func (m *Merge) Close() error {
	var first error
	for _, c := range m.children {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ---------------------------------------------------------------- View

// View turns a version-level iterator into the user-level view at a snapshot:
// one entry per live user key, hiding versions newer than the snapshot,
// shadowed older versions, tombstones and expired keys. This is what GET-style
// range reads (SCAN) and compaction-free reads use.
//
// The key and value it exposes are copies owned by the View and stay valid
// until the next call that moves it.
type View struct {
	src  Iterator
	snap uint64
	now  int64

	valid bool
	key   []byte
	value []byte
	exp   int64
	seq   uint64
	err   error
}

// NewView wraps src. snap is the highest visible sequence number; now is the
// clock (unix ms) used to hide expired keys. View takes ownership of src.
func NewView(src Iterator, snap uint64, now int64) *View {
	return &View{src: src, snap: snap, now: now}
}

func (v *View) SeekToFirst() { v.src.SeekToFirst(); v.settle() }
func (v *View) Seek(key []byte) {
	v.src.Seek(key)
	v.settle()
}
func (v *View) Next() { v.settle() } // src already sits past the current key

func (v *View) Valid() bool     { return v.valid && v.err == nil }
func (v *View) Key() []byte     { return v.key }
func (v *View) Value() []byte   { return v.value }
func (v *View) ExpireAt() int64 { return v.exp }
func (v *View) Seq() uint64     { return v.seq }
func (v *View) Err() error      { return v.err }
func (v *View) Close() error    { return v.src.Close() }

// settle advances src to the next live user key and copies it into the view,
// leaving src positioned at the first entry AFTER that key's versions.
func (v *View) settle() {
	v.valid = false
	for v.src.Valid() {
		e := v.src.Entry()
		if e.Seq > v.snap {
			v.src.Next() // version written after the snapshot: invisible
			continue
		}
		// First visible version == newest visible version of this key.
		dead := e.Kind == KindDelete || e.Expired(v.now)
		if !dead {
			v.key = append(v.key[:0], e.Key...)
			v.value = append(v.value[:0], e.Value...)
			v.exp, v.seq = e.ExpireAt, e.Seq
		}
		// Skip every remaining version of this key (older ones are shadowed).
		// Compare against a stable copy: e.Key may move with src.
		k := e.Key
		if dead {
			k = append([]byte(nil), e.Key...)
		} else {
			k = v.key
		}
		for v.src.Valid() {
			if !bytes.Equal(v.src.Entry().Key, k) {
				break
			}
			v.src.Next()
		}
		if !dead {
			v.valid = true
			break
		}
	}
	if err := v.src.Err(); err != nil {
		v.err = err
		v.valid = false
	}
}
