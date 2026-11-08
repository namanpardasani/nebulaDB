// Package memtable implements the in-memory sorted write buffer of the LSM
// tree: a skip list ordered by (user key ascending, sequence number descending).
//
// # Skip list in one paragraph
//
// A skip list is a sorted linked list with extra "express lanes". Every node
// has a random height h (P(h>=n) = 4^-(n-1)); it is linked into the lists of
// levels 0..h-1. Searching starts on the sparsest top level and drops down a
// level whenever the next node would overshoot, giving expected O(log n) time
// without any rebalancing — which is what makes a lock-free reader possible.
//
// # Concurrency contract
//
//   - One writer at a time (Add serialises on an internal mutex, and the engine
//     additionally only calls it from the WAL group leader).
//   - Any number of concurrent readers (Get, iterators) with NO locks.
//
// Readers are safe because a node is fully initialised *before* being published
// with an atomic store into its predecessor's next pointer, nodes are never
// modified or removed afterwards (deletes are tombstone versions), and Go's
// atomics give the needed happens-before edges. A reader may or may not see an
// entry inserted concurrently; the engine filters by sequence number so that
// does not matter.
package memtable

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/naman/nebuladb/internal/iterator"
)

const (
	maxHeight = 12
	// nodeOverhead approximates per-node memory beyond key/value bytes (struct,
	// slice header, next pointers) for flush-threshold accounting.
	nodeOverhead = 64
)

type node struct {
	key      []byte
	value    []byte
	expireAt int64
	seq      uint64
	kind     iterator.Kind
	next     []atomic.Pointer[node] // len == height
}

// Memtable is a concurrent-read, single-write sorted map of versions.
type Memtable struct {
	head   *node
	height atomic.Int32

	wmu sync.Mutex // serialises writers; also guards rnd
	rnd uint64

	size    atomic.Int64
	count   atomic.Int64
	deletes atomic.Int64
	minSeq  atomic.Uint64 // 0 until first Add
	maxSeq  atomic.Uint64
}

// New returns an empty memtable.
func New() *Memtable {
	m := &Memtable{
		head: &node{next: make([]atomic.Pointer[node], maxHeight)},
		rnd:  0x9E3779B97F4A7C15,
	}
	m.height.Store(1)
	return m
}

// randomHeight draws a geometric height with p = 1/4 (xorshift64*).
// Caller holds wmu.
func (m *Memtable) randomHeight() int {
	m.rnd ^= m.rnd >> 12
	m.rnd ^= m.rnd << 25
	m.rnd ^= m.rnd >> 27
	r := m.rnd * 0x2545F4914F6CDD1D
	h := 1
	for h < maxHeight && r&3 == 0 {
		h++
		r >>= 2
	}
	return h
}

// before reports whether node n sorts strictly before (key, seq).
func before(n *node, key []byte, seq uint64) bool {
	return iterator.Compare(n.key, n.seq, key, seq) < 0
}

// findGE returns the first node >= (key, seq) or nil. If prev != nil it is
// filled with the predecessor at every level (for insertion).
//
// Concurrency subtlety: the result MUST be the node `nx` that the level-0 loop
// itself observed failing the "before target" test. Re-reading x.next[0] after
// the loop is a bug: a concurrent insert may have linked a new node right after
// x in the meantime, and that node can sort before the target (e.g. a newer
// version of the same key with seq above the snapshot), so returning it would
// hand the reader a version it must not see. (Found by
// TestSnapshotBatchesNeverTorn; see NOTES.md "Bugs found by tests".)
func (m *Memtable) findGE(key []byte, seq uint64, prev *[maxHeight]*node) *node {
	x := m.head
	var nx *node
	for level := int(m.height.Load()) - 1; level >= 0; level-- {
		for {
			nx = x.next[level].Load()
			if nx != nil && before(nx, key, seq) {
				x = nx
				continue
			}
			break
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return nx
}

// Add inserts a version. key and value are copied. (key, seq) must be unique;
// the engine guarantees it because sequence numbers are never reused.
func (m *Memtable) Add(seq uint64, kind iterator.Kind, key, value []byte, expireAt int64) {
	n := &node{
		key:      append([]byte(nil), key...),
		value:    append([]byte(nil), value...),
		seq:      seq,
		kind:     kind,
		expireAt: expireAt,
	}

	m.wmu.Lock()
	defer m.wmu.Unlock()

	var prev [maxHeight]*node
	m.findGE(n.key, seq, &prev)

	h := m.randomHeight()
	cur := int(m.height.Load())
	if h > cur {
		for i := cur; i < h; i++ {
			prev[i] = m.head
		}
		// Publishing the larger height before linking is safe: a reader that
		// sees it just finds head.next[i]==nil on the new levels and drops down.
		m.height.Store(int32(h))
	}
	n.next = make([]atomic.Pointer[node], h)
	for i := 0; i < h; i++ {
		n.next[i].Store(prev[i].next[i].Load())
	}
	// Publish bottom-up: once visible at level 0 the node is a full member of
	// the list; higher levels are only shortcuts.
	for i := 0; i < h; i++ {
		prev[i].next[i].Store(n)
	}

	m.size.Add(int64(len(key) + len(value) + nodeOverhead + 8*h))
	m.count.Add(1)
	if kind == iterator.KindDelete {
		m.deletes.Add(1)
	}
	if m.minSeq.Load() == 0 {
		m.minSeq.Store(seq)
	}
	if seq > m.maxSeq.Load() {
		m.maxSeq.Store(seq)
	}
}

// Get returns the newest version of key with Seq <= snap, which may be a
// tombstone or an expired put; interpreting it is the caller's job (an expired
// or deleted newest version must hide older versions in lower levels).
func (m *Memtable) Get(key []byte, snap uint64) (iterator.Entry, bool) {
	// Target (key, snap) sorts just before the newest version <= snap.
	n := m.findGE(key, snap, nil)
	if n == nil || string(n.key) != string(key) {
		return iterator.Entry{}, false
	}
	return n.entry(), true
}

func (n *node) entry() iterator.Entry {
	return iterator.Entry{Key: n.key, Seq: n.seq, Kind: n.kind, Value: n.value, ExpireAt: n.expireAt}
}

// ApproxSize is the approximate memory footprint in bytes, used to decide when
// to rotate.
func (m *Memtable) ApproxSize() int64 { return m.size.Load() }

// Len is the number of versions stored (puts + tombstones).
func (m *Memtable) Len() int64 { return m.count.Load() }

// Tombstones is the number of delete versions.
func (m *Memtable) Tombstones() int64 { return m.deletes.Load() }

// SeqRange returns the smallest and largest sequence numbers stored
// (0, 0 if empty).
func (m *Memtable) SeqRange() (min, max uint64) { return m.minSeq.Load(), m.maxSeq.Load() }

// Empty reports whether nothing has been added.
func (m *Memtable) Empty() bool { return m.count.Load() == 0 }

// ---------------------------------------------------------------- iterator

// Iterator walks the memtable in internal-key order. It is safe to use while
// writers are active and never needs closing.
type Iterator struct {
	m   *Memtable
	cur *node
}

// NewIterator returns an unpositioned iterator.
func (m *Memtable) NewIterator() *Iterator { return &Iterator{m: m} }

func (it *Iterator) SeekToFirst() { it.cur = it.m.head.next[0].Load() }
func (it *Iterator) Seek(key []byte) {
	it.cur = it.m.findGE(key, math.MaxUint64, nil)
}
func (it *Iterator) Next() {
	if it.cur != nil {
		it.cur = it.cur.next[0].Load()
	}
}
func (it *Iterator) Valid() bool           { return it.cur != nil }
func (it *Iterator) Entry() iterator.Entry { return it.cur.entry() }
func (it *Iterator) Err() error            { return nil }
func (it *Iterator) Close() error          { return nil }

var _ iterator.Iterator = (*Iterator)(nil)
