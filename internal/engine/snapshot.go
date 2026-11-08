package engine

import (
	"sync"

	"github.com/naman/nebuladb/internal/iterator"
)

// Snapshot is a consistent, point-in-time read view of the database.
//
// It fixes (1) a sequence number: only writes with seq <= that number are
// visible, so later writes, deletes and TTL-independent compaction effects never
// change what the snapshot returns; and (2) a pinned table version: the SSTables
// that existed when it was taken stay open and on disk until Close, even if
// flushes and compactions have since replaced them. Together this is the
// guarantee "reads see a consistent view and compaction never deletes a file an
// active iterator still uses".
//
// Close must be called to release the files. Long-lived snapshots delay space
// reclamation (obsolete files cannot be deleted while pinned).
type Snapshot struct {
	d    *DB
	v    view
	once sync.Once
}

// NewSnapshot pins the current state of the database.
func (d *DB) NewSnapshot() (*Snapshot, error) {
	v, err := d.view()
	if err != nil {
		return nil, err
	}
	return &Snapshot{d: d, v: v}, nil
}

// Seq returns the snapshot's sequence number.
func (s *Snapshot) Seq() uint64 { return s.v.snap }

// Get returns the value of key as of the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, bool, error) {
	e, ok, err := s.v.live(key)
	if err != nil || !ok {
		return nil, false, err
	}
	return clone(e.Value), true, nil
}

// Iter walks live keys in ascending order as of the snapshot.
type Iter struct{ v *iterator.View }

// NewIterator returns an iterator over the snapshot. Call Seek or SeekToFirst
// before use. Close it before closing the snapshot.
func (s *Snapshot) NewIterator() *Iter {
	return &Iter{v: iterator.NewView(s.v.newMerged(), s.v.snap, s.v.now)}
}

func (it *Iter) SeekToFirst()  { it.v.SeekToFirst() }
func (it *Iter) Seek(k []byte) { it.v.Seek(k) }
func (it *Iter) Next()         { it.v.Next() }
func (it *Iter) Valid() bool   { return it.v.Valid() }
func (it *Iter) Key() []byte   { return it.v.Key() }
func (it *Iter) Value() []byte { return it.v.Value() }
func (it *Iter) Err() error    { return it.v.Err() }
func (it *Iter) Close() error  { return it.v.Close() }

// Close releases the pinned files. It is safe to call more than once.
func (s *Snapshot) Close() { s.once.Do(s.v.release) }
