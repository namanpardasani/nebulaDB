package engine

import (
	"bytes"
	"sort"
	"sync/atomic"

	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/sstable"
)

// tableFile is one live SSTable plus its reader and a reference count.
//
// # Reference counting
//
// A *version* is an immutable snapshot of "which files are live at which
// level". Every in-flight read or iterator pins the version it started with;
// the DB itself pins the current one. A version holds one reference on each of
// its files. When a flush or compaction installs a new version the superseded
// files are marked obsolete, but they are only closed and deleted when the last
// version (hence the last reader/iterator) that references them is released.
// That is the guarantee "compaction never deletes a file an active iterator
// still uses".
type tableFile struct {
	meta     manifest.FileMeta
	r        *sstable.Reader
	path     string
	db       *DB
	refs     atomic.Int32
	obsolete atomic.Bool
}

func (t *tableFile) ref() { t.refs.Add(1) }

func (t *tableFile) unref() {
	if t.refs.Add(-1) == 0 && t.obsolete.Load() {
		t.r.Close()
		if err := t.db.opts.FS.Remove(t.path); err == nil {
			t.db.deletedFiles.Add(1)
		}
		if t.db.cache != nil {
			t.db.cache.EvictFile(t.meta.Num)
		}
		t.db.mu.Lock()
		delete(t.db.tables, t.meta.Num)
		t.db.mu.Unlock()
		t.db.emit(Event{Type: EventFileDeleted, File: t.meta.Num, Level: t.meta.Level})
	}
}

// version is an immutable set of tables per level. L0 files are ordered by
// file number ascending (so the newest is last); L1+ files are ordered by key
// and do not overlap.
type version struct {
	levels [manifest.NumLevels][]*tableFile
	refs   atomic.Int32
}

func (v *version) ref() { v.refs.Add(1) }

func (v *version) unref() {
	if v.refs.Add(-1) == 0 {
		for _, l := range v.levels {
			for _, f := range l {
				f.unref()
			}
		}
	}
}

// next builds the version that results from removing `del` (by file number,
// any level) and adding `add`. The new version takes its own reference on
// every file it contains. Removed files are marked obsolete; they are deleted
// when no version references them any more.
func (v *version) next(add []*tableFile, del map[uint64]bool) *version {
	nv := &version{}
	for lvl := range v.levels {
		for _, f := range v.levels[lvl] {
			if del[f.meta.Num] {
				f.obsolete.Store(true)
				continue
			}
			nv.levels[lvl] = append(nv.levels[lvl], f)
		}
	}
	for _, f := range add {
		nv.levels[f.meta.Level] = append(nv.levels[f.meta.Level], f)
	}
	sort.Slice(nv.levels[0], func(i, j int) bool { return nv.levels[0][i].meta.Num < nv.levels[0][j].meta.Num })
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		fs := nv.levels[lvl]
		sort.Slice(fs, func(i, j int) bool { return bytes.Compare(fs[i].meta.Smallest, fs[j].meta.Smallest) < 0 })
	}
	for _, l := range nv.levels {
		for _, f := range l {
			f.ref()
		}
	}
	nv.refs.Store(1) // the caller's (DB's) reference
	return nv
}

// get finds the newest version of key with seq <= snap among the tables.
// Search order encodes recency: L0 newest file first (L0 files cover disjoint,
// increasing sequence ranges), then L1, L2, ... where each level has at most
// one candidate file. The first table that contains any visible version wins.
func (v *version) get(key []byte, snap uint64) (iterator.Entry, bool, error) {
	l0 := v.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		f := l0[i]
		if !f.r.MayContainRange(key) {
			continue
		}
		if e, ok, err := f.r.Get(key, snap); err != nil || ok {
			return e, ok, err
		}
	}
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		fs := v.levels[lvl]
		if len(fs) == 0 {
			continue
		}
		// First file whose largest key >= key; it is the only candidate.
		i := sort.Search(len(fs), func(i int) bool { return bytes.Compare(fs[i].meta.Largest, key) >= 0 })
		if i == len(fs) || bytes.Compare(fs[i].meta.Smallest, key) > 0 {
			continue
		}
		if e, ok, err := fs[i].r.Get(key, snap); err != nil || ok {
			return e, ok, err
		}
	}
	return iterator.Entry{}, false, nil
}

// iterators returns one iterator per table (callers merge them).
func (v *version) iterators() []iterator.Iterator {
	var its []iterator.Iterator
	// Newest first so that equal-key ties (which should not occur) favour recency.
	l0 := v.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		its = append(its, l0[i].r.NewIterator())
	}
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		for _, f := range v.levels[lvl] {
			its = append(its, f.r.NewIterator())
		}
	}
	return its
}

func (v *version) numFiles() (n int) {
	for _, l := range v.levels {
		n += len(l)
	}
	return
}
