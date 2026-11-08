// Package manifest records which SSTable files make up the database.
//
// # Why a MANIFEST
//
// Listing the directory is not enough to know the live files: after a crash
// there may be half-written outputs, files already replaced by a compaction but
// not yet deleted, and so on. The MANIFEST is the single source of truth. Every
// change to the file set (a flush adds a file; a compaction adds some and
// removes others) is one *version edit* appended to the MANIFEST log and
// fsynced; only after that is the change real. Startup replays the log to
// rebuild the exact set of live files, and deletes everything else.
//
// An edit is atomic: it is one checksummed record, so after a crash either the
// whole edit is applied or none of it. That is what makes "add the compaction
// output AND remove its inputs" crash-safe.
//
// # Files
//
//	CURRENT            one line: the name of the live manifest, e.g. "MANIFEST-000007"
//	MANIFEST-NNNNNN    log of edits: len u32 | crc32c u32 | payload
//
// When the log grows past a threshold it is *rewritten* as a single snapshot
// edit into a new MANIFEST file, which becomes live by atomically replacing
// CURRENT (write temp, fsync, rename, fsync directory). A crash at any point
// leaves either the old or the new manifest fully valid.
package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// NumLevels is the number of LSM levels (L0 .. L6).
const NumLevels = 7

// FileMeta describes one live SSTable.
type FileMeta struct {
	Num      uint64
	Level    int
	Size     int64
	Entries  uint64
	Deletes  uint64
	MinSeq   uint64
	MaxSeq   uint64
	Smallest []byte // smallest user key
	Largest  []byte // largest user key
}

// DeletedFile identifies a file removed by an edit.
type DeletedFile struct {
	Level int
	Num   uint64
}

// Edit is one atomic change to the database's file set and counters.
type Edit struct {
	HasLogNumber bool
	// LogNumber: WAL segments numbered below it are fully covered by SSTables
	// and may be deleted.
	LogNumber uint64

	HasNextFileNum bool
	NextFileNum    uint64

	HasLastSeq bool
	LastSeq    uint64 // highest sequence number present in SSTables

	Added   []FileMeta
	Deleted []DeletedFile
}

// Edit field tags.
const (
	tagLogNumber   = 1
	tagNextFileNum = 2
	tagLastSeq     = 3
	tagAddFile     = 4
	tagDeleteFile  = 5
)

// Encode serialises the edit.
func (e *Edit) Encode() []byte {
	var b []byte
	put := func(v uint64) { b = binary.AppendUvarint(b, v) }
	if e.HasLogNumber {
		put(tagLogNumber)
		put(e.LogNumber)
	}
	if e.HasNextFileNum {
		put(tagNextFileNum)
		put(e.NextFileNum)
	}
	if e.HasLastSeq {
		put(tagLastSeq)
		put(e.LastSeq)
	}
	for _, f := range e.Added {
		put(tagAddFile)
		put(uint64(f.Level))
		put(f.Num)
		put(uint64(f.Size))
		put(f.Entries)
		put(f.Deletes)
		put(f.MinSeq)
		put(f.MaxSeq)
		put(uint64(len(f.Smallest)))
		b = append(b, f.Smallest...)
		put(uint64(len(f.Largest)))
		b = append(b, f.Largest...)
	}
	for _, d := range e.Deleted {
		put(tagDeleteFile)
		put(uint64(d.Level))
		put(d.Num)
	}
	return b
}

// DecodeEdit parses an encoded edit strictly: unknown tags, truncation and
// out-of-range levels are errors.
func DecodeEdit(b []byte) (Edit, error) {
	var e Edit
	bad := func(what string) (Edit, error) { return Edit{}, fmt.Errorf("manifest: bad edit: %s", what) }
	next := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	bytesField := func() ([]byte, bool) {
		l, ok := next()
		if !ok || l > uint64(len(b)) {
			return nil, false
		}
		out := append(make([]byte, 0, l), b[:l]...) // non-nil even when empty
		b = b[l:]
		return out, true
	}
	for len(b) > 0 {
		tag, _ := next()
		switch tag {
		case tagLogNumber:
			v, ok := next()
			if !ok {
				return bad("truncated log number")
			}
			e.HasLogNumber, e.LogNumber = true, v
		case tagNextFileNum:
			v, ok := next()
			if !ok {
				return bad("truncated next file number")
			}
			e.HasNextFileNum, e.NextFileNum = true, v
		case tagLastSeq:
			v, ok := next()
			if !ok {
				return bad("truncated last seq")
			}
			e.HasLastSeq, e.LastSeq = true, v
		case tagAddFile:
			var f FileMeta
			var lvl, size uint64
			ok := true
			for _, dst := range []*uint64{&lvl, &f.Num, &size, &f.Entries, &f.Deletes, &f.MinSeq, &f.MaxSeq} {
				var o bool
				*dst, o = next()
				ok = ok && o
			}
			if !ok || lvl >= NumLevels {
				return bad("bad add-file record")
			}
			f.Level, f.Size = int(lvl), int64(size)
			var o1, o2 bool
			f.Smallest, o1 = bytesField()
			f.Largest, o2 = bytesField()
			if !o1 || !o2 {
				return bad("truncated file key range")
			}
			e.Added = append(e.Added, f)
		case tagDeleteFile:
			lvl, ok1 := next()
			num, ok2 := next()
			if !ok1 || !ok2 || lvl >= NumLevels {
				return bad("bad delete-file record")
			}
			e.Deleted = append(e.Deleted, DeletedFile{int(lvl), num})
		default:
			return bad(fmt.Sprintf("unknown tag %d", tag))
		}
	}
	return e, nil
}

// Version is the database's file set at a point in time plus counters.
type Version struct {
	// Levels[0] is ordered by file number ascending (oldest first); deeper
	// levels are ordered by smallest key and never overlap.
	Levels      [NumLevels][]FileMeta
	LogNumber   uint64
	NextFileNum uint64
	LastSeq     uint64
}

// Clone deep-copies the file lists (metadata slices are treated as immutable).
func (v *Version) Clone() *Version {
	c := *v
	for i := range v.Levels {
		c.Levels[i] = append([]FileMeta(nil), v.Levels[i]...)
	}
	return &c
}

// NumFiles returns the total number of live files.
func (v *Version) NumFiles() int {
	n := 0
	for _, l := range v.Levels {
		n += len(l)
	}
	return n
}

// Apply validates and applies an edit. On error v may be partially modified;
// callers apply to a Clone.
func (v *Version) Apply(e *Edit) error {
	for _, d := range e.Deleted {
		files := v.Levels[d.Level]
		i := -1
		for j := range files {
			if files[j].Num == d.Num {
				i = j
				break
			}
		}
		if i < 0 {
			return fmt.Errorf("manifest: edit deletes file %d which is not in level %d", d.Num, d.Level)
		}
		v.Levels[d.Level] = append(files[:i:i], files[i+1:]...)
	}
	for _, f := range e.Added {
		for l := range v.Levels {
			for _, x := range v.Levels[l] {
				if x.Num == f.Num {
					return fmt.Errorf("manifest: edit adds file %d which already exists in level %d", f.Num, l)
				}
			}
		}
		v.Levels[f.Level] = append(v.Levels[f.Level], f)
		if f.Num >= v.NextFileNum {
			v.NextFileNum = f.Num + 1 // never hand out a number that is in use
		}
	}
	// Re-establish ordering and check the non-overlap invariant.
	sort.Slice(v.Levels[0], func(i, j int) bool { return v.Levels[0][i].Num < v.Levels[0][j].Num })
	for l := 1; l < NumLevels; l++ {
		fs := v.Levels[l]
		sort.Slice(fs, func(i, j int) bool { return bytes.Compare(fs[i].Smallest, fs[j].Smallest) < 0 })
		for i := 1; i < len(fs); i++ {
			if bytes.Compare(fs[i-1].Largest, fs[i].Smallest) >= 0 {
				return fmt.Errorf("manifest: level %d files %d and %d overlap", l, fs[i-1].Num, fs[i].Num)
			}
		}
	}
	if e.HasLogNumber {
		if e.LogNumber < v.LogNumber {
			return fmt.Errorf("manifest: log number moved backwards (%d -> %d)", v.LogNumber, e.LogNumber)
		}
		v.LogNumber = e.LogNumber
	}
	if e.HasNextFileNum && e.NextFileNum > v.NextFileNum {
		v.NextFileNum = e.NextFileNum
	}
	if e.HasLastSeq && e.LastSeq > v.LastSeq {
		v.LastSeq = e.LastSeq
	}
	return nil
}

// Snapshot returns an edit that, applied to an empty Version, reproduces v.
func (v *Version) Snapshot() *Edit {
	e := &Edit{
		HasLogNumber: true, LogNumber: v.LogNumber,
		HasNextFileNum: true, NextFileNum: v.NextFileNum,
		HasLastSeq: true, LastSeq: v.LastSeq,
	}
	for _, l := range v.Levels {
		e.Added = append(e.Added, l...)
	}
	return e
}
