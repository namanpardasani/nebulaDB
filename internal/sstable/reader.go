package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"sync/atomic"

	"github.com/naman/nebuladb/internal/bloom"
	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/vfs"
)

// BlockCache caches decoded (checksum-verified) data blocks. Implementations
// must treat stored slices as immutable. See internal/cache.
type BlockCache interface {
	Get(fileNum, offset uint64) ([]byte, bool)
	Put(fileNum, offset uint64, block []byte)
}

// Stats are shared counters that readers update; the engine exposes them in
// INFO/metrics. All fields are safe for concurrent use.
type Stats struct {
	BloomChecked       atomic.Uint64 // point lookups that consulted a filter
	BloomSkipped       atomic.Uint64 // filter said "definitely absent": I/O avoided
	BloomFalsePositive atomic.Uint64 // filter said "maybe" but the file had nothing
	BlockReads         atomic.Uint64 // data blocks fetched from disk
	CacheHits          atomic.Uint64
	CacheMisses        atomic.Uint64
}

// ReaderOptions configures Open.
type ReaderOptions struct {
	FileNum uint64
	Cache   BlockCache // optional
	Stats   *Stats     // optional
}

type indexEntry struct {
	lastKey []byte
	lastSeq uint64
	handle  blockHandle
}

// Reader serves point lookups and iteration over one SSTable. It is immutable
// and safe for concurrent use.
type Reader struct {
	f       vfs.File
	size    int64
	fileNum uint64
	path    string
	index   []indexEntry
	filter  []byte
	props   Properties
	cache   BlockCache
	stats   *Stats
}

// Open opens and validates a table: footer magic/checksum, index and
// properties blocks. Data blocks are verified lazily when read.
func Open(fs vfs.FS, path string, opts ReaderOptions) (*Reader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := openReader(f, path, opts)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return r, nil
}

func openReader(f vfs.File, path string, opts ReaderOptions) (*Reader, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	r := &Reader{f: f, size: fi.Size(), fileNum: opts.FileNum, path: path, cache: opts.Cache, stats: opts.Stats}
	if r.size < footerSize {
		return nil, corrupt("file of %d bytes is smaller than a footer", r.size)
	}
	fb := make([]byte, footerSize)
	if _, err := f.ReadAt(fb, r.size-footerSize); err != nil {
		return nil, err
	}
	ft, err := decodeFooter(fb)
	if err != nil {
		return nil, err
	}
	// Index.
	ib, err := r.readBlockRaw(ft.index)
	if err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}
	it, err := newBlockIter(ib)
	if err != nil {
		return nil, fmt.Errorf("index block: %w", err)
	}
	for it.first(); it.valid; it.step() {
		h, err := decodeHandle(it.value)
		if err != nil {
			return nil, err
		}
		if h.offset+h.size+blockTrailer > uint64(r.size-footerSize) {
			return nil, corrupt("index points outside the file")
		}
		r.index = append(r.index, indexEntry{append([]byte(nil), it.key...), it.seq, h})
	}
	if it.err != nil {
		return nil, fmt.Errorf("index block: %w", it.err)
	}
	if len(r.index) == 0 {
		return nil, corrupt("empty index")
	}
	// Properties.
	pb, err := r.readBlockRaw(ft.props)
	if err != nil {
		return nil, fmt.Errorf("properties block: %w", err)
	}
	if r.props, err = decodeProperties(pb); err != nil {
		return nil, err
	}
	// Filter.
	if ft.filter.size > 0 {
		if r.filter, err = r.readBlockRaw(ft.filter); err != nil {
			return nil, fmt.Errorf("filter block: %w", err)
		}
	}
	return r, nil
}

// readBlockRaw reads a block from disk and verifies its checksum.
func (r *Reader) readBlockRaw(h blockHandle) ([]byte, error) {
	end := h.offset + h.size + blockTrailer
	if end < h.offset || end > uint64(r.size) || h.size > 1<<30 {
		return nil, corrupt("block handle (%d,%d) outside file of %d bytes", h.offset, h.size, r.size)
	}
	buf := make([]byte, h.size+blockTrailer)
	if _, err := r.f.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, err
	}
	contents, trailer := buf[:h.size], buf[h.size:]
	if trailer[0] != compressNone {
		return nil, corrupt("unknown compression type %d", trailer[0])
	}
	want := binary.LittleEndian.Uint32(trailer[1:])
	got := crc32.Update(crc32.Checksum(contents, crcTable), crcTable, trailer[:1])
	if got != want {
		return nil, corrupt("block at offset %d: checksum mismatch (stored %08x, computed %08x)", h.offset, want, got)
	}
	return contents, nil
}

// readDataBlock goes through the block cache.
func (r *Reader) readDataBlock(h blockHandle) ([]byte, error) {
	if r.cache != nil {
		if b, ok := r.cache.Get(r.fileNum, h.offset); ok {
			if r.stats != nil {
				r.stats.CacheHits.Add(1)
			}
			return b, nil
		}
		if r.stats != nil {
			r.stats.CacheMisses.Add(1)
		}
	}
	b, err := r.readBlockRaw(h)
	if err != nil {
		return nil, err
	}
	if r.stats != nil {
		r.stats.BlockReads.Add(1)
	}
	if r.cache != nil {
		r.cache.Put(r.fileNum, h.offset, b)
	}
	return b, nil
}

// Props returns the table's properties.
func (r *Reader) Props() Properties { return r.props }

// Size returns the file size in bytes.
func (r *Reader) Size() int64 { return r.size }

// FileNum returns the file number given at open.
func (r *Reader) FileNum() uint64 { return r.fileNum }

// Close closes the file.
func (r *Reader) Close() error { return r.f.Close() }

// findBlock returns the index of the first block whose last internal key is
// >= (key, seq), or len(index) if none.
func (r *Reader) findBlock(key []byte, seq uint64) int {
	return sort.Search(len(r.index), func(i int) bool {
		e := &r.index[i]
		return iterator.Compare(e.lastKey, e.lastSeq, key, seq) >= 0
	})
}

// Get returns the newest version of key with Seq <= snap stored in this table
// (possibly a tombstone or expired put). found is false if the table has no
// such version. The returned entry owns its memory.
func (r *Reader) Get(key []byte, snap uint64) (iterator.Entry, bool, error) {
	if r.filter != nil {
		if r.stats != nil {
			r.stats.BloomChecked.Add(1)
		}
		if !bloom.MayContain(r.filter, key) {
			if r.stats != nil {
				r.stats.BloomSkipped.Add(1)
			}
			return iterator.Entry{}, false, nil
		}
	}
	miss := func() (iterator.Entry, bool, error) {
		if r.filter != nil && r.stats != nil {
			r.stats.BloomFalsePositive.Add(1)
		}
		return iterator.Entry{}, false, nil
	}
	i := r.findBlock(key, snap)
	if i == len(r.index) {
		return miss()
	}
	blk, err := r.readDataBlock(r.index[i].handle)
	if err != nil {
		return iterator.Entry{}, false, err
	}
	bi, err := newBlockIter(blk)
	if err != nil {
		return iterator.Entry{}, false, err
	}
	bi.seek(key, snap)
	if bi.err != nil {
		return iterator.Entry{}, false, bi.err
	}
	if !bi.valid || !bytes.Equal(bi.key, key) {
		return miss()
	}
	e := bi.entry()
	e.Key = append([]byte(nil), e.Key...)
	return e, true, nil
}

// MayContainRange reports whether key lies within [smallest, largest].
func (r *Reader) MayContainRange(key []byte) bool {
	return bytes.Compare(key, r.props.Smallest) >= 0 && bytes.Compare(key, r.props.Largest) <= 0
}

// Verify reads every block, checks every checksum, fully decodes every entry,
// and cross-checks ordering and the properties. Used by `nebula verify`.
func (r *Reader) Verify() error {
	var n, deletes uint64
	var prevKey []byte
	var prevSeq uint64
	for bi, ie := range r.index {
		blk, err := r.readBlockRaw(ie.handle)
		if err != nil {
			return fmt.Errorf("block %d: %w", bi, err)
		}
		it, err := newBlockIter(blk)
		if err != nil {
			return fmt.Errorf("block %d: %w", bi, err)
		}
		var last iterator.Entry
		for it.first(); it.valid; it.step() {
			e := it.entry()
			if n > 0 && iterator.Compare(prevKey, prevSeq, e.Key, e.Seq) >= 0 {
				return corrupt("block %d: entries out of order", bi)
			}
			prevKey, prevSeq = append(prevKey[:0], e.Key...), e.Seq
			last = e
			n++
			if e.Kind == iterator.KindDelete {
				deletes++
			}
		}
		if it.err != nil {
			return fmt.Errorf("block %d: %w", bi, it.err)
		}
		if !bytes.Equal(last.Key, ie.lastKey) || last.Seq != ie.lastSeq {
			return corrupt("block %d: index key does not match last entry", bi)
		}
	}
	if n != r.props.NumEntries || deletes != r.props.NumDeletes {
		return corrupt("entry counts (%d, %d deletes) disagree with properties (%d, %d)", n, deletes, r.props.NumEntries, r.props.NumDeletes)
	}
	return nil
}

// ---------------------------------------------------------------- iterator

// Iter iterates a table in internal-key order, reading one block at a time.
type Iter struct {
	r     *Reader
	idx   int // current block index
	blk   *blockIter
	err   error
	valid bool
}

// NewIterator returns an unpositioned iterator over r.
func (r *Reader) NewIterator() *Iter { return &Iter{r: r} }

func (it *Iter) loadBlock() bool {
	if it.idx >= len(it.r.index) {
		it.valid, it.blk = false, nil
		return false
	}
	b, err := it.r.readDataBlock(it.r.index[it.idx].handle)
	if err == nil {
		it.blk, err = newBlockIter(b)
	}
	if err != nil {
		it.err, it.valid = err, false
		return false
	}
	return true
}

// settle moves to the next block while the current one is exhausted.
func (it *Iter) settle() {
	for {
		if it.blk != nil && it.blk.err != nil {
			it.err, it.valid = it.blk.err, false
			return
		}
		if it.blk != nil && it.blk.valid {
			it.valid = true
			return
		}
		it.idx++
		if !it.loadBlock() {
			return
		}
		it.blk.first()
	}
}

func (it *Iter) SeekToFirst() {
	it.err, it.idx = nil, 0
	if !it.loadBlock() {
		return
	}
	it.blk.first()
	it.settle()
}

func (it *Iter) Seek(key []byte) {
	it.err = nil
	it.idx = it.r.findBlock(key, math.MaxUint64)
	if !it.loadBlock() {
		return
	}
	it.blk.seek(key, math.MaxUint64)
	it.settle()
}

func (it *Iter) Next() {
	if !it.valid {
		return
	}
	it.blk.step()
	it.settle()
}

func (it *Iter) Valid() bool           { return it.valid && it.err == nil }
func (it *Iter) Entry() iterator.Entry { return it.blk.entry() }
func (it *Iter) Err() error            { return it.err }
func (it *Iter) Close() error          { return nil }

var _ iterator.Iterator = (*Iter)(nil)
