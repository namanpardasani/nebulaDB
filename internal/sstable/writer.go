package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/naman/nebuladb/internal/bloom"
	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/vfs"
)

// WriterOptions configures a Writer.
type WriterOptions struct {
	BlockSize       int // target data block size in bytes (default 4096)
	RestartInterval int // entries between full keys (default 16)
	BloomBitsPerKey int // 0 disables the filter block
}

func (o *WriterOptions) fill() {
	if o.BlockSize <= 0 {
		o.BlockSize = defaultBlock
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = restartDefault
	}
}

// Writer builds an SSTable. Entries must be added in strictly increasing
// internal-key order (user key ascending, seq descending). It writes straight
// to the file it was given; crash-safe publication (temp name, fsync, rename,
// directory fsync) is the caller's job — see engine flush/compaction.
type Writer struct {
	f    vfs.File
	opts WriterOptions

	data   *blockBuilder
	index  *blockBuilder
	filter bloom.Builder
	off    uint64 // bytes written so far

	props    Properties
	lastKey  []byte
	lastSeq  uint64
	haveLast bool
	err      error
	finished bool
}

// NewWriter starts a table writing to f.
func NewWriter(f vfs.File, opts WriterOptions) *Writer {
	opts.fill()
	return &Writer{
		f: f, opts: opts,
		data:  newBlockBuilder(opts.RestartInterval),
		index: newBlockBuilder(1), // index entries are tiny; every one a restart point
	}
}

// Add appends an entry.
func (w *Writer) Add(e iterator.Entry) error {
	if w.err != nil {
		return w.err
	}
	if w.haveLast && iterator.Compare(w.lastKey, w.lastSeq, e.Key, e.Seq) >= 0 {
		w.err = errors.New("sstable: entries added out of order")
		return w.err
	}
	newUserKey := !w.haveLast || !bytes.Equal(w.lastKey, e.Key)
	if newUserKey && w.opts.BloomBitsPerKey > 0 {
		w.filter.Add(e.Key)
	}
	if w.props.NumEntries == 0 {
		w.props.Smallest = append([]byte(nil), e.Key...)
		w.props.MinSeq, w.props.MaxSeq = e.Seq, e.Seq
	}
	if e.Seq < w.props.MinSeq {
		w.props.MinSeq = e.Seq
	}
	if e.Seq > w.props.MaxSeq {
		w.props.MaxSeq = e.Seq
	}
	w.props.NumEntries++
	if e.Kind == iterator.KindDelete {
		w.props.NumDeletes++
	}

	w.data.add(e.Key, e.Seq, e.Kind, e.ExpireAt, e.Value)
	w.lastKey = append(w.lastKey[:0], e.Key...)
	w.lastSeq, w.haveLast = e.Seq, true

	if w.data.estimatedSize() >= w.opts.BlockSize {
		return w.flushDataBlock()
	}
	return nil
}

// writeBlock writes contents + trailer and returns its handle.
func (w *Writer) writeBlock(contents []byte) (blockHandle, error) {
	h := blockHandle{offset: w.off, size: uint64(len(contents))}
	var trailer [blockTrailer]byte
	trailer[0] = compressNone
	crc := crc32.Update(crc32.Checksum(contents, crcTable), crcTable, trailer[:1])
	binary.LittleEndian.PutUint32(trailer[1:], crc)
	if _, err := w.f.Write(contents); err != nil {
		return h, err
	}
	if _, err := w.f.Write(trailer[:]); err != nil {
		return h, err
	}
	w.off += uint64(len(contents)) + blockTrailer
	return h, nil
}

// flushDataBlock writes the current data block and its index entry. The index
// key is the block's last internal key, which is exactly lastKey/lastSeq.
func (w *Writer) flushDataBlock() error {
	if w.data.empty() {
		return nil
	}
	h, err := w.writeBlock(w.data.finish())
	if err != nil {
		w.err = err
		return err
	}
	w.index.add(w.lastKey, w.lastSeq, iterator.KindPut, 0, h.encode(nil))
	w.props.NumBlocks++
	w.data.reset()
	return nil
}

// EstimatedSize is the number of bytes written so far plus the pending block;
// compaction uses it to split outputs into target-sized files.
func (w *Writer) EstimatedSize() uint64 { return w.off + uint64(w.data.estimatedSize()) }

// NumEntries returns how many entries were added.
func (w *Writer) NumEntries() uint64 { return w.props.NumEntries }

// Finish writes the filter, index, properties and footer, then fsyncs the
// file. It does not close it. The returned properties describe the table.
func (w *Writer) Finish() (Properties, error) {
	if w.err != nil {
		return Properties{}, w.err
	}
	if w.finished {
		return Properties{}, errors.New("sstable: Finish called twice")
	}
	w.finished = true
	if w.props.NumEntries == 0 {
		w.err = errors.New("sstable: cannot finish an empty table")
		return Properties{}, w.err
	}
	if err := w.flushDataBlock(); err != nil {
		return Properties{}, err
	}
	w.props.DataSize = w.off
	w.props.Largest = append([]byte(nil), w.lastKey...)

	var ft footer
	if f := w.filter.Build(w.opts.BloomBitsPerKey); f != nil {
		h, err := w.writeBlock(f)
		if err != nil {
			w.err = err
			return Properties{}, err
		}
		ft.filter = h
		w.props.FilterSize = uint64(len(f))
	}
	var err error
	if ft.index, err = w.writeBlock(w.index.finish()); err != nil {
		w.err = err
		return Properties{}, err
	}
	if ft.props, err = w.writeBlock(w.props.encode()); err != nil {
		w.err = err
		return Properties{}, err
	}
	if _, err := w.f.Write(ft.encode()); err != nil {
		w.err = err
		return Properties{}, err
	}
	w.off += footerSize
	// The table must be durable before anything (the MANIFEST) refers to it.
	if err := w.f.Sync(); err != nil {
		w.err = err
		return Properties{}, err
	}
	return w.props, nil
}
