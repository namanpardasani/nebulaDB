package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/naman/nebuladb/internal/iterator"
)

// ErrCorrupt is wrapped by every error caused by malformed or damaged data.
var ErrCorrupt = errors.New("sstable: corrupt")

func corrupt(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, a...))
}

// # Block layout
//
//	entry*  restart_offset u32 * numRestarts  numRestarts u32
//
//	entry = shared uv | unshared uv | valueLen uv | kind u8 | seq uv | expireAt uv
//	        | key[shared:] (unshared bytes) | value (valueLen bytes)
//
// Keys are prefix-compressed against the previous key in the block (`shared`
// leading bytes are reused). Every restartInterval-th entry stores its full key
// (shared == 0) and its offset goes in the restart array; that lets a seek
// binary-search the restart points and then scan at most restartInterval
// entries. uv = unsigned varint.
//
// Blocks hold *internal* keys: the same user key may appear several times with
// descending seq, and prefix compression squeezes those repeats to almost
// nothing.

// blockBuilder builds one block.
type blockBuilder struct {
	buf             []byte
	restarts        []uint32
	sinceRestart    int
	restartInterval int
	lastKey         []byte
	n               int
}

func newBlockBuilder(restartInterval int) *blockBuilder {
	return &blockBuilder{restartInterval: restartInterval, restarts: []uint32{0}}
}

func (b *blockBuilder) empty() bool { return b.n == 0 }

// estimatedSize is the size the block would have if finished now.
func (b *blockBuilder) estimatedSize() int { return len(b.buf) + 4*len(b.restarts) + 4 }

func (b *blockBuilder) add(key []byte, seq uint64, kind iterator.Kind, expireAt int64, value []byte) {
	shared := 0
	if b.sinceRestart >= b.restartInterval {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.sinceRestart = 0
	} else {
		for shared < len(key) && shared < len(b.lastKey) && key[shared] == b.lastKey[shared] {
			shared++
		}
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, byte(kind))
	b.buf = binary.AppendUvarint(b.buf, seq)
	b.buf = binary.AppendUvarint(b.buf, uint64(expireAt))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.sinceRestart++
	b.n++
}

// finish appends the restart array and returns the finished block contents.
func (b *blockBuilder) finish() []byte {
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
	return b.buf
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = append(b.restarts[:0], 0)
	b.sinceRestart = 0
	b.lastKey = b.lastKey[:0]
	b.n = 0
}

// blockIter iterates the entries of one decoded block. All slices it exposes
// alias either the block bytes or its own key buffer and are valid until the
// next move.
type blockIter struct {
	data     []byte // entries region
	restarts []byte // restart array (4 bytes each)
	nrst     int
	next_    int // offset of the next entry to decode

	valid bool
	key   []byte
	seq   uint64
	kind  iterator.Kind
	exp   int64
	value []byte
	err   error
}

func newBlockIter(block []byte) (*blockIter, error) {
	if len(block) < 4 {
		return nil, corrupt("block shorter than its trailer")
	}
	n := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if n < 1 || n > (len(block)-4)/4 {
		return nil, corrupt("bad restart count %d for block of %d bytes", n, len(block))
	}
	rstart := len(block) - 4 - 4*n
	it := &blockIter{data: block[:rstart], restarts: block[rstart : len(block)-4], nrst: n}
	return it, nil
}

func (it *blockIter) restart(i int) int {
	return int(binary.LittleEndian.Uint32(it.restarts[4*i:]))
}

// decode parses the entry at off into it.key/seq/kind/exp/value. If usePrev,
// the `shared` leading bytes are taken from the current it.key (the previous
// entry's key); otherwise the entry must be a restart entry (shared == 0).
// It returns the offset after the entry.
func (it *blockIter) decode(off int, usePrev bool) (next int, err error) {
	d := it.data
	if off >= len(d) {
		return 0, corrupt("entry offset %d past data end %d", off, len(d))
	}
	rd := func() (uint64, bool) {
		v, n := binary.Uvarint(d[off:])
		if n <= 0 {
			return 0, false
		}
		off += n
		return v, true
	}
	shared, ok1 := rd()
	unshared, ok2 := rd()
	vlen, ok3 := rd()
	if !ok1 || !ok2 || !ok3 || off >= len(d) {
		return 0, corrupt("truncated entry header")
	}
	kind := iterator.Kind(d[off])
	off++
	seq, ok4 := rd()
	exp, ok5 := rd()
	if !ok4 || !ok5 {
		return 0, corrupt("truncated entry header")
	}
	prevLen := uint64(0)
	if usePrev {
		prevLen = uint64(len(it.key))
	}
	if shared > prevLen {
		return 0, corrupt("shared prefix %d longer than previous key %d", shared, prevLen)
	}
	if unshared > uint64(len(d)-off) || vlen > uint64(len(d)-off)-unshared {
		return 0, corrupt("entry body exceeds block")
	}
	if kind != iterator.KindPut && kind != iterator.KindDelete {
		return 0, corrupt("unknown entry kind %d", kind)
	}
	// Reuse the previous key's prefix in place (d never aliases it.key).
	it.key = append(it.key[:shared], d[off:off+int(unshared)]...)
	off += int(unshared)
	it.value = d[off : off+int(vlen) : off+int(vlen)]
	off += int(vlen)
	it.seq, it.kind, it.exp = seq, kind, int64(exp)
	return off, nil
}

func (it *blockIter) seekToRestart(i int) {
	it.key = it.key[:0]
	it.next_ = it.restart(i)
	it.step()
}

func (it *blockIter) first() { it.seekToRestart(0) }

// step advances to the next entry (or invalidates at the end).
func (it *blockIter) step() {
	if it.err != nil {
		it.valid = false
		return
	}
	if it.next_ >= len(it.data) {
		it.valid = false
		return
	}
	nx, err := it.decode(it.next_, true)
	if err != nil {
		it.err, it.valid = err, false
		return
	}
	it.next_, it.valid = nx, true
}

// seek positions at the first entry >= (key, seq) in internal order.
func (it *blockIter) seek(key []byte, seq uint64) {
	lo, hi := 0, it.nrst-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		// A restart entry has shared == 0, so it decodes standalone.
		if _, err := it.decode(it.restart(mid), false); err != nil {
			it.err, it.valid = err, false
			return
		}
		if iterator.Compare(it.key, it.seq, key, seq) < 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	it.seekToRestart(lo)
	for it.valid && iterator.Compare(it.key, it.seq, key, seq) < 0 {
		it.step()
	}
}

func (it *blockIter) entry() iterator.Entry {
	return iterator.Entry{Key: it.key, Seq: it.seq, Kind: it.kind, Value: it.value, ExpireAt: it.exp}
}
