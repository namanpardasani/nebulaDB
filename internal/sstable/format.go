// Package sstable implements NebulaDB's immutable sorted string table files.
//
// # File layout
//
//	+-----------------------------+
//	| data block 0                |  sorted entries, prefix compressed
//	| data block 1                |
//	| ...                         |
//	+-----------------------------+
//	| filter block (optional)     |  Bloom filter over user keys
//	| index block                 |  sparse index: one entry per data block
//	| properties block            |  counts, key range, seq range
//	+-----------------------------+
//	| footer (64 bytes)           |  block handles, version, crc, magic
//	+-----------------------------+
//
// Every block is stored as `contents | compression u8 (0 = none) | crc32c u32`
// where the CRC covers contents and the compression byte. A BlockHandle is
// (offset u64, size u64) of the *contents* (trailer excluded).
//
// # Sparse index
//
// The index block is itself a block whose entries map the LAST internal key
// (user key + seq) of each data block to that block's handle. To find
// (key, snapshot) we binary-search the index for the first block whose last key
// is >= the target: that block is the only one that can contain the answer, so
// a point lookup costs at most one data-block read (plus the filter check).
//
// # Footer (64 bytes, little endian)
//
//	 0  filter handle   offset u64, size u64   (size 0 = no filter)
//	16  index handle    offset u64, size u64
//	32  props handle    offset u64, size u64
//	48  format version  u32
//	52  crc32c of bytes [0,52)  u32
//	56  magic           u64  "NEBULSST"
//
// The footer is written last, so a file truncated by a crash mid-write has no
// valid footer and is rejected at open. Writers also use temp-file + fsync +
// rename, so a partially written table never carries its final name.
package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	footerSize     = 64
	blockTrailer   = 5 // compression byte + crc32c
	formatVersion  = 1
	footerMagic    = uint64(0x5453534c554245_4e) // "NEBULSST" read little endian
	compressNone   = 0
	defaultBlock   = 4 << 10
	restartDefault = 16
	handleSize     = 16
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// blockHandle locates a block's contents within the file.
type blockHandle struct{ offset, size uint64 }

func (h blockHandle) encode(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, h.offset)
	return binary.LittleEndian.AppendUint64(dst, h.size)
}

func decodeHandle(b []byte) (blockHandle, error) {
	if len(b) < handleSize {
		return blockHandle{}, corrupt("short block handle")
	}
	return blockHandle{binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:])}, nil
}

type footer struct{ filter, index, props blockHandle }

func (f footer) encode() []byte {
	b := make([]byte, 0, footerSize)
	b = f.filter.encode(b)
	b = f.index.encode(b)
	b = f.props.encode(b)
	b = binary.LittleEndian.AppendUint32(b, formatVersion)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
	b = binary.LittleEndian.AppendUint64(b, footerMagic)
	return b
}

func decodeFooter(b []byte) (footer, error) {
	if len(b) != footerSize {
		return footer{}, corrupt("footer is %d bytes, want %d", len(b), footerSize)
	}
	if binary.LittleEndian.Uint64(b[56:]) != footerMagic {
		return footer{}, corrupt("bad magic number (not an SSTable, or truncated)")
	}
	if crc32.Checksum(b[:52], crcTable) != binary.LittleEndian.Uint32(b[52:]) {
		return footer{}, corrupt("footer checksum mismatch")
	}
	if v := binary.LittleEndian.Uint32(b[48:]); v != formatVersion {
		return footer{}, fmt.Errorf("sstable: unsupported format version %d", v)
	}
	var f footer
	f.filter, _ = decodeHandle(b[0:])
	f.index, _ = decodeHandle(b[16:])
	f.props, _ = decodeHandle(b[32:])
	return f, nil
}

// Properties describe a table; they are stored in the properties block and
// recorded in the MANIFEST.
type Properties struct {
	NumEntries uint64
	NumDeletes uint64
	NumBlocks  uint64
	DataSize   uint64 // bytes of data blocks (incl. trailers)
	MinSeq     uint64
	MaxSeq     uint64
	Smallest   []byte // smallest user key
	Largest    []byte // largest user key
	FilterSize uint64
}

func (p *Properties) encode() []byte {
	b := []byte{1} // properties encoding version
	for _, v := range []uint64{p.NumEntries, p.NumDeletes, p.NumBlocks, p.DataSize, p.MinSeq, p.MaxSeq, p.FilterSize} {
		b = binary.AppendUvarint(b, v)
	}
	b = binary.AppendUvarint(b, uint64(len(p.Smallest)))
	b = append(b, p.Smallest...)
	b = binary.AppendUvarint(b, uint64(len(p.Largest)))
	return append(b, p.Largest...)
}

func decodeProperties(b []byte) (Properties, error) {
	var p Properties
	if len(b) < 1 || b[0] != 1 {
		return p, corrupt("unknown properties encoding")
	}
	b = b[1:]
	next := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	for _, dst := range []*uint64{&p.NumEntries, &p.NumDeletes, &p.NumBlocks, &p.DataSize, &p.MinSeq, &p.MaxSeq, &p.FilterSize} {
		v, ok := next()
		if !ok {
			return p, corrupt("truncated properties")
		}
		*dst = v
	}
	for _, dst := range []*[]byte{&p.Smallest, &p.Largest} {
		l, ok := next()
		if !ok || l > uint64(len(b)) {
			return p, corrupt("truncated properties key")
		}
		*dst = append([]byte(nil), b[:l]...)
		b = b[l:]
	}
	return p, nil
}
