// Package wal implements NebulaDB's write-ahead log: an append-only sequence of
// checksummed records split over numbered segment files, with configurable
// fsync policy, group commit, and crash recovery by replay.
//
// # Why a WAL
//
// The memtable lives in RAM. Before a write is acknowledged it is appended to
// the WAL (and, with fsync=always, forced to stable storage). After a crash the
// memtable is rebuilt by replaying the WAL, so an acknowledged write is never
// lost, while the expensive sorted-file work is deferred to background flushes.
//
// # Record format (little endian)
//
//	+---------+---------+----------------------------------------------+
//	| len u32 | crc u32 | payload (len bytes)                          |
//	+---------+---------+----------------------------------------------+
//	crc  = CRC32C(Castagnoli) of the payload
//	payload = seq u64 | count u32 | count x op
//	op      = type u8 | keyLen u32 | valLen u32 | expireAt i64 | key | value
//
// One record is one *batch* of 1..n operations, written with a single write
// call and covered by a single CRC. That makes a multi-key command (MSET,
// FLUSHDB) atomic across crashes: after recovery either every op of the batch
// is present or none is. The ops of a batch get consecutive sequence numbers
// seq, seq+1, ... (seq is the first one).
//
// # Torn tails vs corruption
//
// A crash in the middle of an append leaves a partial or zeroed record at the
// end of the newest segment. That is expected and harmless: nobody was told the
// write succeeded. Recovery rule (ScanSegment): a bad record is a *torn tail*
// iff no valid record exists anywhere after it; the tail is then truncated. If a
// valid record does exist after a bad one, bytes in the middle of the log were
// damaged, which a crash cannot cause. Recovery refuses to guess and returns a
// CorruptionError (the `nebula repair` tool can salvage what is readable).
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// OpType is the kind of a logged operation.
type OpType uint8

const (
	// OpPut sets Key to Value with optional ExpireAt (unix ms, 0 = never).
	OpPut OpType = 1
	// OpDelete writes a tombstone for Key.
	OpDelete OpType = 2
)

const (
	headerSize     = 8
	batchHeader    = 8 + 4         // seq + count
	opHeader       = 1 + 4 + 4 + 8 // type + keyLen + valLen + expireAt
	minPayload     = batchHeader + opHeader
	maxPayloadSize = 128 << 20 // sanity bound on a single record
)

// Op is one logged operation.
type Op struct {
	Type     OpType
	Key      []byte
	Value    []byte
	ExpireAt int64
}

// Batch is a group of operations logged and applied atomically.
type Batch struct {
	// Seq is the sequence number of Ops[0]; Ops[i] has Seq+i. It is assigned by
	// WAL.Commit; callers leave it zero.
	Seq uint64
	Ops []Op
}

// LastSeq returns the sequence number of the batch's final op.
func (b *Batch) LastSeq() uint64 { return b.Seq + uint64(len(b.Ops)) - 1 }

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrBatchTooLarge is returned when a batch cannot be encoded within limits.
var ErrBatchTooLarge = errors.New("wal: batch too large")

// CorruptionError reports damage that is not an ordinary torn tail.
type CorruptionError struct {
	Segment uint64 // segment number (0 if unknown)
	Offset  int64  // byte offset of the first bad record
	Reason  string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption in segment %06d at offset %d: %s "+
		"(refusing to start; run `nebula repair` to salvage readable data)", e.Segment, e.Offset, e.Reason)
}

func encodedSize(b *Batch) int {
	n := headerSize + batchHeader
	for i := range b.Ops {
		n += opHeader + len(b.Ops[i].Key) + len(b.Ops[i].Value)
	}
	return n
}

// appendRecord encodes b (with b.Seq already set) onto dst.
func appendRecord(dst []byte, b *Batch) ([]byte, error) {
	size := encodedSize(b)
	if len(b.Ops) == 0 || size-headerSize > maxPayloadSize {
		return dst, ErrBatchTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, headerSize)...) // len+crc patched below
	dst = binary.LittleEndian.AppendUint64(dst, b.Seq)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.Ops)))
	for i := range b.Ops {
		op := &b.Ops[i]
		dst = append(dst, byte(op.Type))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(op.Key)))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(op.Value)))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(op.ExpireAt))
		dst = append(dst, op.Key...)
		dst = append(dst, op.Value...)
	}
	payload := dst[start+headerSize:]
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(payload, crcTable))
	return dst, nil
}

// decodeRecord parses one record at the start of data. On success it returns
// the batch (whose keys/values alias data) and the record's total length.
// On failure it returns a human-readable reason.
func decodeRecord(data []byte) (b Batch, n int, reason string) {
	if len(data) < headerSize {
		return b, 0, "short header"
	}
	plen := binary.LittleEndian.Uint32(data)
	if plen < minPayload || plen > maxPayloadSize {
		return b, 0, fmt.Sprintf("implausible record length %d", plen)
	}
	if uint64(len(data)) < uint64(headerSize)+uint64(plen) {
		return b, 0, "record extends past end of file"
	}
	payload := data[headerSize : headerSize+int(plen)]
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(data[4:]) {
		return b, 0, "checksum mismatch"
	}
	// The CRC passed, so structure errors below mean a writer bug or an
	// astronomically unlucky collision; treat them as bad records all the same.
	b.Seq = binary.LittleEndian.Uint64(payload)
	count := binary.LittleEndian.Uint32(payload[8:])
	rest := payload[batchHeader:]
	if count == 0 || uint64(count)*opHeader > uint64(len(rest)) {
		return Batch{}, 0, "invalid op count"
	}
	b.Ops = make([]Op, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(rest) < opHeader {
			return Batch{}, 0, "truncated op header"
		}
		t := OpType(rest[0])
		kl := binary.LittleEndian.Uint32(rest[1:])
		vl := binary.LittleEndian.Uint32(rest[5:])
		exp := int64(binary.LittleEndian.Uint64(rest[9:]))
		rest = rest[opHeader:]
		if t != OpPut && t != OpDelete {
			return Batch{}, 0, fmt.Sprintf("unknown op type %d", t)
		}
		if uint64(kl)+uint64(vl) > uint64(len(rest)) {
			return Batch{}, 0, "op body exceeds record"
		}
		b.Ops = append(b.Ops, Op{Type: t, Key: rest[:kl:kl], Value: rest[kl : kl+vl : kl+vl], ExpireAt: exp})
		rest = rest[kl+vl:]
	}
	if len(rest) != 0 {
		return Batch{}, 0, "trailing bytes in record"
	}
	return b, headerSize + int(plen), ""
}

// ScanResult describes how a segment ended.
type ScanResult struct {
	ValidEnd int64  // offset just past the last good record
	Torn     bool   // data[ValidEnd:] is a torn tail that should be truncated
	Batches  int    // good records seen
	LastSeq  uint64 // last sequence number seen (0 if none)
}

// ScanSegment walks a whole segment, calling fn for each valid batch in order.
// afterSeq is the highest sequence number already seen; batches must carry
// strictly increasing sequence numbers above it. Keys and values passed to fn
// alias data and are only valid during the call.
//
// It returns a *CorruptionError for mid-file damage (see package comment),
// or whatever fn returns.
func ScanSegment(data []byte, afterSeq uint64, fn func(*Batch) error) (ScanResult, error) {
	var res ScanResult
	res.LastSeq = afterSeq
	off := 0
	for off < len(data) {
		b, n, reason := decodeRecord(data[off:])
		if reason != "" {
			if at := findValidRecord(data, off+1); at >= 0 {
				return res, &CorruptionError{Offset: int64(off),
					Reason: fmt.Sprintf("%s, but a valid record follows at offset %d", reason, at)}
			}
			res.ValidEnd, res.Torn = int64(off), true
			return res, nil
		}
		if b.Seq <= res.LastSeq { // sequence numbers start at 1
			return res, &CorruptionError{Offset: int64(off),
				Reason: fmt.Sprintf("sequence regression: record seq %d <= previous %d", b.Seq, res.LastSeq)}
		}
		if err := fn(&b); err != nil {
			return res, err
		}
		res.LastSeq = b.LastSeq()
		res.Batches++
		off += n
		res.ValidEnd = int64(off)
	}
	return res, nil
}

// findValidRecord returns the offset of the first fully valid record starting
// at or after from, or -1. Used only on the (rare) failure path to tell a torn
// tail (nothing valid after it) from mid-file corruption.
func findValidRecord(data []byte, from int) int {
	for i := from; i+headerSize+minPayload <= len(data); i++ {
		// Cheap prefilter: a plausible length that fits in the remaining bytes.
		plen := binary.LittleEndian.Uint32(data[i:])
		if plen < minPayload || plen > maxPayloadSize || uint64(i)+headerSize+uint64(plen) > uint64(len(data)) {
			continue
		}
		if _, _, reason := decodeRecord(data[i:]); reason == "" {
			return i
		}
	}
	return -1
}

// SalvageResult describes a best-effort scan.
type SalvageResult struct {
	Batches      int
	SkippedBytes int64 // bytes that did not belong to any valid record
	Skipped      []Range
}

// Range is a byte range [Start, End) of a segment.
type Range struct{ Start, End int64 }

// SalvageSegment extracts every valid record from a possibly damaged segment.
// Unlike ScanSegment it never refuses: at a bad record it searches forward for
// the next valid one (resynchronising) and reports what was skipped. Records are
// self-validating (length + CRC + structure), so a false resync would need a
// CRC collision. Used by `nebula repair`; sequence numbers are not enforced.
func SalvageSegment(data []byte, fn func(*Batch) error) (SalvageResult, error) {
	var res SalvageResult
	off := 0
	for off < len(data) {
		b, n, reason := decodeRecord(data[off:])
		if reason == "" {
			if err := fn(&b); err != nil {
				return res, err
			}
			res.Batches++
			off += n
			continue
		}
		next := findValidRecord(data, off+1)
		end := len(data)
		if next >= 0 {
			end = next
		}
		res.Skipped = append(res.Skipped, Range{int64(off), int64(end)})
		res.SkippedBytes += int64(end - off)
		off = end
	}
	return res, nil
}
