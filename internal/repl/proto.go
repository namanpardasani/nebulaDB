// Package repl implements leader/follower replication by LOGICAL WAL SHIPPING.
//
// What is shipped: every batch the leader's engine commits, as the list of final
// operations (put key/value/expiry, delete key). It is the same unit the WAL
// logs, so each batch is applied on the follower atomically. Conditional or
// read-modify-write commands (SET NX, INCR, ...) have already been resolved by
// the leader, so the follower just replays the outcome; it never re-evaluates.
//
// Stream = one TCP connection per follower, framed and CRC32C-checked:
//
//	follower -> HELLO  (leader id it last followed, last applied leader seq)
//	leader   -> WELCOME(mode: continue | resync, start seq)
//	  resync: leader -> SNAP* ... SNAPEND   (consistent snapshot, then batches)
//	leader   -> BATCH* / PING               follower -> ACK (applied + durable)
//
// Positions are the LEADER's sequence numbers (the seq of the last op of the
// last batch applied). Replaying a suffix of batches that were already applied
// is harmless (puts and deletes are idempotent and applied in order, so the end
// state is the same), which is what makes the follower's position file safe to
// write lazily and what makes snapshot + overlapping batches consistent.
package repl

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/naman/nebuladb/internal/wal"
)

const magic = "NBREPL01"

const (
	frameHello   byte = 1
	frameWelcome byte = 2
	frameSnap    byte = 3
	frameSnapEnd byte = 4
	frameBatch   byte = 5
	frameAck     byte = 6
	framePing    byte = 7
)

const (
	modeContinue byte = 0
	modeResync   byte = 1
)

const maxFrame = 64 << 20 // sanity bound

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrProtocol reports a malformed or corrupt frame.
var ErrProtocol = errors.New("repl: protocol error")

// writeFrame writes [len u32][type u8][payload][crc32c(type|payload) u32].
func writeFrame(w *bufio.Writer, typ byte, payload []byte) error {
	var hdr [5]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	hdr[4] = typ
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	c := crc32.Update(crc32.Update(0, crcTable, hdr[4:5]), crcTable, payload)
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], c)
	_, err := w.Write(tail[:])
	return err
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:4])
	if n > maxFrame {
		return 0, nil, fmt.Errorf("%w: frame of %d bytes", ErrProtocol, n)
	}
	buf := make([]byte, int(n)+4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	payload, tail := buf[:n], buf[n:]
	c := crc32.Update(crc32.Update(0, crcTable, hdr[4:5]), crcTable, payload)
	if c != binary.LittleEndian.Uint32(tail) {
		return 0, nil, fmt.Errorf("%w: checksum mismatch", ErrProtocol)
	}
	return hdr[4], payload, nil
}

// encodeBatch: first u64 | n u32 | n x (type u8, klen u32, vlen u32, exp i64, key, value).
func encodeBatch(first uint64, ops []wal.Op) []byte {
	size := 12
	for i := range ops {
		size += 17 + len(ops[i].Key) + len(ops[i].Value)
	}
	b := make([]byte, 0, size)
	b = binary.LittleEndian.AppendUint64(b, first)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(ops)))
	for i := range ops {
		o := &ops[i]
		b = append(b, byte(o.Type))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(o.Key)))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(o.Value)))
		b = binary.LittleEndian.AppendUint64(b, uint64(o.ExpireAt))
		b = append(b, o.Key...)
		b = append(b, o.Value...)
	}
	return b
}

// decodeBatch is the inverse; returned ops own their memory.
func decodeBatch(b []byte) (first uint64, ops []wal.Op, err error) {
	if len(b) < 12 {
		return 0, nil, fmt.Errorf("%w: short batch", ErrProtocol)
	}
	first = binary.LittleEndian.Uint64(b)
	n := int(binary.LittleEndian.Uint32(b[8:]))
	b = b[12:]
	ops = make([]wal.Op, 0, min(n, 1<<16))
	for i := 0; i < n; i++ {
		if len(b) < 17 {
			return 0, nil, fmt.Errorf("%w: truncated op", ErrProtocol)
		}
		t := wal.OpType(b[0])
		kl := int(binary.LittleEndian.Uint32(b[1:]))
		vl := int(binary.LittleEndian.Uint32(b[5:]))
		exp := int64(binary.LittleEndian.Uint64(b[9:]))
		b = b[17:]
		if (t != wal.OpPut && t != wal.OpDelete) || kl < 0 || vl < 0 || len(b) < kl+vl {
			return 0, nil, fmt.Errorf("%w: bad op", ErrProtocol)
		}
		ops = append(ops, wal.Op{Type: t, Key: append([]byte(nil), b[:kl]...), Value: append([]byte(nil), b[kl:kl+vl]...), ExpireAt: exp})
		b = b[kl+vl:]
	}
	if len(b) != 0 {
		return 0, nil, fmt.Errorf("%w: trailing bytes", ErrProtocol)
	}
	if len(ops) == 0 {
		return 0, nil, fmt.Errorf("%w: empty batch", ErrProtocol)
	}
	return first, ops, nil
}
