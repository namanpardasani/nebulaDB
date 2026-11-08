package main

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// # The acknowledged-write model
//
// Every key is owned by exactly one writer, and a writer has at most one
// operation in flight (depth-1 request/response). That makes the model exact:
//
//   - acked:   the state after the last operation whose reply the writer received.
//              Durability promise: after ANY crash this state (or a later one) must
//              be what the database holds.
//   - pending: the single operation that was sent but not answered when the
//              server was killed. It may or may not have been applied; both are
//              legal, but nothing else is.
//
// After a restart the driver reads the database, checks the observed state is
// one of {acked, acked+pending applied}, and adopts the observation as the new
// baseline.

// Kinds of keys a writer owns.
const (
	keysPerWriter = 48
	groupSize     = 3
	ttlKeys       = 3
)

type opKind int

const (
	opSet opKind = iota
	opDel
	opIncr
	opMSet
	opSetShortTTL // PX shortTTLms
	opSetLongTTL  // PX longTTLms
)

const (
	shortTTLms = 1200
	longTTLms  = 3_600_000
)

// payload deterministically generates the value body for (writer, seq) so the
// driver can verify bytes exactly without storing them.
func payload(writer int, seq uint64) string {
	rng := rand.New(rand.NewSource(int64(writer)<<40 ^ int64(seq)*2654435761))
	var n int
	switch r := rng.Intn(100); {
	case r < 85:
		n = 8 + rng.Intn(200)
	case r < 98:
		n = 1000 + rng.Intn(4000)
	default:
		n = 60_000 + rng.Intn(60_000) // big values force frequent rotations
	}
	var sb strings.Builder
	sb.Grow(n)
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	for i := 0; i < n; i++ {
		sb.WriteByte(alpha[rng.Intn(len(alpha))])
	}
	return sb.String()
}

// makeValue builds "<writer>:<seq>:<crc32c(body)>:<body>".
func makeValue(writer int, seq uint64) string {
	body := payload(writer, seq)
	return fmt.Sprintf("%d:%d:%08x:%s", writer, seq, crc32.Checksum([]byte(body), crc32.MakeTable(crc32.Castagnoli)), body)
}

// parseValue validates integrity and returns (writer, seq). A mismatch means the
// database returned bytes nobody ever wrote.
func parseValue(v string) (writer int, seq uint64, err error) {
	parts := strings.SplitN(v, ":", 4)
	if len(parts) != 4 {
		return 0, 0, fmt.Errorf("malformed value (%d bytes)", len(v))
	}
	w, e1 := strconv.Atoi(parts[0])
	s, e2 := strconv.ParseUint(parts[1], 10, 64)
	crc, e3 := strconv.ParseUint(parts[2], 16, 32)
	if e1 != nil || e2 != nil || e3 != nil {
		return 0, 0, fmt.Errorf("malformed value header")
	}
	if uint32(crc) != crc32.Checksum([]byte(parts[3]), crc32.MakeTable(crc32.Castagnoli)) {
		return 0, 0, fmt.Errorf("value CORRUPT: body checksum mismatch (len %d)", len(v))
	}
	if parts[3] != payload(w, s) {
		return 0, 0, fmt.Errorf("value CORRUPT: body differs from what writer %d wrote for seq %d", w, s)
	}
	return w, s, nil
}

// keyState is what the model knows about one key.
type keyState struct {
	Present bool
	Seq     uint64 // value seq when present
	// TTL bookkeeping (short-TTL keys): server deadline is in [sentAt+ttl, ackAt+ttl].
	TTLms  int64
	SentAt time.Time
	AckAt  time.Time // zero while unacknowledged
}

// pendingOp is the in-flight operation at kill time.
type pendingOp struct {
	Kind   opKind
	Keys   []string
	Seq    uint64
	SentAt time.Time
}

// writerModel is the model for one writer's key space.
type writerModel struct {
	ID      int
	Acked   map[string]keyState
	Counter int64 // acked INCR count
	Pending *pendingOp
	NextSeq uint64
	// Anomaly is set by the writer goroutine when a live check (error reply,
	// wrong INCR result, read-your-write mismatch) fails.
	Anomaly string
}

func newWriterModel(id int) *writerModel {
	return &writerModel{ID: id, Acked: map[string]keyState{}, NextSeq: 1}
}

func (w *writerModel) key(i int) string      { return fmt.Sprintf("w%d:k%02d", w.ID, i) }
func (w *writerModel) ctrKey() string        { return fmt.Sprintf("w%d:ctr", w.ID) }
func (w *writerModel) groupKey(i int) string { return fmt.Sprintf("w%d:g%d", w.ID, i) }
func (w *writerModel) ttlKey(i int) string   { return fmt.Sprintf("w%d:t%d", w.ID, i) }

// allKeys lists every key this writer may ever have written.
func (w *writerModel) allKeys() []string {
	var ks []string
	for i := 0; i < keysPerWriter; i++ {
		ks = append(ks, w.key(i))
	}
	ks = append(ks, w.ctrKey())
	for i := 0; i < groupSize; i++ {
		ks = append(ks, w.groupKey(i))
	}
	for i := 0; i < ttlKeys; i++ {
		ks = append(ks, w.ttlKey(i))
	}
	return ks
}

// ack applies a completed operation to the acked state.
func (w *writerModel) ack(p *pendingOp, ackAt time.Time) {
	switch p.Kind {
	case opSet, opSetShortTTL, opSetLongTTL:
		ks := keyState{Present: true, Seq: p.Seq, SentAt: p.SentAt, AckAt: ackAt}
		if p.Kind == opSetShortTTL {
			ks.TTLms = shortTTLms
		} else if p.Kind == opSetLongTTL {
			ks.TTLms = longTTLms
		}
		w.Acked[p.Keys[0]] = ks
	case opDel:
		w.Acked[p.Keys[0]] = keyState{}
	case opIncr:
		w.Counter++
	case opMSet:
		for _, k := range p.Keys {
			w.Acked[k] = keyState{Present: true, Seq: p.Seq, SentAt: p.SentAt, AckAt: ackAt}
		}
	}
}
