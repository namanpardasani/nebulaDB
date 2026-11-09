package repl

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/wal"
)

// ErrNotEnoughAcks: the write IS applied on the leader but fewer replicas than
// required confirmed it in time. The client must treat the outcome as unknown
// (like a lost reply), not as "not written".
var ErrNotEnoughAcks = errors.New("write applied on the leader but not enough replicas acknowledged it in time")

type entry struct {
	first, last uint64
	data        []byte // encodeBatch
}

type follower struct {
	addr    string
	acked   atomic.Uint64
	sentSeq atomic.Uint64
	resync  bool
}

// Leader keeps a bounded in-memory log of recently committed batches and ships
// it to followers. The log is only a CACHE for catch-up: the durable truth is
// the engine; a follower that needs more than the log holds gets a snapshot.
type Leader struct {
	db   *engine.DB
	log  *slog.Logger
	id   [16]byte
	max  int64 // backlog bytes
	dir  string
	done chan struct{}
	once sync.Once

	mu       sync.Mutex
	ring     []entry
	ringSize int64
	lastSeq  uint64
	notify   chan struct{} // closed and replaced on every append (broadcast)
	follows  map[*follower]struct{}

	ackMu     sync.Mutex
	ackNotify chan struct{}

	snapshots atomic.Uint64
	conts     atomic.Uint64
}

// NewLeader loads (or creates) the replication identity in dir. Pass
// l.OnCommit as engine.Options.OnCommit, open the engine, then call Attach.
func NewLeader(dir string, backlog int64, log *slog.Logger) (*Leader, error) {
	l := &Leader{dir: dir, max: backlog, log: log, done: make(chan struct{}),
		notify: make(chan struct{}), ackNotify: make(chan struct{}), follows: map[*follower]struct{}{}}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "REPLID")
	if b, err := os.ReadFile(path); err == nil && len(b) == 32 {
		if _, err := hex.Decode(l.id[:], b); err != nil {
			return nil, fmt.Errorf("repl: corrupt %s", path)
		}
	} else {
		if _, err := rand.Read(l.id[:]); err != nil {
			return nil, err
		}
		if err := writeFileSync(path, []byte(hex.EncodeToString(l.id[:]))); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// Attach connects the opened engine.
func (l *Leader) Attach(db *engine.DB) {
	l.db = db
	l.mu.Lock()
	if l.lastSeq < db.LastSeq() {
		l.lastSeq = db.LastSeq()
	}
	l.mu.Unlock()
}

// OnCommit is the engine hook: called on the WAL leader for each batch.
func (l *Leader) OnCommit(first uint64, ops []wal.Op) {
	e := entry{first: first, last: first + uint64(len(ops)) - 1, data: encodeBatch(first, ops)}
	l.mu.Lock()
	l.ring = append(l.ring, e)
	l.ringSize += int64(len(e.data))
	l.lastSeq = e.last
	for l.ringSize > l.max && len(l.ring) > 1 { // always keep the newest batch
		l.ringSize -= int64(len(l.ring[0].data))
		l.ring[0] = entry{}
		l.ring = l.ring[1:]
	}
	if cap(l.ring) > 4*len(l.ring)+1024 { // release the evicted head
		l.ring = append([]entry(nil), l.ring...)
	}
	old := l.notify
	l.notify = make(chan struct{})
	l.mu.Unlock()
	close(old)
}

// LastSeq is the sequence number of the newest committed op.
func (l *Leader) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSeq
}

// Serve accepts followers until ln is closed.
func (l *Leader) Serve(ln net.Listener) error {
	go func() { <-l.done; ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-l.done:
				return nil
			default:
				return err
			}
		}
		go l.serveConn(c)
	}
}

// Close stops Serve and disconnects followers.
func (l *Leader) Close() { l.once.Do(func() { close(l.done) }) }

// Stats for INFO.
func (l *Leader) Stats() []engine.Stat {
	l.mu.Lock()
	n, last, size := len(l.follows), l.lastSeq, l.ringSize
	var lag []string
	for f := range l.follows {
		lag = append(lag, fmt.Sprintf("%s:acked=%d,lag=%d", f.addr, f.acked.Load(), int64(last)-int64(f.acked.Load())))
	}
	l.mu.Unlock()
	sort.Strings(lag)
	st := []engine.Stat{
		{Name: "repl_role", Value: "leader"}, {Name: "repl_id", Value: hex.EncodeToString(l.id[:])},
		{Name: "repl_connected_followers", Value: fmt.Sprint(n)}, {Name: "repl_last_seq", Value: fmt.Sprint(last)},
		{Name: "repl_backlog_bytes", Value: fmt.Sprint(size)},
		{Name: "repl_full_resyncs", Value: fmt.Sprint(l.snapshots.Load())}, {Name: "repl_partial_resyncs", Value: fmt.Sprint(l.conts.Load())},
	}
	for i, s := range lag {
		st = append(st, engine.Stat{Name: fmt.Sprintf("repl_follower%d", i), Value: s})
	}
	return st
}

// AckedCount is how many streaming followers have durably applied seq.
func (l *Leader) AckedCount(seq uint64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for f := range l.follows {
		if !f.resync && f.acked.Load() >= seq {
			n++
		}
	}
	return n
}

// WaitAcks blocks until n followers have acknowledged seq or the timeout passes.
func (l *Leader) WaitAcks(seq uint64, n int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		l.ackMu.Lock()
		ch := l.ackNotify
		l.ackMu.Unlock()
		if l.AckedCount(seq) >= n {
			return nil
		}
		select {
		case <-ch:
		case <-deadline.C:
			return ErrNotEnoughAcks
		case <-l.done:
			return ErrNotEnoughAcks
		}
	}
}

func (l *Leader) bumpAck() {
	l.ackMu.Lock()
	old := l.ackNotify
	l.ackNotify = make(chan struct{})
	l.ackMu.Unlock()
	close(old)
}

// after returns the ring entries whose last seq > seq, or ok=false if the ring
// no longer starts at or before seq+1 (the follower fell too far behind).
func (l *Leader) after(seq uint64) (es []entry, notify <-chan struct{}, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	notify = l.notify
	if seq >= l.lastSeq {
		return nil, notify, true
	}
	if len(l.ring) == 0 || l.ring[0].first > seq+1 {
		return nil, notify, false
	}
	i := sort.Search(len(l.ring), func(i int) bool { return l.ring[i].last > seq })
	return append([]entry(nil), l.ring[i:]...), notify, true
}

func (l *Leader) serveConn(c net.Conn) {
	defer c.Close()
	go func() { <-l.done; c.Close() }()
	r, w := bufio.NewReaderSize(c, 64<<10), bufio.NewWriterSize(c, 256<<10)
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	typ, p, err := readFrame(r)
	if err != nil || typ != frameHello || len(p) != 8+16+8 || string(p[:8]) != magic {
		l.log.Warn("replication: bad HELLO", "remote", c.RemoteAddr().String(), "err", err)
		return
	}
	c.SetReadDeadline(time.Time{})
	var theirID [16]byte
	copy(theirID[:], p[8:24])
	pos := binary.LittleEndian.Uint64(p[24:])
	f := &follower{addr: c.RemoteAddr().String()}

	// Decide continue vs resync under the lock so "position valid" and "register
	// for the stream" are one atomic step: nothing can be evicted in between.
	l.mu.Lock()
	cont := theirID == l.id && pos <= l.lastSeq && (pos == l.lastSeq || (len(l.ring) > 0 && l.ring[0].first <= pos+1))
	cursor := pos
	if !cont {
		f.resync = true
		cursor = l.lastSeq // batches after this point are kept for us in the ring
	}
	l.follows[f] = struct{}{}
	l.mu.Unlock()
	defer func() { l.mu.Lock(); delete(l.follows, f); l.mu.Unlock(); l.bumpAck() }()

	welcome := make([]byte, 16+1+8)
	copy(welcome, l.id[:])
	if cont {
		welcome[16] = modeContinue
		binary.LittleEndian.PutUint64(welcome[17:], pos)
		l.conts.Add(1)
	} else {
		welcome[16] = modeResync
	}
	if !cont {
		// The snapshot seq is only known after pinning the snapshot, so WELCOME is
		// sent after it is taken.
		snap, err := l.db.NewSnapshot()
		if err != nil {
			l.log.Error("replication: snapshot", "err", err)
			return
		}
		binary.LittleEndian.PutUint64(welcome[17:], snap.Seq())
		if err := writeFrame(w, frameWelcome, welcome); err != nil || w.Flush() != nil {
			snap.Close()
			return
		}
		l.snapshots.Add(1)
		l.log.Info("replication: full resync", "follower", f.addr, "snapshot_seq", snap.Seq())
		err = l.sendSnapshot(w, snap)
		cursor = snap.Seq()
		snap.Close()
		if err != nil {
			l.log.Warn("replication: snapshot send failed", "follower", f.addr, "err", err)
			return
		}
		f.resync = false // from here its acks count
	} else {
		if err := writeFrame(w, frameWelcome, welcome); err != nil || w.Flush() != nil {
			return
		}
		l.log.Info("replication: follower continues", "follower", f.addr, "from_seq", pos)
	}

	// ACK reader.
	errc := make(chan error, 1)
	go func() {
		for {
			t, p, err := readFrame(r)
			if err != nil {
				errc <- err
				return
			}
			if t == frameAck && len(p) == 8 {
				s := binary.LittleEndian.Uint64(p)
				if s > f.acked.Load() {
					f.acked.Store(s)
					l.bumpAck()
				}
			}
		}
	}()

	ping := time.NewTicker(time.Second)
	defer ping.Stop()
	for {
		es, notify, ok := l.after(cursor)
		if !ok {
			l.log.Warn("replication: follower fell behind the backlog; forcing resync", "follower", f.addr)
			return
		}
		for _, e := range es {
			if e.last <= cursor {
				continue
			}
			if err := writeFrame(w, frameBatch, e.data); err != nil {
				return
			}
			cursor = e.last
			f.sentSeq.Store(cursor)
		}
		if len(es) > 0 {
			if w.Flush() != nil {
				return
			}
			continue
		}
		select {
		case <-notify:
		case <-ping.C:
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], l.LastSeq())
			if writeFrame(w, framePing, b[:]) != nil || w.Flush() != nil {
				return
			}
		case <-errc:
			return
		case <-l.done:
			return
		}
	}
}

// sendSnapshot streams every live key (with its expiry) of snap in chunks.
func (l *Leader) sendSnapshot(w *bufio.Writer, snap *engine.Snapshot) error {
	it := snap.NewIterator()
	defer it.Close()
	var chunk []wal.Op
	size := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		err := writeFrame(w, frameSnap, encodeBatch(0, chunk))
		chunk, size = chunk[:0], 0
		return err
	}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		chunk = append(chunk, wal.Op{Type: wal.OpPut, Key: append([]byte(nil), it.Key()...), Value: append([]byte(nil), it.Value()...), ExpireAt: it.ExpireAt()})
		size += len(it.Key()) + len(it.Value()) + 17
		if size >= 1<<20 || len(chunk) >= 4096 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := writeFrame(w, frameSnapEnd, nil); err != nil {
		return err
	}
	return w.Flush()
}

// writeFileSync writes data atomically (tmp + fsync + rename + dir fsync).
func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}
