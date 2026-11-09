package repl

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/engine"
)

// state is the follower's persisted resume point (file REPLSTATE in its dir).
type state struct {
	LeaderID string `json:"leader_id"` // hex; empty = never followed anyone
	Pos      uint64 `json:"pos"`       // leader seq of the last batch applied
	Syncing  bool   `json:"syncing"`   // a full resync was started and not finished
}

// Follower pulls the leader's committed batches and applies them to its own
// engine. It writes ONLY through DB.Apply, in leader order, one leader batch =
// one local atomic batch.
type Follower struct {
	db    *engine.DB
	log   *slog.Logger
	addr  string
	path  string
	state state

	connected atomic.Bool
	pos       atomic.Uint64
	leaderSeq atomic.Uint64 // from PINGs and batches: how far the leader is
	resyncs   atomic.Uint64
	lastErr   atomic.Value // string

	done     chan struct{} // closed when Run returns
	saveMu   sync.Mutex
	lastSave time.Time
}

// NewFollower loads the persisted position from dir.
func NewFollower(db *engine.DB, dir, leaderAddr string, log *slog.Logger) (*Follower, error) {
	f := &Follower{db: db, log: log, addr: leaderAddr, path: filepath.Join(dir, "REPLSTATE"), done: make(chan struct{})}
	b, err := os.ReadFile(f.path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &f.state); err != nil {
			return nil, fmt.Errorf("repl: corrupt %s: %w", f.path, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	f.pos.Store(f.state.Pos)
	return f, nil
}

// Wait blocks until Run has returned (after its context was cancelled).
func (f *Follower) Wait() { <-f.done }

// Pos is the leader sequence number applied so far.
func (f *Follower) Pos() uint64 { return f.pos.Load() }

// Connected reports whether the stream is up.
func (f *Follower) Connected() bool { return f.connected.Load() }

func (f *Follower) save(force bool) error {
	f.saveMu.Lock()
	defer f.saveMu.Unlock()
	if !force && time.Since(f.lastSave) < 100*time.Millisecond {
		return nil
	}
	f.state.Pos = f.pos.Load()
	b, _ := json.Marshal(f.state)
	f.lastSave = time.Now()
	return writeFileSync(f.path, b)
}

// Run follows the leader until ctx is cancelled, reconnecting with backoff.
func (f *Follower) Run(ctx context.Context) {
	defer close(f.done)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		start := time.Now()
		err := f.session(ctx)
		f.connected.Store(false)
		if err != nil && ctx.Err() == nil {
			f.lastErr.Store(err.Error())
			f.log.Warn("replication: disconnected", "leader", f.addr, "err", err)
		}
		if time.Since(start) > 5*time.Second {
			backoff = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	_ = f.save(true)
}

func (f *Follower) session(ctx context.Context) error {
	d := net.Dialer{Timeout: 3 * time.Second}
	c, err := d.DialContext(ctx, "tcp", f.addr)
	if err != nil {
		return err
	}
	defer c.Close()
	go func() { <-ctx.Done(); c.Close() }()
	r, w := bufio.NewReaderSize(c, 256<<10), bufio.NewWriterSize(c, 4<<10)

	// An interrupted resync leaves a partial copy: it must not be trusted as a
	// resume point, so announce position 0 (which forces a new snapshot).
	pos := f.pos.Load()
	idHex := f.state.LeaderID
	if f.state.Syncing {
		pos, idHex = 0, ""
	}
	hello := make([]byte, 0, 32)
	hello = append(hello, magic...)
	var id [16]byte
	if b, err := hex.DecodeString(idHex); err == nil && len(b) == 16 {
		copy(id[:], b)
	}
	hello = append(hello, id[:]...)
	hello = binary.LittleEndian.AppendUint64(hello, pos)
	if err := writeFrame(w, frameHello, hello); err != nil || w.Flush() != nil {
		return errors.New("sending HELLO failed")
	}
	c.SetReadDeadline(time.Now().Add(15 * time.Second))
	typ, p, err := readFrame(r)
	if err != nil {
		return err
	}
	if typ != frameWelcome || len(p) != 25 {
		return fmt.Errorf("%w: expected WELCOME", ErrProtocol)
	}
	leaderID := hex.EncodeToString(p[:16])
	mode, startSeq := p[16], binary.LittleEndian.Uint64(p[17:])

	if mode == modeResync {
		f.log.Info("replication: full resync from leader", "leader", f.addr, "snapshot_seq", startSeq)
		f.resyncs.Add(1)
		if err := f.loadSnapshot(r, leaderID, startSeq); err != nil {
			return err
		}
	} else if leaderID != f.state.LeaderID {
		return fmt.Errorf("%w: leader changed without resync", ErrProtocol)
	}
	f.connected.Store(true)
	f.log.Info("replication: streaming", "leader", f.addr, "pos", f.pos.Load())
	return f.stream(c, r, w)
}

// loadSnapshot replaces the local data with the leader's snapshot.
func (f *Follower) loadSnapshot(r *bufio.Reader, leaderID string, snapSeq uint64) error {
	// 1. Persist "resync started" BEFORE touching data. A crash from here on makes
	//    the next start announce position 0 and take a fresh snapshot.
	f.pos.Store(0)
	f.state = state{LeaderID: leaderID, Pos: 0, Syncing: true}
	if err := f.save(true); err != nil {
		return err
	}
	// 2. Drop whatever was here (it may contain writes the leader never had).
	if err := f.db.Flush(); err != nil {
		return err
	}
	// 3. Stream the snapshot in.
	for {
		typ, p, err := readFrame(r)
		if err != nil {
			return err
		}
		switch typ {
		case frameSnap:
			_, ops, err := decodeBatch(p)
			if err != nil {
				return err
			}
			if err := f.db.Apply(ops); err != nil {
				return err
			}
		case frameSnapEnd:
			f.state.Syncing = false
			f.pos.Store(snapSeq)
			f.leaderSeq.Store(snapSeq)
			return f.save(true)
		default:
			return fmt.Errorf("%w: unexpected frame %d during snapshot", ErrProtocol, typ)
		}
	}
}

func (f *Follower) stream(c net.Conn, r *bufio.Reader, w *bufio.Writer) error {
	var acked uint64 = f.pos.Load()
	ack := func(seq uint64) error {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], seq)
		if err := writeFrame(w, frameAck, b[:]); err != nil {
			return err
		}
		return w.Flush()
	}
	if err := ack(acked); err != nil { // lets the leader count us right away
		return err
	}
	for {
		c.SetReadDeadline(time.Now().Add(10 * time.Second)) // leader pings every second
		typ, p, err := readFrame(r)
		if err != nil {
			return err
		}
		switch typ {
		case framePing:
			if len(p) == 8 {
				f.leaderSeq.Store(binary.LittleEndian.Uint64(p))
			}
		case frameBatch:
			first, ops, err := decodeBatch(p)
			if err != nil {
				return err
			}
			last := first + uint64(len(ops)) - 1
			if last <= f.pos.Load() {
				continue // already applied (overlap with a snapshot or a replay)
			}
			if err := f.db.Apply(ops); err != nil { // returns once durable per this node's fsync policy
				return fmt.Errorf("apply: %w", err)
			}
			f.pos.Store(last)
			if last > f.leaderSeq.Load() {
				f.leaderSeq.Store(last)
			}
			_ = f.save(false)
			// Coalesce acks: only answer when nothing else is already waiting.
			if r.Buffered() == 0 {
				if err := ack(last); err != nil {
					return err
				}
				acked = last
			}
		default:
			return fmt.Errorf("%w: unexpected frame %d", ErrProtocol, typ)
		}
		_ = acked
	}
}

// Stats for INFO.
func (f *Follower) Stats() []engine.Stat {
	last, _ := f.lastErr.Load().(string)
	lag := int64(f.leaderSeq.Load()) - int64(f.pos.Load())
	if lag < 0 {
		lag = 0
	}
	return []engine.Stat{
		{Name: "repl_role", Value: "replica"}, {Name: "repl_leader", Value: f.addr},
		{Name: "repl_link", Value: map[bool]string{true: "up", false: "down"}[f.connected.Load()]},
		{Name: "repl_applied_seq", Value: fmt.Sprint(f.pos.Load())}, {Name: "repl_lag_ops", Value: fmt.Sprint(lag)},
		{Name: "repl_full_resyncs", Value: fmt.Sprint(f.resyncs.Load())}, {Name: "repl_last_error", Value: last},
	}
}
