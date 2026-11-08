package engine

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/naman/nebuladb/internal/wal"
)

type memEntry struct {
	val      []byte
	expireAt int64
}

// Mem is a map-backed Engine. With WAL options (OpenMem) it is *durable*: every
// mutation is logged and made durable per the fsync policy before it is applied
// and acknowledged, and the map is rebuilt from the log on restart. It is the
// vehicle for proving the write path and recovery before the real LSM tree
// exists, and stays useful afterwards as a simple reference implementation.
//
// # Concurrency design (the same shape the LSM engine uses)
//
//   - Readers take mu.RLock.
//   - Writers first take a per-key *stripe lock* (hash(key) % 256). The stripe
//     is held across read -> decide -> WAL commit -> apply, which is what makes
//     read-modify-write commands (INCR, SET NX, EXPIRE) linearizable per key.
//     Different keys use different stripes, so concurrent writers still reach
//     the WAL together and share one fsync (group commit).
//   - Multi-key operations lock their stripes in ascending order (no deadlock).
//   - The WAL calls our apply callback after the batch is durable, one batch at
//     a time in log order; only then is the map updated (mu.Lock). So readers
//     never observe a write that is not yet durable.
type Mem struct {
	mu   sync.RWMutex
	m    map[string]memEntry
	now  func() int64 // unix ms; injectable for tests
	wal  *wal.WAL     // nil => volatile
	stat struct{ recovered wal.RecoveryInfo }

	kl     keyLocks
	closed bool
}

// NewMem returns an empty volatile engine using the wall clock.
func NewMem() *Mem {
	return NewMemClock(func() int64 { return time.Now().UnixMilli() })
}

// NewMemClock is NewMem with an injectable clock (unix ms).
func NewMemClock(now func() int64) *Mem {
	return &Mem{m: map[string]memEntry{}, now: now}
}

// MemOptions configures a durable Mem engine.
type MemOptions struct {
	Now func() int64 // nil = wall clock
	WAL wal.Options
}

// OpenMem opens (recovering if needed) a durable engine.
func OpenMem(o MemOptions) (*Mem, wal.RecoveryInfo, error) {
	m := NewMem()
	if o.Now != nil {
		m.now = o.Now
	}
	w, info, err := wal.Open(o.WAL, func(b *wal.Batch) error {
		m.applyOps(b.Ops) // replay: same code path as live writes
		return nil
	})
	if err != nil {
		return nil, info, err
	}
	m.wal = w
	m.stat.recovered = info
	return m, info, nil
}

// applyOps updates the map. Called by the WAL leader after durability, and
// during replay.
func (m *Mem) applyOps(ops []wal.Op) {
	m.mu.Lock()
	for i := range ops {
		op := &ops[i]
		switch op.Type {
		case wal.OpPut:
			m.m[string(op.Key)] = memEntry{val: clone(op.Value), expireAt: op.ExpireAt}
		case wal.OpDelete:
			delete(m.m, string(op.Key))
		}
	}
	m.mu.Unlock()
}

// commit makes ops durable (if durable) and applies them.
func (m *Mem) commit(ops []wal.Op) error {
	if m.wal == nil {
		m.applyOps(ops)
		return nil
	}
	return m.wal.Commit(&wal.Batch{Ops: ops}, func(uint64) { m.applyOps(ops) })
}

// live returns the entry if present and unexpired. Caller holds mu (R or W).
func (m *Mem) live(key []byte) (memEntry, bool) {
	e, ok := m.m[string(key)]
	if !ok || (e.expireAt != 0 && e.expireAt <= m.now()) {
		return e, false
	}
	return e, true
}

func (m *Mem) lookup(key []byte) (memEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.live(key)
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }

func (m *Mem) isClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

func (m *Mem) Get(key []byte) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, false, ErrClosed
	}
	e, ok := m.live(key)
	if !ok {
		return nil, false, nil
	}
	return clone(e.val), true, nil
}

func (m *Mem) Set(key, value []byte, opt SetOptions) (bool, error) {
	if m.isClosed() {
		return false, ErrClosed
	}
	defer m.kl.lock(key)()
	old, exists := m.lookup(key)
	if (opt.NX && exists) || (opt.XX && !exists) {
		return false, nil
	}
	exp := opt.ExpireAtMs
	if opt.KeepTTL {
		exp = 0
		if exists {
			exp = old.expireAt
		}
	}
	err := m.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: value, ExpireAt: exp}})
	return err == nil, err
}

func (m *Mem) MSet(pairs [][]byte) error {
	if m.isClosed() {
		return ErrClosed
	}
	keys := make([][]byte, 0, len(pairs)/2)
	ops := make([]wal.Op, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, pairs[i])
		ops = append(ops, wal.Op{Type: wal.OpPut, Key: pairs[i], Value: pairs[i+1]})
	}
	if len(ops) == 0 {
		return nil
	}
	defer m.kl.lock(keys...)()
	return m.commit(ops) // one WAL record => all-or-nothing across a crash
}

func (m *Mem) Del(keys [][]byte) (int, error) {
	if m.isClosed() {
		return 0, ErrClosed
	}
	defer m.kl.lock(keys...)()
	var ops []wal.Op
	seen := map[string]bool{}
	for _, k := range keys {
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		if _, ok := m.lookup(k); ok {
			ops = append(ops, wal.Op{Type: wal.OpDelete, Key: k})
		}
	}
	if len(ops) == 0 {
		return 0, nil
	}
	if err := m.commit(ops); err != nil {
		return 0, err
	}
	return len(ops), nil
}

func (m *Mem) Exists(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return false, ErrClosed
	}
	_, ok := m.live(key)
	return ok, nil
}

func (m *Mem) SetExpiry(key []byte, atMs int64) (bool, error) {
	if m.isClosed() {
		return false, ErrClosed
	}
	defer m.kl.lock(key)()
	e, ok := m.lookup(key)
	if !ok {
		return false, nil
	}
	err := m.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.val, ExpireAt: atMs}})
	return err == nil, err
}

func (m *Mem) Persist(key []byte) (bool, error) {
	if m.isClosed() {
		return false, ErrClosed
	}
	defer m.kl.lock(key)()
	e, ok := m.lookup(key)
	if !ok || e.expireAt == 0 {
		return false, nil
	}
	err := m.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.val}})
	return err == nil, err
}

func (m *Mem) TTL(key []byte) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0, ErrClosed
	}
	e, ok := m.live(key)
	switch {
	case !ok:
		return -2, nil
	case e.expireAt == 0:
		return -1, nil
	}
	return e.expireAt - m.now(), nil
}

func (m *Mem) IncrBy(key []byte, delta int64) (int64, error) {
	if m.isClosed() {
		return 0, ErrClosed
	}
	defer m.kl.lock(key)()
	e, ok := m.lookup(key)
	var cur int64
	if ok {
		n, err := strconv.ParseInt(string(e.val), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	}
	// Overflow-safe add.
	if (delta > 0 && cur > 1<<63-1-delta) || (delta < 0 && cur < -1<<63-delta) {
		return 0, ErrNotInteger
	}
	cur += delta
	exp := int64(0)
	if ok {
		exp = e.expireAt // INCR preserves the TTL
	}
	if err := m.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: strconv.AppendInt(nil, cur, 10), ExpireAt: exp}}); err != nil {
		return 0, err
	}
	return cur, nil
}

func (m *Mem) Scan(start []byte, limit int) ([][]byte, []byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, nil, ErrClosed
	}
	now := m.now()
	all := make([]string, 0, len(m.m))
	for k, e := range m.m {
		if e.expireAt != 0 && e.expireAt <= now {
			continue
		}
		if string(start) <= k {
			all = append(all, k)
		}
	}
	sort.Strings(all)
	if limit < 1 {
		limit = 1
	}
	var next []byte
	if len(all) > limit {
		next = []byte(all[limit])
		all = all[:limit]
	}
	out := make([][]byte, len(all))
	for i, k := range all {
		out[i] = []byte(k)
	}
	return out, next, nil
}

func (m *Mem) ApproxSize() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.m)), nil
}

// Flush deletes every key as ONE atomic batch (a single WAL record), so a crash
// leaves either the old data or an empty database.
func (m *Mem) Flush() error {
	if m.isClosed() {
		return ErrClosed
	}
	defer m.kl.lockAll()()
	m.mu.RLock()
	ops := make([]wal.Op, 0, len(m.m))
	for k := range m.m {
		ops = append(ops, wal.Op{Type: wal.OpDelete, Key: []byte(k)})
	}
	m.mu.RUnlock()
	if len(ops) == 0 {
		return nil
	}
	return m.commit(ops)
}

func (m *Mem) Stats() []Stat {
	m.mu.RLock()
	n := len(m.m)
	m.mu.RUnlock()
	st := []Stat{{"engine", "mem"}, {"keys", strconv.Itoa(n)}}
	if m.wal != nil {
		ws := m.wal.Stats()
		st = append(st,
			Stat{"wal_segment", strconv.FormatUint(m.wal.ActiveSegment(), 10)},
			Stat{"wal_last_seq", strconv.FormatUint(m.wal.LastSeq(), 10)},
			Stat{"wal_bytes_written", strconv.FormatUint(ws.Bytes, 10)},
			Stat{"wal_batches", strconv.FormatUint(ws.Batches, 10)},
			Stat{"wal_groups", strconv.FormatUint(ws.Groups, 10)},
			Stat{"wal_fsyncs", strconv.FormatUint(ws.Syncs, 10)},
			Stat{"wal_max_group", strconv.FormatUint(ws.MaxGroup, 10)},
			Stat{"recovery_batches", strconv.Itoa(m.stat.recovered.Batches)},
			Stat{"recovery_torn_tail", strconv.FormatBool(m.stat.recovered.TornTail)},
			Stat{"recovery_truncated_bytes", strconv.FormatInt(m.stat.recovered.TruncatedBytes, 10)},
		)
	}
	return st
}

// Close syncs and closes the log. Callers must have stopped issuing writes.
func (m *Mem) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()
	if m.wal != nil {
		return m.wal.Close()
	}
	return nil
}
