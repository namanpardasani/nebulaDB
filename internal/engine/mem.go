package engine

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

type memEntry struct {
	val      []byte
	expireAt int64
}

// Mem is a non-durable in-memory Engine. It exists to build and test the
// network layer before the real storage engine, and as the oracle-like
// reference for the conformance suite.
type Mem struct {
	mu     sync.Mutex
	m      map[string]memEntry
	now    func() int64 // unix ms; injectable for tests
	closed bool
}

// NewMem returns an empty in-memory engine using the wall clock.
func NewMem() *Mem {
	return NewMemClock(func() int64 { return time.Now().UnixMilli() })
}

// NewMemClock is NewMem with an injectable clock (unix ms).
func NewMemClock(now func() int64) *Mem {
	return &Mem{m: map[string]memEntry{}, now: now}
}

// live returns the entry if present and unexpired, lazily deleting expired
// ones. Caller holds m.mu.
func (m *Mem) live(key []byte) (memEntry, bool) {
	e, ok := m.m[string(key)]
	if !ok {
		return e, false
	}
	if e.expireAt != 0 && e.expireAt <= m.now() {
		delete(m.m, string(key))
		return e, false
	}
	return e, true
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }

func (m *Mem) Get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, ErrClosed
	}
	old, exists := m.live(key)
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
	m.m[string(key)] = memEntry{val: clone(value), expireAt: exp}
	return true, nil
}

func (m *Mem) MSet(pairs [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		m.m[string(pairs[i])] = memEntry{val: clone(pairs[i+1])}
	}
	return nil
}

func (m *Mem) Del(keys [][]byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	n := 0
	for _, k := range keys {
		if _, ok := m.live(k); ok {
			delete(m.m, string(k))
			n++
		}
	}
	return n, nil
}

func (m *Mem) Exists(key []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, ErrClosed
	}
	_, ok := m.live(key)
	return ok, nil
}

func (m *Mem) SetExpiry(key []byte, atMs int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, ErrClosed
	}
	e, ok := m.live(key)
	if !ok {
		return false, nil
	}
	e.expireAt = atMs
	m.m[string(key)] = e
	return true, nil
}

func (m *Mem) Persist(key []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, ErrClosed
	}
	e, ok := m.live(key)
	if !ok || e.expireAt == 0 {
		return false, nil
	}
	e.expireAt = 0
	m.m[string(key)] = e
	return true, nil
}

func (m *Mem) TTL(key []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	e, ok := m.live(key)
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
	e.val = strconv.AppendInt(nil, cur, 10)
	m.m[string(key)] = e // e.expireAt preserved (zero for new keys)
	return cur, nil
}

func (m *Mem) Scan(start []byte, limit int) ([][]byte, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.m)), nil
}

func (m *Mem) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	m.m = map[string]memEntry{}
	return nil
}

func (m *Mem) Stats() []Stat {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []Stat{{"engine", "mem"}, {"keys", strconv.Itoa(len(m.m))}}
}

func (m *Mem) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
