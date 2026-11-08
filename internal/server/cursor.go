package server

import "sync"

// cursorTable maps the integer SCAN cursors that Redis clients expect to the
// engine's real resume point (a key). Redis cursors are opaque uint64 values
// that clients echo back; an LSM scan resumes from a *key*, which cannot be
// packed into 64 bits losslessly. So we hand out small integer ids and
// remember the key server-side.
//
// The table is bounded (oldest ids are evicted first); a client that presents
// an evicted or fabricated cursor gets "ERR invalid cursor" and restarts at 0.
// Cursor 0 always means "start" and "done", as in Redis.
type cursorTable struct {
	mu    sync.Mutex
	max   int
	next  uint64
	keys  map[uint64][]byte
	order []uint64 // FIFO of live ids for eviction
}

func newCursorTable(max int) *cursorTable {
	return &cursorTable{max: max, next: 1, keys: map[uint64][]byte{}}
}

// put stores a resume key and returns its cursor id (never 0).
func (t *cursorTable) put(key []byte) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.next
	t.next++
	t.keys[id] = append([]byte(nil), key...)
	t.order = append(t.order, id)
	for len(t.order) > t.max {
		delete(t.keys, t.order[0])
		t.order = t.order[1:]
	}
	return id
}

// get returns the resume key for id.
func (t *cursorTable) get(id uint64) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.keys[id]
	return k, ok
}
