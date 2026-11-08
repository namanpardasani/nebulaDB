// Package engine defines the storage Engine interface the server talks to.
//
// Two implementations exist over the project's life: Mem (this phase, a plain
// map used to build and test the network layer) and the LSM engine (later
// phases). Both must satisfy the same behavioural contract, which the shared
// conformance tests in conformance_test.go enforce.
//
// Time: expiry timestamps are absolute Unix milliseconds. 0 means "no expiry".
package engine

import (
	"errors"
)

// Errors returned by engines.
var (
	// ErrNotInteger: value is not a base-10 int64 (INCR on text).
	ErrNotInteger = errors.New("value is not an integer or out of range")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("engine closed")
)

// SetOptions are the conditions/options of a Set.
type SetOptions struct {
	ExpireAtMs int64 // absolute expiry, 0 = none
	NX         bool  // only set if the key does not exist
	XX         bool  // only set if the key exists
	KeepTTL    bool  // retain the existing key's expiry (ignores ExpireAtMs)
}

// Stat is one line of engine statistics for INFO.
type Stat struct {
	Name  string
	Value string
}

// Engine is a sorted, durable-or-not key-value store with TTLs.
//
// All methods are safe for concurrent use. Each method is atomic with respect
// to the others (e.g. IncrBy is a true read-modify-write; Set with NX is a true
// check-and-set). Returned byte slices belong to the caller.
type Engine interface {
	// Get returns the live value for key.
	Get(key []byte) (value []byte, found bool, err error)
	// Set writes key. It returns false (and writes nothing) when NX/XX
	// conditions fail.
	Set(key, value []byte, opt SetOptions) (applied bool, err error)
	// MSet writes all pairs atomically (pairs = k1,v1,k2,v2,...).
	MSet(pairs [][]byte) error
	// Del removes keys and returns how many existed.
	Del(keys [][]byte) (removed int, err error)
	// Exists reports whether key is live.
	Exists(key []byte) (bool, error)
	// SetExpiry sets an absolute expiry on an existing key. Returns false if
	// the key does not exist. atMs<=0 is not allowed; use Persist.
	SetExpiry(key []byte, atMs int64) (bool, error)
	// Persist removes a key's expiry. Returns true if the key existed with one.
	Persist(key []byte) (bool, error)
	// TTL returns remaining milliseconds: -2 if the key does not exist, -1 if
	// it has no expiry.
	TTL(key []byte) (ms int64, err error)
	// IncrBy atomically adds delta to the integer stored at key (missing = 0),
	// preserving any TTL. Overflow returns ErrNotInteger.
	IncrBy(key []byte, delta int64) (int64, error)
	// Scan returns up to limit live keys in ascending order starting at the
	// first key >= start (start==nil begins at the beginning). next is the
	// resume point (the first key not returned) or nil when the keyspace is
	// exhausted. Keys written or deleted during iteration may or may not be
	// seen, but a key that exists for the whole scan is returned exactly once.
	Scan(start []byte, limit int) (keys [][]byte, next []byte, err error)
	// ApproxSize is the (approximate) number of live keys. Approximate because
	// an LSM tree cannot count without merging every level: it may include
	// overwritten, deleted-but-not-yet-compacted and expired entries.
	ApproxSize() (int64, error)
	// Flush deletes every key (FLUSHDB).
	Flush() error
	// Stats returns engine statistics for INFO.
	Stats() []Stat
	// Close flushes and releases resources.
	Close() error
}

// MultiGetter is an optional Engine capability: read several keys from ONE
// consistent snapshot, so a concurrent multi-key write (MSET) is seen either
// entirely or not at all. The server's MGET uses it when available.
type MultiGetter interface {
	MGet(keys [][]byte) (values [][]byte, found []bool, err error)
}
