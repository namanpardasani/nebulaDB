// Package nebuladb is the embedded Go API: open a NebulaDB database directly in
// your process, no server or network involved.
//
//	db, err := nebuladb.Open("./data", nil)
//	if err != nil { ... }
//	defer db.Close()
//
//	db.Set([]byte("user:1"), []byte("ada"))
//	v, err := db.Get([]byte("user:1")) // v == "ada"
//	db.SetTTL([]byte("session"), []byte("x"), time.Minute)
//
// All methods are safe for concurrent use. With the default options a write
// returns only after it has been fsynced (group-committed with concurrent
// writers), so an acknowledged write survives a crash or power loss.
package nebuladb

import (
	"errors"
	"log/slog"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/wal"
)

// ErrNotFound is returned by Get when the key does not exist (or has expired).
var ErrNotFound = errors.New("nebuladb: key not found")

// ErrNotInteger is returned by Incr when the stored value is not an integer.
var ErrNotInteger = engine.ErrNotInteger

// SyncPolicy selects the durability level of acknowledged writes.
type SyncPolicy int

const (
	// SyncAlways fsyncs before acknowledging (default). No acknowledged write is
	// lost on process kill or power failure.
	SyncAlways SyncPolicy = iota
	// SyncEvery fsyncs every SyncInterval. Survives process kill; power loss may
	// lose up to one interval of acknowledged writes.
	SyncEvery
	// SyncNever never fsyncs. Survives process kill only. Unsafe.
	SyncNever
)

// Options configures Open. The zero value (or nil) gives safe defaults.
type Options struct {
	Sync         SyncPolicy
	SyncInterval time.Duration // for SyncEvery; default 100ms
	// DisableGroupCommit makes every write do its own fsync (slower; for
	// measurement and comparison).
	DisableGroupCommit bool
	// MemtableSize is the in-memory buffer size before it is sealed; default 4 MiB.
	MemtableSize int64
	Logger       *slog.Logger

	// CompactionStyle is "leveled" (default) or "size-tiered".
	CompactionStyle string
	// BlockCacheBytes sizes the block cache: 0 = default (32 MiB), <0 = disabled.
	BlockCacheBytes int64
	// BloomBitsPerKey: 0 = default (10), <0 = no Bloom filters.
	BloomBitsPerKey int
	// Compaction tuning (0 = engine default): L0 file-count trigger, L1 target
	// size, size ratio between levels, output file size.
	L0CompactTrigger int
	L1MaxBytes       int64
	LevelSizeRatio   int
	TargetFileBytes  int64
}

// DB is an open database.
type DB struct{ e *engine.DB }

// Open opens or creates the database in dir, recovering from the write-ahead
// log if the previous run did not shut down cleanly.
func Open(dir string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	p := wal.SyncAlways
	switch opts.Sync {
	case SyncEvery:
		p = wal.SyncEvery
	case SyncNever:
		p = wal.SyncNever
	}
	bloom := opts.BloomBitsPerKey
	switch {
	case bloom == 0:
		bloom = 10
	case bloom < 0:
		bloom = 0
	}
	e, _, err := engine.Open(engine.Options{
		Dir: dir, Sync: p, SyncEvery: opts.SyncInterval,
		GroupCommit: !opts.DisableGroupCommit, MemtableSize: opts.MemtableSize, BloomBits: bloom, Logger: opts.Logger,
		BlockCacheBytes: opts.BlockCacheBytes,
		Compaction: engine.CompactionOptions{
			Style: opts.CompactionStyle, L0CompactTrigger: opts.L0CompactTrigger, L1MaxBytes: opts.L1MaxBytes,
			LevelSizeRatio: opts.LevelSizeRatio, TargetFileBytes: opts.TargetFileBytes,
		},
	})
	if err != nil {
		return nil, err
	}
	return &DB{e: e}, nil
}

// Close flushes and closes the database.
func (d *DB) Close() error { return d.e.Close() }

// Get returns the value for key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) {
	v, ok, err := d.e.Get(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// Set stores value under key with no expiry.
func (d *DB) Set(key, value []byte) error {
	_, err := d.e.Set(key, value, engine.SetOptions{})
	return err
}

// SetTTL stores value under key, expiring after ttl.
func (d *DB) SetTTL(key, value []byte, ttl time.Duration) error {
	_, err := d.e.Set(key, value, engine.SetOptions{ExpireAtMs: time.Now().Add(ttl).UnixMilli()})
	return err
}

// SetNX stores value only if key does not exist; reports whether it did.
func (d *DB) SetNX(key, value []byte) (bool, error) {
	return d.e.Set(key, value, engine.SetOptions{NX: true})
}

// Delete removes key and reports whether it existed.
func (d *DB) Delete(key []byte) (bool, error) {
	n, err := d.e.Del([][]byte{key})
	return n == 1, err
}

// Incr atomically adds delta to the integer at key (missing = 0).
func (d *DB) Incr(key []byte, delta int64) (int64, error) { return d.e.IncrBy(key, delta) }

// Expire sets key to expire after ttl; reports whether the key existed.
func (d *DB) Expire(key []byte, ttl time.Duration) (bool, error) {
	return d.e.SetExpiry(key, time.Now().Add(ttl).UnixMilli())
}

// TTL returns the remaining lifetime. ok is false if the key does not exist;
// a zero duration with ok true means the key never expires.
func (d *DB) TTL(key []byte) (ttl time.Duration, ok bool, err error) {
	ms, err := d.e.TTL(key)
	switch {
	case err != nil:
		return 0, false, err
	case ms == -2:
		return 0, false, nil
	case ms == -1:
		return 0, true, nil
	}
	return time.Duration(ms) * time.Millisecond, true, nil
}

// MSet atomically stores key/value pairs (pairs = k1, v1, k2, v2, ...): after
// a crash either all are present or none.
func (d *DB) MSet(pairs ...[]byte) error { return d.e.MSet(pairs) }

// Keys returns up to limit live keys >= start in ascending order and the key
// to pass as start for the next page (nil when finished).
func (d *DB) Keys(start []byte, limit int) (keys [][]byte, next []byte, err error) {
	return d.e.Scan(start, limit)
}

// Stats returns engine statistics as name/value pairs.
func (d *DB) Stats() map[string]string {
	m := map[string]string{}
	for _, s := range d.e.Stats() {
		m[s.Name] = s.Value
	}
	return m
}

// FlushAndWait seals the in-memory buffer and waits until it is on disk
// (useful before measuring on-disk size).
func (d *DB) FlushAndWait() error { return d.e.FlushMemtable() }

// CompactAll flushes and merges all data down to the deepest level.
func (d *DB) CompactAll() error { return d.e.CompactAll() }

// WaitIdle blocks until background flushes are done.
func (d *DB) WaitIdle() error { return d.e.WaitForFlush() }
