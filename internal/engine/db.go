package engine

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/memtable"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

// Options configures the LSM engine (DB).
type Options struct {
	FS  vfs.FS // nil = real filesystem
	Dir string

	Sync        wal.SyncPolicy
	SyncEvery   time.Duration
	GroupCommit bool

	// MemtableSize is the approximate byte size at which the active memtable is
	// sealed (becomes immutable) and a new memtable + WAL segment start.
	MemtableSize int64

	Now    func() int64 // unix ms clock; nil = wall clock
	Logger *slog.Logger
}

func (o *Options) fill() {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 << 20
	}
	if o.Now == nil {
		o.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// immutable is a sealed memtable awaiting flush to an SSTable.
type immutable struct {
	mt *memtable.Memtable
	// walBoundary: every record of this memtable lives in WAL segments numbered
	// below it, so once the memtable is safely in an SSTable, segments
	// < walBoundary can be deleted.
	walBoundary uint64
}

// DB is the LSM-tree engine.
//
// # Write path
//
//	command -> key stripe lock(s) -> read current state, decide, build ops
//	        -> wal.Commit (group commit: assign seq, write, fsync per policy)
//	        -> [WAL leader, in log order] insert into active memtable
//	        -> publish visible seq -> maybe seal memtable -> ack client
//
// # Read path
//
// A reader loads the published `visible` sequence number as its snapshot, then
// searches the active memtable and the immutable memtables newest-first for the
// newest version with seq <= snapshot. Because `visible` only advances after a
// whole batch is inserted, readers see multi-key batches atomically and never
// see a write that is not yet durable.
type DB struct {
	opts Options
	wal  *wal.WAL

	mu     sync.RWMutex // guards mem, imms, closed
	mem    *memtable.Memtable
	imms   []*immutable // newest first
	closed bool

	visible atomic.Uint64 // highest fully applied sequence number
	kl      keyLocks

	recovery   wal.RecoveryInfo
	rotations  atomic.Uint64
	rotateErrs atomic.Uint64
}

// Open opens (creating if needed) the database in opts.Dir, replaying the WAL.
func Open(opts Options) (*DB, wal.RecoveryInfo, error) {
	opts.fill()
	d := &DB{opts: opts, mem: memtable.New()}
	w, info, err := wal.Open(wal.Options{
		FS: opts.FS, Dir: opts.Dir, Policy: opts.Sync, Interval: opts.SyncEvery,
		GroupCommit: opts.GroupCommit, Logger: opts.Logger,
	}, func(b *wal.Batch) error {
		d.insert(b.Seq, b.Ops)
		return nil
	})
	if err != nil {
		return nil, info, err
	}
	d.wal = w
	d.recovery = info
	d.visible.Store(info.LastSeq)
	d.maybeRotate() // replay may have produced an over-threshold memtable
	return d, info, nil
}

// insert adds ops (first op has sequence number first) to the active memtable.
// Called only from the WAL leader (or single-threaded recovery).
func (d *DB) insert(first uint64, ops []wal.Op) {
	d.mu.RLock()
	mem := d.mem
	d.mu.RUnlock()
	for i := range ops {
		op := &ops[i]
		mem.Add(first+uint64(i), iterator.Kind(op.Type), op.Key, op.Value, op.ExpireAt)
	}
}

// commit makes ops durable and visible.
func (d *DB) commit(ops []wal.Op) error {
	return d.wal.Commit(&wal.Batch{Ops: ops}, func(first uint64) {
		d.insert(first, ops)
		d.visible.Store(first + uint64(len(ops)) - 1)
		d.maybeRotate()
	})
}

// maybeRotate seals the active memtable if it has outgrown MemtableSize.
// It runs on the WAL leader, so no other writer is inserting concurrently.
func (d *DB) maybeRotate() {
	d.mu.RLock()
	mem := d.mem
	d.mu.RUnlock()
	if mem.ApproxSize() < d.opts.MemtableSize {
		return
	}
	// 1. Seal the WAL segment (fsync) and start a new one. If this fails we
	//    simply keep the big memtable and try again on the next write.
	boundary, err := d.wal.Rotate()
	if err != nil {
		d.rotateErrs.Add(1)
		d.opts.Logger.Error("memtable rotation failed", "err", err)
		return
	}
	// 2. Swap memtables. Readers holding the old pointer still see all data.
	d.mu.Lock()
	d.imms = append([]*immutable{{mt: d.mem, walBoundary: boundary}}, d.imms...)
	d.mem = memtable.New()
	d.mu.Unlock()
	d.rotations.Add(1)
	d.opts.Logger.Debug("memtable sealed", "wal_boundary", boundary)
}

// view captures a consistent read view: the snapshot sequence number plus the
// memtables that contain everything up to it.
type view struct {
	snap uint64
	mem  *memtable.Memtable
	imms []*immutable
	now  int64
}

func (d *DB) view() (view, error) {
	snap := d.visible.Load() // before copying tables: tables only ever grow
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return view{}, ErrClosed
	}
	return view{snap: snap, mem: d.mem, imms: d.imms, now: d.opts.Now()}, nil
}

// lookup finds the newest version of key visible in v. found is false if the
// key has never been written; the returned entry may be a tombstone or expired.
func (v *view) lookup(key []byte) (iterator.Entry, bool) {
	if e, ok := v.mem.Get(key, v.snap); ok {
		return e, true
	}
	for _, im := range v.imms {
		if e, ok := im.mt.Get(key, v.snap); ok {
			return e, true
		}
	}
	return iterator.Entry{}, false
}

// live returns the current live value of key.
func (v *view) live(key []byte) (e iterator.Entry, ok bool) {
	e, found := v.lookup(key)
	if !found || e.Kind == iterator.KindDelete || e.Expired(v.now) {
		return e, false
	}
	return e, true
}

func (d *DB) Get(key []byte) ([]byte, bool, error) {
	v, err := d.view()
	if err != nil {
		return nil, false, err
	}
	e, ok := v.live(key)
	if !ok {
		return nil, false, nil
	}
	return clone(e.Value), true, nil
}

// MGet reads keys from a single snapshot (see MultiGetter).
func (d *DB) MGet(keys [][]byte) ([][]byte, []bool, error) {
	v, err := d.view()
	if err != nil {
		return nil, nil, err
	}
	vals := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	for i, k := range keys {
		if e, ok := v.live(k); ok {
			vals[i], found[i] = clone(e.Value), true
		}
	}
	return vals, found, nil
}

func (d *DB) Exists(key []byte) (bool, error) {
	v, err := d.view()
	if err != nil {
		return false, err
	}
	_, ok := v.live(key)
	return ok, nil
}

func (d *DB) TTL(key []byte) (int64, error) {
	v, err := d.view()
	if err != nil {
		return 0, err
	}
	e, ok := v.live(key)
	switch {
	case !ok:
		return -2, nil
	case e.ExpireAt == 0:
		return -1, nil
	}
	return e.ExpireAt - v.now, nil
}

func (d *DB) Set(key, value []byte, opt SetOptions) (bool, error) {
	if _, err := d.view(); err != nil {
		return false, err
	}
	defer d.kl.lock(key)()
	v, _ := d.view()
	old, exists := v.live(key)
	if (opt.NX && exists) || (opt.XX && !exists) {
		return false, nil
	}
	exp := opt.ExpireAtMs
	if opt.KeepTTL {
		exp = 0
		if exists {
			exp = old.ExpireAt
		}
	}
	err := d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: value, ExpireAt: exp}})
	return err == nil, err
}

func (d *DB) MSet(pairs [][]byte) error {
	if _, err := d.view(); err != nil {
		return err
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
	defer d.kl.lock(keys...)()
	return d.commit(ops)
}

func (d *DB) Del(keys [][]byte) (int, error) {
	if _, err := d.view(); err != nil {
		return 0, err
	}
	defer d.kl.lock(keys...)()
	v, _ := d.view()
	var ops []wal.Op
	seen := map[string]bool{}
	for _, k := range keys {
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		if _, ok := v.live(k); ok {
			ops = append(ops, wal.Op{Type: wal.OpDelete, Key: k})
		}
	}
	if len(ops) == 0 {
		return 0, nil
	}
	if err := d.commit(ops); err != nil {
		return 0, err
	}
	return len(ops), nil
}

func (d *DB) SetExpiry(key []byte, atMs int64) (bool, error) {
	if _, err := d.view(); err != nil {
		return false, err
	}
	defer d.kl.lock(key)()
	v, _ := d.view()
	e, ok := v.live(key)
	if !ok {
		return false, nil
	}
	err := d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.Value, ExpireAt: atMs}})
	return err == nil, err
}

func (d *DB) Persist(key []byte) (bool, error) {
	if _, err := d.view(); err != nil {
		return false, err
	}
	defer d.kl.lock(key)()
	v, _ := d.view()
	e, ok := v.live(key)
	if !ok || e.ExpireAt == 0 {
		return false, nil
	}
	err := d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.Value}})
	return err == nil, err
}

func (d *DB) IncrBy(key []byte, delta int64) (int64, error) {
	if _, err := d.view(); err != nil {
		return 0, err
	}
	defer d.kl.lock(key)()
	v, _ := d.view()
	e, ok := v.live(key)
	var cur int64
	if ok {
		n, err := strconv.ParseInt(string(e.Value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	}
	if (delta > 0 && cur > 1<<63-1-delta) || (delta < 0 && cur < -1<<63-delta) {
		return 0, ErrNotInteger
	}
	cur += delta
	exp := int64(0)
	if ok {
		exp = e.ExpireAt // INCR preserves the TTL
	}
	if err := d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: strconv.AppendInt(nil, cur, 10), ExpireAt: exp}}); err != nil {
		return 0, err
	}
	return cur, nil
}

// newMerged builds the merged version iterator over the view's memtables.
func (v *view) newMerged() *iterator.Merge {
	children := []iterator.Iterator{v.mem.NewIterator()}
	for _, im := range v.imms {
		children = append(children, im.mt.NewIterator())
	}
	return iterator.NewMerge(children)
}

func (d *DB) Scan(start []byte, limit int) ([][]byte, []byte, error) {
	v, err := d.view()
	if err != nil {
		return nil, nil, err
	}
	if limit < 1 {
		limit = 1
	}
	it := iterator.NewView(v.newMerged(), v.snap, v.now)
	defer it.Close()
	it.Seek(start)
	var keys [][]byte
	for it.Valid() && len(keys) < limit {
		keys = append(keys, clone(it.Key()))
		it.Next()
	}
	if err := it.Err(); err != nil {
		return nil, nil, err
	}
	var next []byte
	if it.Valid() {
		next = clone(it.Key())
	}
	return keys, next, nil
}

// ApproxSize estimates the number of live keys as (puts - tombstones) across
// the memtables. It over-counts overwritten and expired keys and (once
// SSTables exist) data below the memtables; computing the exact number would
// need a full merge. DBSIZE is documented as approximate for this reason.
func (d *DB) ApproxSize() (int64, error) {
	v, err := d.view()
	if err != nil {
		return 0, err
	}
	n := v.mem.Len() - 2*v.mem.Tombstones()
	for _, im := range v.imms {
		n += im.mt.Len() - 2*im.mt.Tombstones()
	}
	if n < 0 {
		n = 0
	}
	return n, nil
}

// flushChunk bounds the size of one FLUSHDB delete batch.
const flushChunk = 10000

// Flush deletes every key. Deletes are written in batches of up to flushChunk
// keys; each batch is atomic, but a crash between batches leaves a subset of
// keys deleted. (A single range-tombstone record would be atomic; deliberately
// not implemented — FLUSHDB is an administrative command.)
func (d *DB) Flush() error {
	if _, err := d.view(); err != nil {
		return err
	}
	defer d.kl.lockAll()()
	v, _ := d.view()
	it := iterator.NewView(v.newMerged(), v.snap, v.now)
	defer it.Close()
	var ops []wal.Op
	for it.SeekToFirst(); it.Valid(); it.Next() {
		ops = append(ops, wal.Op{Type: wal.OpDelete, Key: clone(it.Key())})
		if len(ops) >= flushChunk {
			if err := d.commit(ops); err != nil {
				return err
			}
			ops = ops[:0]
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	if len(ops) > 0 {
		return d.commit(ops)
	}
	return nil
}

func (d *DB) Stats() []Stat {
	d.mu.RLock()
	mem, imms := d.mem, d.imms
	d.mu.RUnlock()
	ws := d.wal.Stats()
	u := func(n uint64) string { return strconv.FormatUint(n, 10) }
	st := []Stat{
		{"engine", "lsm"},
		{"last_seq", u(d.visible.Load())},
		{"memtable_bytes", strconv.FormatInt(mem.ApproxSize(), 10)},
		{"memtable_entries", strconv.FormatInt(mem.Len(), 10)},
		{"immutable_memtables", strconv.Itoa(len(imms))},
		{"memtable_rotations", u(d.rotations.Load())},
		{"wal_segment", u(d.wal.ActiveSegment())},
		{"wal_active_bytes", strconv.FormatInt(d.wal.Size(), 10)},
		{"wal_bytes_written", u(ws.Bytes)},
		{"wal_batches", u(ws.Batches)},
		{"wal_groups", u(ws.Groups)},
		{"wal_fsyncs", u(ws.Syncs)},
		{"wal_max_group", u(ws.MaxGroup)},
		{"recovery_batches", strconv.Itoa(d.recovery.Batches)},
		{"recovery_torn_tail", strconv.FormatBool(d.recovery.TornTail)},
		{"recovery_truncated_bytes", strconv.FormatInt(d.recovery.TruncatedBytes, 10)},
	}
	if err := d.wal.Err(); err != nil {
		st = append(st, Stat{"wal_failed", fmt.Sprint(err)})
	}
	return st
}

// Close syncs and closes the WAL. Callers must have stopped issuing writes.
func (d *DB) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	return d.wal.Close()
}

var (
	_ Engine      = (*DB)(nil)
	_ MultiGetter = (*DB)(nil)
)
