package engine

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/cache"
	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/memtable"
	"github.com/naman/nebuladb/internal/sstable"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

// ErrWriteStall is returned when writers have been blocked for longer than
// Options.StallTimeout because background flushing cannot keep up (or is
// failing, e.g. disk full).
var ErrWriteStall = errors.New("write stalled: memtable flush is not keeping up")

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
	// MaxImmutables is how many sealed memtables may await flushing before
	// writers stall (backpressure). Default 2.
	MaxImmutables int
	// StallTimeout bounds how long a writer waits for flush progress. Default 30s.
	StallTimeout time.Duration

	// BlockCacheBytes sizes the SSTable block cache: 0 = default (32 MiB),
	// negative = disabled.
	BlockCacheBytes int64

	// Compaction tunes background compaction and L0 backpressure.
	Compaction CompactionOptions

	BlockSize   int // SSTable data block size (default 4096)
	BloomBits   int // Bloom filter bits per key (0 disables; default set by caller)
	ManifestMax int64

	Now     func() int64 // unix ms clock; nil = wall clock
	Logger  *slog.Logger
	OnEvent func(Event) // optional engine event sink (must not block)
}

func (o *Options) fill() {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 << 20
	}
	if o.MaxImmutables <= 0 {
		o.MaxImmutables = 2
	}
	if o.StallTimeout <= 0 {
		o.StallTimeout = 30 * time.Second
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.BlockCacheBytes == 0 {
		o.BlockCacheBytes = 32 << 20
	}
	o.Compaction.fill()
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
	// below it, so once the memtable is safely in an SSTable (and the MANIFEST
	// says so) segments < walBoundary can be deleted.
	walBoundary uint64
}

// DB is the LSM-tree engine.
//
// # Write path
//
//	command -> [stall if flush is behind] -> key stripe lock(s)
//	        -> read current state, decide, build ops
//	        -> wal.Commit (group commit: assign seq, write, fsync per policy)
//	        -> [WAL leader, in log order] insert into active memtable
//	        -> publish visible seq -> maybe seal memtable -> ack client
//
// # Read path
//
// A reader pins a view: the published `visible` sequence number as its
// snapshot, the active and immutable memtables, and a reference-counted
// version of the SSTable set. It searches memtable -> immutables (newest
// first) -> L0 files (newest first) -> L1.. (one candidate file per level) for
// the newest version with seq <= snapshot. Because `visible` only advances
// after a whole batch is inserted, readers see multi-key batches atomically and
// never see a write that is not yet durable.
//
// # Flush
//
// A background goroutine turns the oldest immutable memtable into an L0
// SSTable (temp file, fsync, rename, directory fsync), records it in the
// MANIFEST together with the WAL boundary, installs it into the in-memory
// version, and only then deletes the WAL segments it covered.
type DB struct {
	opts Options
	wal  *wal.WAL
	mf   *manifest.Manifest

	mu     sync.RWMutex // guards mem, imms, cur, tables, closed
	cond   *sync.Cond   // signalled when imms shrinks (writers waiting in stall)
	mem    *memtable.Memtable
	imms   []*immutable // newest first
	cur    *version
	tables map[uint64]*tableFile
	closed bool

	visible atomic.Uint64 // highest fully applied sequence number
	kl      keyLocks

	cache     *cache.Cache // nil when disabled
	flushCh   chan struct{}
	stopCh    chan struct{}
	flushWG   sync.WaitGroup
	compactCh chan struct{}
	compactWG sync.WaitGroup
	// compactMu serialises compactions (background worker vs CompactAll) and
	// guards compactPtr, the per-level round-robin pick position.
	compactMu  sync.Mutex
	compactPtr [manifest.NumLevels][]byte
	tstats     sstable.Stats
	recovery   wal.RecoveryInfo
	mrecover   manifest.Info

	rotations    atomic.Uint64
	rotateErrs   atomic.Uint64
	flushes      atomic.Uint64
	flushErrs    atomic.Uint64
	flushBytes   atomic.Uint64
	flushNanos   atomic.Uint64
	stallNanos   atomic.Uint64
	stalls       atomic.Uint64
	deletedFiles atomic.Uint64

	compactions    atomic.Uint64
	compactErrs    atomic.Uint64
	compactNanos   atomic.Uint64
	compactRead    atomic.Uint64
	compactWritten atomic.Uint64
	compactDropped atomic.Uint64
	throttled      atomic.Uint64 // nanoseconds slept by the rate limiter
	slowdowns      atomic.Uint64
	slowdownNanos  atomic.Uint64
	userBytes      atomic.Uint64 // key+value bytes accepted from clients

	sampleCtr atomic.Uint64
	samples   [sampleRing]atomic.Pointer[WriteSample]
	unlock    func() // releases the directory LOCK
}

func sstPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}

// Open opens (creating if needed) the database in opts.Dir.
//
// Recovery order matters:
//  1. MANIFEST: which SSTables are live, and the WAL boundary (LogNumber).
//  2. Open every live SSTable (a missing/corrupt live file is a hard error).
//  3. Delete files the MANIFEST does not know (half-written flush outputs, tables
//     replaced by a finished compaction whose deletion was interrupted).
//  4. WAL: delete segments below LogNumber, replay the rest into the memtable,
//     truncating a torn tail.
func Open(opts Options) (*DB, wal.RecoveryInfo, error) {
	opts.fill()
	d := &DB{
		opts: opts, mem: memtable.New(),
		tables:  map[uint64]*tableFile{},
		flushCh: make(chan struct{}, 1), stopCh: make(chan struct{}),
		compactCh: make(chan struct{}, 1),
	}
	d.cond = sync.NewCond(&d.mu)
	if opts.BlockCacheBytes > 0 {
		d.cache = cache.New(opts.BlockCacheBytes)
	}

	if err := opts.FS.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, wal.RecoveryInfo{}, err
	}
	unlock, err := lockDir(opts.Dir)
	if err != nil {
		return nil, wal.RecoveryInfo{}, err
	}
	d.unlock = unlock
	mf, minfo, err := manifest.Open(opts.FS, opts.Dir, manifest.Options{RewriteThreshold: opts.ManifestMax})
	if err != nil {
		unlock()
		return nil, wal.RecoveryInfo{}, err
	}
	d.mf, d.mrecover = mf, minfo
	state := mf.Current()

	d.cur = &version{}
	live := map[uint64]bool{}
	for lvl := range state.Levels {
		for _, fm := range state.Levels[lvl] {
			tf, err := d.openTable(fm)
			if err != nil {
				d.closeTables()
				mf.Close()
				unlock()
				return nil, wal.RecoveryInfo{}, fmt.Errorf("live table %06d.sst (level %d): %w", fm.Num, lvl, err)
			}
			d.cur.levels[lvl] = append(d.cur.levels[lvl], tf)
			tf.ref()
			live[fm.Num] = true
		}
	}
	d.cur.buildIndexes()
	d.cur.refs.Store(1)
	if err := d.removeOrphans(live); err != nil {
		d.closeTables()
		mf.Close()
		unlock()
		return nil, wal.RecoveryInfo{}, err
	}

	w, info, err := wal.Open(wal.Options{
		FS: opts.FS, Dir: opts.Dir, Policy: opts.Sync, Interval: opts.SyncEvery,
		GroupCommit: opts.GroupCommit, Logger: opts.Logger,
		MinSegment: state.LogNumber, StartSeq: state.LastSeq,
		AfterGroup: d.maybeRotate,
	}, func(b *wal.Batch) error {
		d.insert(b.Seq, b.Ops)
		return nil
	})
	if err != nil {
		d.closeTables()
		mf.Close()
		unlock()
		return nil, info, err
	}
	d.wal = w
	d.recovery = info
	last := info.LastSeq
	if state.LastSeq > last {
		last = state.LastSeq
	}
	d.visible.Store(last)
	d.emit(Event{Type: EventRecovery, Count: int64(info.Batches), Detail: fmt.Sprintf("tables=%d torn_tail=%v", d.cur.numFiles(), info.TornTail)})

	d.flushWG.Add(1)
	go d.flushLoop()
	d.compactWG.Add(1)
	go d.compactionLoop()
	d.kickCompaction() // the recovered tree may already need compaction
	d.maybeRotate()    // replay may have produced an over-threshold memtable
	return d, info, nil
}

// readerOpts builds reader options; the cache is only set when enabled (a nil
// *cache.Cache stored in the interface would not be a nil interface).
func (d *DB) readerOpts(num uint64) sstable.ReaderOptions {
	ro := sstable.ReaderOptions{FileNum: num, Stats: &d.tstats}
	if d.cache != nil {
		ro.Cache = d.cache
	}
	return ro
}

func (d *DB) openTable(fm manifest.FileMeta) (*tableFile, error) {
	path := sstPath(d.opts.Dir, fm.Num)
	r, err := sstable.Open(d.opts.FS, path, d.readerOpts(fm.Num))
	if err != nil {
		return nil, err
	}
	tf := &tableFile{meta: fm, r: r, path: path, db: d}
	d.tables[fm.Num] = tf
	return tf, nil
}

func (d *DB) closeTables() {
	for _, t := range d.tables {
		t.r.Close()
	}
}

// removeOrphans deletes SSTable and temp files that the MANIFEST does not list.
func (d *DB) removeOrphans(live map[uint64]bool) error {
	names, err := d.opts.FS.List(d.opts.Dir)
	if err != nil {
		return err
	}
	removed := false
	for _, n := range names {
		orphan := false
		if strings.HasSuffix(n, ".sst.tmp") {
			orphan = true
		} else if strings.HasSuffix(n, ".sst") {
			num, perr := strconv.ParseUint(strings.TrimSuffix(n, ".sst"), 10, 64)
			orphan = perr == nil && !live[num]
		}
		if orphan {
			if err := d.opts.FS.Remove(filepath.Join(d.opts.Dir, n)); err != nil {
				return err
			}
			d.opts.Logger.Warn("removed orphan file", "file", n)
			removed = true
		}
	}
	if removed {
		return d.opts.FS.SyncDir(d.opts.Dir)
	}
	return nil
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
	var n uint64
	for i := range ops {
		n += uint64(len(ops[i].Key) + len(ops[i].Value))
	}
	d.userBytes.Add(n)
	return d.wal.Commit(&wal.Batch{Ops: ops}, func(first uint64) {
		d.insert(first, ops)
		d.recordSample(first, ops[0].Key, len(ops[0].Value), ops[0].Type == wal.OpDelete)
		d.visible.Store(first + uint64(len(ops)) - 1)
		// NOT rotating here: see wal.Options.AfterGroup.
	})
}

// maybeRotate seals the active memtable if it has outgrown MemtableSize.
// It runs on the WAL leader, so no other writer is inserting concurrently.
func (d *DB) maybeRotate() { d.rotate(false) }

// rotate seals the active memtable if force or it is over the size threshold.
func (d *DB) rotate(force bool) {
	d.mu.RLock()
	mem := d.mem
	d.mu.RUnlock()
	if mem.Empty() || (!force && mem.ApproxSize() < d.opts.MemtableSize) {
		return
	}
	// Safety invariant: sealing is only correct if every record written to the
	// WAL so far has been applied to THIS memtable. Otherwise a record in the
	// segment we are about to retire would live in the next memtable, and
	// deleting the segment after this memtable's flush would lose it. Rotation
	// is therefore only done at group boundaries (wal.Options.AfterGroup); if the
	// invariant is ever violated we refuse to rotate rather than risk data.
	if _, maxSeq := mem.SeqRange(); maxSeq != d.wal.LastSeq() {
		d.rotateErrs.Add(1)
		d.opts.Logger.Error("refusing to rotate: memtable does not cover the WAL", "mem_max_seq", maxSeq, "wal_last_seq", d.wal.LastSeq())
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
	d.emit(Event{Type: EventMemtableSealed, Bytes: mem.ApproxSize(), Count: mem.Len()})
	d.emit(Event{Type: EventWALRotated, File: boundary})
	d.kickFlush()
}

func (d *DB) kickFlush() {
	select {
	case d.flushCh <- struct{}{}:
	default:
	}
}

// l0Limits returns the L0 file counts at which writers are slowed / stopped.
// Tiered trees legitimately keep several files per size tier, so their limits
// are scaled up.
func (d *DB) l0Limits() (slow, stop int) {
	co := d.opts.Compaction
	slow, stop = co.L0SlowdownTrigger, co.L0StopTrigger
	if d.opts.tiered() {
		slow, stop = slow*4, stop*4
	}
	return
}

// waitForRoom is the write backpressure, applied before a writer takes any key
// lock (so the flush and compaction goroutines, which need none, can always make
// progress and wake us). Three regimes:
//
//  1. Flush backlog >= MaxImmutables, or L0 files >= stop trigger: writers BLOCK
//     until background work catches up, failing with ErrWriteStall after
//     StallTimeout so a dead disk cannot hang clients forever.
//  2. L0 files >= slowdown trigger: each write is delayed by 1 ms, which gives
//     compaction time to drain L0 without a hard stop (smooths latency).
//  3. Otherwise: no delay.
func (d *DB) waitForRoom() error {
	slowStart, stopAt := d.l0Limits()
	d.mu.Lock()
	var t0 time.Time
	var timer *time.Timer
	stalled := false
	for {
		if d.closed {
			d.mu.Unlock()
			return ErrClosed
		}
		l0 := len(d.cur.levels[0])
		if len(d.imms) < d.opts.MaxImmutables && l0 < stopAt {
			break
		}
		if !stalled {
			stalled = true
			t0 = time.Now()
			d.stalls.Add(1)
			d.emit(Event{Type: EventWriteStall, Count: int64(len(d.imms)), Level: l0, Detail: "flush backlog or L0 file count"})
			// sync.Cond has no timeout; a timer broadcasts so we can give up.
			timer = time.AfterFunc(d.opts.StallTimeout, func() { d.mu.Lock(); d.cond.Broadcast(); d.mu.Unlock() })
			d.kickCompaction()
		}
		if time.Since(t0) >= d.opts.StallTimeout {
			d.stallNanos.Add(uint64(time.Since(t0)))
			timer.Stop()
			d.mu.Unlock()
			return ErrWriteStall
		}
		d.cond.Wait()
	}
	if stalled {
		d.stallNanos.Add(uint64(time.Since(t0)))
		timer.Stop()
	}
	slow := len(d.cur.levels[0]) >= slowStart
	d.mu.Unlock()
	if slow {
		t := time.Now()
		time.Sleep(time.Millisecond)
		d.slowdowns.Add(1)
		d.slowdownNanos.Add(uint64(time.Since(t)))
	}
	return nil
}

// ---------------------------------------------------------------- flush

func (d *DB) flushLoop() {
	defer d.flushWG.Done()
	var retry <-chan time.Time
	for {
		select {
		case <-d.stopCh:
			return
		case <-d.flushCh:
		case <-retry:
		}
		retry = nil
		for {
			ok, err := d.flushOne()
			if err != nil {
				d.flushErrs.Add(1)
				d.opts.Logger.Error("memtable flush failed (will retry)", "err", err)
				retry = time.After(500 * time.Millisecond)
				break
			}
			if !ok {
				break
			}
		}
	}
}

// flushOne flushes the oldest immutable memtable. ok is false if none exist.
func (d *DB) flushOne() (ok bool, err error) {
	d.mu.RLock()
	if len(d.imms) == 0 {
		d.mu.RUnlock()
		return false, nil
	}
	im := d.imms[len(d.imms)-1]
	d.mu.RUnlock()

	t0 := time.Now()
	d.emit(Event{Type: EventFlushStart, Count: im.mt.Len(), Bytes: im.mt.ApproxSize()})

	edit := &manifest.Edit{HasLogNumber: true, LogNumber: im.walBoundary}
	_, maxSeq := im.mt.SeqRange()
	edit.HasLastSeq, edit.LastSeq = true, maxSeq

	var tf *tableFile
	if !im.mt.Empty() {
		if tf, err = d.writeTable(im.mt, 0); err != nil {
			return false, err
		}
		edit.Added = []manifest.FileMeta{tf.meta}
	}
	// The table is complete and durable. Make it real: MANIFEST first.
	if err := d.mf.LogAndApply(edit); err != nil {
		if tf != nil {
			tf.r.Close()
			d.opts.FS.Remove(tf.path)
		}
		return false, err
	}
	// Install atomically with respect to readers: they see the memtable or
	// the table, never neither.
	d.mu.Lock()
	old := d.cur
	if tf != nil {
		d.cur = old.next([]*tableFile{tf}, nil)
		d.tables[tf.meta.Num] = tf
	}
	for i, x := range d.imms {
		if x == im {
			d.imms = append(d.imms[:i:i], d.imms[i+1:]...)
			break
		}
	}
	d.cond.Broadcast()
	d.mu.Unlock()
	if tf != nil {
		old.unref()
		d.kickCompaction()
	}

	// Only now are the WAL segments redundant.
	if err := d.wal.RemoveBefore(im.walBoundary); err != nil {
		d.opts.Logger.Warn("could not delete old WAL segments (will be removed at next start)", "err", err)
	}
	dur := time.Since(t0)
	d.flushes.Add(1)
	d.flushNanos.Add(uint64(dur))
	if tf != nil {
		d.flushBytes.Add(uint64(tf.meta.Size))
	}
	d.emit(Event{Type: EventFlushEnd, Nanos: int64(dur), Bytes: func() int64 {
		if tf != nil {
			return tf.meta.Size
		}
		return 0
	}(), File: func() uint64 {
		if tf != nil {
			return tf.meta.Num
		}
		return 0
	}()})
	return true, nil
}

// writeTable writes the memtable as a new SSTable at `level`, crash-safely:
// temp file -> fsync -> rename -> directory fsync. It returns the opened table.
func (d *DB) writeTable(mt *memtable.Memtable, level int) (*tableFile, error) {
	num := d.mf.NewFileNumber()
	final := sstPath(d.opts.Dir, num)
	tmp := final + ".tmp"
	f, err := d.opts.FS.Create(tmp)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*tableFile, error) {
		f.Close()
		d.opts.FS.Remove(tmp)
		return nil, err
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{BlockSize: d.opts.BlockSize, BloomBitsPerKey: d.opts.BloomBits})
	it := mt.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if err := w.Add(it.Entry()); err != nil {
			return fail(err)
		}
	}
	props, err := w.Finish() // includes fsync of the file
	if err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		d.opts.FS.Remove(tmp)
		return nil, err
	}
	if err := d.opts.FS.Rename(tmp, final); err != nil {
		d.opts.FS.Remove(tmp)
		return nil, err
	}
	if err := d.opts.FS.SyncDir(d.opts.Dir); err != nil {
		return nil, err
	}
	fi, err := d.opts.FS.Stat(final)
	if err != nil {
		return nil, err
	}
	meta := manifest.FileMeta{
		Num: num, Level: level, Size: fi.Size(),
		Entries: props.NumEntries, Deletes: props.NumDeletes,
		MinSeq: props.MinSeq, MaxSeq: props.MaxSeq,
		Smallest: props.Smallest, Largest: props.Largest,
	}
	r, err := sstable.Open(d.opts.FS, final, d.readerOpts(num))
	if err != nil {
		return nil, err
	}
	d.emit(Event{Type: EventFileCreated, File: num, Level: level, Bytes: meta.Size, Count: int64(meta.Entries)})
	return &tableFile{meta: meta, r: r, path: final, db: d}, nil
}

// ---------------------------------------------------------------- reads

// view captures a consistent read view: the snapshot sequence number, the
// memtables that contain everything up to it, and a pinned table version.
// Callers must call release.
type view struct {
	snap uint64
	mem  *memtable.Memtable
	imms []*immutable
	ver  *version
	now  int64
}

func (d *DB) view() (view, error) {
	snap := d.visible.Load() // before copying tables: tables only ever grow
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return view{}, ErrClosed
	}
	d.cur.ref()
	return view{snap: snap, mem: d.mem, imms: d.imms, ver: d.cur, now: d.opts.Now()}, nil
}

func (v *view) release() { v.ver.unref() }

// lookup finds the newest version of key visible in v. found is false if the
// key has never been written; the returned entry may be a tombstone or expired.
func (v *view) lookup(key []byte) (iterator.Entry, bool, error) {
	if e, ok := v.mem.Get(key, v.snap); ok {
		return e, true, nil
	}
	for _, im := range v.imms {
		if e, ok := im.mt.Get(key, v.snap); ok {
			return e, true, nil
		}
	}
	return v.ver.get(key, v.snap)
}

// live returns the current live value of key.
func (v *view) live(key []byte) (e iterator.Entry, ok bool, err error) {
	e, found, err := v.lookup(key)
	if err != nil || !found || e.Kind == iterator.KindDelete || e.Expired(v.now) {
		return e, false, err
	}
	return e, true, nil
}

func (d *DB) Get(key []byte) ([]byte, bool, error) {
	v, err := d.view()
	if err != nil {
		return nil, false, err
	}
	defer v.release()
	e, ok, err := v.live(key)
	if err != nil || !ok {
		return nil, false, err
	}
	return clone(e.Value), true, nil
}

// MGet reads keys from a single snapshot (see MultiGetter).
func (d *DB) MGet(keys [][]byte) ([][]byte, []bool, error) {
	v, err := d.view()
	if err != nil {
		return nil, nil, err
	}
	defer v.release()
	vals := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	for i, k := range keys {
		e, ok, err := v.live(k)
		if err != nil {
			return nil, nil, err
		}
		if ok {
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
	defer v.release()
	_, ok, err := v.live(key)
	return ok, err
}

func (d *DB) TTL(key []byte) (int64, error) {
	v, err := d.view()
	if err != nil {
		return 0, err
	}
	defer v.release()
	e, ok, err := v.live(key)
	switch {
	case err != nil:
		return 0, err
	case !ok:
		return -2, nil
	case e.ExpireAt == 0:
		return -1, nil
	}
	return e.ExpireAt - v.now, nil
}

// ---------------------------------------------------------------- writes

// beginWrite stalls if needed, locks the keys' stripes and pins a fresh view.
// The returned function releases both. The view is taken AFTER the locks so it
// reflects every earlier write to these keys (each such write finished its
// apply before releasing its stripe).
func (d *DB) beginWrite(keys ...[]byte) (view, func(), error) {
	if err := d.waitForRoom(); err != nil {
		return view{}, nil, err
	}
	unlock := d.kl.lock(keys...)
	v, err := d.view()
	if err != nil {
		unlock()
		return view{}, nil, err
	}
	return v, func() { v.release(); unlock() }, nil
}

func (d *DB) Set(key, value []byte, opt SetOptions) (bool, error) {
	v, done, err := d.beginWrite(key)
	if err != nil {
		return false, err
	}
	defer done()
	old, exists, err := v.live(key)
	if err != nil {
		return false, err
	}
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
	err = d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: value, ExpireAt: exp}})
	return err == nil, err
}

func (d *DB) MSet(pairs [][]byte) error {
	keys := make([][]byte, 0, len(pairs)/2)
	ops := make([]wal.Op, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, pairs[i])
		ops = append(ops, wal.Op{Type: wal.OpPut, Key: pairs[i], Value: pairs[i+1]})
	}
	if len(ops) == 0 {
		return nil
	}
	_, done, err := d.beginWrite(keys...)
	if err != nil {
		return err
	}
	defer done()
	return d.commit(ops)
}

func (d *DB) Del(keys [][]byte) (int, error) {
	v, done, err := d.beginWrite(keys...)
	if err != nil {
		return 0, err
	}
	defer done()
	var ops []wal.Op
	seen := map[string]bool{}
	for _, k := range keys {
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		_, ok, err := v.live(k)
		if err != nil {
			return 0, err
		}
		if ok {
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
	v, done, err := d.beginWrite(key)
	if err != nil {
		return false, err
	}
	defer done()
	e, ok, err := v.live(key)
	if err != nil || !ok {
		return false, err
	}
	err = d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.Value, ExpireAt: atMs}})
	return err == nil, err
}

func (d *DB) Persist(key []byte) (bool, error) {
	v, done, err := d.beginWrite(key)
	if err != nil {
		return false, err
	}
	defer done()
	e, ok, err := v.live(key)
	if err != nil || !ok || e.ExpireAt == 0 {
		return false, err
	}
	err = d.commit([]wal.Op{{Type: wal.OpPut, Key: key, Value: e.Value}})
	return err == nil, err
}

func (d *DB) IncrBy(key []byte, delta int64) (int64, error) {
	v, done, err := d.beginWrite(key)
	if err != nil {
		return 0, err
	}
	defer done()
	e, ok, err := v.live(key)
	if err != nil {
		return 0, err
	}
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

// newMerged builds the merged version iterator over the view's memtables and tables.
func (v *view) newMerged() *iterator.Merge {
	children := []iterator.Iterator{v.mem.NewIterator()}
	for _, im := range v.imms {
		children = append(children, im.mt.NewIterator())
	}
	children = append(children, v.ver.iterators()...)
	return iterator.NewMerge(children)
}

func (d *DB) Scan(start []byte, limit int) ([][]byte, []byte, error) {
	v, err := d.view()
	if err != nil {
		return nil, nil, err
	}
	defer v.release() // the pinned version outlives the iterator below
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
// memtables and tables. It over-counts overwritten and expired keys; computing
// the exact number would need a full merge. DBSIZE is documented as
// approximate for this reason.
func (d *DB) ApproxSize() (int64, error) {
	v, err := d.view()
	if err != nil {
		return 0, err
	}
	defer v.release()
	n := v.mem.Len() - 2*v.mem.Tombstones()
	for _, im := range v.imms {
		n += im.mt.Len() - 2*im.mt.Tombstones()
	}
	for _, l := range v.ver.levels {
		for _, f := range l {
			n += int64(f.meta.Entries) - 2*int64(f.meta.Deletes)
		}
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
	if err := d.waitForRoom(); err != nil {
		return err
	}
	defer d.kl.lockAll()()
	v, err := d.view()
	if err != nil {
		return err
	}
	defer v.release()
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

// ---------------------------------------------------------------- admin

// WaitForFlush blocks until no immutable memtables remain (tests, graceful
// maintenance). It returns an error if the DB is closed first.
func (d *DB) WaitForFlush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.imms) > 0 && !d.closed {
		d.kickFlush()
		d.cond.Wait()
	}
	if d.closed {
		return ErrClosed
	}
	return nil
}

// FlushMemtable seals the active memtable (even if small) and waits until it
// is on disk. Used by tests and `nebula compact`.
func (d *DB) FlushMemtable() error {
	// Rotation must not race with memtable inserts, which only happen on the
	// WAL group leader; a barrier runs on that leader in log order.
	if err := d.wal.Barrier(func() { d.rotate(true) }); err != nil {
		return err
	}
	return d.WaitForFlush()
}

// writeAmp is total bytes written to disk (WAL + flush + compaction) divided by
// the key+value bytes clients submitted.
func (d *DB) writeAmp() float64 {
	user := d.userBytes.Load()
	if user == 0 {
		return 0
	}
	disk := d.wal.Stats().Bytes + d.flushBytes.Load() + d.compactWritten.Load()
	return float64(disk) / float64(user)
}

// Levels reports (file count, bytes) per level for INFO/metrics.
func (d *DB) Levels() (files [manifest.NumLevels]int, bytes [manifest.NumLevels]int64) {
	d.mu.RLock()
	cur := d.cur
	d.mu.RUnlock()
	for i, l := range cur.levels {
		files[i] = len(l)
		for _, f := range l {
			bytes[i] += f.meta.Size
		}
	}
	return
}

// Stats renders the metrics as INFO lines.
func (d *DB) Stats() []Stat {
	m := d.Metrics()
	u := func(n uint64) string { return strconv.FormatUint(n, 10) }
	i := func(n int64) string { return strconv.FormatInt(n, 10) }
	f3 := func(x float64) string { return fmt.Sprintf("%.3f", x) }
	st := []Stat{
		{"engine", "lsm"},
		{"last_seq", u(m.LastSeq)},
		{"memtable_bytes", i(m.MemtableBytes)},
		{"memtable_entries", i(m.MemtableEntries)},
		{"immutable_memtables", strconv.Itoa(m.Immutables)},
		{"memtable_rotations", u(m.Rotations)},
		{"rotation_errors", u(d.rotateErrs.Load())},
		{"flushes", u(m.Flushes)},
		{"flush_errors", u(m.FlushErrors)},
		{"flush_bytes", u(m.FlushBytes)},
		{"flush_seconds", f3(m.FlushSeconds)},
		{"write_stalls", u(m.WriteStalls)},
		{"write_stall_seconds", f3(m.WriteStallSeconds)},
		{"write_slowdowns", u(m.WriteSlowdowns)},
		{"table_files", strconv.Itoa(m.TableFiles)},
		{"l0_files", strconv.Itoa(m.Levels[0].Files)},
		{"compactions", u(m.Compactions)},
		{"compaction_errors", u(m.CompactionErrors)},
		{"compaction_bytes_read", u(m.CompactionBytesIn)},
		{"compaction_bytes_written", u(m.CompactionBytesOut)},
		{"compaction_entries_dropped", u(m.CompactionDropped)},
		{"compaction_seconds", f3(m.CompactionSeconds)},
		{"compaction_throttled_seconds", f3(m.ThrottledSeconds)},
		{"user_bytes_written", u(m.UserBytes)},
		{"write_amplification", fmt.Sprintf("%.2f", m.WriteAmplification)},
		{"bloom_checked", u(m.BloomChecked)},
		{"bloom_skipped", u(m.BloomSkipped)},
		{"bloom_false_positive", u(m.BloomFalsePositive)},
		{"bloom_useful_rate", fmt.Sprintf("%.4f", m.BloomUsefulRate)},
		{"block_reads", u(m.BlockReads)},
		{"block_cache_hits", u(m.CacheHits)},
		{"block_cache_misses", u(m.CacheMisses)},
		{"wal_segment", u(m.WALSegment)},
		{"wal_active_bytes", i(m.WALActiveBytes)},
		{"wal_bytes_written", u(m.WALBytesWritten)},
		{"wal_batches", u(m.WALBatches)},
		{"wal_groups", u(m.WALGroups)},
		{"wal_fsyncs", u(m.WALFsyncs)},
		{"wal_max_group", u(m.WALMaxGroup)},
		{"recovery_batches", strconv.Itoa(m.RecoveryBatches)},
		{"recovery_torn_tail", strconv.FormatBool(m.RecoveryTornTail)},
		{"recovery_truncated_bytes", i(m.RecoveryTruncatedBytes)},
		{"manifest_number", u(m.ManifestNumber)},
	}
	for lvl, l := range m.Levels {
		if l.Files > 0 {
			st = append(st, Stat{fmt.Sprintf("level%d", lvl), fmt.Sprintf("files=%d,bytes=%d", l.Files, l.Bytes)})
		}
	}
	if m.CacheEnabled {
		st = append(st,
			Stat{"block_cache_bytes", i(m.CacheBytes)},
			Stat{"block_cache_entries", i(m.CacheEntries)},
			Stat{"block_cache_evictions", u(m.CacheEvictions)},
			Stat{"block_cache_hit_rate", fmt.Sprintf("%.4f", m.CacheHitRate)})
	}
	if m.WALFailed != "" {
		st = append(st, Stat{"wal_failed", m.WALFailed})
	}
	return st
}

// Close stops background work, syncs and closes the WAL, MANIFEST and tables.
// Callers must have stopped issuing operations. Unflushed memtable contents are
// not written to SSTables: they are recovered from the WAL on the next Open.
func (d *DB) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.cond.Broadcast()
	d.mu.Unlock()
	close(d.stopCh)
	d.flushWG.Wait()
	d.compactWG.Wait()

	err := d.wal.Close()
	if merr := d.mf.Close(); err == nil {
		err = merr
	}
	d.mu.Lock()
	d.closeTables()
	d.mu.Unlock()
	d.unlock()
	return err
}

var (
	_ Engine      = (*DB)(nil)
	_ MultiGetter = (*DB)(nil)
)
