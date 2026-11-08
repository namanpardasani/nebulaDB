package wal

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/vfs"
)

// SyncPolicy says when appended data is forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways: fsync before Commit returns. Acknowledged writes survive power
	// loss and process kill. Concurrent commits share one fsync (group commit).
	SyncAlways SyncPolicy = iota
	// SyncEvery: fsync from a background timer. Acknowledged writes survive
	// process kill (they are in the OS page cache) but a power loss or kernel
	// crash can lose up to one interval of them.
	SyncEvery
	// SyncNever: never fsync (except at rotation and Close). Survives process
	// kill; power loss can lose an unbounded amount. Unsafe by design.
	SyncNever
)

func (p SyncPolicy) String() string { return [...]string{"always", "every", "never"}[p] }

// Errors.
var (
	ErrClosed = errors.New("wal: closed")
)

// Options configures a WAL.
type Options struct {
	FS       vfs.FS
	Dir      string
	Policy   SyncPolicy
	Interval time.Duration // for SyncEvery (default 100ms)
	// GroupCommit lets concurrent writers share a single write+fsync. When
	// false every Commit does its own, which is correct but slow with
	// SyncAlways; kept as an option so the benefit can be measured.
	GroupCommit bool
	// MaxGroupBytes bounds the size of one group (default 4 MiB).
	MaxGroupBytes int
	// MinSegment: segments numbered below this are obsolete (already flushed to
	// SSTables) and are deleted by Open instead of replayed.
	MinSegment uint64
	// StartSeq is the highest sequence number already durable elsewhere (in
	// SSTables). New sequence numbers start above max(StartSeq, replayed).
	StartSeq uint64
	Logger   *slog.Logger
	// AfterGroup, if set, runs on the group leader after EVERY apply callback of
	// a group has finished (and after any barriers). At that moment each record
	// in the active segment has been applied, so it is the one safe point for
	// sealing the segment together with the memtable that holds its contents.
	// Rotating between two batches of the same group would put a batch whose
	// record lives in the old segment into the NEW memtable, and deleting the old
	// segment after the old memtable's flush would then lose it.
	AfterGroup func()
}

// RecoveryInfo reports what Open found.
type RecoveryInfo struct {
	Segments       []uint64 // segments replayed, in order
	Batches        int
	LastSeq        uint64 // highest sequence number seen (0 if none)
	TornTail       bool   // a torn tail was found and truncated
	TornSegment    uint64
	TruncatedBytes int64
	RemovedStale   []uint64 // obsolete segments deleted
}

// Stats are cumulative counters, safe to read concurrently.
type Stats struct {
	Batches   uint64 // batches committed
	Ops       uint64
	Bytes     uint64 // bytes appended
	Groups    uint64 // write calls (one per group)
	Syncs     uint64
	SyncNanos uint64 // total time spent in fsync
	MaxGroup  uint64 // largest group seen (batches)
	Rotations uint64
}

// WAL is the write-ahead log. Commit may be called concurrently.
type WAL struct {
	opts Options
	log  *slog.Logger

	// Group-commit queue (see Commit).
	qmu     sync.Mutex
	queue   []*request
	writing bool // a leader is active

	// fmu guards the active file and everything about the append position.
	fmu     sync.Mutex
	f       vfs.File
	segNum  uint64
	size    int64 // bytes in the active segment
	nextSeq uint64
	failed  error // sticky: once set, every Commit fails
	closed  bool
	dirty   bool // unsynced appended data exists
	buf     []byte

	stopSync chan struct{}
	syncDone chan struct{}

	st struct {
		batches, ops, bytes, groups, syncs, syncNanos, maxGroup, rotations atomic.Uint64
	}
}

type request struct {
	batch    *Batch // nil for a barrier
	barrier  func() // non-nil for a barrier: runs in log order, writes nothing
	apply    func(firstSeq uint64)
	err      error
	finished bool          // guarded by the leader hand-off protocol
	wake     chan struct{} // signalled when finished or promoted to leader
}

const segSuffix = ".wal"

func segName(dir string, n uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", n, segSuffix))
}

func parseSeg(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segSuffix) {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(name, segSuffix), 10, 64)
	return n, err == nil
}

// ListSegments returns the WAL segment numbers present in dir, ascending.
func ListSegments(fs vfs.FS, dir string) ([]uint64, error) {
	names, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, nm := range names {
		if n, ok := parseSeg(nm); ok {
			segs = append(segs, n)
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

// SegmentPath returns the path of segment n in dir.
func SegmentPath(dir string, n uint64) string { return segName(dir, n) }

// Open recovers the log in opts.Dir and opens it for appending.
//
// Recovery: every segment >= MinSegment is scanned in order and replay is
// called once per batch (its keys/values alias a buffer: copy what you keep).
// A torn tail in the newest segment is truncated and reported; a torn tail in
// an older segment, or any mid-file damage, is a *CorruptionError.
func Open(opts Options, replay func(*Batch) error) (*WAL, RecoveryInfo, error) {
	if opts.Interval <= 0 {
		opts.Interval = 100 * time.Millisecond
	}
	if opts.MaxGroupBytes <= 0 {
		opts.MaxGroupBytes = 4 << 20
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	fs := opts.FS
	var info RecoveryInfo
	if err := fs.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, info, err
	}
	segs, err := ListSegments(fs, opts.Dir)
	if err != nil {
		return nil, info, err
	}

	// Drop obsolete segments (left behind if we crashed after the manifest
	// recorded the flush but before the files were deleted).
	var live []uint64
	for _, n := range segs {
		if n < opts.MinSegment {
			if err := fs.Remove(segName(opts.Dir, n)); err != nil {
				return nil, info, err
			}
			info.RemovedStale = append(info.RemovedStale, n)
			continue
		}
		live = append(live, n)
	}
	if len(info.RemovedStale) > 0 {
		if err := fs.SyncDir(opts.Dir); err != nil {
			return nil, info, err
		}
	}

	// lastSeq tracks the highest sequence number seen in the WAL itself. It is
	// passed into ScanSegment so monotonicity is also enforced across segment
	// boundaries.
	var lastSeq uint64
	var tailValidEnd int64
	for i, n := range live {
		data, err := vfs.ReadFile(fs, segName(opts.Dir, n))
		if err != nil {
			return nil, info, err
		}
		res, err := ScanSegment(data, lastSeq, func(b *Batch) error {
			if replay != nil {
				return replay(b)
			}
			return nil
		})
		if err != nil {
			var ce *CorruptionError
			if errors.As(err, &ce) && ce.Segment == 0 {
				ce.Segment = n
			}
			return nil, info, err
		}
		info.Segments = append(info.Segments, n)
		info.Batches += res.Batches
		lastSeq = res.LastSeq
		if res.Torn {
			if i != len(live)-1 {
				// Older segments were fsynced at rotation, so damage there is
				// not a crash artifact.
				return nil, info, &CorruptionError{Segment: n, Offset: res.ValidEnd,
					Reason: "damaged record in a non-final segment"}
			}
			info.TornTail, info.TornSegment = true, n
			info.TruncatedBytes = int64(len(data)) - res.ValidEnd
			opts.Logger.Warn("wal: truncating torn tail", "segment", n,
				"valid_bytes", res.ValidEnd, "discarded_bytes", info.TruncatedBytes)
		}
		tailValidEnd = res.ValidEnd
	}
	info.LastSeq = lastSeq

	next := lastSeq
	if opts.StartSeq > next {
		next = opts.StartSeq // sequence numbers never go backwards past flushed data
	}
	w := &WAL{opts: opts, log: opts.Logger, nextSeq: next + 1}

	if len(live) == 0 {
		start := opts.MinSegment
		if start == 0 {
			start = 1
		}
		if err := w.createSegment(start); err != nil {
			return nil, info, err
		}
	} else {
		last := live[len(live)-1]
		f, err := fs.OpenFile(segName(opts.Dir, last), os.O_RDWR, 0o644)
		if err != nil {
			return nil, info, err
		}
		if info.TornTail {
			if err := f.Truncate(tailValidEnd); err != nil {
				f.Close()
				return nil, info, err
			}
			// Make the truncation durable before appending after it.
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, info, err
			}
		}
		if _, err := f.Seek(tailValidEnd, 0); err != nil {
			f.Close()
			return nil, info, err
		}
		w.f, w.segNum, w.size = f, last, tailValidEnd
	}

	if opts.Policy == SyncEvery {
		w.stopSync = make(chan struct{})
		w.syncDone = make(chan struct{})
		go w.syncLoop()
	}
	return w, info, nil
}

// createSegment makes a new empty segment active. Caller holds fmu (or is Open).
func (w *WAL) createSegment(n uint64) error {
	f, err := w.opts.FS.Create(segName(w.opts.Dir, n))
	if err != nil {
		return err
	}
	// The directory entry must be durable before we rely on records in this
	// file; otherwise an fsynced file could vanish after power loss.
	if err := w.opts.FS.SyncDir(w.opts.Dir); err != nil {
		f.Close()
		return err
	}
	w.f, w.segNum, w.size = f, n, 0
	return nil
}

// Commit appends b and returns once it is durable according to the policy.
//
// Sequence numbers are assigned here, in log order. After the batch is durable
// (and before Commit returns) apply is called with the batch's first sequence
// number. Apply callbacks run one at a time, in log order, on whichever
// goroutine is currently the group leader; the engine uses it to insert into
// the memtable, which is how a single-writer skip list stays consistent with
// the WAL order while many clients write concurrently.
//
// # Group commit
//
// Callers enqueue a request. The first caller to find no active leader becomes
// the leader: it takes everything currently queued (up to MaxGroupBytes),
// assigns consecutive sequence numbers, writes all records with ONE write call,
// issues ONE fsync, runs the apply callbacks and wakes the followers. While it
// is in fsync (milliseconds), new requests pile up and become the next group.
// With N concurrent writers the fsync cost is amortised ~N ways. After its own
// group the leader hands leadership to the oldest queued request so no caller
// is stuck serving others' groups.
func (w *WAL) Commit(b *Batch, apply func(firstSeq uint64)) error {
	if len(b.Ops) == 0 {
		return ErrBatchTooLarge
	}
	if encodedSize(b)-headerSize > maxPayloadSize {
		return ErrBatchTooLarge
	}
	return w.submit(&request{batch: b, apply: apply, wake: make(chan struct{}, 1)})
}

// Barrier runs fn on the group-commit leader without writing anything. fn runs
// after all apply callbacks of the group the barrier landed in, so every record
// written so far has been applied and no apply callback is running: the safe
// place for operations that must not race with memtable inserts (forced memtable
// rotation).
func (w *WAL) Barrier(fn func()) error {
	return w.submit(&request{barrier: fn, wake: make(chan struct{}, 1)})
}

// submit enqueues r and drives the leader/follower protocol (see Commit).
func (w *WAL) submit(r *request) error {
	w.qmu.Lock()
	w.queue = append(w.queue, r)
	leader := !w.writing
	if leader {
		w.writing = true
	}
	w.qmu.Unlock()

	if !leader {
		<-r.wake
		w.qmu.Lock()
		fin := r.finished
		w.qmu.Unlock()
		if fin {
			return r.err
		}
		// Promoted: the previous leader finished its group and handed us the baton.
	}

	// Leader: build one group.
	w.qmu.Lock()
	n, bytes := 0, 0
	for n < len(w.queue) {
		if !w.opts.GroupCommit && n == 1 {
			break
		}
		sz := 0
		if w.queue[n].batch != nil {
			sz = encodedSize(w.queue[n].batch)
		}
		if n > 0 && bytes+sz > w.opts.MaxGroupBytes {
			break
		}
		bytes += sz
		n++
	}
	group := append([]*request(nil), w.queue[:n]...)
	w.queue = w.queue[n:]
	w.qmu.Unlock()

	w.commitGroup(group)

	// Hand-off: publish results, then promote the next leader (if any).
	w.qmu.Lock()
	for _, g := range group {
		g.finished = true
	}
	if len(w.queue) > 0 {
		w.queue[0].wake <- struct{}{}
	} else {
		w.writing = false
	}
	w.qmu.Unlock()
	for _, g := range group {
		if g != r {
			g.wake <- struct{}{}
		}
	}
	return r.err
}

// commitGroup writes, syncs and applies one group. It sets err on each request.
func (w *WAL) commitGroup(group []*request) {
	err := w.writeGroup(group)
	for _, g := range group {
		g.err = err
	}
	if err != nil {
		return
	}
	// Durable: apply in log order. Done outside fmu so apply may call Rotate.
	for _, g := range group {
		if g.batch != nil && g.apply != nil {
			g.apply(g.batch.Seq)
		}
	}
	// Barriers and AfterGroup run only once the whole group is applied.
	for _, g := range group {
		if g.batch == nil {
			g.barrier()
		}
	}
	if w.opts.AfterGroup != nil {
		w.opts.AfterGroup()
	}
}

func (w *WAL) writeGroup(group []*request) error {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	startSeq := w.nextSeq
	seq := startSeq
	w.buf = w.buf[:0]
	var ops uint64
	for _, g := range group {
		if g.batch == nil {
			continue // barrier: nothing to write
		}
		g.batch.Seq = seq
		var err error
		if w.buf, err = appendRecord(w.buf, g.batch); err != nil {
			return err
		}
		seq += uint64(len(g.batch.Ops))
		ops += uint64(len(g.batch.Ops))
	}

	if len(w.buf) == 0 { // group of barriers only
		return nil
	}
	if n, err := w.f.Write(w.buf); err != nil || n != len(w.buf) {
		if err == nil {
			err = errors.New("wal: short write")
		}
		return w.recoverFromWriteError(err, startSeq)
	}
	w.size += int64(len(w.buf))
	w.dirty = true
	w.nextSeq = seq

	if w.opts.Policy == SyncAlways {
		if err := w.syncLocked(); err != nil {
			return err
		}
	}
	w.st.batches.Add(uint64(len(group)))
	w.st.ops.Add(ops)
	w.st.bytes.Add(uint64(len(w.buf)))
	w.st.groups.Add(1)
	for {
		cur := w.st.maxGroup.Load()
		if uint64(len(group)) <= cur || w.st.maxGroup.CompareAndSwap(cur, uint64(len(group))) {
			break
		}
	}
	return nil
}

// recoverFromWriteError handles a failed/short append (typically ENOSPC).
// The file may now end with a partial record. If we can cut it off we do, and
// the WAL stays usable once the disk has space again; if we cannot, the log is
// poisoned because appending after garbage would hide later records behind it.
func (w *WAL) recoverFromWriteError(cause error, startSeq uint64) error {
	w.nextSeq = startSeq
	if err := w.f.Truncate(w.size); err == nil {
		if _, err := w.f.Seek(w.size, 0); err == nil {
			return cause
		}
	}
	w.failed = fmt.Errorf("wal: write failed and tail could not be repaired: %w", cause)
	w.log.Error("wal poisoned", "err", w.failed)
	return w.failed
}

// syncLocked fsyncs the active segment. A failed fsync poisons the log: after
// an fsync error the kernel may have dropped the dirty pages and marked them
// clean, so a retry can "succeed" without the data being on disk (the
// "fsyncgate" problem). The only safe response is to stop acknowledging writes
// and let a restart re-establish truth from what is really on disk.
func (w *WAL) syncLocked() error {
	if !w.dirty {
		return nil
	}
	t0 := time.Now()
	err := w.f.Sync()
	w.st.syncs.Add(1)
	w.st.syncNanos.Add(uint64(time.Since(t0)))
	if err != nil {
		w.failed = fmt.Errorf("wal: fsync failed, log is now read-only: %w", err)
		w.log.Error("wal poisoned", "err", w.failed)
		return w.failed
	}
	w.dirty = false
	return nil
}

// Sync forces everything appended so far to stable storage.
func (w *WAL) Sync() error {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	return w.syncLocked()
}

func (w *WAL) syncLoop() {
	defer close(w.syncDone)
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.fmu.Lock()
			if !w.closed && w.failed == nil {
				w.syncLocked() // failure poisons; next Commit reports it
			}
			w.fmu.Unlock()
		case <-w.stopSync:
			return
		}
	}
}

// Rotate seals the active segment (fsync, regardless of policy) and starts a
// new one, returning the new segment's number. Everything in earlier segments
// is therefore complete and durable. Must not be called concurrently with an
// in-flight group's write; the engine calls it from an apply callback or while
// holding its write lock.
func (w *WAL) Rotate() (uint64, error) {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if w.failed != nil {
		return 0, w.failed
	}
	if err := w.syncLocked(); err != nil {
		return 0, err
	}
	old, oldNum, oldSize := w.f, w.segNum, w.size
	if err := w.createSegment(oldNum + 1); err != nil {
		// Keep appending to the old segment; nothing was lost.
		w.f, w.segNum, w.size = old, oldNum, oldSize
		return 0, err
	}
	old.Close()
	w.st.rotations.Add(1)
	return w.segNum, nil
}

// RemoveBefore deletes all segments numbered below n (they have been flushed
// to SSTables and recorded in the MANIFEST) and fsyncs the directory.
func (w *WAL) RemoveBefore(n uint64) error {
	segs, err := ListSegments(w.opts.FS, w.opts.Dir)
	if err != nil {
		return err
	}
	removed := false
	for _, s := range segs {
		if s < n {
			if err := w.opts.FS.Remove(segName(w.opts.Dir, s)); err != nil {
				return err
			}
			removed = true
		}
	}
	if removed {
		return w.opts.FS.SyncDir(w.opts.Dir)
	}
	return nil
}

// ActiveSegment returns the number of the segment being appended to.
func (w *WAL) ActiveSegment() uint64 {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	return w.segNum
}

// Size returns the byte size of the active segment.
func (w *WAL) Size() int64 {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	return w.size
}

// LastSeq returns the highest sequence number assigned so far.
func (w *WAL) LastSeq() uint64 {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	return w.nextSeq - 1
}

// Err returns the sticky failure, if the log is poisoned.
func (w *WAL) Err() error {
	w.fmu.Lock()
	defer w.fmu.Unlock()
	return w.failed
}

// Stats returns a snapshot of the counters.
func (w *WAL) Stats() Stats {
	return Stats{
		Batches: w.st.batches.Load(), Ops: w.st.ops.Load(), Bytes: w.st.bytes.Load(),
		Groups: w.st.groups.Load(), Syncs: w.st.syncs.Load(), SyncNanos: w.st.syncNanos.Load(),
		MaxGroup: w.st.maxGroup.Load(), Rotations: w.st.rotations.Load(),
	}
}

// Close syncs (under every policy: a clean shutdown must not lose data) and
// closes the log. No Commit may be in flight.
func (w *WAL) Close() error {
	if w.stopSync != nil {
		close(w.stopSync)
		<-w.syncDone
		w.stopSync = nil
	}
	w.fmu.Lock()
	defer w.fmu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var err error
	if w.failed == nil {
		err = w.syncLocked()
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}
