package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/sstable"
)

// Compaction styles.
const (
	StyleLeveled    = "leveled"
	StyleSizeTiered = "size-tiered"
)

// CompactionOptions tunes background compaction and write backpressure.
type CompactionOptions struct {
	Style string // StyleLeveled (default) or StyleSizeTiered

	// L0CompactTrigger: number of L0 files that makes L0 eligible for
	// compaction (leveled), or the minimum files in a size bucket (tiered).
	L0CompactTrigger int
	// L0SlowdownTrigger / L0StopTrigger: L0 file counts at which writers are
	// delayed / blocked (backpressure). Tiered mode multiplies both by 4,
	// because tiered trees legitimately hold several files per size tier.
	L0SlowdownTrigger int
	L0StopTrigger     int
	// LevelSizeRatio: each level may hold this many times more bytes than the
	// previous one. L1MaxBytes is the size target of L1.
	LevelSizeRatio int
	L1MaxBytes     int64
	// TargetFileBytes: leveled compaction splits its output into files of about
	// this size. Default 2 MiB.
	TargetFileBytes int64
	// RateBytesPerSec throttles compaction writes (0 = unlimited) so a big
	// merge does not starve foreground I/O.
	RateBytesPerSec int64
	// Disabled turns background compaction off (tests, `nebula verify`).
	Disabled bool
}

func (c *CompactionOptions) fill() {
	if c.Style == "" {
		c.Style = StyleLeveled
	}
	if c.L0CompactTrigger <= 0 {
		c.L0CompactTrigger = 4
	}
	if c.L0SlowdownTrigger <= 0 {
		c.L0SlowdownTrigger = 8
	}
	if c.L0StopTrigger <= 0 {
		c.L0StopTrigger = 12
	}
	if c.LevelSizeRatio <= 1 {
		c.LevelSizeRatio = 10
	}
	if c.L1MaxBytes <= 0 {
		c.L1MaxBytes = 16 << 20
	}
	if c.TargetFileBytes <= 0 {
		c.TargetFileBytes = 2 << 20
	}
}

func (o *Options) tiered() bool { return o.Compaction.Style == StyleSizeTiered }

// compaction is one unit of merge work.
type compaction struct {
	base     *version // pinned: the file set the decision was made on
	level    int      // source level
	outLevel int
	inputs   []*tableFile // all files being merged (both levels)
	inSet    map[uint64]bool
	reason   string
}

func (c *compaction) inputBytes() (n int64) {
	for _, f := range c.inputs {
		n += f.meta.Size
	}
	return
}

// maxBytesForLevel is the size target of level n >= 1.
func (d *DB) maxBytesForLevel(n int) int64 {
	b := d.opts.Compaction.L1MaxBytes
	for i := 1; i < n; i++ {
		b *= int64(d.opts.Compaction.LevelSizeRatio)
	}
	return b
}

func levelBytes(files []*tableFile) (n int64) {
	for _, f := range files {
		n += f.meta.Size
	}
	return
}

// overlapping returns the files of `files` whose key range intersects [lo, hi].
func overlapping(files []*tableFile, lo, hi []byte) []*tableFile {
	var out []*tableFile
	for _, f := range files {
		if bytes.Compare(f.meta.Smallest, hi) <= 0 && bytes.Compare(f.meta.Largest, lo) >= 0 {
			out = append(out, f)
		}
	}
	return out
}

func keyRange(files []*tableFile) (lo, hi []byte) {
	for i, f := range files {
		if i == 0 || bytes.Compare(f.meta.Smallest, lo) < 0 {
			lo = f.meta.Smallest
		}
		if i == 0 || bytes.Compare(f.meta.Largest, hi) > 0 {
			hi = f.meta.Largest
		}
	}
	return
}

// pickCompaction chooses the most urgent compaction on v, or nil.
func (d *DB) pickCompaction(v *version) *compaction {
	if d.opts.tiered() {
		return d.pickTiered(v)
	}
	return d.pickLeveled(v)
}

// pickLeveled implements leveled compaction.
//
// Each level has a score (L0: files / trigger; Ln: bytes / target) and the
// highest score >= 1 is compacted into the next level:
//   - L0 -> L1: every L0 file (they overlap each other) plus the L1 files
//     overlapping their combined key range.
//   - Ln -> Ln+1 (n >= 1): one file, chosen round-robin by key so that all
//     parts of the keyspace get compacted in turn, plus the overlapping Ln+1
//     files.
func (d *DB) pickLeveled(v *version) *compaction {
	co := d.opts.Compaction
	bestLevel, bestScore := -1, 0.0
	if s := float64(len(v.levels[0])) / float64(co.L0CompactTrigger); s >= 1 {
		bestLevel, bestScore = 0, s
	}
	for n := 1; n < manifest.NumLevels-1; n++ {
		if s := float64(levelBytes(v.levels[n])) / float64(d.maxBytesForLevel(n)); s >= 1 && s > bestScore {
			bestLevel, bestScore = n, s
		}
	}
	if bestLevel < 0 {
		return nil
	}
	c := &compaction{base: v, level: bestLevel, outLevel: bestLevel + 1, reason: fmt.Sprintf("score %.2f", bestScore)}
	if bestLevel == 0 {
		c.inputs = append(c.inputs, v.levels[0]...)
	} else {
		fs := v.levels[bestLevel]
		pick := fs[0]
		for _, f := range fs { // first file after the level's compaction pointer
			if bytes.Compare(f.meta.Smallest, d.compactPtr[bestLevel]) > 0 {
				pick = f
				break
			}
		}
		d.compactPtr[bestLevel] = pick.meta.Largest
		c.inputs = append(c.inputs, pick)
	}
	lo, hi := keyRange(c.inputs)
	c.inputs = append(c.inputs, overlapping(v.levels[c.outLevel], lo, hi)...)
	c.finish()
	return c
}

// pickTiered implements size-tiered compaction: files of similar size are
// merged together once a tier holds enough of them. Everything lives in L0
// (overlapping files), trading read amplification for lower write amplification.
func (d *DB) pickTiered(v *version) *compaction {
	co := d.opts.Compaction
	files := append([]*tableFile(nil), v.levels[0]...)
	if len(files) < co.L0CompactTrigger {
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].meta.Size < files[j].meta.Size })
	// Slide over files in size order; a bucket is a run where every file is at
	// most 2x the smallest in the run. Compact the first bucket with enough files.
	for i := 0; i < len(files); {
		j := i + 1
		for j < len(files) && files[j].meta.Size <= 2*files[i].meta.Size+1024 {
			j++
		}
		if j-i >= co.L0CompactTrigger {
			end := j
			if end-i > 32 {
				end = i + 32
			}
			c := &compaction{base: v, level: 0, outLevel: 0, inputs: files[i:end:end], reason: fmt.Sprintf("tier of %d files", end-i)}
			c.finish()
			return c
		}
		i = j
	}
	return nil
}

func (c *compaction) finish() {
	c.inSet = make(map[uint64]bool, len(c.inputs))
	for _, f := range c.inputs {
		c.inSet[f.meta.Num] = true
	}
}

// canDropTombstone reports whether a tombstone (or expired put) for key with
// the given seq can be discarded: only if no file OUTSIDE this compaction might
// still hold an older version it needs to keep shadowing. Otherwise dropping it
// would resurrect the older value.
func (c *compaction) canDropTombstone(key []byte, seq uint64) bool {
	v := c.base
	for lvl := c.outLevel + 1; lvl < manifest.NumLevels; lvl++ {
		for _, f := range v.levels[lvl] {
			if !c.inSet[f.meta.Num] && bytes.Compare(f.meta.Smallest, key) <= 0 && bytes.Compare(key, f.meta.Largest) <= 0 {
				return false
			}
		}
	}
	if c.outLevel == 0 { // tiered: other overlapping L0 files may be older
		for _, f := range v.levels[0] {
			if !c.inSet[f.meta.Num] && f.meta.MinSeq < seq &&
				bytes.Compare(f.meta.Smallest, key) <= 0 && bytes.Compare(key, f.meta.Largest) <= 0 {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------- loop

func (d *DB) kickCompaction() {
	select {
	case d.compactCh <- struct{}{}:
	default:
	}
}

func (d *DB) compactionLoop() {
	defer d.compactWG.Done()
	var retry <-chan time.Time
	for {
		select {
		case <-d.stopCh:
			return
		case <-d.compactCh:
		case <-retry:
		}
		retry = nil
		if d.opts.Compaction.Disabled {
			continue
		}
		for {
			ran, err := d.compactOnce()
			if err != nil {
				d.compactErrs.Add(1)
				d.opts.Logger.Error("compaction failed (will retry)", "err", err)
				retry = time.After(500 * time.Millisecond)
				break
			}
			if !ran {
				break
			}
			select {
			case <-d.stopCh:
				return
			default:
			}
		}
	}
}

// compactOnce picks and runs one compaction. ran is false if there was none.
func (d *DB) compactOnce() (ran bool, err error) {
	d.compactMu.Lock()
	defer d.compactMu.Unlock()
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return false, nil
	}
	base := d.cur
	base.ref()
	d.mu.RUnlock()
	c := d.pickCompaction(base)
	if c == nil {
		base.unref()
		return false, nil
	}
	defer base.unref()
	return true, d.runCompaction(c)
}

// ---------------------------------------------------------------- execution

var errStopped = errors.New("compaction stopped: database closing")

// runCompaction merges c.inputs into new tables and atomically swaps them in.
//
// Crash safety: output tables are written to temp names, fsynced, renamed and
// the directory is fsynced BEFORE the MANIFEST edit that adds them and removes
// the inputs in one atomic record. Crash before the edit: outputs are orphans,
// deleted at startup, inputs untouched. Crash after: inputs are orphans.
// Either way exactly one consistent set of files is live.
func (d *DB) runCompaction(c *compaction) (err error) {
	if err := d.mf.Err(); err != nil {
		return err // read-only manifest: merging would only create unusable files
	}
	for _, f := range c.inputs {
		f.ref() // keep inputs readable/openable for the whole merge
	}
	defer func() {
		for _, f := range c.inputs {
			f.unref()
		}
	}()
	t0 := time.Now()
	var inNums []uint64
	for _, f := range c.inputs {
		inNums = append(inNums, f.meta.Num)
	}
	d.emit(Event{Type: EventCompactionStart, Level: c.level, Inputs: inNums, Bytes: c.inputBytes(), Detail: c.reason})

	children := make([]iterator.Iterator, 0, len(c.inputs))
	// Shallower/newer files first (only matters for equal-key tie-breaks).
	ins := append([]*tableFile(nil), c.inputs...)
	sort.Slice(ins, func(i, j int) bool { return ins[i].meta.MaxSeq > ins[j].meta.MaxSeq })
	for _, f := range ins {
		children = append(children, f.r.NewIterator())
	}
	it := iterator.NewMerge(children)
	defer it.Close()

	var (
		outputs  []*tableFile
		cur      *sstable.Writer
		curFile  vfsFile
		curNum   uint64
		curTmp   string
		curFinal string
		written  int64
		dropped  uint64
		lastSync = time.Now()
		keyBuf   []byte
		haveKey  bool
	)
	cleanup := func() {
		if cur != nil {
			curFile.Close()
			d.opts.FS.Remove(curTmp)
		}
		for _, o := range outputs {
			o.r.Close()
			d.opts.FS.Remove(o.path)
		}
	}
	finishFile := func() error {
		if cur == nil {
			return nil
		}
		props, err := cur.Finish() // fsyncs
		if err != nil {
			return err
		}
		if err := curFile.Close(); err != nil {
			return err
		}
		if err := d.opts.FS.Rename(curTmp, curFinal); err != nil {
			return err
		}
		fi, err := d.opts.FS.Stat(curFinal)
		if err != nil {
			return err
		}
		meta := manifest.FileMeta{
			Num: curNum, Level: c.outLevel, Size: fi.Size(),
			Entries: props.NumEntries, Deletes: props.NumDeletes, MinSeq: props.MinSeq, MaxSeq: props.MaxSeq,
			Smallest: props.Smallest, Largest: props.Largest,
		}
		r, err := sstable.Open(d.opts.FS, curFinal, d.readerOpts(curNum))
		if err != nil {
			return err
		}
		outputs = append(outputs, &tableFile{meta: meta, r: r, path: curFinal, db: d})
		cur, curFile = nil, nil
		written += fi.Size()
		return nil
	}
	startFile := func() error {
		curNum = d.mf.NewFileNumber()
		curFinal = sstPath(d.opts.Dir, curNum)
		curTmp = curFinal + ".tmp"
		f, err := d.opts.FS.Create(curTmp)
		if err != nil {
			return err
		}
		curFile = f
		cur = sstable.NewWriter(f, sstable.WriterOptions{BlockSize: d.opts.BlockSize, BloomBitsPerKey: d.opts.BloomBits})
		return nil
	}

	now := d.opts.Now()
	var processed int64
	for it.SeekToFirst(); it.Valid(); it.Next() {
		e := it.Entry()
		if haveKey && bytes.Equal(e.Key, keyBuf) {
			dropped++ // shadowed by a newer version of the same key inside the inputs
			continue
		}
		keyBuf = append(keyBuf[:0], e.Key...)
		haveKey = true

		if e.Kind == iterator.KindDelete || e.Expired(now) {
			if c.canDropTombstone(e.Key, e.Seq) {
				dropped++
				continue
			}
			if e.Kind == iterator.KindPut {
				// An expired put must keep shadowing older versions, but it no
				// longer needs its value: store a tombstone instead.
				e.Kind, e.Value, e.ExpireAt = iterator.KindDelete, nil, 0
			}
		}
		// Start a new output file at a key boundary once the target size is
		// reached (leveled only; tiered makes one output).
		if cur != nil && c.outLevel > 0 && int64(cur.EstimatedSize()) >= d.opts.Compaction.TargetFileBytes {
			if err := finishFile(); err != nil {
				cleanup()
				return err
			}
		}
		if cur == nil {
			if err := startFile(); err != nil {
				cleanup()
				return err
			}
		}
		if err := cur.Add(e); err != nil {
			cleanup()
			return err
		}
		processed += int64(len(e.Key) + len(e.Value) + 16)
		if processed >= 64<<10 {
			if err := d.throttle(processed, &lastSync); err != nil {
				cleanup()
				return err
			}
			processed = 0
		}
	}
	if err := it.Err(); err != nil {
		cleanup()
		return err
	}
	if cur != nil && cur.NumEntries() > 0 {
		if err := finishFile(); err != nil {
			cleanup()
			return err
		}
	} else if cur != nil { // opened but nothing written
		curFile.Close()
		d.opts.FS.Remove(curTmp)
		cur = nil
	}
	if len(outputs) > 0 {
		if err := d.opts.FS.SyncDir(d.opts.Dir); err != nil { // make all the renames durable
			cleanup()
			return err
		}
	}

	// Atomic switch: one MANIFEST record adds the outputs and removes the inputs.
	edit := &manifest.Edit{}
	var outNums []uint64
	for _, o := range outputs {
		edit.Added = append(edit.Added, o.meta)
		outNums = append(outNums, o.meta.Num)
	}
	del := map[uint64]bool{}
	for _, f := range c.inputs {
		edit.Deleted = append(edit.Deleted, manifest.DeletedFile{Level: f.meta.Level, Num: f.meta.Num})
		del[f.meta.Num] = true
	}
	if err := d.mf.LogAndApply(edit); err != nil {
		if manifest.IsUncertain(err) {
			// The edit may be durable: keep the outputs (see flushOne). Inputs are
			// untouched, so either outcome after a restart is consistent.
			for _, o := range outputs {
				o.r.Close()
			}
			return err
		}
		cleanup()
		return err
	}
	d.mu.Lock()
	old := d.cur
	d.cur = old.next(outputs, del) // marks inputs obsolete; they are deleted when unreferenced
	for _, o := range outputs {
		d.tables[o.meta.Num] = o
	}
	d.cond.Broadcast() // L0 shrank: wake stalled writers
	d.mu.Unlock()
	old.unref()

	dur := time.Since(t0)
	d.compactions.Add(1)
	d.compactNanos.Add(uint64(dur))
	d.compactRead.Add(uint64(c.inputBytes()))
	d.compactWritten.Add(uint64(written))
	d.compactDropped.Add(dropped)
	for _, o := range outputs {
		d.emit(Event{Type: EventFileCreated, File: o.meta.Num, Level: o.meta.Level, Bytes: o.meta.Size, Count: int64(o.meta.Entries)})
	}
	d.emit(Event{Type: EventCompactionEnd, Level: c.outLevel, Inputs: inNums, Outputs: outNums, Bytes: written, Count: int64(dropped), Nanos: int64(dur)})
	d.kickCompaction() // the output may have pushed the next level over its target
	return nil
}

// vfsFile is the subset of vfs.File the compaction output path needs.
type vfsFile interface {
	Close() error
}

// throttle sleeps so that average compaction throughput stays under
// RateBytesPerSec. It also returns early when the DB is closing.
func (d *DB) throttle(bytes int64, last *time.Time) error {
	select {
	case <-d.stopCh:
		return errStopped
	default:
	}
	rate := d.opts.Compaction.RateBytesPerSec
	if rate <= 0 {
		return nil
	}
	want := time.Duration(float64(bytes) / float64(rate) * float64(time.Second))
	if sleep := want - time.Since(*last); sleep > 0 {
		d.throttled.Add(uint64(sleep))
		select {
		case <-time.After(sleep):
		case <-d.stopCh:
			return errStopped
		}
	}
	*last = time.Now()
	return nil
}

// ---------------------------------------------------------------- manual

// CompactAll flushes the memtable and then merges every table down to the
// deepest level (leveled) or into a single table (size-tiered), ignoring size
// triggers. After it returns, shadowed versions, tombstones and expired keys
// are gone from disk wherever that is safe, so on-disk size is close to live
// data size. Used by `nebula compact`, tests and amplification measurements.
func (d *DB) CompactAll() error {
	if err := d.FlushMemtable(); err != nil {
		return err
	}
	// Quiesce the background worker's work first.
	d.compactMu.Lock()
	defer d.compactMu.Unlock()
	for {
		d.mu.RLock()
		base := d.cur
		base.ref()
		d.mu.RUnlock()
		c := d.forcedPick(base)
		if c == nil {
			base.unref()
			return nil
		}
		err := d.runCompaction(c)
		base.unref()
		if err != nil {
			return err
		}
	}
}

// forcedPick returns a compaction that moves data one step toward the bottom,
// or nil when everything already sits in a single bottom-most place.
func (d *DB) forcedPick(v *version) *compaction {
	if d.opts.tiered() {
		if len(v.levels[0]) <= 1 {
			return nil
		}
		c := &compaction{base: v, level: 0, outLevel: 0, inputs: append([]*tableFile(nil), v.levels[0]...), reason: "manual full compaction"}
		c.finish()
		return c
	}
	// Leveled: find the deepest non-empty level; compact each shallower
	// non-empty level into the next. Stop when only one level holds data and it
	// is the deepest used one with a single sorted run.
	deepest := -1
	for n := manifest.NumLevels - 1; n >= 0; n-- {
		if len(v.levels[n]) > 0 {
			deepest = n
			break
		}
	}
	if deepest < 0 {
		return nil
	}
	for n := 0; n < deepest; n++ {
		if len(v.levels[n]) == 0 {
			continue
		}
		c := &compaction{base: v, level: n, outLevel: n + 1, reason: "manual full compaction"}
		c.inputs = append(c.inputs, v.levels[n]...)
		lo, hi := keyRange(c.inputs)
		c.inputs = append(c.inputs, overlapping(v.levels[n+1], lo, hi)...)
		c.finish()
		return c
	}
	// Everything is in the deepest level; if that is L0 (never compacted),
	// push it down once so tombstones can be dropped.
	if deepest == 0 {
		c := &compaction{base: v, level: 0, outLevel: 1, reason: "manual full compaction", inputs: append([]*tableFile(nil), v.levels[0]...)}
		c.finish()
		return c
	}
	return nil
}
