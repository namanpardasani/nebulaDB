// Package tools implements the offline maintenance operations behind the
// `nebula` CLI: stats, dump-wal, dump-sstable, dump-manifest, verify, repair and
// compact. They work on a database *directory* and, except for compact and
// repair's output, never modify it. They are plain functions writing to an
// io.Writer so they can be unit-tested.
package tools

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/iterator"
	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/sstable"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

var fs vfs.FS = vfs.Default

func q(b []byte) string {
	const max = 64
	if len(b) > max {
		return strconv.Quote(string(b[:max])) + fmt.Sprintf("...(+%d bytes)", len(b)-max)
	}
	return strconv.Quote(string(b))
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func sstFiles(dir string) (map[uint64]string, error) {
	names, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	out := map[uint64]string{}
	for _, n := range names {
		if strings.HasSuffix(n, ".sst") {
			if num, err := strconv.ParseUint(strings.TrimSuffix(n, ".sst"), 10, 64); err == nil {
				out[num] = filepath.Join(dir, n)
			}
		}
	}
	return out, nil
}

// ---------------------------------------------------------------- stats

// Stats prints an offline summary of a database directory.
func Stats(w io.Writer, dir string) error {
	_, v, info, err := manifest.Dump(fs, dir)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}
	fmt.Fprintf(w, "Directory:        %s\n", dir)
	fmt.Fprintf(w, "Manifest:         MANIFEST-%06d (%d edits%s)\n", info.ManifestNum, info.Edits, map[bool]string{true: ", torn tail present", false: ""}[info.TornTail])
	fmt.Fprintf(w, "Last seq (in tables): %d   WAL boundary (log number): %d   Next file number: %d\n\n", v.LastSeq, v.LogNumber, v.NextFileNum)
	var total int64
	var entries, deletes uint64
	fmt.Fprintf(w, "%-6s %6s %12s %12s %10s\n", "Level", "Files", "Size", "Entries", "Tombstones")
	for lvl, files := range v.Levels {
		if len(files) == 0 {
			continue
		}
		var sz int64
		var en, de uint64
		for _, f := range files {
			sz += f.Size
			en += f.Entries
			de += f.Deletes
		}
		total += sz
		entries += en
		deletes += de
		fmt.Fprintf(w, "L%-5d %6d %12s %12d %10d\n", lvl, len(files), human(sz), en, de)
	}
	fmt.Fprintf(w, "%-6s %6d %12s %12d %10d\n\n", "total", v.NumFiles(), human(total), entries, deletes)
	segs, _ := wal.ListSegments(fs, dir)
	var walBytes int64
	for _, s := range segs {
		if fi, err := fs.Stat(wal.SegmentPath(dir, s)); err == nil {
			walBytes += fi.Size()
		}
	}
	fmt.Fprintf(w, "WAL segments:     %d (%s) %v\n", len(segs), human(walBytes), segs)
	return nil
}

// ---------------------------------------------------------------- dump-wal

// DumpWAL prints the records of one segment file, or of every segment in a
// directory. limit caps the batches printed per segment (0 = all).
func DumpWAL(w io.Writer, path string, limit int) error {
	var files []string
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		segs, err := wal.ListSegments(fs, path)
		if err != nil {
			return err
		}
		for _, s := range segs {
			files = append(files, wal.SegmentPath(path, s))
		}
	} else {
		files = []string{path}
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "== %s (%d bytes)\n", filepath.Base(f), len(data))
		n := 0
		res, err := wal.ScanSegment(data, 0, func(b *wal.Batch) error {
			if limit > 0 && n >= limit {
				n++
				return nil
			}
			n++
			fmt.Fprintf(w, "batch seq=%d ops=%d\n", b.Seq, len(b.Ops))
			for i, op := range b.Ops {
				switch op.Type {
				case wal.OpPut:
					exp := ""
					if op.ExpireAt != 0 {
						exp = fmt.Sprintf(" expire_at=%d", op.ExpireAt)
					}
					fmt.Fprintf(w, "  [%d] PUT key=%s value=%s%s\n", b.Seq+uint64(i), q(op.Key), q(op.Value), exp)
				case wal.OpDelete:
					fmt.Fprintf(w, "  [%d] DEL key=%s\n", b.Seq+uint64(i), q(op.Key))
				}
			}
			return nil
		})
		if limit > 0 && n > limit {
			fmt.Fprintf(w, "... %d more batches not shown\n", n-limit)
		}
		if err != nil {
			fmt.Fprintf(w, "!! %v\n", err)
			continue
		}
		fmt.Fprintf(w, "-- %d batches, valid bytes %d/%d", res.Batches, res.ValidEnd, len(data))
		if res.Torn {
			fmt.Fprintf(w, "  TORN TAIL: %d bytes would be truncated on startup", int64(len(data))-res.ValidEnd)
		}
		fmt.Fprintln(w)
	}
	return nil
}

// ---------------------------------------------------------------- dump-sstable

// DumpSSTable prints a table's properties, index and (up to limit) entries.
func DumpSSTable(w io.Writer, path string, limit int) error {
	r, err := sstable.Open(fs, path, sstable.ReaderOptions{})
	if err != nil {
		return err
	}
	defer r.Close()
	p := r.Props()
	fmt.Fprintf(w, "file:       %s (%s)\n", filepath.Base(path), human(r.Size()))
	fmt.Fprintf(w, "entries:    %d (%d tombstones)\n", p.NumEntries, p.NumDeletes)
	fmt.Fprintf(w, "blocks:     %d   data bytes: %d   filter bytes: %d\n", p.NumBlocks, p.DataSize, p.FilterSize)
	fmt.Fprintf(w, "key range:  %s .. %s\n", q(p.Smallest), q(p.Largest))
	fmt.Fprintf(w, "seq range:  %d .. %d\n\n", p.MinSeq, p.MaxSeq)
	it := r.NewIterator()
	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && n >= limit {
			fmt.Fprintf(w, "... (stopped after %d entries; use -n 0 for all)\n", limit)
			break
		}
		e := it.Entry()
		kind := "PUT"
		if e.Kind == iterator.KindDelete {
			kind = "DEL"
		}
		exp := ""
		if e.ExpireAt != 0 {
			exp = fmt.Sprintf(" expire_at=%d", e.ExpireAt)
		}
		fmt.Fprintf(w, "%s seq=%d key=%s value=%s%s\n", kind, e.Seq, q(e.Key), q(e.Value), exp)
		n++
	}
	return it.Err()
}

// ---------------------------------------------------------------- dump-manifest

// DumpManifest prints every edit and the resulting version.
func DumpManifest(w io.Writer, dir string) error {
	edits, v, info, err := manifest.Dump(fs, dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "MANIFEST-%06d: %d edits", info.ManifestNum, info.Edits)
	if info.TornTail {
		fmt.Fprintf(w, " (torn tail: %d bytes would be truncated)", info.TruncatedBytes)
	}
	fmt.Fprintln(w)
	for i, e := range edits {
		fmt.Fprintf(w, "edit %d:", i)
		if e.HasLogNumber {
			fmt.Fprintf(w, " log_number=%d", e.LogNumber)
		}
		if e.HasLastSeq {
			fmt.Fprintf(w, " last_seq=%d", e.LastSeq)
		}
		if e.HasNextFileNum {
			fmt.Fprintf(w, " next_file=%d", e.NextFileNum)
		}
		fmt.Fprintln(w)
		for _, f := range e.Added {
			fmt.Fprintf(w, "    + L%d %06d.sst size=%d entries=%d seq=%d..%d keys=%s..%s\n", f.Level, f.Num, f.Size, f.Entries, f.MinSeq, f.MaxSeq, q(f.Smallest), q(f.Largest))
		}
		for _, d := range e.Deleted {
			fmt.Fprintf(w, "    - L%d %06d.sst\n", d.Level, d.Num)
		}
	}
	fmt.Fprintf(w, "\nlive version: log_number=%d last_seq=%d next_file=%d\n", v.LogNumber, v.LastSeq, v.NextFileNum)
	for lvl, files := range v.Levels {
		for _, f := range files {
			fmt.Fprintf(w, "  L%d %06d.sst %s entries=%d keys=%s..%s\n", lvl, f.Num, human(f.Size), f.Entries, q(f.Smallest), q(f.Largest))
		}
	}
	return nil
}

// ---------------------------------------------------------------- verify

// VerifyReport is the outcome of Verify.
type VerifyReport struct {
	Problems []string
	Warnings []string
	Tables   int
	Entries  uint64
	WALBatch int
}

// OK reports whether no problems were found.
func (r *VerifyReport) OK() bool { return len(r.Problems) == 0 }

// Verify checks every checksum and cross-reference of a database directory
// without modifying it: MANIFEST, every live SSTable (all blocks, entry order,
// agreement with the manifest), and every live WAL segment.
func Verify(w io.Writer, dir string) (*VerifyReport, error) {
	rep := &VerifyReport{}
	problem := func(f string, a ...any) { rep.Problems = append(rep.Problems, fmt.Sprintf(f, a...)) }
	warn := func(f string, a ...any) { rep.Warnings = append(rep.Warnings, fmt.Sprintf(f, a...)) }

	_, v, info, err := manifest.Dump(fs, dir)
	if err != nil {
		problem("MANIFEST: %v", err)
		report(w, rep)
		return rep, nil
	}
	if info.TornTail {
		warn("MANIFEST has a torn tail (%d bytes) that would be truncated at startup", info.TruncatedBytes)
	}
	live := map[uint64]bool{}
	for lvl, files := range v.Levels {
		for _, f := range files {
			live[f.Num] = true
			path := filepath.Join(dir, fmt.Sprintf("%06d.sst", f.Num))
			fi, err := os.Stat(path)
			if err != nil {
				problem("L%d %06d.sst is listed in the MANIFEST but missing: %v", lvl, f.Num, err)
				continue
			}
			if fi.Size() != f.Size {
				problem("L%d %06d.sst size %d differs from MANIFEST %d", lvl, f.Num, fi.Size(), f.Size)
			}
			r, err := sstable.Open(fs, path, sstable.ReaderOptions{FileNum: f.Num})
			if err != nil {
				problem("L%d %06d.sst: %v", lvl, f.Num, err)
				continue
			}
			if err := r.Verify(); err != nil {
				problem("L%d %06d.sst: %v", lvl, f.Num, err)
			}
			p := r.Props()
			if p.NumEntries != f.Entries || p.MinSeq != f.MinSeq || p.MaxSeq != f.MaxSeq ||
				string(p.Smallest) != string(f.Smallest) || string(p.Largest) != string(f.Largest) {
				problem("L%d %06d.sst properties disagree with the MANIFEST", lvl, f.Num)
			}
			rep.Tables++
			rep.Entries += p.NumEntries
			r.Close()
		}
	}
	all, _ := sstFiles(dir)
	var orphans []uint64
	for n := range all {
		if !live[n] {
			orphans = append(orphans, n)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i] < orphans[j] })
	for _, n := range orphans {
		warn("orphan table %06d.sst is not in the MANIFEST (harmless; removed at next startup)", n)
	}
	names, _ := fs.List(dir)
	for _, n := range names {
		if strings.HasSuffix(n, ".tmp") {
			warn("leftover temp file %s (removed at next startup)", n)
		}
	}
	segs, _ := wal.ListSegments(fs, dir)
	var lastSeq uint64
	for i, s := range segs {
		if s < v.LogNumber {
			warn("WAL segment %06d.wal is older than the flush boundary %d (stale; removed at next startup)", s, v.LogNumber)
			continue
		}
		data, err := os.ReadFile(wal.SegmentPath(dir, s))
		if err != nil {
			problem("WAL %06d: %v", s, err)
			continue
		}
		res, err := wal.ScanSegment(data, lastSeq, func(*wal.Batch) error { return nil })
		if err != nil {
			problem("WAL %06d: %v", s, err)
			continue
		}
		lastSeq = res.LastSeq
		rep.WALBatch += res.Batches
		if res.Torn {
			if i == len(segs)-1 {
				warn("WAL %06d has a torn tail (%d bytes) that startup will truncate (normal after a crash)", s, int64(len(data))-res.ValidEnd)
			} else {
				problem("WAL %06d (not the newest segment) has a damaged tail", s)
			}
		}
	}
	report(w, rep)
	return rep, nil
}

func report(w io.Writer, rep *VerifyReport) {
	for _, x := range rep.Warnings {
		fmt.Fprintf(w, "WARN  %s\n", x)
	}
	for _, x := range rep.Problems {
		fmt.Fprintf(w, "FAIL  %s\n", x)
	}
	if rep.OK() {
		fmt.Fprintf(w, "OK    %d tables (%d entries) and %d WAL batches verified\n", rep.Tables, rep.Entries, rep.WALBatch)
	} else {
		fmt.Fprintf(w, "%d problem(s) found. See docs/FAILURES.md; `nebula repair` can salvage readable data into a new directory.\n", len(rep.Problems))
	}
}

// ---------------------------------------------------------------- compact

// Compact opens the database (taking its directory lock, so it fails if a
// server is running), flushes the memtable and merges everything down.
func Compact(w io.Writer, dir string) error {
	before, _ := dirTableStats(dir)
	db, info, err := engine.Open(engine.Options{Dir: dir, Sync: wal.SyncNever, BloomBits: 10, Compaction: engine.CompactionOptions{Disabled: true}})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "opened %s (replayed %d WAL batches)\n", dir, info.Batches)
	if err := db.CompactAll(); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	after, _ := dirTableStats(dir)
	fmt.Fprintf(w, "tables: %d files / %s  ->  %d files / %s\n", before.files, human(before.bytes), after.files, human(after.bytes))
	return nil
}

type tableStats struct {
	files int
	bytes int64
}

func dirTableStats(dir string) (tableStats, error) {
	var ts tableStats
	m, err := sstFiles(dir)
	if err != nil {
		return ts, err
	}
	for _, p := range m {
		if fi, err := os.Stat(p); err == nil {
			ts.files++
			ts.bytes += fi.Size()
		}
	}
	return ts, nil
}

// ---------------------------------------------------------------- repair

// RepairReport summarises a repair run.
type RepairReport struct {
	TablesRead       int
	TablesUnreadable []string
	BlocksSkipped    int
	WALSegments      int
	WALBytesSkipped  int64
	KeysRecovered    int
	UsedManifest     bool
}

// Repair salvages every readable entry from src into a NEW database in dst.
//
// Sources, newest wins by sequence number: SSTables (only those the MANIFEST
// lists when it is readable; otherwise every *.sst, in which case a deleted key
// whose tombstone was compacted away may reappear) and WAL records (resynchronising
// past damage). Tombstoned and expired keys are dropped. Damaged SSTable data
// blocks are skipped whole. src is never modified. Memory use is proportional to
// the number of live keys (newest version of each is held in RAM).
func Repair(w io.Writer, src, dst string) (*RepairReport, error) {
	if entries, err := fs.List(dst); err == nil && len(entries) > 0 {
		return nil, fmt.Errorf("destination %s must be empty or not exist", dst)
	}
	rep := &RepairReport{}
	best := map[string]iterator.Entry{}
	consider := func(e iterator.Entry) {
		k := string(e.Key)
		if old, ok := best[k]; !ok || e.Seq > old.Seq {
			e.Key = []byte(k)
			e.Value = append([]byte(nil), e.Value...)
			best[k] = e
		}
	}

	var logNumber uint64
	var useTables map[uint64]bool
	if _, v, _, err := manifest.Dump(fs, src); err == nil {
		rep.UsedManifest = true
		logNumber = v.LogNumber
		useTables = map[uint64]bool{}
		for _, l := range v.Levels {
			for _, f := range l {
				useTables[f.Num] = true
			}
		}
		fmt.Fprintf(w, "MANIFEST readable: using its %d live tables and WAL segments >= %d\n", v.NumFiles(), v.LogNumber)
	} else {
		fmt.Fprintf(w, "MANIFEST unreadable (%v): salvaging every .sst and every WAL segment found\n", err)
	}

	tables, err := sstFiles(src)
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for n := range tables {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	for _, n := range nums {
		if useTables != nil && !useTables[n] {
			continue
		}
		r, err := sstable.Open(fs, tables[n], sstable.ReaderOptions{FileNum: n})
		if err != nil {
			rep.TablesUnreadable = append(rep.TablesUnreadable, fmt.Sprintf("%06d.sst: %v", n, err))
			fmt.Fprintf(w, "  table %06d.sst unreadable (footer/index damaged): %v\n", n, err)
			continue
		}
		res, err := r.Salvage(func(e iterator.Entry) error { consider(e); return nil })
		r.Close()
		if err != nil {
			return nil, err
		}
		rep.TablesRead++
		rep.BlocksSkipped += res.BadBlocks
		fmt.Fprintf(w, "  table %06d.sst: %d entries recovered, %d/%d blocks unreadable\n", n, res.Entries, res.BadBlocks, res.Blocks)
		for _, bi := range res.BadBlockInfos {
			fmt.Fprintf(w, "      skipped %s\n", bi)
		}
	}

	segs, _ := wal.ListSegments(fs, src)
	for _, s := range segs {
		if s < logNumber {
			continue
		}
		data, err := os.ReadFile(wal.SegmentPath(src, s))
		if err != nil {
			fmt.Fprintf(w, "  wal %06d unreadable: %v\n", s, err)
			continue
		}
		res, _ := wal.SalvageSegment(data, func(b *wal.Batch) error {
			for i, op := range b.Ops {
				kind := iterator.KindPut
				if op.Type == wal.OpDelete {
					kind = iterator.KindDelete
				}
				consider(iterator.Entry{Key: op.Key, Seq: b.Seq + uint64(i), Kind: kind, Value: op.Value, ExpireAt: op.ExpireAt})
			}
			return nil
		})
		rep.WALSegments++
		rep.WALBytesSkipped += res.SkippedBytes
		fmt.Fprintf(w, "  wal %06d: %d batches recovered, %d bytes skipped\n", s, res.Batches, res.SkippedBytes)
	}

	// Write the survivors into a brand-new database.
	db, _, err := engine.Open(engine.Options{Dir: dst, Sync: wal.SyncNever, BloomBits: 10, Compaction: engine.CompactionOptions{Disabled: true}})
	if err != nil {
		return nil, err
	}
	now := nowMs()
	keys := make([]string, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		e := best[k]
		if e.Kind == iterator.KindDelete || e.Expired(now) {
			continue
		}
		if _, err := db.Set([]byte(k), e.Value, engine.SetOptions{ExpireAtMs: e.ExpireAt}); err != nil {
			db.Close()
			return nil, err
		}
		rep.KeysRecovered++
	}
	if err := db.CompactAll(); err != nil {
		db.Close()
		return nil, err
	}
	if err := db.Close(); err != nil {
		return nil, err
	}
	fmt.Fprintf(w, "\nrepair complete: %d keys written to %s (%d tables read, %d unreadable, %d data blocks skipped, %d WAL bytes skipped)\n",
		rep.KeysRecovered, dst, rep.TablesRead, len(rep.TablesUnreadable), rep.BlocksSkipped, rep.WALBytesSkipped)
	return rep, nil
}
