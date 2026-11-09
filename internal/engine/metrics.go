package engine

import (
	"bytes"

	"github.com/naman/nebuladb/internal/manifest"
	"github.com/naman/nebuladb/internal/wal"
)

// LevelMetrics summarises one level.
type LevelMetrics struct {
	Files int   `json:"files"`
	Bytes int64 `json:"bytes"`
}

// EngineMetrics is a point-in-time numeric snapshot of everything the engine
// counts. INFO, the Prometheus endpoint and the admin JSON API all derive from it.
type EngineMetrics struct {
	LastSeq uint64 `json:"last_seq"`

	MemtableBytes   int64  `json:"memtable_bytes"`
	MemtableEntries int64  `json:"memtable_entries"`
	Immutables      int    `json:"immutables"`
	Rotations       uint64 `json:"rotations"`

	Flushes      uint64  `json:"flushes"`
	FlushErrors  uint64  `json:"flush_errors"`
	FlushBytes   uint64  `json:"flush_bytes"`
	FlushSeconds float64 `json:"flush_seconds"`

	Compactions        uint64  `json:"compactions"`
	CompactionErrors   uint64  `json:"compaction_errors"`
	CompactionBytesIn  uint64  `json:"compaction_bytes_in"`
	CompactionBytesOut uint64  `json:"compaction_bytes_out"`
	CompactionDropped  uint64  `json:"compaction_dropped"`
	CompactionSeconds  float64 `json:"compaction_seconds"`
	ThrottledSeconds   float64 `json:"throttled_seconds"`

	WriteStalls          uint64  `json:"write_stalls"`
	WriteStallSeconds    float64 `json:"write_stall_seconds"`
	WriteSlowdowns       uint64  `json:"write_slowdowns"`
	WriteSlowdownSeconds float64 `json:"write_slowdown_seconds"`

	UserBytes          uint64  `json:"user_bytes"`
	WriteAmplification float64 `json:"write_amplification"`

	Levels     [manifest.NumLevels]LevelMetrics `json:"levels"`
	TableFiles int                              `json:"table_files"`

	BloomChecked       uint64 `json:"bloom_checked"`
	BloomSkipped       uint64 `json:"bloom_skipped"`
	BloomFalsePositive uint64 `json:"bloom_false_positive"`
	// BloomUsefulRate = skipped / checked: the fraction of table lookups the
	// filter answered without any I/O.
	BloomUsefulRate float64 `json:"bloom_useful_rate"`
	BlockReads      uint64  `json:"block_reads"`

	CacheEnabled   bool    `json:"cache_enabled"`
	CacheHits      uint64  `json:"cache_hits"`
	CacheMisses    uint64  `json:"cache_misses"`
	CacheEvictions uint64  `json:"cache_evictions"`
	CacheBytes     int64   `json:"cache_bytes"`
	CacheEntries   int64   `json:"cache_entries"`
	CacheHitRate   float64 `json:"cache_hit_rate"`

	WALBytesWritten uint64  `json:"wal_bytes_written"`
	WALBatches      uint64  `json:"wal_batches"`
	WALGroups       uint64  `json:"wal_groups"`
	WALFsyncs       uint64  `json:"wal_fsyncs"`
	WALFsyncSeconds float64 `json:"wal_fsync_seconds"`
	WALMaxGroup     uint64  `json:"wal_max_group"`
	WALActiveBytes  int64   `json:"wal_active_bytes"`
	WALSegment      uint64  `json:"wal_segment"`
	WALFailed       string  `json:"wal_failed"`

	RecoverySegments       int    `json:"recovery_segments"`
	RecoveryBatches        int    `json:"recovery_batches"`
	RecoveryTornTail       bool   `json:"recovery_torn_tail"`
	RecoveryTruncatedBytes int64  `json:"recovery_truncated_bytes"`
	ManifestNumber         uint64 `json:"manifest_number"`
}

// Metrics returns a snapshot of all engine counters and gauges.
func (d *DB) Metrics() EngineMetrics {
	d.mu.RLock()
	mem, imms, cur := d.mem, d.imms, d.cur
	d.mu.RUnlock()
	ws := d.wal.Stats()
	m := EngineMetrics{
		LastSeq:         d.visible.Load(),
		MemtableBytes:   mem.ApproxSize(),
		MemtableEntries: mem.Len(),
		Immutables:      len(imms),
		Rotations:       d.rotations.Load(),

		Flushes: d.flushes.Load(), FlushErrors: d.flushErrs.Load(), FlushBytes: d.flushBytes.Load(),
		FlushSeconds: float64(d.flushNanos.Load()) / 1e9,

		Compactions: d.compactions.Load(), CompactionErrors: d.compactErrs.Load(),
		CompactionBytesIn: d.compactRead.Load(), CompactionBytesOut: d.compactWritten.Load(),
		CompactionDropped: d.compactDropped.Load(), CompactionSeconds: float64(d.compactNanos.Load()) / 1e9,
		ThrottledSeconds: float64(d.throttled.Load()) / 1e9,

		WriteStalls: d.stalls.Load(), WriteStallSeconds: float64(d.stallNanos.Load()) / 1e9,
		WriteSlowdowns: d.slowdowns.Load(), WriteSlowdownSeconds: float64(d.slowdownNanos.Load()) / 1e9,

		UserBytes: d.userBytes.Load(), WriteAmplification: d.writeAmp(),
		TableFiles: cur.numFiles(),

		BloomChecked: d.tstats.BloomChecked.Load(), BloomSkipped: d.tstats.BloomSkipped.Load(),
		BloomFalsePositive: d.tstats.BloomFalsePositive.Load(), BlockReads: d.tstats.BlockReads.Load(),

		WALBytesWritten: ws.Bytes, WALBatches: ws.Batches, WALGroups: ws.Groups, WALFsyncs: ws.Syncs,
		WALFsyncSeconds: float64(ws.SyncNanos) / 1e9, WALMaxGroup: ws.MaxGroup,
		WALActiveBytes: d.wal.Size(), WALSegment: d.wal.ActiveSegment(),

		RecoverySegments: len(d.recovery.Segments), RecoveryBatches: d.recovery.Batches, RecoveryTornTail: d.recovery.TornTail,
		RecoveryTruncatedBytes: d.recovery.TruncatedBytes, ManifestNumber: d.mf.Number(),
	}
	if m.BloomChecked > 0 {
		m.BloomUsefulRate = float64(m.BloomSkipped) / float64(m.BloomChecked)
	}
	for i, l := range cur.levels {
		m.Levels[i].Files = len(l)
		m.Levels[i].Bytes = levelBytes(l)
	}
	if d.cache != nil {
		cs := d.cache.Stats()
		m.CacheEnabled = true
		m.CacheHits, m.CacheMisses, m.CacheEvictions = cs.Hits, cs.Misses, cs.Evictions
		m.CacheBytes, m.CacheEntries, m.CacheHitRate = cs.Bytes, cs.Entries, cs.HitRate()
	}
	if err := d.wal.Err(); err != nil {
		m.WALFailed = err.Error()
	}
	return m
}

// FileInfo describes one live SSTable (inspector / `nebula stats`).
type FileInfo struct {
	Num      uint64 `json:"num"`
	Level    int    `json:"level"`
	Size     int64  `json:"size"`
	Entries  uint64 `json:"entries"`
	Deletes  uint64 `json:"deletes"`
	MinSeq   uint64 `json:"min_seq"`
	MaxSeq   uint64 `json:"max_seq"`
	Smallest string `json:"smallest"`
	Largest  string `json:"largest"`
}

// WALSeg is one write-ahead-log segment file on disk.
type WALSeg struct {
	Num   uint64 `json:"num"`
	Bytes int64  `json:"bytes"`
}

// MemInfo describes the write buffers.
type MemInfo struct {
	ActiveBytes    int64   `json:"active_bytes"`
	ActiveEntries  int64   `json:"active_entries"`
	Threshold      int64   `json:"threshold"`
	ImmutableBytes []int64 `json:"immutable_bytes"` // newest first
	WALActiveBytes int64   `json:"wal_active_bytes"`
	WALSegment     uint64  `json:"wal_segment"`
	// WALSegments lists every segment file currently on disk, oldest first. A
	// segment disappears once the memtable it fed has been flushed.
	WALSegments     []WALSeg `json:"wal_segments"`
	L0SlowdownAt    int      `json:"l0_slowdown_at"`
	L0StopAt        int      `json:"l0_stop_at"`
	MaxImmutables   int      `json:"max_immutables"`
	CompactionStyle string   `json:"compaction_style"`
}

// printable renders a key for display: printable ASCII as is, anything else
// escaped, truncated to 48 bytes.
func printable(b []byte) string {
	if len(b) > 48 {
		b = b[:48]
	}
	var out bytes.Buffer
	for _, c := range b {
		if c >= 0x20 && c < 0x7f {
			out.WriteByte(c)
		} else {
			out.WriteString("\\x")
			out.WriteByte("0123456789abcdef"[c>>4])
			out.WriteByte("0123456789abcdef"[c&15])
		}
	}
	return out.String()
}

// LevelDetails lists every live table per level.
func (d *DB) LevelDetails() [][]FileInfo {
	d.mu.RLock()
	cur := d.cur
	d.mu.RUnlock()
	out := make([][]FileInfo, manifest.NumLevels)
	for lvl, l := range cur.levels {
		out[lvl] = []FileInfo{}
		for _, f := range l {
			m := f.meta
			out[lvl] = append(out[lvl], FileInfo{
				Num: m.Num, Level: lvl, Size: m.Size, Entries: m.Entries, Deletes: m.Deletes,
				MinSeq: m.MinSeq, MaxSeq: m.MaxSeq, Smallest: printable(m.Smallest), Largest: printable(m.Largest),
			})
		}
	}
	return out
}

// MemoryInfo describes the memtables and thresholds.
func (d *DB) MemoryInfo() MemInfo {
	d.mu.RLock()
	mem, imms := d.mem, d.imms
	d.mu.RUnlock()
	slow, stop := d.l0Limits()
	mi := MemInfo{
		ActiveBytes: mem.ApproxSize(), ActiveEntries: mem.Len(), Threshold: d.opts.MemtableSize,
		ImmutableBytes: []int64{}, WALActiveBytes: d.wal.Size(), WALSegment: d.wal.ActiveSegment(),
		L0SlowdownAt: slow, L0StopAt: stop, MaxImmutables: d.opts.MaxImmutables,
		CompactionStyle: d.opts.Compaction.Style,
	}
	for _, im := range imms {
		mi.ImmutableBytes = append(mi.ImmutableBytes, im.mt.ApproxSize())
	}
	mi.WALSegments = []WALSeg{}
	if nums, err := wal.ListSegments(d.opts.FS, d.opts.Dir); err == nil {
		for _, n := range nums {
			var size int64
			if fi, err := d.opts.FS.Stat(wal.SegmentPath(d.opts.Dir, n)); err == nil {
				size = fi.Size()
			}
			mi.WALSegments = append(mi.WALSegments, WALSeg{Num: n, Bytes: size})
		}
	}
	return mi
}

// WriteSample is a recently committed write, for the live inspector view.
type WriteSample struct {
	Seq      uint64 `json:"seq"`
	Key      string `json:"key"`
	ValueLen int    `json:"value_len"`
	Delete   bool   `json:"delete"`
}

const sampleRing = 64

// recordSample remembers about one in sixteen commits (the first op's key,
// truncated) in a small lock-free ring; the admin server polls it. Sampling
// keeps the cost off the write path: one atomic add per commit, and an
// allocation only for sampled ones.
func (d *DB) recordSample(first uint64, key []byte, vlen int, del bool) {
	n := d.sampleCtr.Add(1)
	if n&15 != 1 {
		return
	}
	d.samples[(n>>4)%sampleRing].Store(&WriteSample{Seq: first, Key: printable(key), ValueLen: vlen, Delete: del})
}

// RecentWrites returns the sampled recent writes (unordered).
func (d *DB) RecentWrites() []WriteSample {
	var out []WriteSample
	for i := range d.samples {
		if s := d.samples[i].Load(); s != nil {
			out = append(out, *s)
		}
	}
	return out
}
