// Types mirroring the admin server's JSON (internal/admin, internal/engine).

export interface Health {
  status: string
  version: string
  dev: boolean
  engine: boolean
  supervised: boolean
  addr?: string
}

export interface LevelMetrics { files: number; bytes: number }

export interface EngineMetrics {
  last_seq: number
  memtable_bytes: number
  memtable_entries: number
  immutables: number
  rotations: number
  flushes: number
  flush_errors: number
  flush_bytes: number
  flush_seconds: number
  compactions: number
  compaction_errors: number
  compaction_bytes_in: number
  compaction_bytes_out: number
  compaction_dropped: number
  compaction_seconds: number
  throttled_seconds: number
  write_stalls: number
  write_stall_seconds: number
  write_slowdowns: number
  write_slowdown_seconds: number
  user_bytes: number
  write_amplification: number
  levels: LevelMetrics[]
  table_files: number
  bloom_checked: number
  bloom_skipped: number
  bloom_false_positive: number
  bloom_useful_rate: number
  block_reads: number
  cache_enabled: boolean
  cache_hits: number
  cache_misses: number
  cache_evictions: number
  cache_bytes: number
  cache_entries: number
  cache_hit_rate: number
  wal_bytes_written: number
  wal_batches: number
  wal_groups: number
  wal_fsyncs: number
  wal_fsync_seconds: number
  wal_max_group: number
  wal_active_bytes: number
  wal_segment: number
  wal_failed: string
  recovery_batches: number
  recovery_torn_tail: boolean
  recovery_truncated_bytes: number
  manifest_number: number
  recovery_segments: number
}

export interface LatencyStats { count: number; p50_us: number; p99_us: number; p999_us: number; mean_us: number }

export interface ServerStats {
  uptime_seconds: number
  active_connections: number
  total_connections: number
  total_commands: number
  unknown_commands: number
  ops_per_sec: number
  per_command: Record<string, number>
  latency: Record<string, LatencyStats>
}

export interface StatsResponse { server?: ServerStats; engine?: EngineMetrics }

export interface FileInfo {
  num: number
  level: number
  size: number
  entries: number
  deletes: number
  min_seq: number
  max_seq: number
  smallest: string
  largest: string
}

export interface WALSeg { num: number; bytes: number }

export interface MemInfo {
  wal_segments?: WALSeg[] | null
  active_bytes: number
  active_entries: number
  threshold: number
  immutable_bytes: number[]
  wal_active_bytes: number
  wal_segment: number
  l0_slowdown_at: number
  l0_stop_at: number
  max_immutables: number
  compaction_style: string
}

export interface LevelsResponse { levels: FileInfo[][]; memory: MemInfo }

export interface WriteSample { seq: number; key: string; value_len: number; delete: boolean }

/** Aggregated 250 ms message from the server (see tickLoop in internal/admin). */
export interface Tick {
  type: 'tick'
  time: string
  ops_per_sec: number
  writes_per_sec: number
  write_bytes_per_sec: number
  memory: MemInfo
  levels: LevelMetrics[]
  cache_hit_rate: number
  cache_hits: number
  cache_misses: number
  bloom_checked: number
  bloom_skipped: number
  wal_fsyncs: number
  wal_bytes: number
  last_seq: number
  table_files: number
  write_amp: number
  bloom_useful: number
  uptime_seconds: number
  stalled: boolean
  slowed: boolean
  recent_writes: WriteSample[] | null
}

export type EngineEventType =
  | 'memtable_sealed' | 'flush_start' | 'flush_end' | 'file_created' | 'file_deleted'
  | 'compaction_start' | 'compaction_end' | 'write_stall' | 'wal_rotated' | 'recovery'
  /** client-side only: the stream dropped / came back */
  | 'connection_lost' | 'connection_restored'

export interface EngineEvent {
  type: EngineEventType
  time: string
  file?: number
  level?: number
  bytes?: number
  count?: number
  detail?: string
  inputs?: number[]
  outputs?: number[]
  nanos?: number
}

export type StreamMessage = Tick | EngineEvent

export interface CommandReply { reply?: unknown; error?: string }
