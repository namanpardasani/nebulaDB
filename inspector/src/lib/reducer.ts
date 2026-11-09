import type {
  EngineEvent, Health, LevelsResponse, StatsResponse, StreamMessage, Tick, WriteSample,
} from '../api/types'

export type ConnState = 'connecting' | 'live' | 'down'

export interface HistoryPoint {
  t: number
  writesPerSec: number
  opsPerSec: number
  bloomSkipRate: number // skipped / checked within the tick, NaN if nothing was checked
  cacheHitRate: number
}

export interface LogItem { id: number; at: number; event: EngineEvent }

export interface InspectorState {
  conn: ConnState
  downSince?: number
  /** Set when the stream came back after an outage: how long we were away. */
  recoveredAfterMs?: number
  recoveredAt?: number
  health?: Health
  stats?: StatsResponse
  levels?: LevelsResponse
  tick?: Tick
  history: HistoryPoint[]
  recentWrites: WriteSample[]
  log: LogItem[]
  /** file number -> expiry timestamp (ms) while the table is highlighted as new */
  fresh: Record<number, number>
  /** tables that are inputs of a running compaction */
  merging: number[]
  flushing: boolean
  compacting: boolean
  stalledUntil: number
  totals: { bloomChecked: number; bloomSkipped: number; cacheHits: number; cacheMisses: number; writes: number }
  nextLogId: number
}

export const HISTORY_LEN = 120
export const LOG_LEN = 150
export const WRITES_LEN = 24
export const FRESH_MS = 3000

export function initialState(): InspectorState {
  return {
    conn: 'connecting', history: [], recentWrites: [], log: [], fresh: {}, merging: [],
    flushing: false, compacting: false, stalledUntil: 0, nextLogId: 1,
    totals: { bloomChecked: 0, bloomSkipped: 0, cacheHits: 0, cacheMisses: 0, writes: 0 },
  }
}

export type Action =
  | { t: 'conn'; state: ConnState; now: number }
  | { t: 'health'; health: Health }
  | { t: 'stats'; stats: StatsResponse }
  | { t: 'levels'; levels: LevelsResponse }
  | { t: 'batch'; messages: StreamMessage[]; now: number }
  | { t: 'expire'; now: number }

function applyTick(s: InspectorState, m: Tick, now: number): InspectorState {
  const checked = m.bloom_checked
  const point: HistoryPoint = {
    t: now,
    writesPerSec: m.writes_per_sec,
    opsPerSec: m.ops_per_sec,
    bloomSkipRate: checked > 0 ? m.bloom_skipped / checked : NaN,
    cacheHitRate: m.cache_hits + m.cache_misses > 0 ? m.cache_hits / (m.cache_hits + m.cache_misses) : NaN,
  }
  const history = s.history.length >= HISTORY_LEN ? [...s.history.slice(1), point] : [...s.history, point]
  // Merge sampled writes: newest first, de-duplicated by sequence number.
  let recent = s.recentWrites
  if (m.recent_writes && m.recent_writes.length) {
    const seen = new Set(recent.map((w) => w.seq))
    const fresh = m.recent_writes.filter((w) => !seen.has(w.seq)).sort((a, b) => b.seq - a.seq)
    recent = [...fresh, ...recent].slice(0, WRITES_LEN)
  }
  return {
    ...s,
    tick: m,
    history,
    recentWrites: recent,
    stalledUntil: m.stalled ? now + 1500 : s.stalledUntil,
    totals: {
      bloomChecked: s.totals.bloomChecked + m.bloom_checked,
      bloomSkipped: s.totals.bloomSkipped + m.bloom_skipped,
      cacheHits: s.totals.cacheHits + m.cache_hits,
      cacheMisses: s.totals.cacheMisses + m.cache_misses,
      writes: s.totals.writes,
    },
  }
}

function applyEvent(s: InspectorState, e: EngineEvent, now: number): InspectorState {
  const next: InspectorState = {
    ...s,
    log: [{ id: s.nextLogId, at: now, event: e }, ...s.log].slice(0, LOG_LEN),
    nextLogId: s.nextLogId + 1,
  }
  switch (e.type) {
    case 'flush_start': next.flushing = true; break
    case 'flush_end': next.flushing = false; break
    case 'compaction_start': next.compacting = true; next.merging = e.inputs ?? []; break
    case 'compaction_end': next.compacting = false; next.merging = []; break
    case 'file_created':
      if (e.file) next.fresh = { ...s.fresh, [e.file]: now + FRESH_MS }
      break
    case 'write_stall': next.stalledUntil = now + 2000; break
    default: break
  }
  return next
}

export function reducer(s: InspectorState, a: Action): InspectorState {
  switch (a.t) {
    case 'conn': {
      if (a.state === s.conn) return s
      if (a.state === 'down') return { ...s, conn: 'down', downSince: s.downSince ?? a.now }
      // coming back from an outage
      if (s.conn === 'down' && s.downSince !== undefined) {
        return { ...s, conn: a.state, downSince: undefined, recoveredAfterMs: a.now - s.downSince, recoveredAt: a.now }
      }
      return { ...s, conn: a.state, downSince: undefined }
    }
    case 'health': return { ...s, health: a.health }
    case 'stats': return { ...s, stats: a.stats }
    case 'levels': return { ...s, levels: a.levels }
    case 'batch': {
      let cur = s
      for (const m of a.messages) cur = m.type === 'tick' ? applyTick(cur, m as Tick, a.now) : applyEvent(cur, m as EngineEvent, a.now)
      return cur
    }
    case 'expire': {
      const keys = Object.keys(s.fresh)
      if (!keys.length) return s
      const fresh: Record<number, number> = {}
      let changed = false
      for (const k of keys) {
        const n = Number(k)
        if (s.fresh[n] > a.now) fresh[n] = s.fresh[n]
        else changed = true
      }
      return changed ? { ...s, fresh } : s
    }
  }
}
