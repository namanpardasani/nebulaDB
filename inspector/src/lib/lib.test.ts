import { describe, expect, it } from 'vitest'
import { keyToFraction, keySpan, packLanes } from './keymap'
import { splitArgs } from './args'
import { bytes, compact, micros } from './format'
import { initialState, reducer, FRESH_MS, WRITES_LEN } from './reducer'
import type { Tick } from '../api/types'

describe('keymap', () => {
  it('preserves key order', () => {
    const keys = ['', 'a', 'a0', 'aa', 'b', 'key:0001', 'key:0002', 'key:1000', 'key:9999', 'z', '~']
    const fs = keys.map(keyToFraction)
    for (let i = 1; i < fs.length; i++) expect(fs[i]).toBeGreaterThanOrEqual(fs[i - 1])
    expect(fs[0]).toBe(0)
    expect(fs[fs.length - 1]).toBeLessThan(1)
  })
  it('maps spans into the viewport and widens tiny tables', () => {
    const lo = keyToFraction('a'), hi = keyToFraction('z')
    const whole = keySpan('a', 'z', lo, hi, 1000)
    expect(whole.x).toBeCloseTo(0, 5)
    expect(whole.w).toBeCloseTo(1000, 0)
    const tiny = keySpan('m', 'm', lo, hi, 1000)
    expect(tiny.w).toBeGreaterThanOrEqual(5)
    expect(keySpan('a', 'b', 1, 1, 500)).toEqual({ x: 0, w: 500 }) // degenerate range
  })
  it('packs overlapping spans into separate lanes', () => {
    const { lanes, count } = packLanes([{ x: 0, w: 50 }, { x: 10, w: 50 }, { x: 60, w: 10 }])
    expect(count).toBe(2)
    expect(lanes[0]).not.toBe(lanes[1])
    expect(lanes[2]).toBe(lanes[0]) // fits after the first
  })
})

describe('splitArgs', () => {
  it('handles quotes and escapes', () => {
    expect(splitArgs('SET a b')).toEqual(['SET', 'a', 'b'])
    expect(splitArgs('  SET  "hello world"   \'x y\'  ')).toEqual(['SET', 'hello world', 'x y'])
    expect(splitArgs('SET k "a\\"b\\n"')).toEqual(['SET', 'k', 'a"b\n'])
    expect(splitArgs('')).toEqual([])
    expect(splitArgs('SET "oops')).toBeNull()
    expect(splitArgs("SET 'oops")).toBeNull()
  })
})

describe('format', () => {
  it('formats sizes, counts and times', () => {
    expect(bytes(512)).toBe('512 B')
    expect(bytes(4 * 1024 * 1024)).toBe('4.0 MiB')
    expect(compact(12345)).toBe('12.3k')
    expect(compact(999)).toBe('999')
    expect(micros(250)).toBe('250 µs')
    expect(micros(2500)).toBe('2.5 ms')
  })
})

const tick = (over: Partial<Tick> = {}): Tick => ({
  type: 'tick', time: new Date().toISOString(), ops_per_sec: 1000, writes_per_sec: 500, write_bytes_per_sec: 5e4,
  memory: { active_bytes: 1, active_entries: 1, threshold: 4, immutable_bytes: [], wal_active_bytes: 0, wal_segment: 1, l0_slowdown_at: 8, l0_stop_at: 12, max_immutables: 2, compaction_style: 'leveled' },
  levels: [], cache_hit_rate: 0.5, cache_hits: 5, cache_misses: 5, bloom_checked: 10, bloom_skipped: 9,
  wal_fsyncs: 1, wal_bytes: 10, stalled: false, slowed: false, recent_writes: null, ...over,
})

describe('reducer', () => {
  it('accumulates ticks, dedupes and bounds recent writes', () => {
    let s = initialState()
    const writes = Array.from({ length: 40 }, (_, i) => ({ seq: i + 1, key: `k${i}`, value_len: 10, delete: false }))
    s = reducer(s, { t: 'batch', messages: [tick({ recent_writes: writes })], now: 1000 })
    expect(s.recentWrites.length).toBe(WRITES_LEN)
    expect(s.recentWrites[0].seq).toBe(40) // newest first
    s = reducer(s, { t: 'batch', messages: [tick({ recent_writes: writes.slice(35) })], now: 1250 })
    expect(new Set(s.recentWrites.map((w) => w.seq)).size).toBe(s.recentWrites.length) // no duplicates
    expect(s.totals.bloomSkipped).toBe(18)
    expect(s.history.length).toBe(2)
    expect(s.history[0].bloomSkipRate).toBeCloseTo(0.9)
  })
  it('tracks flush/compaction state and highlights new files, then expires them', () => {
    let s = initialState()
    s = reducer(s, { t: 'batch', now: 0, messages: [
      { type: 'flush_start', time: '' }, { type: 'compaction_start', time: '', inputs: [3, 4] },
      { type: 'file_created', time: '', file: 9, level: 0 },
    ] })
    expect(s.flushing && s.compacting).toBe(true)
    expect(s.merging).toEqual([3, 4])
    expect(s.fresh[9]).toBe(FRESH_MS)
    s = reducer(s, { t: 'expire', now: FRESH_MS - 1 })
    expect(s.fresh[9]).toBeDefined()
    s = reducer(s, { t: 'expire', now: FRESH_MS + 1 })
    expect(s.fresh[9]).toBeUndefined()
    s = reducer(s, { t: 'batch', now: 5, messages: [{ type: 'flush_end', time: '' }, { type: 'compaction_end', time: '' }] })
    expect(s.flushing || s.compacting).toBe(false)
    expect(s.merging).toEqual([])
    expect(s.log[0].event.type).toBe('compaction_end') // newest first
  })
  it('measures how long a crash outage lasted', () => {
    let s = initialState()
    s = reducer(s, { t: 'conn', state: 'live', now: 100 })
    s = reducer(s, { t: 'conn', state: 'down', now: 1000 })
    s = reducer(s, { t: 'conn', state: 'down', now: 1500 }) // repeated errors keep the first timestamp
    expect(s.downSince).toBe(1000)
    s = reducer(s, { t: 'conn', state: 'live', now: 2600 })
    expect(s.recoveredAfterMs).toBe(1600)
    expect(s.downSince).toBeUndefined()
  })
  it('flags a write stall for a short while', () => {
    let s = initialState()
    s = reducer(s, { t: 'batch', now: 100, messages: [{ type: 'write_stall', time: '' }] })
    expect(s.stalledUntil).toBeGreaterThan(100)
  })
})
