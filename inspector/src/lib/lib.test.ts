import { describe, expect, it } from 'vitest'
import { keyToFraction, keySpan, packLanes } from './keymap'
import { splitArgs } from './args'
import { bytes, compact, micros } from './format'
import { initialState, reducer, FRESH_MS, WRITES_LEN } from './reducer'
import type { EngineMetrics, FileInfo, Tick } from '../api/types'
import { formatReply, writtenKeys } from './reply'
import { sparkPoints } from './spark'
import { layoutStrata, L0_MIN_H, MIN_LEVELS } from './strata'
import { recoverySteps } from './crash'
import { ACTIVITY_LEN, YOU_LEN } from './reducer'

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
  wal_fsyncs: 1, wal_bytes: 10, last_seq: 7, table_files: 2, write_amp: 1.5, bloom_useful: 0.9, uptime_seconds: 12, stalled: false, slowed: false, recent_writes: null, ...over,
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

const file = (num: number, level: number, smallest: string, largest: string, size = 1000): FileInfo =>
  ({ num, level, size, entries: 10, deletes: 0, min_seq: 1, max_seq: 2, smallest, largest })

describe('layoutStrata', () => {
  it('always shows at least four bands and stacks them without overlap', () => {
    const l = layoutStrata([[], [], [], []], 800)
    expect(l.bands.length).toBe(MIN_LEVELS)
    expect(l.bands[0].h).toBe(L0_MIN_H)
    for (let i = 1; i < l.bands.length; i++) expect(l.bands[i].top).toBeGreaterThanOrEqual(l.bands[i - 1].top + l.bands[i - 1].h)
  })
  it('grows to the deepest non-empty level and puts overlapping L0 tables in different lanes', () => {
    const levels = [[file(1, 0, 'a', 'm'), file(2, 0, 'c', 'p')], [], [], [], [], [file(3, 5, 'a', 'z')]]
    const l = layoutStrata(levels, 800)
    expect(l.bands.length).toBe(6)
    const [a, b] = l.placed.filter((p) => p.level === 0)
    expect(a.y).not.toBe(b.y)
    expect(l.lo).toBe('a')
    expect(l.hi).toBe('z')
    const deep = l.placed.find((p) => p.level === 5)!
    expect(deep.x).toBeCloseTo(0, 3)
    expect(deep.y + deep.h).toBeLessThanOrEqual(l.bands[5].top + l.bands[5].h)
  })
  it('places deeper-level tables left to right in key order and keeps them inside the width', () => {
    const l = layoutStrata([[], [file(1, 1, 'a', 'f'), file(2, 1, 'g', 'p'), file(3, 1, 'q', 'z')]], 600)
    const xs = l.placed.map((p) => p.x)
    expect([...xs].sort((p, q) => p - q)).toEqual(xs)
    for (const p of l.placed) expect(p.x + p.w).toBeLessThanOrEqual(600 + 1e-6)
  })
})

describe('layoutStrata with lumpy key spaces', () => {
  it('does not squeeze tables when one stray key is far from the rest', () => {
    const levels = [[], [file(1, 1, 'demo:1', 'demo:1'), file(2, 1, 'user:000', 'user:300'), file(3, 1, 'user:301', 'user:600'), file(4, 1, 'user:601', 'user:999')]]
    const l = layoutStrata(levels, 900)
    for (const p of l.placed.filter((q) => q.f.num > 1)) expect(p.w).toBeGreaterThan(100)
  })
})

describe('reply formatting', () => {
  it('prints like redis-cli', () => {
    expect(formatReply('OK').text).toBe('OK')
    expect(formatReply('hello').text).toBe('"hello"')
    expect(formatReply(null).text).toBe('(nil)')
    expect(formatReply(3).text).toBe('(integer) 3')
    expect(formatReply(['0', ['a', 'b']]).text).toBe('1) "0"\n2) 1) "a"\n   2) "b"')
    expect(formatReply([]).text).toBe('(empty array)')
    expect(formatReply({ error: 'ERR boom' })).toEqual({ text: '(error) ERR boom', error: true })
  })
  it('knows which commands write which keys', () => {
    expect(writtenKeys(['set', 'a', '1'])).toEqual({ op: 'SET', keys: ['a'] })
    expect(writtenKeys(['MSET', 'a', '1', 'b', '2'])).toEqual({ op: 'SET', keys: ['a', 'b'] })
    expect(writtenKeys(['DEL', 'a', 'b'])).toEqual({ op: 'DEL', keys: ['a', 'b'] })
    expect(writtenKeys(['GET', 'a'])).toBeNull()
  })
})

describe('sparkPoints', () => {
  it('drops NaN, clamps, and needs two points', () => {
    expect(sparkPoints([1], 22, 0, 1)).toBe('')
    expect(sparkPoints([NaN, NaN], 22, 0, 1)).toBe('')
    expect(sparkPoints([0, 1], 22, 0, 1)).toBe('0.0,20.0 120.0,2.0')
    expect(sparkPoints([0, 5], 22, 0, 1)).toBe('0.0,20.0 120.0,2.0') // clamped to max
  })
})

describe('recoverySteps', () => {
  const e = { manifest_number: 7, table_files: 3, recovery_segments: 2, recovery_batches: 41, recovery_torn_tail: true, recovery_truncated_bytes: 100 } as EngineMetrics
  it('is built from what the restarted server reports plus the canary check', () => {
    const steps = recoverySteps(e, 'present')
    expect(steps.map((s) => s.value)).toEqual(['#7', '3 files', '41 batches', 'truncated 100 B', 'still here'])
    expect(steps[2].label).toBe('replay wal (2 segments)')
  })
  it('flags a lost canary and tolerates missing stats', () => {
    expect(recoverySteps(e, 'lost').at(-1)).toEqual({ label: 'acknowledged canary write', value: 'LOST', bad: true })
    expect(recoverySteps(undefined, 'none')).toEqual([])
  })
})

describe('reducer (design-system state)', () => {
  it('keeps a bounded seismograph that spikes on a compaction and decays', () => {
    let s = initialState()
    expect(s.activity.length).toBe(ACTIVITY_LEN)
    s = reducer(s, { t: 'batch', now: 1, messages: [{ type: 'compaction_end', time: '' }, tick()] })
    expect(s.activity.length).toBe(ACTIVITY_LEN)
    const spiked = Math.abs(s.activity[ACTIVITY_LEN - 1])
    expect(spiked).toBeGreaterThan(0.9)
    for (let i = 0; i < 30; i++) s = reducer(s, { t: 'batch', now: 2 + i, messages: [tick()] })
    expect(Math.abs(s.activity[ACTIVITY_LEN - 1])).toBeLessThan(spiked)
  })
  it('counts crashes survived and logs the outage', () => {
    let s = initialState()
    s = reducer(s, { t: 'conn', state: 'live', now: 1 })
    s = reducer(s, { t: 'conn', state: 'down', now: 2 })
    s = reducer(s, { t: 'conn', state: 'live', now: 3 })
    expect(s.crashes).toBe(1)
    expect(s.log.map((l) => l.event.type)).toEqual(['connection_restored', 'connection_lost'])
  })
  it('remembers your own writes (bounded) and lets a stall expire', () => {
    let s = initialState()
    for (let i = 0; i < YOU_LEN + 3; i++) s = reducer(s, { t: 'you', op: 'SET', key: `k${i}` })
    expect(s.youWrites.length).toBe(YOU_LEN)
    expect(s.youWrites[0].key).toBe(`k${YOU_LEN + 2}`)
    s = reducer(s, { t: 'batch', now: 100, messages: [{ type: 'write_stall', time: '' }] })
    s = reducer(s, { t: 'expire', now: 100 + 2001 })
    expect(s.stalledUntil).toBe(0)
  })
})
