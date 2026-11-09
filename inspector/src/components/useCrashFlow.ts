import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchStats, runCommand, type DevAction } from '../api/client'
import type { EngineMetrics } from '../api/types'
import { recoverySteps, type Canary, type Step } from '../lib/crash'
import { reducedMotion } from '../lib/hooks'
import type { ConnState } from '../lib/reducer'

export type Phase = 'idle' | 'lost' | 'recovering' | 'online'
export interface CrashView { phase: Phase; steps: Step[]; title: string; sub: string; verified: boolean }
export interface CrashFlow {
  view: CrashView
  /** Writes a canary key, waits for its acknowledgement, then SIGKILLs the server. */
  start: () => Promise<{ ok: boolean; message: string }>
}

const IDLE: CrashView = { phase: 'idle', steps: [], title: '', sub: '', verified: false }
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, reducedMotion() ? Math.min(ms, 120) : ms))

async function statsWithRetry(): Promise<EngineMetrics | undefined> {
  for (let i = 0; i < 8; i++) {
    try { const s = await fetchStats(); if (s.engine) return s.engine } catch { /* server still coming up */ }
    await sleep(300)
  }
  return undefined
}

/**
 * Drives the crash/recovery overlay from REAL signals: the SSE connection
 * dropping is "signal lost"; its return starts recovery, whose steps are what
 * the restarted server reports (/api/stats) plus a canary check: a key the page
 * wrote and saw acknowledged before the kill must be readable afterwards.
 */
export function useCrashFlow(conn: ConnState, devAction: (a: DevAction) => Promise<{ ok: boolean; message: string }>): CrashFlow {
  const [view, setView] = useState<CrashView>(IDLE)
  const phase = useRef<Phase>('idle')
  const killed = useRef(false)
  const canary = useRef<{ key: string; val: string } | null>(null)

  const set = useCallback((v: CrashView | ((c: CrashView) => CrashView)) => {
    setView((cur) => { const n = typeof v === 'function' ? v(cur) : v; phase.current = n.phase; return n })
  }, [])

  const start = useCallback(async () => {
    const key = `user:~canary-${Date.now()}`
    const val = Math.random().toString(36).slice(2)
    const w = await runCommand(['SET', key, val])
    if (w.error || w.reply !== 'OK') return { ok: false, message: `canary write failed: ${w.error ?? JSON.stringify(w.reply)}` }
    canary.current = { key, val }
    killed.current = true
    const r = await devAction('crash')
    if (!r.ok) { killed.current = false; canary.current = null }
    return r
  }, [devAction])

  useEffect(() => {
    if (conn === 'down') {
      set({ phase: 'lost', steps: [], title: 'signal lost', verified: false,
        sub: killed.current ? 'server received SIGKILL · no shutdown, no flush, no goodbye' : 'the connection to the server dropped' })
      return
    }
    if (conn !== 'live' || (phase.current !== 'lost' && phase.current !== 'recovering')) return
    let cancelled = false
    void (async () => {
      set((v) => ({ ...v, phase: 'recovering', steps: [], title: 'recovering', sub: killed.current ? 'supervisor restarted the process' : 'the server is back; reading what it recovered' }))
      await sleep(500)
      const engine = await statsWithRetry()
      let state: Canary = 'none'
      if (canary.current) {
        const r = await runCommand(['GET', canary.current.key])
        state = r.reply === canary.current.val ? 'present' : 'lost'
      }
      const steps = recoverySteps(engine, state)
      for (const s of steps) {
        await sleep(450)
        if (cancelled) return
        set((v) => ({ ...v, steps: [...v.steps, s] }))
      }
      await sleep(800)
      if (cancelled) return
      const verified = state === 'present'
      set((v) => ({ ...v, phase: 'online', verified, title: state === 'lost' ? 'recovered, but a write is missing' : 'back online',
        sub: verified ? 'the write acknowledged just before the crash is still here' : 'the server is serving again' }))
      await sleep(1400)
      if (cancelled) return
      killed.current = false; canary.current = null
      set(IDLE)
    })()
    return () => { cancelled = true }
  }, [conn, set])

  return { view, start }
}
