import type { CommandReply, EngineEvent, Health, LevelsResponse, StatsResponse, StreamMessage } from './types'

/** Base URL of the admin server. Override with ?admin=http://host:port. */
export function adminBase(): string {
  const q = new URLSearchParams(window.location.search).get('admin')
  return (q ?? 'http://127.0.0.1:7380').replace(/\/$/, '')
}

async function getJSON<T>(path: string, signal?: AbortSignal): Promise<T> {
  const r = await fetch(adminBase() + path, { signal })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return (await r.json()) as T
}

export const fetchHealth = (s?: AbortSignal) => getJSON<Health>('/api/health', s)
export const fetchStats = (s?: AbortSignal) => getJSON<StatsResponse>('/api/stats', s)
export const fetchLevels = (s?: AbortSignal) => getJSON<LevelsResponse>('/api/levels', s)

async function postJSON<T>(path: string, body: unknown): Promise<{ status: number; data: T }> {
  const r = await fetch(adminBase() + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' }, // required by the server's CSRF guard
    body: JSON.stringify(body),
  })
  let data: unknown = {}
  try { data = await r.json() } catch { /* empty body */ }
  return { status: r.status, data: data as T }
}

/** Runs a command through the admin proxy (whitelisted commands only). */
export async function runCommand(args: string[]): Promise<CommandReply> {
  const { status, data } = await postJSON<CommandReply & { error?: string }>('/api/cmd', { args })
  if (status !== 200) return { error: (data as { error?: string }).error ?? `HTTP ${status}` }
  return data
}

export type DevAction = 'flush' | 'compact' | 'crash'
export async function devAction(a: DevAction): Promise<{ ok: boolean; message: string }> {
  const { status, data } = await postJSON<{ status?: string; error?: string }>(`/api/dev/${a}`, {})
  return { ok: status >= 200 && status < 300, message: data.status ?? data.error ?? `HTTP ${status}` }
}

export interface StreamHandlers {
  onMessage: (m: StreamMessage) => void
  onOpen: () => void
  onError: () => void
}

/** Opens the SSE stream. The browser reconnects by itself and resends Last-Event-ID,
 *  so lifecycle events missed during a short outage are replayed by the server. */
export function openStream(h: StreamHandlers): () => void {
  const es = new EventSource(adminBase() + '/api/events')
  es.onopen = h.onOpen
  es.onerror = h.onError
  es.onmessage = (ev) => {
    try { h.onMessage(JSON.parse(ev.data) as StreamMessage) } catch { /* ignore malformed */ }
  }
  return () => es.close()
}

export function isTick(m: StreamMessage): m is import('./types').Tick {
  return m.type === 'tick'
}
export type { EngineEvent }
