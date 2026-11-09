import { useState } from 'react'
import type { InspectorState } from '../lib/reducer'

function hms(sec: number): string {
  const s = Math.max(0, Math.floor(sec))
  return [s / 3600, (s % 3600) / 60, s % 60].map((x) => String(Math.floor(x)).padStart(2, '0')).join(':')
}

function effectiveTheme(): 'light' | 'dark' {
  const t = document.documentElement.dataset.theme
  if (t === 'light' || t === 'dark') return t
  return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

function Mark() {
  return (
    <svg width="26" height="26" viewBox="0 0 26 26" aria-hidden="true">
      <rect x="3" y="3" width="20" height="20" rx="4" fill="none" stroke="currentColor" strokeWidth="1.4" />
      <path d="M3 9h20" stroke="var(--l0)" strokeWidth="2.4" />
      <path d="M3 13.5h20" stroke="var(--l1)" strokeWidth="2.4" />
      <path d="M3 18h20" stroke="var(--l2)" strokeWidth="2.4" />
    </svg>
  )
}

export function Header({ state, route, recovering }: { state: InspectorState; route: string; recovering: boolean }) {
  const [, force] = useState(0)
  const cls = state.conn === 'down' ? ' down' : recovering ? ' rec' : ''
  const label = state.conn === 'down' ? 'disconnected'
    : recovering ? 'recovering…'
    : state.conn === 'live' ? `connected${state.health?.addr ? ` · ${state.health.addr}` : ''}`
    : 'connecting…'
  const up = state.tick?.uptime_seconds ?? state.stats?.server?.uptime_seconds

  function toggleTheme() {
    const next = effectiveTheme() === 'dark' ? 'light' : 'dark'
    document.documentElement.dataset.theme = next
    try { localStorage.setItem('theme', next) } catch { /* storage unavailable */ }
    force((n) => n + 1)
  }

  return (
    <header className="top">
      <a className="brand" href="#/" aria-label="nebuladb inspector home"><Mark />nebuladb <small>inspector</small></a>
      <div className={`conn${cls}`} role="status" aria-live="polite"><span className="dot" aria-hidden="true" /><span>{label}</span></div>
      <span className="top-meta" title="Crashes survived counts outages seen by this page">
        uptime {up === undefined ? '–' : hms(up)} · crashes survived {state.crashes}
      </span>
      <span className="sp" />
      <nav className="tabs" aria-label="Pages">
        <a href="#/" aria-current={route === '/' ? 'page' : undefined}>Inspector</a>
        <a href="#/about" aria-current={route === '/about' ? 'page' : undefined}>About</a>
      </nav>
      <button className="ghost" type="button" onClick={toggleTheme} aria-label="Toggle light or dark theme">light / dark</button>
    </header>
  )
}
