import { useEffect, useState } from 'react'
import type { InspectorState } from '../lib/reducer'
import { Badge } from './primitives'

type Theme = 'auto' | 'light' | 'dark'

function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(() => (localStorage.getItem('theme') as Theme | null) ?? 'auto')
  useEffect(() => {
    const el = document.documentElement
    if (theme === 'auto') el.removeAttribute('data-theme')
    else el.setAttribute('data-theme', theme)
    try { localStorage.setItem('theme', theme) } catch { /* storage unavailable */ }
  }, [theme])
  return (
    <label className="card-sub">
      Theme{' '}
      <select value={theme} onChange={(e) => setTheme(e.target.value as Theme)} aria-label="theme">
        <option value="auto">System</option><option value="light">Light</option><option value="dark">Dark</option>
      </select>
    </label>
  )
}

export function Header({ state, route }: { state: InspectorState; route: string }) {
  const tone = state.conn === 'live' ? 'good' : state.conn === 'down' ? 'bad' : 'warn'
  const label = state.conn === 'live' ? 'live' : state.conn === 'down' ? 'disconnected' : 'connecting'
  return (
    <header className="app-header">
      <span className="app-title">NebulaDB Inspector</span>
      <nav className="app-nav" aria-label="main">
        <a href="#/" aria-current={route === '/' ? 'page' : undefined}>Inspector</a>
        <a href="#/about" aria-current={route === '/about' ? 'page' : undefined}>About</a>
      </nav>
      <span className="app-spacer" />
      {state.health && <span className="card-sub">server {state.health.version}</span>}
      <Badge tone={tone}>{label}</Badge>
      <ThemeToggle />
    </header>
  )
}
