import type { ReactNode } from 'react'
import type { HistoryPoint } from '../lib/reducer'

export function Card({ title, sub, right, children }: { title: string; sub?: string; right?: ReactNode; children: ReactNode }) {
  return (
    <section className="card">
      <div className="card-head">
        <div>
          <h2 className="card-title">{title}</h2>
          {sub && <p className="card-sub">{sub}</p>}
        </div>
        {right}
      </div>
      {children}
    </section>
  )
}

export function StatCard({ label, value, note }: { label: string; value: ReactNode; note?: ReactNode }) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {note && <div className="stat-note">{note}</div>}
    </div>
  )
}

export function Badge({ tone, children }: { tone?: 'good' | 'warn' | 'bad'; children: ReactNode }) {
  return (
    <span className={`badge${tone ? ` badge--${tone}` : ''}`}>
      <span className="badge-dot" aria-hidden="true" />
      {children}
    </span>
  )
}

/** Horizontal meter: value/max with optional threshold markers (as fractions of max). */
export function Meter({ value, max, label, right, markers = [], tone }: {
  value: number; max: number; label: string; right?: string; markers?: number[]; tone?: 'warn' | 'bad'
}) {
  const frac = max > 0 ? Math.min(1, value / max) : 0
  return (
    <div>
      <div className="meter-label"><span>{label}</span><span>{right}</span></div>
      <div className="meter" role="meter" aria-label={label} aria-valuemin={0} aria-valuemax={max} aria-valuenow={Math.min(value, max)}>
        <div className={`meter-fill${tone ? ` meter-fill--${tone}` : ''}`} style={{ width: `${frac * 100}%` }} />
        {markers.map((m, i) => <div key={i} className="meter-marker" style={{ left: `${Math.min(1, m) * 100}%` }} />)}
      </div>
    </div>
  )
}

/** Sparkline of one metric over the recent history. NaN points are skipped. */
export function Sparkline({ points, pick, max }: { points: HistoryPoint[]; pick: (p: HistoryPoint) => number; max?: number }) {
  const vals = points.map(pick)
  const finite = vals.filter((v) => Number.isFinite(v))
  if (finite.length < 2) return <svg className="spark" viewBox="0 0 100 36" aria-hidden="true" />
  const top = max ?? Math.max(...finite, 1e-9)
  const step = 100 / Math.max(1, vals.length - 1)
  let line = ''
  vals.forEach((v, i) => {
    if (!Number.isFinite(v)) return
    line += `${line ? 'L' : 'M'}${(i * step).toFixed(2)} ${(34 - (v / top) * 32).toFixed(2)}`
  })
  return (
    <svg className="spark" viewBox="0 0 100 36" preserveAspectRatio="none" aria-hidden="true">
      <path className="spark-line" d={line} />
    </svg>
  )
}
