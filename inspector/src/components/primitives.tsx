import type { ReactNode } from 'react'
import { sparkPoints } from '../lib/spark'

/** A numbered instrument panel: mono caps title, index on the right. */
export function Panel({ id, title, num, right, className, children }: {
  id: string; title: string; num: string; right?: ReactNode; className?: string; children: ReactNode
}) {
  return (
    <section className={`panel${className ? ` ${className}` : ''}`} aria-labelledby={id}>
      <div className="ph"><b id={id}>{title}</b><span className="id">{right ?? num}</span></div>
      {children}
    </section>
  )
}

/** Decorative sparkline (the value next to it is the accessible one). */
export function Spark({ values, h, min, max, stroke = 'var(--amber)' }: {
  values: number[]; h: number; min: number; max: number; stroke?: string
}) {
  return (
    <svg viewBox={`0 0 120 ${h}`} preserveAspectRatio="none" aria-hidden="true">
      <polyline points={sparkPoints(values, h, min, max)} fill="none" stroke={stroke} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
  )
}
