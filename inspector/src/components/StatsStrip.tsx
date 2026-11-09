import type { InspectorState } from '../lib/reducer'
import { compact, percent } from '../lib/format'
import { Spark } from './primitives'

/** Six headline numbers. Everything comes from the live 250 ms tick (or /api/stats before the first tick). */
export function StatsStrip({ state }: { state: InspectorState }) {
  const t = state.tick
  const e = state.stats?.engine
  const h = state.history
  const num = (v: number | undefined, f: (n: number) => string) => (v === undefined ? '–' : f(v))
  const peak = (pick: (p: (typeof h)[number]) => number) => Math.max(1, ...h.map(pick))
  return (
    <div className="stats" aria-label="Headline numbers">
      <div className="stat"><div className="k">writes / s</div><div className="v">{num(t?.writes_per_sec, compact)}</div>
        <Spark values={h.map((p) => p.writesPerSec)} h={22} min={0} max={peak((p) => p.writesPerSec)} /></div>
      <div className="stat"><div className="k">commands / s</div><div className="v">{num(t?.ops_per_sec, compact)}</div>
        <Spark values={h.map((p) => p.opsPerSec)} h={22} min={0} max={peak((p) => p.opsPerSec)} stroke="var(--text-2)" /></div>
      <div className="stat"><div className="k">last seq</div><div className="v">{num(t?.last_seq ?? e?.last_seq, (n) => n.toLocaleString('en-US'))}</div></div>
      <div className="stat"><div className="k">sstables</div><div className="v">{num(t?.table_files ?? e?.table_files, String)}</div></div>
      <div className="stat"><div className="k">write amp</div><div className="v">{num(t?.write_amp ?? e?.write_amplification, (n) => n.toFixed(2))}<small>×</small></div></div>
      <div className="stat"><div className="k">bloom useful</div><div className="v">{num(t?.bloom_useful ?? e?.bloom_useful_rate, (n) => percent(n))}</div></div>
    </div>
  )
}
