import type { InspectorState } from '../lib/reducer'
import { useWidth } from '../lib/hooks'
import { Panel } from './primitives'

/**
 * A ruler of L0 file counts with the slowdown and stop thresholds marked and a
 * needle at the current count. The needle is transform-positioned (px from the
 * measured ruler width) and eases like a paper gauge.
 */
export function Backpressure({ state }: { state: InspectorState }) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const m = state.tick?.memory ?? state.levels?.memory
  const e = state.stats?.engine
  const l0 = state.tick?.levels?.[0]?.files ?? e?.levels?.[0]?.files ?? 0
  const slow = m?.l0_slowdown_at ?? 8
  const stop = m?.l0_stop_at ?? 12
  const max = Math.max(stop + 1, Math.ceil(stop * 1.2))
  const sealed = m?.immutable_bytes?.length ?? 0
  const stalled = state.stalledUntil > 0 || l0 >= stop
  const slowed = !stalled && (!!state.tick?.slowed || l0 >= slow)
  const text = stalled ? 'writers stalled' : slowed ? 'slowing writers' : 'flowing'
  const pct = (n: number) => `${(n / max) * 100}%`
  return (
    <Panel id="h-bp" title="backpressure" num="04">
      <div className="pb">
        <div className="ruler" ref={ref} aria-hidden="true">
          <div className="track">
            <div className="zone slow" style={{ left: pct(slow), width: `${((stop - slow) / max) * 100}%` }} />
            <div className="zone stop" style={{ left: pct(stop), right: 0 }} />
          </div>
          <div className="tick" style={{ left: pct(slow) }} /><div className="tl" style={{ left: pct(slow) }}>slow {slow}</div>
          <div className="tick" style={{ left: pct(stop) }} /><div className="tl" style={{ left: pct(stop) }}>stop {stop}</div>
          <div className="needle" style={{ transform: `translateX(${Math.min(1, l0 / max) * width}px)`, background: stalled ? 'var(--magma)' : slowed ? 'var(--amber)' : 'var(--text)' }} />
        </div>
        <div className={`bp-state${stalled ? ' stall' : slowed ? ' slow' : ''}`} role="status">
          <span>L0 files <b>{l0}</b></span><b>{text}</b>
        </div>
        <dl className="bp-facts">
          <dt>sealed memtables</dt><dd>{sealed}{m ? ` / ${m.max_immutables}` : ''}</dd>
          <dt>write stalls so far</dt><dd>{e ? `${e.write_stalls} (${e.write_stall_seconds.toFixed(2)} s)` : '–'}</dd>
          <dt>slowed writes so far</dt><dd>{e ? `${e.write_slowdowns} (${e.write_slowdown_seconds.toFixed(2)} s)` : '–'}</dd>
        </dl>
      </div>
    </Panel>
  )
}
