import type { InspectorState } from '../lib/reducer'
import { Badge, Card, Meter } from './primitives'

export function BackpressurePanel({ state }: { state: InspectorState }) {
  const m = state.tick?.memory ?? state.levels?.memory
  const e = state.stats?.engine
  const l0 = state.tick?.levels?.[0]?.files ?? e?.levels?.[0]?.files ?? 0
  const now = Date.now()
  const stalled = state.stalledUntil > now
  const slowed = !!state.tick?.slowed
  const stop = m?.l0_stop_at ?? 12
  const slow = m?.l0_slowdown_at ?? 8
  const tone = l0 >= stop ? 'bad' : l0 >= slow ? 'warn' : undefined
  return (
    <Card title="Backpressure" sub="Writers slow down when L0 piles up and block when it reaches the stop limit"
      right={stalled ? <Badge tone="bad">writers stalled</Badge> : slowed ? <Badge tone="warn">writes slowed</Badge> : <Badge tone="good">flowing</Badge>}>
      <div className="stack">
        <Meter label="L0 files" value={l0} max={stop} right={`${l0} / stop at ${stop}`} markers={[slow / stop]} tone={tone} />
        <dl className="kv">
          <dt>Slowdown from</dt><dd>{slow} L0 files</dd>
          <dt>Write stalls so far</dt><dd>{e ? `${e.write_stalls} (${e.write_stall_seconds.toFixed(2)} s blocked)` : '–'}</dd>
          <dt>Slowed writes so far</dt><dd>{e ? `${e.write_slowdowns} (${e.write_slowdown_seconds.toFixed(2)} s added)` : '–'}</dd>
          <dt>Flushes / compactions</dt><dd>{e ? `${e.flushes} / ${e.compactions}` : '–'}</dd>
        </dl>
      </div>
    </Card>
  )
}
