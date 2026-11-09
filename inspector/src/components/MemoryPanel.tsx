import type { InspectorState } from '../lib/reducer'
import { bytes } from '../lib/format'
import { Badge, Card, Meter } from './primitives'

export function MemoryPanel({ state }: { state: InspectorState }) {
  const m = state.tick?.memory ?? state.levels?.memory
  if (!m) return <Card title="Memory and WAL"><p className="empty-note">Waiting for data…</p></Card>
  const imms = m.immutable_bytes ?? []
  return (
    <Card title="Memtable and WAL" sub={`Sealed at ${bytes(m.threshold)}; up to ${m.max_immutables} sealed memtables may await a flush`}
      right={state.flushing ? <Badge tone="good">flushing</Badge> : undefined}>
      <div className="stack">
        <Meter label="Active memtable" value={m.active_bytes} max={m.threshold} right={`${bytes(m.active_bytes)} · ${m.active_entries} entries`} />
        {imms.length === 0
          ? <p className="empty-note">No sealed memtables waiting for a flush.</p>
          : imms.map((b, i) => (
              <Meter key={i} label={`Immutable memtable ${i + 1} (newest first)`} value={b} max={m.threshold} right={bytes(b)} tone={imms.length >= m.max_immutables ? 'warn' : undefined} />
            ))}
        <dl className="kv">
          <dt>WAL segment</dt><dd>{m.wal_segment}</dd>
          <dt>Active WAL size</dt><dd>{bytes(m.wal_active_bytes)}</dd>
          <dt>Compaction style</dt><dd>{m.compaction_style}</dd>
        </dl>
      </div>
    </Card>
  )
}
