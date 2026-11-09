import type { EngineEvent } from '../api/types'
import type { InspectorState } from '../lib/reducer'
import { bytes, clock, duration } from '../lib/format'
import { Card } from './primitives'

function describe(e: EngineEvent): { tone: string; text: string } {
  switch (e.type) {
    case 'memtable_sealed': return { tone: 'flush', text: `memtable sealed (${bytes(e.bytes ?? 0)}, ${e.count ?? 0} entries)` }
    case 'wal_rotated': return { tone: 'flush', text: `new WAL segment ${e.file}` }
    case 'flush_start': return { tone: 'flush', text: `flush started (${bytes(e.bytes ?? 0)})` }
    case 'flush_end': return { tone: 'flush', text: `flush finished in ${duration(e.nanos ?? 0)} → ${e.file ? `table ${e.file} (${bytes(e.bytes ?? 0)})` : 'no table'}` }
    case 'file_created': return { tone: 'file', text: `table ${e.file} created at L${e.level ?? 0} (${bytes(e.bytes ?? 0)}, ${e.count ?? 0} entries)` }
    case 'file_deleted': return { tone: 'file', text: `table ${e.file} deleted (L${e.level ?? 0})` }
    case 'compaction_start': return { tone: 'compaction', text: `compaction started from L${e.level ?? 0}: tables ${(e.inputs ?? []).join(', ')} (${bytes(e.bytes ?? 0)}) ${e.detail ?? ''}` }
    case 'compaction_end': return { tone: 'compaction', text: `compaction finished in ${duration(e.nanos ?? 0)}: → tables ${(e.outputs ?? []).join(', ') || 'none'} at L${e.level ?? 0}, dropped ${e.count ?? 0} entries` }
    case 'write_stall': return { tone: 'stall', text: `writers stalled: ${e.detail ?? ''} (${e.count ?? 0} sealed memtables, ${e.level ?? 0} L0 files)` }
    case 'recovery': return { tone: 'file', text: `recovery: replayed ${e.count ?? 0} WAL batches (${e.detail ?? ''})` }
  }
}

export function ActivityLog({ state }: { state: InspectorState }) {
  return (
    <Card title="Engine activity" sub="Flushes, compactions, file creation and deletion, stalls, recovery">
      {state.log.length === 0
        ? <p className="empty-note">Nothing has happened yet.</p>
        : (
          <ul className="log" aria-label="engine events">
            {state.log.map((it) => {
              const d = describe(it.event)
              return (
                <li key={it.id} className="log-item">
                  <span className="log-time">{clock(it.event.time)}</span>
                  <span className={`log-type log-type--${d.tone}`}>{it.event.type}</span>
                  <span>{d.text}</span>
                </li>
              )
            })}
          </ul>
        )}
    </Card>
  )
}
