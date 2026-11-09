import type { InspectorState } from '../lib/reducer'
import { Panel } from './primitives'

const ROWS = 10

/** Sampled recent commits (server samples ~1 in 16) plus the writes you made from this page, marked "you". */
export function WriteFeed({ state }: { state: InspectorState }) {
  const you = state.youWrites.map((w) => ({ k: `y${w.id}`, op: w.op, key: w.key, label: 'you', you: true }))
  const srv = state.recentWrites.map((w) => ({ k: `s${w.seq}`, op: w.delete ? 'DEL' : 'SET', key: w.key, label: w.seq.toLocaleString('en-US'), you: false }))
  const rows = [...you, ...srv].slice(0, ROWS)
  return (
    <Panel id="h-writes" title="incoming writes" num="01">
      {rows.length === 0
        ? <div className="empty">No writes yet. Run “write 500 keys”, or point redis-cli at the server.</div>
        : (
          <ul className="writes" aria-label="Recent commits">
            {rows.map((r) => (
              <li key={r.k} className={`enter${r.you ? ' you' : ''}`}>
                <span className="op">{r.op}</span><span className="key" title={r.key}>{r.key}</span><span>{r.label}</span>
              </li>
            ))}
          </ul>
        )}
    </Panel>
  )
}
