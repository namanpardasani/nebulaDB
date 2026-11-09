import type { InspectorState } from '../lib/reducer'
import { bytes } from '../lib/format'
import { Card } from './primitives'

/** Sampled recent writes (the server samples about 1 in 16 commits). */
export function WriteFeed({ state }: { state: InspectorState }) {
  const writes = state.recentWrites
  const newest = writes[0]?.seq ?? 0
  return (
    <Card title="Incoming writes" sub="A sample of recent commits: sequence number, key, value size">
      {writes.length === 0
        ? <p className="empty-note">No writes seen yet. Use “Write 500 random keys” or point redis-cli at the server.</p>
        : (
          <ul className="feed" aria-label="recent writes">
            {writes.map((w) => (
              <li key={w.seq} className={`feed-item${newest - w.seq < 32 ? ' feed-item--new' : ''}`}>
                <span className="feed-seq">#{w.seq}</span>
                <span className="feed-key mono" title={w.key}>{w.delete ? 'DEL ' : 'SET '}{w.key}</span>
                <span className="feed-meta">{w.delete ? 'tombstone' : bytes(w.value_len)}</span>
              </li>
            ))}
          </ul>
        )}
    </Card>
  )
}
