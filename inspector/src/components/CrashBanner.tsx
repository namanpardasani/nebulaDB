import type { InspectorState } from '../lib/reducer'
import { clock } from '../lib/format'

/** Shows the outage after "Crash it" and what recovery did when the server returned. */
export function CrashBanner({ state }: { state: InspectorState }) {
  if (state.conn === 'down') {
    return (
      <div className="banner banner--bad" role="alert">
        Server unreachable since {state.downSince ? clock(new Date(state.downSince).toISOString()) : '…'}. If you pressed “Crash it”, the supervisor restarts it in a moment and the recovery will be shown here.
      </div>
    )
  }
  const recent = state.recoveredAt !== undefined && Date.now() - state.recoveredAt < 30_000
  if (recent) {
    const rec = state.log.find((l) => l.event.type === 'recovery')?.event
    return (
      <div className="banner banner--good" role="status">
        Recovered after {((state.recoveredAfterMs ?? 0) / 1000).toFixed(1)} s down.{' '}
        {rec ? `Recovery replayed ${rec.count ?? 0} WAL batches (${rec.detail ?? ''}). ` : ''}
        Every acknowledged write is still there: try a GET below.
      </div>
    )
  }
  return null
}
