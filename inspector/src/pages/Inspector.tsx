import type { DevAction } from '../api/client'
import type { InspectorState } from '../lib/reducer'
import { Activity } from '../components/Activity'
import { Backpressure } from '../components/Backpressure'
import { Controls } from '../components/Controls'
import { Filters } from '../components/Filters'
import { MemoryPanel } from '../components/MemoryPanel'
import { StatsStrip } from '../components/StatsStrip'
import { StrataPanel } from '../components/StrataPanel'
import { TryIt } from '../components/TryIt'
import { WriteFeed } from '../components/WriteFeed'

export function Inspector({ state, action, crash, onWrite }: {
  state: InspectorState
  action: (a: DevAction) => Promise<{ ok: boolean; message: string }>
  crash: () => Promise<{ ok: boolean; message: string }>
  onWrite: (op: string, key: string) => void
}) {
  return (
    <main>
      <StatsStrip state={state} />
      <div className="grid">
        <div className="col"><WriteFeed state={state} /><MemoryPanel state={state} /></div>
        <div className="col center"><StrataPanel state={state} /></div>
        <div className="col"><Backpressure state={state} /><Activity state={state} /></div>
      </div>
      <div className="row2">
        <Filters state={state} />
        <TryIt onWrite={onWrite} />
        <Controls state={state} action={action} crash={crash} />
      </div>
    </main>
  )
}
