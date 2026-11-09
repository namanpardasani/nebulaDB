import type { DevAction } from '../api/client'
import type { InspectorState } from '../lib/reducer'
import { ActivityLog } from '../components/ActivityLog'
import { BackpressurePanel } from '../components/BackpressurePanel'
import { CrashBanner } from '../components/CrashBanner'
import { DevPanel } from '../components/DevPanel'
import { FiltersPanel } from '../components/FiltersPanel'
import { LevelsPanel } from '../components/LevelsPanel'
import { MemoryPanel } from '../components/MemoryPanel'
import { Overview } from '../components/Overview'
import { TryIt } from '../components/TryIt'
import { WriteFeed } from '../components/WriteFeed'

export function Inspector({ state, action }: { state: InspectorState; action: (a: DevAction) => Promise<{ ok: boolean; message: string }> }) {
  return (
    <main className="page">
      <CrashBanner state={state} />
      <Overview state={state} />
      <div className="grid-2">
        <WriteFeed state={state} />
        <MemoryPanel state={state} />
      </div>
      <LevelsPanel state={state} />
      <div className="grid-2">
        <BackpressurePanel state={state} />
        <ActivityLog state={state} />
      </div>
      <FiltersPanel state={state} />
      <div className="grid-2">
        <TryIt />
        <DevPanel state={state} action={action} />
      </div>
    </main>
  )
}
