import type { EngineMetrics } from '../api/types'
import { bytes } from './format'

export interface Step { label: string; value: string; bad?: boolean }
export type Canary = 'present' | 'lost' | 'none'

/**
 * The recovery steps the overlay prints, built ONLY from what the restarted
 * server reports about its own recovery (/api/stats) plus the canary check:
 * before the kill the page writes a key and waits for the acknowledgement;
 * after recovery it reads the key back. "Lost" is shown if that fails.
 */
export function recoverySteps(e: EngineMetrics | undefined, canary: Canary): Step[] {
  const steps: Step[] = []
  if (e) {
    steps.push({ label: 'read CURRENT → MANIFEST', value: `#${e.manifest_number}` })
    steps.push({ label: 'open live sstables', value: `${e.table_files} file${e.table_files === 1 ? '' : 's'}` })
    const seg = e.recovery_segments
    steps.push({ label: `replay wal (${seg} segment${seg === 1 ? '' : 's'})`, value: `${e.recovery_batches} batch${e.recovery_batches === 1 ? '' : 'es'}` })
    steps.push({ label: 'torn tail record', value: e.recovery_torn_tail ? `truncated ${bytes(e.recovery_truncated_bytes)}` : 'none' })
  }
  if (canary === 'present') steps.push({ label: 'acknowledged canary write', value: 'still here' })
  else if (canary === 'lost') steps.push({ label: 'acknowledged canary write', value: 'LOST', bad: true })
  return steps
}
