import { useEffect, useRef, useState } from 'react'
import type { InspectorState } from '../lib/reducer'
import { bytes } from '../lib/format'
import { useGhosts } from '../lib/hooks'
import type { WALSeg } from '../api/types'
import { Panel } from './primitives'

/** Active memtable as a tank that fills, sealed memtables as dashed capsules, WAL segments as bars. */
export function MemoryPanel({ state }: { state: InspectorState }) {
  const m = state.tick?.memory ?? state.levels?.memory
  const segs: WALSeg[] = m?.wal_segments ?? []
  const shown = useGhosts(segs.slice(-5), (s) => s.num).sort((a, b) => a.item.num - b.item.num)

  // Replay the "seal" squeeze whenever the active WAL segment number changes (= a memtable was sealed).
  const lastSeg = useRef<number | undefined>(undefined)
  const [seal, setSeal] = useState(0)
  useEffect(() => {
    if (!m) return
    if (lastSeg.current !== undefined && lastSeg.current !== m.wal_segment) setSeal((n) => n + 1)
    lastSeg.current = m.wal_segment
  }, [m])

  if (!m) return <Panel id="h-mem" title="memtable & wal" num="02"><div className="empty">Waiting for data…</div></Panel>
  const frac = m.threshold > 0 ? Math.min(1, m.active_bytes / m.threshold) : 0
  const imms = m.immutable_bytes ?? []
  const scale = Math.max(m.threshold * 1.05, ...segs.map((s) => s.bytes), 1)
  return (
    <Panel id="h-mem" title="memtable & wal" num="02">
      <div className="pb">
        <div className="mem">
          <div key={seal} className={`tank${seal ? ' seal' : ''}`} role="img" aria-label={`Active memtable ${Math.round(frac * 100)}% full`}>
            <div className="fill" style={{ height: `${frac * 100}%` }} /><div className="marks" />
          </div>
          <div className="memv">
            <div className="big">{bytes(m.active_bytes)}</div>
            <div className="sub">of {bytes(m.threshold)} · active</div>
            <div className="sub">{m.active_entries.toLocaleString('en-US')} entries</div>
            <div className="sealed" aria-live="polite">
              {imms.slice(0, 3).map((b, i) => <div key={i} className="cap"><span>sealed · {i === 0 ? 'newest' : `#${i + 1}`}</span><span>{bytes(b)}</span></div>)}
              {imms.length > 3 && <div className="cap"><span>+{imms.length - 3} more waiting</span><span /></div>}
            </div>
          </div>
        </div>
        <div className="wal">
          <div className="lbl">wal segments</div>
          <div className="segs">
            {shown.length === 0 && <div className="sub" style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--muted)' }}>none yet</div>}
            {shown.map(({ item, gone }) => (
              <div key={item.num} className={`seg${item.num === m.wal_segment ? ' cur' : ''}${gone ? ' gone' : ''}`} title={`wal-${String(item.num).padStart(6, '0')}.log · ${bytes(item.bytes)}`}>
                <div className="bar"><i style={{ transform: `scaleX(${Math.min(1, item.bytes / scale)})` }} /></div>
                <span>{String(item.num).padStart(6, '0')}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Panel>
  )
}
