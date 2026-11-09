import { useEffect, useRef } from 'react'
import type { EngineEvent } from '../api/types'
import { bytes, clock, duration } from '../lib/format'
import { useThemeTick, useWidth } from '../lib/hooks'
import type { InspectorState } from '../lib/reducer'
import { Panel } from './primitives'

const LOG_ROWS = 12

interface Line { cls: string; glyph: string; text: string }

function describe(e: EngineEvent): Line {
  switch (e.type) {
    case 'memtable_sealed': return { cls: 'flush', glyph: '◇', text: `memtable sealed (${bytes(e.bytes ?? 0)}, ${e.count ?? 0} entries)` }
    case 'wal_rotated': return { cls: 'flush', glyph: '◇', text: `new wal segment ${String(e.file ?? 0).padStart(6, '0')}` }
    case 'flush_start': return { cls: 'flush', glyph: 'F', text: `flush started (${bytes(e.bytes ?? 0)})` }
    case 'flush_end': return { cls: 'flush', glyph: 'F', text: `flush done in ${duration(e.nanos ?? 0)} → ${e.file ? `#${e.file} (${bytes(e.bytes ?? 0)})` : 'no table'}` }
    case 'file_created': return { cls: 'new', glyph: '+', text: `sstable #${e.file} created in L${e.level ?? 0} (${bytes(e.bytes ?? 0)})` }
    case 'file_deleted': return { cls: 'del', glyph: '−', text: `sstable #${e.file} removed (L${e.level ?? 0})` }
    case 'compaction_start': return { cls: 'compact', glyph: 'C', text: `compaction from L${e.level ?? 0}: ${(e.inputs ?? []).length} files, ${bytes(e.bytes ?? 0)}` }
    case 'compaction_end': return { cls: 'compact', glyph: 'C', text: `compaction done in ${duration(e.nanos ?? 0)}: ${(e.outputs ?? []).length} files out at L${e.level ?? 0}, ${e.count ?? 0} entries dropped` }
    case 'write_stall': return { cls: 'stall', glyph: '!', text: `writes stalled: ${e.detail ?? ''} (${e.count ?? 0} sealed, ${e.level ?? 0} L0 files)` }
    case 'recovery': return { cls: 'recover', glyph: '↺', text: `recovered: ${e.count ?? 0} wal batches replayed (${e.detail ?? ''})` }
    case 'connection_lost': return { cls: 'crash', glyph: '×', text: 'connection lost: server unreachable' }
    case 'connection_restored': return { cls: 'recover', glyph: '↺', text: 'connection restored' }
  }
}

/** Real activity trace: amplitude follows the measured write rate and spikes on real flush/compaction/stall events. */
function Seismograph({ activity, dead }: { activity: number[]; dead: boolean }) {
  const [wrap, w] = useWidth<HTMLDivElement>()
  const cv = useRef<HTMLCanvasElement>(null)
  const themeTick = useThemeTick()
  useEffect(() => {
    const c = cv.current
    if (!c || w === 0) return
    const h = 96, d = window.devicePixelRatio || 1
    c.width = w * d; c.height = h * d
    const ctx = c.getContext('2d')
    if (!ctx) return
    ctx.setTransform(d, 0, 0, d, 0, 0)
    const st = getComputedStyle(document.documentElement)
    ctx.clearRect(0, 0, w, h)
    ctx.strokeStyle = st.getPropertyValue('--line'); ctx.lineWidth = 1
    for (let y = h / 4; y < h; y += h / 4) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke() }
    const step = w / (activity.length - 1)
    ctx.lineWidth = 1.6
    ctx.strokeStyle = st.getPropertyValue(dead ? '--magma' : '--amber')
    ctx.beginPath()
    activity.forEach((v, i) => { const y = h / 2 - v * (h / 2 - 6); if (i) ctx.lineTo(i * step, y); else ctx.moveTo(0, y) })
    ctx.stroke()
  }, [activity, w, dead, themeTick])
  return <div ref={wrap}><canvas ref={cv} className="seis" aria-hidden="true" style={{ height: 96 }} /></div>
}

export function Activity({ state }: { state: InspectorState }) {
  const items = state.log.slice(0, LOG_ROWS)
  return (
    <Panel id="h-act" title="engine activity" num="05">
      <Seismograph activity={state.activity} dead={state.conn === 'down'} />
      {items.length === 0
        ? <div className="empty">Nothing has happened yet.</div>
        : (
          <ul className="log" aria-live="polite" aria-label="Engine events">
            {items.map((it) => {
              const d = describe(it.event)
              return <li key={it.id} className={`enter ${d.cls}`}><span className="t">{clock(it.event.time)}</span><span className="g" aria-hidden="true">{d.glyph}</span><span>{d.text}</span></li>
            })}
          </ul>
        )}
    </Panel>
  )
}
