import { useEffect, useMemo, useRef } from 'react'
import type { Placed } from '../lib/strata'
import type { FileInfo } from '../api/types'
import { bytes, compact } from '../lib/format'
import { useGhosts, useWidth } from '../lib/hooks'
import { layoutStrata } from '../lib/strata'
import type { InspectorState } from '../lib/reducer'
import { Panel } from './primitives'

const WIDE = 110 // px; wider tables get a label inside

/**
 * The core sample. Every SSTable is a rock slab placed by key range; levels are
 * strata. The view is DIFF-DRIVEN: a table that appears in /api/levels drops in
 * as a new layer, one that disappears is pressed flat and fades; the server's
 * events only add the green "new" ring and the amber "merging" ring. Tables are
 * positioned with CSS transforms so moves are GPU-composited, not layout.
 */
export function StrataPanel({ state }: { state: InspectorState }) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const levels = state.levels?.levels
  const files = useMemo(() => (levels ?? []).flat(), [levels])
  const shown = useGhosts(files, (f: FileInfo) => f.num)

  // Tables present when the page first got data are not "new": only later arrivals animate in.
  const initial = useRef<Set<number> | null>(null)
  if (initial.current === null && levels) initial.current = new Set(files.map((f) => f.num))

  const gone = useMemo(() => new Set(shown.filter((s) => s.gone).map((s) => s.item.num)), [shown])
  // Lanes/positions are computed from LIVE tables only, so a table that is fading out
  // never pushes the others around; it just stays where it was (lastPlaced).
  const grouped = useMemo(() => {
    const out: FileInfo[][] = []
    shown.forEach(({ item, gone: g }) => { if (!g) (out[item.level] ??= []).push(item) })
    for (let i = 0; i < out.length; i++) out[i] ??= []
    return out
  }, [shown])

  const gutter = useRef(68)
  useEffect(() => {
    const el = ref.current
    if (el) gutter.current = parseInt(getComputedStyle(el).getPropertyValue('--gutter'), 10) || 68
  }, [width, ref])
  const g = gutter.current
  const layout = useMemo(() => layoutStrata(grouped, Math.max(0, width - g)), [grouped, width, g])
  const lastPlaced = useRef<Map<number, Placed>>(new Map())
  useEffect(() => { layout.placed.forEach((p) => lastPlaced.current.set(p.f.num, p)) }, [layout])
  const drawn = useMemo(() => {
    const live = new Set(layout.placed.map((p) => p.f.num))
    const ghosts: Placed[] = []
    gone.forEach((n) => { const p = lastPlaced.current.get(n); if (p && !live.has(n)) ghosts.push(p) })
    return [...layout.placed, ...ghosts]
  }, [layout, gone])
  const merging = useMemo(() => new Set(state.merging), [state.merging])

  const summary = layout.bands.map((b) => `L${b.level} ${b.files} table${b.files === 1 ? '' : 's'}`).join(', ')
  return (
    <Panel id="h-lsm" title="lsm tree · core sample" num="03">
      <div className="strata-wrap">
        <div className="strata" ref={ref} style={{ height: layout.height + 22 }} role="img" aria-label={`SSTables by level, placed by key range and sized by bytes. ${summary}`}>
          <span className={`flushing${state.flushing ? ' on' : ''}`}>↓ flushing memtable into L0</span>
          {layout.bands.map((b) => (
            <div key={b.level}>
              <div className="band" style={{ top: b.top, height: b.h }} />
              <div className="blabel" style={{ top: b.top + 6 }}>L{b.level}<small>{b.files ? bytes(b.bytes) : 'empty'}</small></div>
            </div>
          ))}
          {drawn.map((p) => {
            const f = p.f
            const isNew = initial.current ? !initial.current.has(f.num) : false
            const cls = [gone.has(f.num) ? 'gone' : '', state.fresh[f.num] ? 'fresh' : '', merging.has(f.num) ? 'merging' : ''].filter(Boolean).join(' ')
            return (
              <div key={f.num} className={`file-pos ${cls}`} style={{ transform: `translate(${g + p.x}px, ${p.y}px)`, width: p.w, height: p.h }}
                title={`sstable #${String(f.num).padStart(5, '0')} · L${p.level}\n${bytes(f.size)} · ${compact(f.entries)} entries (${compact(f.deletes)} tombstones)\nkeys ${f.smallest} … ${f.largest}\nseq ${f.min_seq}–${f.max_seq}`}>
                <div className={`file L${Math.min(p.level, 6)}${isNew ? ' enter' : ''}${merging.has(f.num) ? ' merging' : ''}`}>
                  {p.w > WIDE && <span>#{String(f.num).padStart(5, '0')} · {bytes(f.size)}</span>}
                </div>
              </div>
            )
          })}
          <div className="axis" style={{ top: layout.height + 4 }} aria-hidden="true">
            <span>{layout.lo ? truncate(layout.lo) : ''}</span><span>key order →</span><span>{layout.hi ? truncate(layout.hi) : ''}</span>
          </div>
        </div>
        {files.length === 0 && levels && <p className="empty" style={{ paddingLeft: 0 }}>No SSTables yet: write some data and a memtable flush will create the first one.</p>}
      </div>
      <div className="strata-foot">
        <span className="legend">
          <span><i style={{ background: 'var(--l0)' }} />L0 fresh, overlapping</span><span><i style={{ background: 'var(--l1)' }} />L1</span>
          <span><i style={{ background: 'var(--l2)' }} />L2</span><span><i style={{ background: 'var(--l3)' }} />L3+ compacted</span>
        </span>
        <span><span className="cnote">solid green</span> new · <span className="mnote">dashed amber</span> merging</span>
      </div>
    </Panel>
  )
}

const truncate = (k: string) => (k.length > 14 ? `${k.slice(0, 13)}…` : k)
