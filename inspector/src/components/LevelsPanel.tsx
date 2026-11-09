import { useMemo } from 'react'
import type { FileInfo } from '../api/types'
import { keySpan, keyToFraction, packLanes, type Span } from '../lib/keymap'
import { bytes, compact } from '../lib/format'
import type { InspectorState } from '../lib/reducer'
import { Badge, Card } from './primitives'

const W = 1000 // diagram width in SVG units
const LABEL_W = 150
const LANE_H = 26
const PAD = 6
const MAX_H = 20
const MIN_H = 6

interface Placed { f: FileInfo; span: Span; lane: number; h: number }

/**
 * The shape of the tree. One row per level; each SSTable is a rectangle placed
 * horizontally by its KEY RANGE (so L1+ rows tile the keyspace without overlap
 * and L0 files visibly overlap, stacked in lanes) and sized vertically by its
 * BYTES. New tables are outlined green; tables being merged by a running
 * compaction are outlined with a dashed amber stroke.
 */
export function LevelsPanel({ state }: { state: InspectorState }) {
  const levels = state.levels?.levels
  const layout = useMemo(() => {
    if (!levels) return null
    const all = levels.flat()
    if (all.length === 0) return { rows: [] as Placed[][], heights: [] as number[], total: 0, maxBytes: 0 }
    let lo = Infinity, hi = -Infinity, maxBytes = 1
    for (const f of all) {
      lo = Math.min(lo, keyToFraction(f.smallest)); hi = Math.max(hi, keyToFraction(f.largest)); maxBytes = Math.max(maxBytes, f.size)
    }
    const rows: Placed[][] = []
    const heights: number[] = []
    levels.forEach((files) => {
      const spans = files.map((f) => keySpan(f.smallest, f.largest, lo, hi, W - LABEL_W - 8))
      const { lanes, count } = packLanes(spans)
      rows.push(files.map((f, i) => ({ f, span: spans[i], lane: lanes[i], h: MIN_H + (MAX_H - MIN_H) * Math.sqrt(f.size / maxBytes) })))
      heights.push(count * LANE_H + PAD * 2)
    })
    return { rows, heights, total: heights.reduce((a, b) => a + b, 0), maxBytes }
  }, [levels])

  if (!layout) return <Card title="LSM tree"><p className="empty-note">Waiting for data…</p></Card>
  if (layout.rows.length === 0 || layout.total === 0) return <Card title="LSM tree"><p className="empty-note">No SSTables yet: write some data and a memtable flush will create the first one.</p></Card>

  let y = 0
  const merging = new Set(state.merging)
  return (
    <Card title="LSM tree" sub="Tables are placed by key range and sized by bytes. L0 tables may overlap; deeper levels never do."
      right={<span className="btn-row">{state.compacting && <Badge tone="warn">compacting</Badge>}{state.flushing && <Badge tone="good">flushing</Badge>}</span>}>
      <svg className="levels-svg" viewBox={`0 0 ${W} ${layout.total}`} role="img" aria-label="SSTables per level, positioned by key range">
        {layout.rows.map((row, lvl) => {
          const top = y
          const h = layout.heights[lvl]
          y += h
          const files = levels![lvl]
          const sz = files.reduce((a, f) => a + f.size, 0)
          return (
            <g key={lvl} transform={`translate(0 ${top})`}>
              <rect className="level-track" x={LABEL_W} y={PAD / 2} width={W - LABEL_W - 4} height={h - PAD} rx={4} />
              <text className="level-label" x={4} y={h / 2 - 2}>L{lvl}</text>
              <text className="level-label" x={4} y={h / 2 + 11}>{files.length} file{files.length === 1 ? '' : 's'} · {bytes(sz)}</text>
              {row.map(({ f, span, lane, h: fh }) => {
                const cy = PAD + lane * LANE_H + (LANE_H - fh) / 2
                const cls = ['file', `file--l${Math.min(lvl, 6)}`]
                if (state.fresh[f.num]) cls.push('file--fresh')
                if (merging.has(f.num)) cls.push('file--merging')
                return (
                  <rect key={f.num} className={cls.join(' ')} x={LABEL_W + span.x} y={cy} width={span.w} height={fh} rx={3}>
                    <title>{`${String(f.num).padStart(6, '0')}.sst · L${lvl}\n${bytes(f.size)} · ${compact(f.entries)} entries (${compact(f.deletes)} tombstones)\nkeys ${f.smallest} … ${f.largest}\nseq ${f.min_seq}–${f.max_seq}`}</title>
                  </rect>
                )
              })}
            </g>
          )
        })}
      </svg>
    </Card>
  )
}
