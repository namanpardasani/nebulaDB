// Geometry of the "core sample" (the LSM tree panel). Pure, so it is unit-tested.
//
// Each level is a horizontal band. A table is a rectangle placed by its KEY
// RANGE (x, width) and, vertically, by level; L0 tables may overlap so they are
// packed into lanes, deeper levels are sized by bytes. Positions are pixels in
// a coordinate system whose origin is the left edge of the bands (the label
// gutter is outside it), so the DOM layer can move files with CSS transforms.

import type { FileInfo } from '../api/types'
import { packLanes, type Span } from './keymap'

export const LANE_H = 27
export const L0_MIN_H = 118
export const BAND_H = 64
export const BAND_GAP = 6
export const MIN_LEVELS = 4

export interface BandBox { level: number; top: number; h: number; files: number; bytes: number }
export interface Placed { f: FileInfo; level: number; x: number; y: number; w: number; h: number }
export interface StrataLayout { bands: BandBox[]; placed: Placed[]; height: number; lo: string; hi: string }

export function layoutStrata(levels: FileInfo[][], width: number): StrataLayout {
  let last = -1
  levels.forEach((l, i) => { if (l.length) last = i })
  const count = Math.max(MIN_LEVELS, last + 1)
  const all = levels.flat()
  let maxBytes = 1
  const keys = new Set<string>()
  for (const f of all) { keys.add(f.smallest); keys.add(f.largest); maxBytes = Math.max(maxBytes, f.size) }
  // Placement is by RANK of the table boundaries, not by the numeric value of the keys:
  // real key spaces are lumpy ("demo:" vs "user:" prefixes), and a proportional axis would
  // squeeze nearly every table into a sliver. Order (and therefore overlap) is preserved.
  const sorted = [...keys].sort((x, y) => (x < y ? -1 : x > y ? 1 : 0))
  const rank = new Map(sorted.map((k, i) => [k, i]))
  const loKey = sorted[0] ?? '', hiKey = sorted[sorted.length - 1] ?? ''
  const denom = Math.max(1, sorted.length - 1)
  const spanOf = (f: FileInfo): Span => {
    const x = ((rank.get(f.smallest) ?? 0) / denom) * width
    const w = Math.max(6, (((rank.get(f.largest) ?? 0) - (rank.get(f.smallest) ?? 0)) / denom) * width)
    return { x: Math.min(x, Math.max(0, width - 6)), w: Math.min(w, width) }
  }
  const bands: BandBox[] = []
  const placed: Placed[] = []
  let top = 0
  for (let lv = 0; lv < count; lv++) {
    const files = levels[lv] ?? []
    const spans = files.map(spanOf)
    let h = BAND_H
    let lanes: number[] = []
    if (lv === 0) {
      const packed = packLanes(spans)
      lanes = packed.lanes
      h = Math.max(L0_MIN_H, 12 + packed.count * LANE_H)
    }
    files.forEach((f, i) => {
      if (lv === 0) {
        placed.push({ f, level: 0, x: spans[i].x, w: spans[i].w, y: top + 6 + lanes[i] * LANE_H, h: 22 })
      } else {
        const fh = Math.round(18 + 36 * Math.sqrt(f.size / maxBytes))
        placed.push({ f, level: lv, x: spans[i].x, w: spans[i].w, y: top + h - 6 - fh, h: fh })
      }
    })
    bands.push({ level: lv, top, h, files: files.length, bytes: files.reduce((a, f) => a + f.size, 0) })
    top += h + BAND_GAP
  }
  return { bands, placed, height: top - BAND_GAP, lo: loKey, hi: hiKey }
}
