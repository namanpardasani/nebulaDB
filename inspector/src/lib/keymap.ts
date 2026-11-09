// Order-preserving mapping from keys to [0, 1], used to place a table on the
// level diagram by its key range. The first KEY_CHARS printable-ASCII
// characters are read as digits of a base-95 fraction, so a < b as strings
// implies f(a) <= f(b) (equal when keys differ only after the cut-off).

const KEY_CHARS = 8
const BASE = 95

export function keyToFraction(key: string): number {
  let f = 0
  let scale = 1
  for (let i = 0; i < KEY_CHARS; i++) {
    scale /= BASE
    const code = i < key.length ? key.charCodeAt(i) : 32 // pad with the smallest digit
    const digit = Math.min(BASE - 1, Math.max(0, code - 32))
    f += digit * scale
  }
  return f
}

export interface Span { x: number; w: number }

/**
 * Maps [smallest, largest] of a table to a horizontal span in [0, width],
 * normalised against the global key range [lo, hi] of everything on screen.
 * Very narrow tables are widened to minW so they stay visible.
 */
export function keySpan(smallest: string, largest: string, lo: number, hi: number, width: number, minW = 5): Span {
  const range = hi - lo
  if (!(range > 0)) return { x: 0, w: width }
  const a = (keyToFraction(smallest) - lo) / range
  const b = (keyToFraction(largest) - lo) / range
  const x = Math.max(0, Math.min(1, a)) * width
  const w = Math.max(minW, (Math.max(a, b) - Math.min(a, b)) * width)
  return { x: Math.min(x, width - minW), w: Math.min(w, width) }
}

/** Greedy interval packing: returns a lane index per interval so overlapping
 *  tables (L0) are stacked instead of drawn on top of each other. */
export function packLanes(spans: Span[]): { lanes: number[]; count: number } {
  const ends: number[] = []
  const order = spans.map((_, i) => i).sort((a, b) => spans[a].x - spans[b].x)
  const lanes = new Array<number>(spans.length).fill(0)
  for (const i of order) {
    let lane = ends.findIndex((e) => e <= spans[i].x)
    if (lane < 0) { lane = ends.length; ends.push(0) }
    ends[lane] = spans[i].x + spans[i].w
    lanes[i] = lane
  }
  return { lanes, count: Math.max(1, ends.length) }
}
