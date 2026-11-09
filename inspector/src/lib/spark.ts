/** SVG polyline `points` for a sparkline in a 120 x h box. NaN points are
 *  dropped (a tick with no lookups has no hit rate). Returns '' for < 2 points. */
export function sparkPoints(values: number[], h: number, min: number, max: number, w = 120): string {
  const v = values.filter((x) => Number.isFinite(x))
  const n = v.length
  if (n < 2) return ''
  const span = max - min || 1
  return v.map((x, i) => `${((i / (n - 1)) * w).toFixed(1)},${(h - 2 - ((Math.min(max, Math.max(min, x)) - min) / span) * (h - 4)).toFixed(1)}`).join(' ')
}
