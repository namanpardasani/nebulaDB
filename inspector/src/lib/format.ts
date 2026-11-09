export function bytes(n: number): string {
  if (!Number.isFinite(n)) return '–'
  const abs = Math.abs(n)
  if (abs < 1024) return `${Math.round(n)} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let v = n / 1024
  let i = 0
  while (Math.abs(v) >= 1024 && i < units.length - 1) { v /= 1024; i++ }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`
}

export function compact(n: number): string {
  if (!Number.isFinite(n)) return '–'
  const abs = Math.abs(n)
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}G`
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (abs >= 1e4) return `${(n / 1e3).toFixed(1)}k`
  return String(Math.round(n))
}

export function percent(x: number, digits = 0): string {
  return Number.isFinite(x) ? `${(x * 100).toFixed(digits)}%` : '–'
}

export function micros(us: number): string {
  if (!Number.isFinite(us)) return '–'
  return us >= 1000 ? `${(us / 1000).toFixed(1)} ms` : `${Math.round(us)} µs`
}

export function duration(nanos: number): string {
  return micros(nanos / 1000)
}

export function clock(iso: string): string {
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleTimeString([], { hour12: false })
}
