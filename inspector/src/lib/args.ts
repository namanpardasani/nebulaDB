// Splits a command line the way redis-cli does: whitespace separated, with
// "double quoted" and 'single quoted' arguments and backslash escapes inside
// double quotes. Returns null for an unterminated quote.
export function splitArgs(line: string): string[] | null {
  const out: string[] = []
  let i = 0
  const n = line.length
  while (i < n) {
    while (i < n && /\s/.test(line[i])) i++
    if (i >= n) break
    let cur = ''
    if (line[i] === '"') {
      i++
      let closed = false
      while (i < n) {
        const c = line[i]
        if (c === '\\' && i + 1 < n) {
          const e = line[i + 1]
          cur += e === 'n' ? '\n' : e === 't' ? '\t' : e === 'r' ? '\r' : e
          i += 2
        } else if (c === '"') { closed = true; i++; break } else { cur += c; i++ }
      }
      if (!closed) return null
    } else if (line[i] === "'") {
      i++
      let closed = false
      while (i < n) {
        if (line[i] === "'") { closed = true; i++; break }
        cur += line[i++]
      }
      if (!closed) return null
    } else {
      while (i < n && !/\s/.test(line[i])) cur += line[i++]
    }
    out.push(cur)
  }
  return out
}
