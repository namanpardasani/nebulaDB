// Formats an admin-proxy reply the way redis-cli prints it. The proxy flattens
// RESP into JSON, so simple strings and bulk strings are both JSON strings; the
// only simple-string replies our commands produce are OK / PONG / QUEUED, which
// are printed bare, everything else is quoted like a bulk string.

const SIMPLE = new Set(['OK', 'PONG', 'QUEUED'])

export interface Formatted { text: string; error: boolean }

export function formatReply(reply: unknown, indent = ''): Formatted {
  if (reply === null || reply === undefined) return { text: '(nil)', error: false }
  if (typeof reply === 'number') return { text: `(integer) ${reply}`, error: false }
  if (typeof reply === 'string') return { text: SIMPLE.has(reply) ? reply : JSON.stringify(reply), error: false }
  if (Array.isArray(reply)) {
    if (reply.length === 0) return { text: '(empty array)', error: false }
    const pad = String(reply.length).length
    const lines = reply.map((e, i) => {
      const label = `${String(i + 1).padStart(pad)}) `
      return indent + label + formatReply(e, indent + ' '.repeat(label.length)).text.replace(/^\s+/, '')
    })
    return { text: lines.join('\n').replace(/^\s+/, ''), error: false }
  }
  if (typeof reply === 'object' && 'error' in (reply as object)) {
    return { text: `(error) ${(reply as { error: string }).error}`, error: true }
  }
  return { text: JSON.stringify(reply), error: false }
}

/** Keys written by a command, for the "you" rows of the writes list. */
export function writtenKeys(args: string[]): { op: string; keys: string[] } | null {
  const cmd = args[0]?.toUpperCase()
  switch (cmd) {
    case 'SET': case 'INCR': case 'DECR': case 'INCRBY': case 'DECRBY': case 'EXPIRE': case 'PEXPIRE': case 'PERSIST':
      return args[1] ? { op: cmd === 'SET' ? 'SET' : cmd, keys: [args[1]] } : null
    case 'DEL': return args.length > 1 ? { op: 'DEL', keys: args.slice(1) } : null
    case 'MSET': return { op: 'SET', keys: args.filter((_, i) => i > 0 && i % 2 === 1) }
    default: return null
  }
}
