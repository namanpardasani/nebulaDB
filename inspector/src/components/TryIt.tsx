import { useEffect, useRef, useState } from 'react'
import { runCommand } from '../api/client'
import { splitArgs } from '../lib/args'
import { formatReply, writtenKeys } from '../lib/reply'
import { Panel } from './primitives'

interface Entry { id: number; kind: 'in' | 'res' | 'err'; text: string }

const PRESETS = ['PING', 'SET demo:1 hello', 'GET demo:1', 'INCR demo:counter', 'DBSIZE', 'SCAN 0 COUNT 5']
const HINT = 'type a command: SET key value, GET key, DEL key, INCR key, SCAN 0, PING, DBSIZE'

/** A terminal that sends commands through the admin server's whitelisted proxy (POST /api/cmd). */
export function TryIt({ onWrite }: { onWrite: (op: string, key: string) => void }) {
  const [line, setLine] = useState('')
  const [entries, setEntries] = useState<Entry[]>([{ id: 0, kind: 'res', text: HINT }])
  const [busy, setBusy] = useState(false)
  const id = useRef(1)
  const hist = useRef<string[]>([])
  const histPos = useRef(0)
  const out = useRef<HTMLDivElement>(null)

  useEffect(() => { const el = out.current; if (el) el.scrollTop = el.scrollHeight }, [entries])

  const push = (...e: Omit<Entry, 'id'>[]) => setEntries((cur) => [...cur, ...e.map((x) => ({ ...x, id: id.current++ }))].slice(-60))

  async function run(text: string) {
    hist.current.push(text); histPos.current = hist.current.length
    const args = splitArgs(text)
    if (!args || args.length === 0) { push({ kind: 'in', text }, { kind: 'err', text: args === null ? '(error) unterminated quote' : '(error) empty command' }); return }
    setBusy(true)
    push({ kind: 'in', text })
    try {
      const r = await runCommand(args)
      if (r.error) push({ kind: 'err', text: `(error) ${r.error}` })
      else {
        const f = formatReply(r.reply)
        push({ kind: f.error ? 'err' : 'res', text: f.text })
        const w = writtenKeys(args)
        if (w && !f.error) w.keys.slice(0, 3).forEach((k) => onWrite(w.op, k))
      }
    } catch (err) {
      push({ kind: 'err', text: `(error) request failed: ${(err as Error).message}` })
    } finally { setBusy(false) }
  }

  function onKey(ev: React.KeyboardEvent<HTMLInputElement>) {
    if (ev.key !== 'ArrowUp' && ev.key !== 'ArrowDown') return
    ev.preventDefault()
    const n = hist.current.length
    histPos.current = Math.max(0, Math.min(n, histPos.current + (ev.key === 'ArrowUp' ? -1 : 1)))
    setLine(hist.current[histPos.current] ?? '')
  }

  return (
    <Panel id="h-try" title="try it" num="07 · via admin proxy" className="termp">
      <div className="term">
        <div className="out" ref={out} aria-live="polite">
          {entries.map((e) => <div key={e.id} className={e.kind}>{e.text}</div>)}
        </div>
        <form onSubmit={(ev) => { ev.preventDefault(); if (line.trim() && !busy) { const t = line.trim(); setLine(''); void run(t) } }}>
          <span aria-hidden="true">nebula&gt;</span>
          <label htmlFor="tin" className="vh">Command</label>
          <input id="tin" value={line} onChange={(e) => setLine(e.target.value)} onKeyDown={onKey} autoComplete="off" spellCheck={false} placeholder="SET user:1 naman" />
        </form>
        <div className="presets">
          {PRESETS.map((p) => <button key={p} type="button" disabled={busy} onClick={() => void run(p)}>{p}</button>)}
        </div>
      </div>
    </Panel>
  )
}
