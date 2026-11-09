import { useRef, useState } from 'react'
import { runCommand } from '../api/client'
import { splitArgs } from '../lib/args'
import { Card } from './primitives'

interface Entry { id: number; cmd: string; out: string; error: boolean }

const PRESETS = ['PING', 'SET demo:1 hello', 'GET demo:1', 'INCR demo:counter', 'SET demo:ttl x EX 30', 'TTL demo:ttl', 'SCAN 0 COUNT 10']

/** Sends commands through the admin server's whitelisted proxy (POST /api/cmd). */
export function TryIt() {
  const [line, setLine] = useState('')
  const [entries, setEntries] = useState<Entry[]>([])
  const [busy, setBusy] = useState(false)
  const id = useRef(0)

  async function run(text: string) {
    const args = splitArgs(text)
    if (!args || args.length === 0) {
      setEntries((e) => [{ id: ++id.current, cmd: text, out: args === null ? 'unterminated quote' : 'empty command', error: true }, ...e])
      return
    }
    setBusy(true)
    try {
      const r = await runCommand(args)
      const isErr = !!r.error || (typeof r.reply === 'object' && r.reply !== null && 'error' in (r.reply as object))
      const out = r.error ?? JSON.stringify(r.reply, null, 2)
      setEntries((e) => [{ id: ++id.current, cmd: text, out: out ?? 'null', error: isErr }, ...e].slice(0, 30))
    } catch (err) {
      setEntries((e) => [{ id: ++id.current, cmd: text, out: `request failed: ${(err as Error).message}`, error: true }, ...e])
    } finally { setBusy(false) }
  }

  return (
    <Card title="Try it" sub="Run commands through the admin proxy (whitelisted: GET, SET, DEL, INCR, SCAN, TTL, …)">
      <div className="stack">
        <form className="try-form" onSubmit={(ev) => { ev.preventDefault(); if (line.trim()) { run(line); setLine('') } }}>
          <input className="input" value={line} onChange={(e) => setLine(e.target.value)} placeholder='SET user:1 "ada lovelace" EX 60' aria-label="command" spellCheck={false} />
          <button className="btn btn--primary" type="submit" disabled={busy || !line.trim()}>Run</button>
        </form>
        <div className="btn-row">
          {PRESETS.map((p) => <button key={p} className="btn" type="button" disabled={busy} onClick={() => run(p)}>{p}</button>)}
        </div>
        {entries.length > 0 && (
          <div className="try-out" aria-live="polite">
            {entries.map((e) => (
              <div key={e.id}>
                <div className="mono try-cmd">&gt; {e.cmd}</div>
                <div className={`mono try-reply${e.error ? ' try-error' : ''}`}>{e.out}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Card>
  )
}
