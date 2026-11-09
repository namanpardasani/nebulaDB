import { useState } from 'react'
import { runCommand, type DevAction } from '../api/client'
import type { InspectorState } from '../lib/reducer'
import { Card } from './primitives'

/** Dev-only controls. The server refuses these unless started with -dev (crash also needs -supervise). */
export function DevPanel({ state, action }: { state: InspectorState; action: (a: DevAction) => Promise<{ ok: boolean; message: string }> }) {
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)
  const dev = !!state.health?.dev
  const supervised = !!state.health?.supervised

  async function act(a: DevAction) {
    if (a === 'crash' && !window.confirm('Kill the server with SIGKILL (no flush, no fsync)? The supervisor will restart it and you can watch recovery.')) return
    setBusy(true)
    const r = await action(a)
    setMsg(`${a}: ${r.message}`)
    setBusy(false)
  }

  async function demoWrites() {
    setBusy(true)
    setMsg('writing…')
    const batches = 20
    for (let b = 0; b < batches; b++) {
      const args = ['MSET']
      for (let i = 0; i < 25; i++) args.push(`demo:${Math.floor(Math.random() * 100000)}`, 'x'.repeat(20 + Math.floor(Math.random() * 200)))
      const r = await runCommand(args)
      if (r.error) { setMsg(`write failed: ${r.error}`); setBusy(false); return }
    }
    setMsg(`wrote ${batches * 25} random keys`)
    setBusy(false)
  }

  return (
    <Card title="Controls" sub={dev ? 'Development controls' : 'Start the server with -dev (or -supervise) to enable flush, compact and crash'}>
      <div className="stack">
        <div className="btn-row">
          <button className="btn" onClick={demoWrites} disabled={busy}>Write 500 random keys</button>
          <button className="btn" onClick={() => act('flush')} disabled={busy || !dev}>Flush memtable</button>
          <button className="btn" onClick={() => act('compact')} disabled={busy || !dev}>Compact everything</button>
          <button className="btn btn--danger" onClick={() => act('crash')} disabled={busy || !dev || !supervised} title={supervised ? '' : 'Requires -supervise so the server is restarted'}>Crash it (kill -9)</button>
        </div>
        {msg && <p className="card-sub" role="status">{msg}</p>}
      </div>
    </Card>
  )
}
