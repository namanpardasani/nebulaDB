import { useEffect, useRef, useState } from 'react'
import { runCommand, type DevAction } from '../api/client'
import type { InspectorState } from '../lib/reducer'
import { Panel } from './primitives'

const rand = (n: number) => Math.floor(Math.random() * n)

function mset(pairs: number): string[] {
  const args = ['MSET']
  for (let i = 0; i < pairs; i++) args.push(`demo:${String(rand(1_000_000)).padStart(7, '0')}`, 'x'.repeat(100 + rand(160)))
  return args
}

/**
 * Hardware-style controls. The crash switch has a safety cover: lift it (arms
 * for 6 s), then press. Pressing writes an acknowledged canary key first, then
 * SIGKILLs the server (the supervisor restarts it).
 */
export function Controls({ state, action, crash }: {
  state: InspectorState
  action: (a: DevAction) => Promise<{ ok: boolean; message: string }>
  crash: () => Promise<{ ok: boolean; message: string }>
}) {
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)
  const [armed, setArmed] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const fire = useRef<HTMLButtonElement>(null)
  const cover = useRef<HTMLButtonElement>(null)
  const dev = !!state.health?.dev
  const supervised = !!state.health?.supervised
  const down = state.conn !== 'live'

  useEffect(() => () => clearTimeout(timer.current), [])
  useEffect(() => { if (armed) fire.current?.focus() }, [armed])

  function disarm(refocus = false) {
    clearTimeout(timer.current); setArmed(false)
    if (refocus) setTimeout(() => cover.current?.focus(), 0)
  }

  async function writeKeys() {
    setBusy(true); setMsg('writing 500 keys…')
    const r = await runCommand(mset(500))
    setMsg(r.error ? `write failed: ${r.error}` : 'wrote 500 random keys')
    setBusy(false)
  }

  // 40 batches x 500 keys, 4 in flight. Against the default limits this is a burst;
  // against small limits it pushes L0 past the slowdown/stop thresholds (backpressure).
  async function flood() {
    setBusy(true)
    let done = 0, failed = ''
    const total = 40
    const worker = async () => {
      while (done < total && !failed) {
        done++
        const r = await runCommand(mset(500))
        if (r.error) failed = r.error
        else setMsg(`flood: ${done * 500} / ${total * 500} keys…`)
      }
    }
    await Promise.all([worker(), worker(), worker(), worker()])
    setMsg(failed ? `flood stopped: ${failed}` : `flood done: ${total * 500} keys`)
    setBusy(false)
  }

  async function act(a: DevAction, label: string) {
    setBusy(true)
    const r = await action(a)
    setMsg(`${label}: ${r.message}`)
    setBusy(false)
  }

  async function fireCrash() {
    disarm()
    setBusy(true)
    const r = await crash()
    setMsg(r.ok ? 'canary written and acknowledged; killing the server' : r.message)
    setBusy(false)
  }

  const hwDisabled = busy || down
  const devOff = busy || down || !dev
  const killOff = busy || down || !dev || !supervised
  return (
    <Panel id="h-ctl" title="controls" num="08">
      <div className="pb ctrls">
        <button className="hw" type="button" onClick={() => void writeKeys()} disabled={hwDisabled}>write 500 keys<small>random SETs</small></button>
        <button className="hw" type="button" onClick={() => void flood()} disabled={hwDisabled}>flood 20k<small>shows backpressure</small></button>
        <button className="hw" type="button" onClick={() => void act('flush', 'flush')} disabled={devOff} title={dev ? '' : 'needs the server started with -dev'}>flush now<small>seal memtable</small></button>
        <button className="hw" type="button" onClick={() => void act('compact', 'compact')} disabled={devOff} title={dev ? '' : 'needs the server started with -dev'}>compact now<small>merge L0 down</small></button>
        <div className={`kill${armed ? ' armed' : ''}`}>
          <button className="fire" ref={fire} type="button" onClick={() => void fireCrash()} disabled={!armed} tabIndex={armed ? 0 : -1} aria-hidden={!armed}>
            CRASH IT<small>kill -9 the server</small>
          </button>
          <button className="cover" ref={cover} type="button" disabled={killOff} aria-hidden={armed} tabIndex={armed ? -1 : 0}
            title={killOff ? 'needs the server started with -supervise' : ''}
            aria-label="Lift the safety cover to arm the crash button"
            onClick={() => { setArmed(true); clearTimeout(timer.current); timer.current = setTimeout(() => disarm(), 6000) }}>
            lift cover to arm
          </button>
        </div>
        {armed && <button className="ghost" type="button" style={{ gridColumn: '1 / -1' }} onClick={() => disarm(true)}>disarm</button>}
        <div className="ctl-msg" role="status">{msg || (dev ? '' : 'start the server with -dev to enable flush and compact; add -supervise for the crash switch')}</div>
      </div>
    </Panel>
  )
}
