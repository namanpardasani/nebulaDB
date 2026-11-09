import type { CrashView } from './useCrashFlow'

/** Full-screen "signal lost" state. Role alertdialog + assertive live region announce each phase. */
export function CrashOverlay({ view }: { view: CrashView }) {
  const on = view.phase !== 'idle'
  const tone = view.phase === 'online' ? ' ok' : view.phase === 'recovering' ? ' rec' : ''
  return (
    <div className={`down-overlay${on ? ' on' : ''}`} role="alertdialog" aria-live="assertive" aria-label="Server crash and recovery" aria-hidden={!on}>
      <div className="down-box">
        <h3 className={tone.trim()}>{view.title}</h3>
        <div className="sub">{view.sub}</div>
        <ol>
          {view.steps.map((s, i) => <li key={i} className={s.bad ? 'bad' : ''}><span>{s.label}</span><b>{s.value}</b></li>)}
        </ol>
      </div>
    </div>
  )
}
