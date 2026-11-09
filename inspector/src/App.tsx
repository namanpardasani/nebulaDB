import { useEffect, useState } from 'react'
import { useAdmin } from './api/useAdmin'
import { CrashOverlay } from './components/CrashOverlay'
import { Header } from './components/Header'
import { useCrashFlow } from './components/useCrashFlow'
import { About } from './pages/About'
import { Inspector } from './pages/Inspector'

function useHashRoute(): string {
  const read = () => (window.location.hash.replace(/^#/, '') || '/')
  const [route, setRoute] = useState(read)
  useEffect(() => {
    const on = () => setRoute(read())
    window.addEventListener('hashchange', on)
    return () => window.removeEventListener('hashchange', on)
  }, [])
  return route
}

export function App() {
  const route = useHashRoute()
  const { state, action, noteWrite } = useAdmin()
  const flow = useCrashFlow(state.conn, action)
  const phase = flow.view.phase
  return (
    <div className={`app${phase !== 'idle' && phase !== 'online' ? ' dim' : ''}`}>
      <a className="skip" href="#main">Skip to content</a>
      <Header state={state} route={route} recovering={phase === 'recovering'} />
      <div id="main">
        {route === '/about' ? <About /> : <Inspector state={state} action={action} crash={flow.start} onWrite={noteWrite} />}
      </div>
      <CrashOverlay view={flow.view} />
    </div>
  )
}
