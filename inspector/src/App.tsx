import { useEffect, useState } from 'react'
import { useAdmin } from './api/useAdmin'
import { Header } from './components/Header'
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
  const { state, action } = useAdmin()
  return (
    <>
      <Header state={state} route={route} />
      {route === '/about' ? <About /> : <Inspector state={state} action={action} />}
    </>
  )
}
