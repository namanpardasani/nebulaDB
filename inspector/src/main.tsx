import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import './styles/tokens.css'
import './styles/base.css'
import './styles/components.css'

// Restore an explicit theme choice before the first paint (no choice = follow the OS).
try {
  const t = localStorage.getItem('theme')
  if (t === 'light' || t === 'dark') document.documentElement.dataset.theme = t
} catch { /* storage unavailable */ }

createRoot(document.getElementById('root')!).render(<StrictMode><App /></StrictMode>)
