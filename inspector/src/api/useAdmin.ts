import { useCallback, useEffect, useReducer, useRef } from 'react'
import { devAction, fetchHealth, fetchLevels, fetchStats, openStream, type DevAction } from './client'
import { initialState, reducer, type InspectorState } from '../lib/reducer'
import type { StreamMessage } from './types'

/**
 * Connects the UI to the admin server.
 *
 * Stream messages are NOT dispatched one by one: they are queued and flushed
 * once per animation frame as a single reducer action, so a burst of events can
 * cause at most one React render per frame no matter how fast they arrive.
 */
export function useAdmin(): { state: InspectorState; action: (a: DevAction) => Promise<{ ok: boolean; message: string }> } {
  const [state, dispatch] = useReducer(reducer, undefined, initialState)
  const queue = useRef<StreamMessage[]>([])

  useEffect(() => {
    let alive = true
    let raf = 0
    let levelsTimer: ReturnType<typeof setTimeout> | undefined
    const ac = new AbortController()

    const loadLevels = () => {
      fetchLevels(ac.signal).then((levels) => alive && dispatch({ t: 'levels', levels })).catch(() => {})
    }
    const scheduleLevels = () => {
      if (levelsTimer) return
      levelsTimer = setTimeout(() => { levelsTimer = undefined; loadLevels() }, 150)
    }
    const loadHealth = () => fetchHealth(ac.signal).then((health) => alive && dispatch({ t: 'health', health })).catch(() => {})

    loadHealth(); loadLevels()
    fetchStats(ac.signal).then((stats) => alive && dispatch({ t: 'stats', stats })).catch(() => {})

    const frame = () => {
      const now = Date.now()
      if (queue.current.length) {
        const messages = queue.current
        queue.current = []
        dispatch({ t: 'batch', messages, now })
        // File set changes: refresh the level diagram shortly after.
        if (messages.some((m) => ['file_created', 'file_deleted', 'compaction_end', 'flush_end', 'recovery'].includes(m.type))) scheduleLevels()
      }
      dispatch({ t: 'expire', now })
      raf = requestAnimationFrame(frame)
    }
    raf = requestAnimationFrame(frame)

    const poll = setInterval(() => {
      fetchStats(ac.signal).then((stats) => alive && dispatch({ t: 'stats', stats })).catch(() => {})
      loadLevels()
    }, 2000)

    const close = openStream({
      onMessage: (m) => { queue.current.push(m) },
      onOpen: () => { dispatch({ t: 'conn', state: 'live', now: Date.now() }); loadHealth(); loadLevels() },
      onError: () => dispatch({ t: 'conn', state: 'down', now: Date.now() }),
    })

    return () => {
      alive = false
      ac.abort()
      close()
      clearInterval(poll)
      cancelAnimationFrame(raf)
      if (levelsTimer) clearTimeout(levelsTimer)
    }
  }, [])

  const action = useCallback((a: DevAction) => devAction(a), [])
  return { state, action }
}
