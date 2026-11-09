import { useEffect, useRef, useState, type RefObject } from 'react'

/** Observed content width of an element in CSS pixels. */
export function useWidth<T extends HTMLElement>(): [RefObject<T | null>, number] {
  const ref = useRef<T | null>(null)
  const [w, setW] = useState(0)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    setW(el.clientWidth)
    const ro = new ResizeObserver(() => setW(el.clientWidth))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  return [ref, w]
}

/**
 * Keeps items that just disappeared on screen for `ms` flagged `gone`, so the
 * view can play an exit animation (diff-driven: the data is the source of
 * truth, the animation follows it). A removed item that comes back is live again.
 */
export function useGhosts<T>(items: T[], key: (t: T) => number, ms = 450): { item: T; gone: boolean }[] {
  const [ghosts, setGhosts] = useState<Map<number, T>>(() => new Map())
  const prev = useRef<Map<number, T>>(new Map())
  const keyRef = useRef(key)
  keyRef.current = key
  useEffect(() => {
    const k = keyRef.current
    const cur = new Map(items.map((i) => [k(i), i] as const))
    const removed: T[] = []
    prev.current.forEach((v, id) => { if (!cur.has(id)) removed.push(v) })
    prev.current = cur
    if (removed.length === 0) return
    setGhosts((g) => { const n = new Map(g); removed.forEach((r) => n.set(k(r), r)); return n })
    setTimeout(() => setGhosts((g) => { const n = new Map(g); removed.forEach((r) => n.delete(k(r))); return n }), ms)
  }, [items, ms])
  const live = new Set(items.map(key))
  const out = items.map((item) => ({ item, gone: false }))
  ghosts.forEach((item, id) => { if (!live.has(id)) out.push({ item, gone: true }) })
  return out
}

/** Bumps whenever the theme changes (data-theme attribute or OS scheme), so canvas code can repaint. */
export function useThemeTick(): number {
  const [n, setN] = useState(0)
  useEffect(() => {
    const bump = () => setN((x) => x + 1)
    const mo = new MutationObserver(bump)
    mo.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })
    const mq = matchMedia('(prefers-color-scheme: dark)')
    mq.addEventListener('change', bump)
    return () => { mo.disconnect(); mq.removeEventListener('change', bump) }
  }, [])
  return n
}

export const reducedMotion = (): boolean => typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches
