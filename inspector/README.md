# NebulaDB Inspector

A live view of the LSM tree: it connects to the server's admin port (`-admin-addr`,
default `127.0.0.1:7380`), listens to the Server-Sent-Events stream of engine
events and polls the JSON endpoints. Functional and deliberately unstyled beyond
a clean baseline: **the visual design is applied later in one place** (see
"Styling" below).

## Run

```sh
go build -o bin/nebuladb ./cmd/nebuladb
bin/nebuladb -dir ./data -supervise          # RESP :6380, admin :7380 (supervise = restart after "Crash it")
cd inspector && npm install && npm run dev   # http://127.0.0.1:5173
```

`?admin=http://host:port` points the UI at another admin server. Tests:
`npm test` (Vitest) and `npm run typecheck`. Screenshots of every screen:
`scripts/run-screenshots.sh` (needs the Playwright browser libraries; see
`../scripts/setup-browser-libs.sh` on machines without sudo).

## Why React + TypeScript + Vite

* The data is a typed stream of events feeding many small panels; **TypeScript**
  types mirror the server's JSON (`src/api/types.ts`), so a field rename on the Go
  side is a compile error here instead of a blank panel.
* **React** gives cheap composition of the ~10 panels and lets all stream updates
  flow through one reducer. The stream is batched through `requestAnimationFrame`
  (`src/api/useAdmin.ts`): at most one render per frame however fast events arrive.
* **Vite** for instant dev builds; **no UI library, no router, no state library**
  (a 15-line hash router and `useReducer`). Production bundle: ~248 KB (78 KB gzip).
* Plain TypeScript would have meant hand-rolled DOM updates for the same panels; the
  redesign (animations, micro-interactions) is easier on components.

## Structure

```
src/api/        types.ts (server JSON), client.ts (fetch + SSE), useAdmin.ts (batching hook)
src/lib/        reducer.ts (all UI state, pure + tested), keymap.ts (key -> x position),
                format.ts, args.ts (redis-cli style argument splitting)
src/components/ one file per panel (+ primitives.tsx: Card, StatCard, Badge, Meter, Sparkline)
src/pages/      Inspector.tsx, About.tsx
src/styles/     tokens.css (ALL colours, sizes, radii, motion durations), base.css, components.css
```

## Styling is centralised

Every colour, size, radius and duration is a CSS variable in `src/styles/tokens.css`
(light and dark defined; `<html data-theme>` or the OS picks one). `components.css`
uses only those variables, and every visual element has a class there. Nothing is
styled inline except computed geometry (meter widths, SVG positions).

## Screens and components (baseline screenshots in `screenshots/baseline/`)

Screens: Inspector (empty / active / after Try it), crash outage, crash recovered,
About; each in light and dark, plus mobile (390 px) for Inspector and About.

Components: Header (title, nav, connection badge, theme select), CrashBanner,
Overview stat row (6 StatCards), WriteFeed, MemoryPanel (Meters), LevelsPanel
(SVG: level rows, table rectangles placed by key range and sized by bytes, fresh
and merging outlines), BackpressurePanel (Meter with threshold marker),
ActivityLog, FiltersPanel (Bloom + Block cache with Sparklines), TryIt,
DevPanel (flush / compact / crash / demo load), and the About page's prose.
