# NebulaDB Inspector

A live view of the LSM tree: it connects to the server's admin port (`-admin-addr`,
default `127.0.0.1:7380`), listens to the Server-Sent-Events stream of engine
events and polls the JSON endpoints. Styled with the **Strata** design system (see NOTES.md, "Design system", and
`design/nebuladb-design-reference.html`, the source of truth).

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
src/api/        types.ts (server JSON), client.ts (fetch + SSE), useAdmin.ts (rAF-batched hook)
src/lib/        reducer.ts (all UI state, pure + tested), strata.ts (core-sample geometry),
                keymap.ts, reply.ts (redis-cli formatting), crash.ts (recovery steps),
                spark.ts, hooks.ts (useWidth, useGhosts = diff-driven exit animations), format.ts, args.ts
src/components/ one file per panel; useCrashFlow.ts drives the crash overlay from real signals
src/pages/      Inspector.tsx, About.tsx
src/styles/     tokens.css (ALL colours, radii, motion), base.css (fonts, reset, focus), components.css
```

## Styling is centralised

Every colour, radius and duration is a CSS variable in `src/styles/tokens.css`
(dark is the default set; light is applied by `prefers-color-scheme` or
`<html data-theme>`). `components.css` uses only those tokens. Inline styles are
used only for computed geometry (positions, widths, fill heights). Fonts (IBM Plex
Sans and Mono) are self-hosted through `@fontsource`, so the page makes no external requests.

## Screenshots

`screenshots/baseline/` (functional Phase 11 UI), `screenshots/reference/` (the design
reference rendered), `screenshots/after/` (Strata UI: light/dark, 1440/1024/768/390 px,
every panel, armed kill switch, crash overlay stages, the "writers stalled" state).
Regenerate: `SCRIPT=scripts/screenshots-strata.mjs OUT=screenshots/after scripts/run-screenshots.sh`.
The script starts a server with deliberately small limits (64 KiB memtable, L0 slowdown 4 / stop 6,
throttled compaction) so a flood reaches backpressure.
