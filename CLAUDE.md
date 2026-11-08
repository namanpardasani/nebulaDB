# NebulaDB — working rules

The full brief and phase plan are in [PROJECT_BRIEF.md](PROJECT_BRIEF.md). Re-read it and [NOTES.md](NOTES.md) after any context reset and continue from the next unfinished task in the Progress log.

## Rules (summary of brief section 0)
- Work phase by phase (brief section 6) without asking. Stop only for: the Phase 11 DESIGN PAUSE; anything costing money or needing accounts; anything destructive outside the repo / irreversible; a real unresolved blocker.
- Local git commit at the end of every phase. UPDATE (user, mid-session): the user will create a GitHub repo and wants everything pushed to it. Remote `origin` = https://github.com/namanpardasani/nebulaDB.git (added; Phase 1 pushed). Push `main` after each phase commit. COMMIT MESSAGES: no Co-Authored-By / Claude attribution lines, ever (user request; applies to commits and PR bodies). Author is the user's git identity only. The one-time force-push cleanup of the Phase 1 trailer is DONE (user approved); push `main` normally after every commit from now on, no further force-pushes.
- After every phase update the Progress log in NOTES.md (done, next, known issues).
- Quality bar each phase: `go build ./...`, `go vet ./...`, `go test -race ./...` clean. No TODO stubs in finished phases. Comment non-obvious logic (the owner will study it).
- NEVER invent benchmark/test/crash numbers; every number comes from a real run with machine and settings recorded.
- Stdlib wherever reasonable. Storage engine (WAL, memtable, SSTable, Bloom, cache, compaction, manifest, recovery, RESP) is our own code. Libraries only for tests, CLI flags, metrics, benchmark comparison targets.
- Record decisions/tradeoffs in NOTES.md; record every test-found bug in "Bugs found by tests".
- Free tools only; verify versions instead of guessing.

## Commands
`make build | test | race | fuzz | crash | bench | inspector | lint`
