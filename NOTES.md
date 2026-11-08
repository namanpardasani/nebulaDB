# NebulaDB Notes

## Architecture

## On-disk formats

## Guarantees

## Decisions and tradeoffs
- (P2) `engine.Engine` interface is the seam between network and storage; every op is atomic inside the engine (IncrBy, Set NX/XX) so the server holds no locks around read-modify-write. `engine/enginetest` is a shared conformance suite (Mem passes; the LSM engine must too). Time is injected (`now func() int64`) so TTL tests are deterministic.
- (P2) SCAN cursors: Redis clients need an integer cursor but an LSM scan resumes from a *key*. Server keeps a bounded (4096) id->resume-key table; unknown/evicted ids give `ERR invalid cursor`. COUNT bounds keys examined, MATCH filters after (pages may be empty), as in Redis.
- (P2) RESP parser limits: bulk <= 64MiB, array <= 1M, line <= 64KiB, nesting <= 32; large bulks are read in 64KiB chunks so a lying length cannot force a huge allocation. Protocol errors reply `-ERR Protocol error` and close (framing cannot be re-synced).
- (P2) Pipelining: replies are flushed only when the read buffer is empty. Graceful shutdown: close listener, set read deadline in the past on all conns (wakes idle readers; in-flight command still completes and flushes), then caller closes engine.
- (P2) HELLO only accepts protocol 2 (HELLO 3 -> NOPROTO, like Redis) so modern clients fall back to RESP2. DBSIZE is approximate by contract (documented in Engine.ApproxSize).
- Commit policy (user request): no Claude/Co-Authored-By attribution in commits.
- Module path `github.com/naman/nebuladb` (local only; no remote). Go 1.27.3 toolchain on dev machine.
- All I/O goes through `internal/vfs` (added to the layout; not in the brief's list). `Faulty` wraps any FS and supports: rule-based errors (skip N, fail M, sticky), torn/short writes (`Partial`), ENOSPC via a disk-size cap, read-time bit flips, and `Crash()` power-loss simulation. Crash drops unsynced file bytes (or keeps a random torn prefix) AND undoes create/rename/remove that were never made durable with `SyncDir` (backing up overwritten content to restore it). This is adversarial on purpose: it is what proves the temp-file + fsync + rename + dir-fsync recipe (`vfs.WriteFileAtomic`) is needed.
- Config is stdlib `flag`; defaults are the safest settings (fsync=always, group commit on).
- Logging is stdlib `log/slog`.

## What happens if X fails

## Test and benchmark results

## Bugs found by tests

## Interview Q&A

## Progress log
- (start) Brief saved, repo scaffolded.
- Phase 1 DONE: module, layout, Makefile, CI, vfs (OS + Faulty, tested), config, logx, cmd stubs. build/vet/test clean.
  RESOLVED: gcc was missing so `-race` could not run; user installed gcc (apt). `go test -race ./...` now works. (An unused zig tarball sits at ~/.local/zig and can be deleted.)
  User authorized pushing to a GitHub repo once they supply the URL (see CLAUDE.md).
- Phase 2 DONE: RESP2 reader/writer (+fuzz targets, 10s fuzz clean), TCP server with pipelining/max-conns/graceful shutdown, 25 commands, Mem engine + conformance suite, redis-cli verified by hand (PING/SET EX/GET/TTL/MSET/MGET/INCR/DECRBY/EXISTS/NX/EXPIRE/PERSIST/DBSIZE/--scan --pattern/--pipe/INFO/unknown cmd/SIGTERM). build/vet/`go test -race ./...` clean.
  KNOWN ISSUE: local Phase 1 commit was reworded to drop the Claude trailer, but force-push to origin was denied by the permission system; origin/main still has the old Phase 1 commit, so local and remote have diverged. User must run `git push --force-with-lease origin main` (or approve it) before normal pushes work.
  Next: Phase 3 (WAL + group commit + replay recovery).
