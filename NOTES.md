# NebulaDB Notes

## Architecture
(Built so far; see PROJECT_BRIEF.md for the full target.)
```
redis-cli ─TCP─> server (RESP2, pipelining) ─> engine.Engine
                                                  │  DB (LSM, this phase: memtables + WAL)
   write: stripe lock(s) → wal.Commit[group commit: seq assign, 1 write, 1 fsync] → leader applies
          to active memtable (skip list) → publish `visible` seq → maybe seal memtable
   read : snapshot = visible seq; active memtable → immutable memtables (newest first)
   scan : Merge(heap) over memtable iterators → View(snapshot, tombstones, TTL, dedupe)
```
Until Phase 5 there is no SSTable flush: sealed (immutable) memtables stay in RAM and WAL segments are kept, so recovery replays everything and reads get slower as immutables accumulate. Phase 5 adds flush, which removes both.

## On-disk formats
### WAL segment (`NNNNNN.wal`, little endian) — internal/wal/record.go
```
record  = len u32 | crc32c(payload) u32 | payload[len]
payload = seq u64 | count u32 | count x op
op      = type u8 (1=put,2=delete) | keyLen u32 | valLen u32 | expireAt i64 (unix ms, 0=none) | key | value
```
One record = one atomic batch; its ops get seq, seq+1, ... Segments are numbered files; the old segment is fsynced on rotation so only the NEWEST segment can have a torn tail.

## Guarantees
### WAL / fsync policies (Phase 3)
| policy | process kill -9 | power loss / kernel crash |
|---|---|---|
| always (default) | no acked write lost | no acked write lost (fsync before ack; concurrent writers share one fsync) |
| every (N ms) | no acked write lost (data is in page cache) | up to N ms of acked writes can be lost; log stays a clean prefix |
| never | no acked write lost | unbounded loss possible (still a clean prefix; recovery never yields holes/garbage). UNSAFE |
- Multi-key commands (MSET, FLUSHDB) are one WAL record: all-or-nothing across a crash.
- Writes become visible to readers only after they are durable (apply runs after fsync).
- After a failed fsync the WAL is poisoned (read-only) until restart ("fsyncgate": retrying fsync can falsely succeed after the kernel drops dirty pages).
- ENOSPC on append: partial record is truncated away, error returned, WAL stays usable; if truncation itself fails the WAL is poisoned.

## Decisions and tradeoffs
- (P2) `engine.Engine` interface is the seam between network and storage; every op is atomic inside the engine (IncrBy, Set NX/XX) so the server holds no locks around read-modify-write. `engine/enginetest` is a shared conformance suite (Mem passes; the LSM engine must too). Time is injected (`now func() int64`) so TTL tests are deterministic.
- (P2) SCAN cursors: Redis clients need an integer cursor but an LSM scan resumes from a *key*. Server keeps a bounded (4096) id->resume-key table; unknown/evicted ids give `ERR invalid cursor`. COUNT bounds keys examined, MATCH filters after (pages may be empty), as in Redis.
- (P2) RESP parser limits: bulk <= 64MiB, array <= 1M, line <= 64KiB, nesting <= 32; large bulks are read in 64KiB chunks so a lying length cannot force a huge allocation. Protocol errors reply `-ERR Protocol error` and close (framing cannot be re-synced).
- (P2) Pipelining: replies are flushed only when the read buffer is empty. Graceful shutdown: close listener, set read deadline in the past on all conns (wakes idle readers; in-flight command still completes and flushes), then caller closes engine.
- (P2) HELLO only accepts protocol 2 (HELLO 3 -> NOPROTO, like Redis) so modern clients fall back to RESP2. DBSIZE is approximate by contract (documented in Engine.ApproxSize).
- (P3) Torn tail vs corruption rule: a bad record is a torn tail iff NO valid record exists anywhere after it (forward scan). Valid record after a bad one => mid-file damage => refuse to start (CorruptionError). Known ambiguity (documented): a bit flip in the very LAST record looks like a torn write and is truncated + reported (LevelDB has the same property). Fail-closed corner: if a torn record's partial payload happens to contain a complete embedded WAL record (value that is itself WAL bytes), startup refuses.
- (P3) Group commit = leader/follower queue in wal.Commit: first arrival becomes leader, drains the queue (<=4MiB), one write + one fsync, runs apply callbacks in log order, hands leadership to the oldest queued request. Sequence numbers are assigned by the WAL in log order. apply runs outside the file lock so the engine may Rotate() from it.
- (P3) Engine write path: per-key stripe locks (256) held across read->decide->WAL commit->apply give linearizable read-modify-write (INCR, SET NX, EXPIRE) while still letting writers on different keys reach the WAL together (so they group-commit). Multi-key ops lock stripes in ascending order. This is the same shape the LSM engine will use.
- (P3) FLUSHDB is a batch of per-key deletes in one record (no range tombstone type); fine for the map engine, revisit for the LSM.
- (P3) Faulty FS Crash() rolls back un-dir-synced creates; WAL createSegment fsyncs the directory so a segment file cannot vanish after its records were acked.
- (P4) Sequence numbers: assigned by the WAL in log order; `DB.visible` (atomic) is the highest fully-applied seq and is the snapshot every read uses. It advances only after a whole batch is in the memtable, so readers see multi-key batches atomically (MGET uses a single snapshot via the optional `MultiGetter`).
- (P4) Version ordering: (user key asc, seq DESC). A tombstone or an *expired* put at the newest visible version hides all older versions (an expired key must not resurrect an older value in a lower level).
- (P4) Rotation happens on the WAL group leader after apply: `wal.Rotate()` (fsync old segment, create+dir-sync new) first, then swap memtables under the write lock. If Rotate fails the big memtable stays and rotation is retried on the next write.
- (P4) DBSIZE = puts - 2*tombstones over memtables (approximate; documented). FLUSHDB deletes in atomic chunks of 10k keys (not atomic as a whole across a crash).
- (P4) Memtable accounting: key+value+64B node overhead+8B per level.
- Commit policy (user request): no Claude/Co-Authored-By attribution in commits.
- Module path `github.com/naman/nebuladb` (local only; no remote). Go 1.27.3 toolchain on dev machine.
- All I/O goes through `internal/vfs` (added to the layout; not in the brief's list). `Faulty` wraps any FS and supports: rule-based errors (skip N, fail M, sticky), torn/short writes (`Partial`), ENOSPC via a disk-size cap, read-time bit flips, and `Crash()` power-loss simulation. Crash drops unsynced file bytes (or keeps a random torn prefix) AND undoes create/rename/remove that were never made durable with `SyncDir` (backing up overwritten content to restore it). This is adversarial on purpose: it is what proves the temp-file + fsync + rename + dir-fsync recipe (`vfs.WriteFileAtomic`) is needed.
- Config is stdlib `flag`; defaults are the safest settings (fsync=always, group commit on).
- Logging is stdlib `log/slog`.

## What happens if X fails

## Test and benchmark results
### Memtable microbenchmarks (Phase 4; raw `go test -bench`, single run, NOT final numbers)
Machine: 11th Gen Intel i7-1165G7 @ 2.80GHz, 8 threads, WSL2 Linux, Go 1.27.3. Keys `key%012d` random, 17-byte values.
- BenchmarkAdd: 4636 ns/op, 154 B/op, 4 allocs/op (1M inserts into one growing list)
- BenchmarkGet: 1945 ns/op, 0 allocs (100k keys, single goroutine)
- BenchmarkGetParallel: 258.9 ns/op (8 goroutines, lock-free reads)
Phase 10 will re-run these properly (multiple counts, benchstat).

## Bugs found by tests
### 1. Skip list returned a version newer than the reader's snapshot (Phase 4)
- **Symptom:** `TestBatchAtomicVisibility` (MGet over a 3-key MSET batch) saw torn batches such as a=13 b=14 c=13; the isolated `TestSnapshotBatchesNeverTorn` reproduced it with `memtable.Get(key, snap=27)` returning a node with seq 29.
- **Cause:** `findGE` finished its search loop, then returned `x.next[0].Load()` — a *second* read of the pointer. With a concurrent writer, a new node could be linked right after `x` between the loop's last check and the reload. That node (a newer version, seq above the snapshot) sorts BEFORE the target, so the function returned something the loop had never accepted.
- **Fix:** return the `nx` that the level-0 loop itself observed failing the "before target" test (never re-read the link).
- **Test added:** `internal/memtable/snap_test.go` (TestSnapshotBatchesNeverTorn) and the engine-level `TestBatchAtomicVisibility`. Lesson: in lock-free structures, a pointer you test must be the pointer you use.

## Interview Q&A

## Progress log
- (start) Brief saved, repo scaffolded.
- Phase 1 DONE: module, layout, Makefile, CI, vfs (OS + Faulty, tested), config, logx, cmd stubs. build/vet/test clean.
  RESOLVED: gcc was missing so `-race` could not run; user installed gcc (apt). `go test -race ./...` now works. (An unused zig tarball sits at ~/.local/zig and can be deleted.)
  User authorized pushing to a GitHub repo once they supply the URL (see CLAUDE.md).
- Phase 2 DONE: RESP2 reader/writer (+fuzz targets, 10s fuzz clean), TCP server with pipelining/max-conns/graceful shutdown, 25 commands, Mem engine + conformance suite, redis-cli verified by hand (PING/SET EX/GET/TTL/MSET/MGET/INCR/DECRBY/EXISTS/NX/EXPIRE/PERSIST/DBSIZE/--scan --pattern/--pipe/INFO/unknown cmd/SIGTERM). build/vet/`go test -race ./...` clean.
  KNOWN ISSUE: local Phase 1 commit was reworded to drop the Claude trailer, but force-push to origin was denied by the permission system; origin/main still has the old Phase 1 commit, so local and remote have diverged. User must run `git push --force-with-lease origin main` (or approve it) before normal pushes work.
- Phase 3 DONE: internal/wal (records, segments, CRC32C, 3 fsync policies, group commit, torn-tail truncate, corruption refusal, rotation, stale-segment cleanup, poison-on-fsync-failure, ENOSPC rollback), durable Mem engine (OpenMem) used by the server by default (`-in-memory` to disable), conformance suite passes on both. Tests: every-byte torn-tail cuts, per-byte bit-flip refusal, 50 randomized power-loss iterations at WAL level and 20 at engine level with concurrent writers (0 acked writes lost), FuzzScanSegment (20s, ~900k execs, clean). Manual kill -9 + redis-cli restart check passed. build/vet/`go test -race ./...` clean.
  No bugs found by tests this phase besides my own test mistakes (shared rand across goroutines; wrong expected seq).
- Phase 4 DONE: internal/iterator (Entry/Iterator, heap Merge, snapshot View), internal/memtable (lock-free-read skip list), engine.DB (LSM engine with memtables+WAL+rotation; server now uses it by default), pkg/nebuladb embedded API, MGET snapshot reads. Tests: skip list vs sorted oracle, concurrent readers+writer under -race, conformance suite on DB at default and 1KiB memtable, shadowing across 3+ immutables, restart across multiple WAL segments, seeded model test vs map oracle (5 seeds x 1500 random ops incl. restarts/TTL/INCR/MSET/PERSIST/SCAN), power-loss test with concurrent writers + rotation (12 rounds, 0 acked writes lost). kill -9 + redis-cli smoke with rotation passed. 1 bug found (above).
  KNOWN ISSUES: origin push still pending (see Phase 2 note). Immutable memtables accumulate until Phase 5 flush.
  Next: Phase 5 (SSTable writer/reader, flush, MANIFEST/CURRENT, startup from SSTables + WAL).
