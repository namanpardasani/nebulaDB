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

### SSTable (`NNNNNN.sst`, little endian) — internal/sstable/format.go
```
data blocks* | filter block (Bloom over USER keys, optional) | index block | properties block | footer(64B)
block   = contents | compression u8 (0) | crc32c(contents+compression byte) u32
entry   = shared uv | unshared uv | valueLen uv | kind u8 | seq uv | expireAt uv | keySuffix | value   (prefix compressed, restart point every 16 entries)
block tail = restart offsets u32* | numRestarts u32
index   = block whose entries are (lastInternalKey of data block -> handle{offset u64,size u64}): sparse, one entry per data block
footer  = filterHandle(16) indexHandle(16) propsHandle(16) version u32 crc32c(first 52 bytes) u32 magic u64 "NEBULSST"
```
Point lookup = footer + (cached) index → binary search → ONE data block read, preceded by a Bloom check. Footer is written last, so a table cut short by a crash has no valid footer and is rejected.
### MANIFEST (`MANIFEST-NNNNNN`) + `CURRENT` — internal/manifest
Record = len u32 | crc32c u32 | edit. Edit = tagged varints: logNumber, nextFileNum, lastSeq, add-file(level,num,size,entries,deletes,minSeq,maxSeq,smallest,largest), delete-file(level,num). CURRENT = `MANIFEST-NNNNNN\n`, replaced atomically (tmp + fsync + rename + dir fsync). Log is rewritten as one snapshot edit when > threshold. Torn-tail rule identical to the WAL's.
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
- (P5) SSTable keeps ALL versions at flush (no dropping); compaction (Phase 7) removes shadowed versions/tombstones. Bloom filter covers user keys, 10 bits/key by default (`-bloom-bits`). Bloom was pulled forward from Phase 6 because the file format needs the filter slot; Phase 6 adds the block cache and measurement.
- (P5) Flush order (crash-safe): write `NNNNNN.sst.tmp` -> fsync -> rename -> fsync dir -> open+validate -> append edit {add L0 file, logNumber=WAL boundary, lastSeq} to MANIFEST (fsync) -> install new in-memory version + drop the immutable memtable atomically under one lock (readers see the memtable or the table, never neither) -> delete WAL segments < boundary. Crash anywhere: orphan .sst/.tmp are deleted at startup; WAL is replayed from `logNumber`.
- (P5) File numbers (tables, manifests) come from the manifest's counter; WAL segment numbers are a separate counter (`logNumber` refers to WAL numbering).
- (P5) Versions are immutable and reference-counted; a table replaced by an edit is marked obsolete and only closed+deleted when the last version/iterator referencing it is released (needed by compaction in Phase 7).
- (P5) Backpressure v1: writers stall (sync.Cond) while `len(immutables) >= MaxImmutables` (default 2); give up with ErrWriteStall after StallTimeout (30s) so a dead disk cannot hang clients. L0-count based slowdown/stop comes with compaction in Phase 7.
- (P5) Missing live table or CURRENT-less dir with .sst files => refuse to start (never silently drop data). Corrupt data block => read returns an error (SSTable CRCs), never wrong data.
- (P5) Close() does not flush the active memtable: WAL replay recovers it. Open file descriptors: all live tables stay open (fd cache is a later refinement).
- (P6) Block cache: 16-shard byte-bounded LRU keyed by (fileNum, offset); cached blocks are immutable so iterators need no pinning (GC keeps an evicted block alive while held); table deletion calls EvictFile. Default 32 MiB in the engine, `-cache-size` flag (0 disables). Tradeoff: a strict LRU `Get` mutates the recency list, so each shard's mutex serialises its readers; CLOCK/sampled-LRU would scale better under heavy read parallelism (possible future work, not needed so far).
- (P6) Perf finding: global atomic hit/miss counters were the bottleneck of parallel cache reads (BenchmarkGetHitParallel 67.6 -> ~51 ns/op after moving counters into the shard's critical section; numbers from the machine below).
- (P7) Compaction picking. Leveled: score(L0)=files/trigger, score(Ln)=bytes/(L1Max*ratio^(n-1)); highest score >= 1 wins. L0->L1 takes ALL L0 files + overlapping L1 files; Ln->Ln+1 takes one file chosen round-robin by key (per-level pointer) + overlapping Ln+1 files. Output is split at user-key boundaries at TargetFileBytes. One compaction at a time (compactMu); no trivial-move optimisation (a moved file would need a second tableFile object sharing one reader).
- (P7) What compaction drops: all versions of a key but the newest (readers needing older ones hold pinned input files, so dropping is safe for them); a tombstone / expired put only if no file OUTSIDE the compaction could still hold an older version of that key (key-range test against deeper levels; for tiered also overlapping L0 files with MinSeq < tombstone seq). An expired put that must stay (older data below) is rewritten as a tombstone to reclaim its value bytes.
- (P7) Size-tiered: everything stays in L0 (overlapping); a bucket = run of files within 2x of the smallest size; compacted when >= trigger files (max 32 per run) into ONE output. Because tiered outputs interleave sequence ranges, L0 lookups are now ordered by MaxSeq with early exit (correct for any layout; with ordinary flush output the newest file still wins immediately). Slowdown/stop thresholds are x4 in tiered mode.
- (P7) Backpressure: (1) imm backlog >= MaxImmutables or L0 >= stop trigger: block (cond var), ErrWriteStall after StallTimeout; (2) L0 >= slowdown trigger: 1 ms sleep per write; counters write_slowdowns / write_stalls / write_stall_seconds exposed.
- (P7) Atomicity: compaction outputs are written (tmp, fsync, rename) and the directory is fsynced BEFORE one MANIFEST record that adds outputs and deletes inputs; inputs become obsolete only in the in-memory version swap and are deleted when the last version referencing them is released. `Snapshot` (seq + pinned version) is the public handle for this.
- (P7) Throttle: optional bytes/sec limit applied every 64 KiB of compaction input; also aborts promptly at Close.
- (P8) Observability stack is stdlib only. `metrics` = atomic counters + fixed-bucket histograms (5 µs .. 10 s, quantiles by in-bucket interpolation, so precise to bucket width) + a Prometheus text-format writer. Latency measured is server-side command processing (not network or flush time). Engine numbers come from one typed `EngineMetrics` snapshot that INFO, /metrics and /api/stats all derive from.
- (P8) Admin server: loopback by default; guard middleware rejects non-loopback Host (DNS rebinding) and non-loopback Origin (cross-site calls), echoes CORS only for loopback origins, and requires `Content-Type: application/json` on POST (forces a preflight; blocks form-based CSRF). Try-it proxy is a whitelist (no CONFIG/SHUTDOWN/CLIENT/FLUSH*); FLUSHDB/FLUSHALL only in -dev. Binding the admin port to a non-loopback address disables the host check and logs a warning (it has NO authentication).
- (P8) SSE: lifecycle events (flush/compaction/file created+deleted/sealed/stall/recovery) are persisted in a 2000-entry ring with ids and replayed via Last-Event-ID; high-rate data is NOT per-operation: a 250 ms `tick` message carries deltas (writes/s, bytes/s, cache hit/miss, bloom skips, WAL fsyncs, level sizes, memtable fill, a sampled list of recent writes) so 100k writes/s costs ~4 messages/s. Tick generation only runs while a client is subscribed. Slow subscribers are dropped, never block the engine.
- (P8) Recent-writes sampling: 1 in 16 commits stores a truncated key in a 64-slot atomic ring (one atomic add per commit on the hot path).
- (P8) Crash demo: `nebuladb -supervise` re-executes itself as a child with NEBULA_SUPERVISED=1 and restarts it if killed by a signal; `POST /api/dev/crash` (dev only, refused unless supervised) SIGKILLs the child ~100 ms after replying. Bad-flag/exit-error ends supervision (no restart loops).
- (P8) Directory LOCK (flock) added to engine.Open: a second process (e.g. `nebula compact` while the server runs) fails with "database ... is in use". Windows has no-op locking.
- (P8) `nebula repair` never modifies the source. Uses the MANIFEST when readable (live tables + WAL >= logNumber); otherwise every .sst/.wal, in which case keys deleted by a since-dropped tombstone can resurrect (reported). Damaged SSTable data blocks are skipped whole; WAL records are salvaged by resynchronising past damage. Newest-by-seq of every key is held in RAM (limitation). Output is a fresh, compacted database.
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
### Phase 6 measurements (same machine; go test output, unit-test scale, not final benchmarks)
- Bloom false-positive rate, 20k keys, 200k absent probes (`TestFalsePositiveRate`): 6 bits/key 5.67% (theory 5.61%); 8 bits 2.15% (2.16%); 10 bits 0.756% (0.819%); 12 bits 0.323% (0.314%); 16 bits 0.040% (0.046%).
- Engine-level (3000 keys, 10 bits/key, 8 tables): 2141 filter checks, 1968 tables skipped without I/O, 23 false positives (1.07% of checks); 50 rounds of repeated hot-key reads: 147 cache hits / 3 misses (98.0% hit rate), 3 block reads.
- Prefix compression: 2000 keys with a 36-byte common prefix: 24,492 data bytes (restart every 16) vs 118,134 (restart every entry).
- BenchmarkGetHit (block cache, 1000 blocks): ~38-40 ns/op serial, ~50-56 ns/op with 8 parallel goroutines (3 runs each). BloomMayContain: 36.6 ns/op.

## Bugs found by tests
### 1. Skip list returned a version newer than the reader's snapshot (Phase 4)
- **Symptom:** `TestBatchAtomicVisibility` (MGet over a 3-key MSET batch) saw torn batches such as a=13 b=14 c=13; the isolated `TestSnapshotBatchesNeverTorn` reproduced it with `memtable.Get(key, snap=27)` returning a node with seq 29.
- **Cause:** `findGE` finished its search loop, then returned `x.next[0].Load()` — a *second* read of the pointer. With a concurrent writer, a new node could be linked right after `x` between the loop's last check and the reload. That node (a newer version, seq above the snapshot) sorts BEFORE the target, so the function returned something the loop had never accepted.
- **Fix:** return the `nx` that the level-0 loop itself observed failing the "before target" test (never re-read the link).
- **Test added:** `internal/memtable/snap_test.go` (TestSnapshotBatchesNeverTorn) and the engine-level `TestBatchAtomicVisibility`. Lesson: in lock-free structures, a pointer you test must be the pointer you use.

### 2. Acknowledged write lost after flush: rotation in the middle of a group commit (Phase 5)
- **Symptom:** `TestPowerLossDuringFlushLosesNothingAcked`, round 0: after a simulated power loss, an acknowledged write (`g3-k043="v3-443"`) was gone; the older `v3-393` was recovered.
- **Cause:** group commit writes a whole group (many batches) into WAL segment S in one write, then applies the batches to the memtable one by one. Memtable/WAL rotation was triggered from inside each batch's apply callback, i.e. possibly BETWEEN two batches of the same group. A later batch of that group (record already in S) was then applied to the NEW memtable. When the sealed memtable was flushed the manifest's WAL boundary allowed S to be deleted, so that batch's only durable copy vanished and it lived only in RAM.
- **Fix:** rotate only at group boundaries: `wal.Options.AfterGroup` runs on the leader after every apply of a group (barriers moved to the same point). Plus a defensive invariant in `DB.rotate`: refuse to seal unless `memtable.maxSeq == wal.LastSeq()` (counts `rotation_errors` instead of risking data).
- **Tests added:** `TestRotationAtGroupBoundaryOnly` (slow-fsync FS forces multi-batch groups; fails with 339 invariant violations when the bug is reintroduced, verified), and the random-moment power-loss test above.
- **Second finding while writing that test (test bug, not engine bug):** after a torn-tail crash the recovered value can be NEWER than the last acknowledged one (a write that was in flight, written but not yet acknowledged, may partially persist). Correct assertion: recovered value >= last acked, never older.

### 3. Manifest append failure poisoned flushing forever (Phase 5, found by TestFlushFailureStagesAreSafe/write_manifest)
- **Symptom:** after a transient write error on the MANIFEST, `WaitForFlush` hung for ever even after the fault cleared.
- **Cause:** any failed append marked the manifest permanently read-only.
- **Fix:** like the WAL, truncate the partial record away and continue (ENOSPC is recoverable); only poison if the tail cannot be repaired or fsync fails. Test: `TestAppendFailureRollbackAndFsyncPoison`.

### 4. After a restart, every L0 table was invisible to reads (Phase 7, found by the restart tests)
- **Symptom:** `TestFlushCreatesTablesTrimsWALAndRecovers` (and 6 other tests) failed right after reopening: the newest ~180 keys (the two newest flushed tables) returned "not found"; WAL replay and the MANIFEST were both correct, and the tables were intact when iterated directly.
- **Cause:** point lookups search L0 through `version.l0BySeq` (L0 ordered by MaxSeq). Only `version.next()` built it; the initial version created in `Open` was assembled by hand and never populated it, so a freshly opened database could not see its L0 files. Versions built after the first flush/compaction were fine, which hid the bug until the data was reopened.
- **Fix:** one `buildIndexes()` called from both `Open` and `next()`, with a comment that every version must run it.
- **Why tests caught it:** restart-then-verify-everything tests (`checkAll` after reopen) with compaction/flush active. Lesson: derived lookup structures should have exactly one constructor.

### 5. Failed MANIFEST fsync deleted a table the MANIFEST might name; next startup refused to open (Phase 9, found by tests/fault TestFsyncFailureByFileClass/MANIFEST)
- **Symptom:** inject an fsync failure on the MANIFEST during a flush, then restart: `open: live table 000002.sst (level 0): no such file or directory` — the database would not start.
- **Cause:** flush (and compaction) treated ANY `LogAndApply` error as "the edit did not happen" and deleted the freshly written SSTable. But when the manifest record was written and only its fsync failed, the edit's fate is unknown: the bytes sit in the page cache and a process restart (no power loss) reads them back, so the MANIFEST then references a file we had already deleted.
- **Fix:** `manifest.UncertainError` / `IsUncertain`: returned when an append's fate is unknown (fsync failure, or a partial append that could not be truncated away). On an uncertain failure flush and compaction KEEP their output files: after a restart the MANIFEST either lists them (live) or not (orphans, deleted at startup); compaction inputs are never touched before the edit, so either outcome is consistent. Definite failures (write rolled back, invalid edit) still clean up. Also: later calls on a poisoned manifest fail fast with a definite error, and the flush/compaction loops check `Manifest.Err()` first so they stop creating files that can never be installed.
- **Rule of thumb for interviews:** after an I/O error, ask "what are the possible on-disk outcomes?" and make every one of them recoverable; cleaning up is only safe when exactly one outcome is possible.

### Test-harness bugs fixed along the way (not engine bugs, kept for honesty)
- Crash harness: two `freePort()` calls returned the same ephemeral port (server could not bind its admin port) -> reserve all ports at once. SCAN-vs-GET cross-check raced with short-TTL expiry -> SCAN-present/GET-absent is legal only for TTL keys.
- Fault chaos test: a clean (non-power-loss) restart showing a state does not prove it is durable (e.g. an fsync-failed record read back from the page cache); a later power loss may still revert it. The model now adopts observations only after power loss.
- Protocol model: the fake clock must not advance inside a pipelined batch.
- Mutation check: re-introducing bug #2 (rotation in the middle of a group commit) into a throwaway binary made the crash harness report 2 lost acknowledged writes in iteration 2 of 200, proving the harness can detect that class of bug.

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
- Phase 5 DONE: internal/bloom (measured FP rates match theory), internal/sstable (blocks, prefix compression, sparse index, filter, footer, reader/iterator, Verify), internal/manifest (edits, CURRENT, torn-tail repair, snapshot rewrite), engine: versions with refcounts, flush worker, crash-safe startup, orphan cleanup, write stall, events hook. Tests: 100% of truncations and single-byte flips of an SSTable detected, 4 fuzz targets (sstable x2, manifest x2, bloom, wal) run clean, flush failure at 5 stages, write stall + resume, 25 random-moment power-loss rounds with concurrent flush (0 acked writes lost), kill -9 + redis-cli check. 3 bugs found (see above). build/vet/`go test -race ./...` clean.
  KNOWN ISSUES: origin push still pending (force-push approval needed, see Phase 2 note). L0 files accumulate until Phase 7 compaction (reads check every L0 file whose key range matches). Block cache not yet wired (Phase 6).
- Phase 6 DONE: sharded LRU block cache (+tests, benchmarks), cache and Bloom stats in INFO, Bloom FP rate measured vs theory, iterator Merge/View proven over memtable + overlapping L0 tables (SCAN with page sizes 1/7/64/all matches an oracle, tombstones and overwrites across tables), engine cache/bloom integration test. build/vet/`go test -race ./...` clean.
  KNOWN ISSUES: origin push pending (force-push approval). No compaction yet: L0 grows; Phase 7 next.
- Phase 7 DONE: leveled + size-tiered compaction, tombstone/TTL-safe dropping, rate limit, L0 slowdown/stop backpressure, ref-counted versions, public Snapshot/Iterator, CompactAll. Tests: seeded model (12k ops with TTL/deletes/overwrites) for both styles incl. restart + CompactAll, tombstone-only-when-safe, expired->tombstone rewrite, snapshot pins files across compaction then they are deleted (5 -> 1 files), L0 backpressure (36 slowdowns then ErrWriteStall at L0=5, resumes after compaction), failure injected at 5 compaction stages (no leaks, version unchanged, retry works), throttle (29 ms unthrottled vs 324 ms at 1 MiB/s on the test data), power loss with concurrent writers+flush+compaction for both styles (24 rounds, 0 acked writes lost). 1 more bug found (#4). build/vet/`go test -race ./...` clean (engine package run twice for flakiness).
  Amplification measurements (write/read/space for leveled vs tiered) are deferred to Phase 10 where the benchmark harness lives; counters (write_amplification, bytes read/written, entries dropped) are already exposed.
  KNOWN ISSUES: origin push pending (force-push approval). Compaction runs one at a time. All live tables keep an fd open.
- Phase 8 DONE: engine.Metrics()/LevelDetails()/MemoryInfo()/RecentWrites(), internal/metrics, server command counters + read/write/other latency histograms + ops/sec sampler + INFO sections, admin HTTP (health, stats, levels, Prometheus /metrics, SSE with replay + ticks, whitelisted command proxy, dev flush/compact/crash, security guard), `-supervise`, directory LOCK, `nebula` CLI (stats, dump-wal, dump-sstable, dump-manifest, verify, repair, compact) with salvage primitives (wal.SalvageSegment, sstable.Reader.Salvage). Tests: admin (guard, proxy whitelist, stats/levels/Prometheus format, SSE events+ticks+replay, slow-subscriber drop, dev gating), tools (dumps, verify healthy/corrupt/missing/torn/orphan, repair after destroying a data block recovered all but 7 of 550 keys and the repaired DB verifies and the source is byte-identical, repair with no MANIFEST + WAL damage, compact refuses a locked DB). Live check: supervised server, SIGKILL via admin endpoint, auto-restart, 4 tables + 177 WAL batches recovered, no data lost. build/vet/`go test -race ./...` clean.
  KNOWN ISSUES: origin push pending (force-push approval). Windows: no directory lock, no supervise (syscall.Kill) — Linux/macOS are the supported platforms for dev endpoints.
  Next: Phase 9 (torture testing: model/fuzz/crash harness/fault injection/stress; record results).
