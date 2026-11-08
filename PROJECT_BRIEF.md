You are the lead engineer building a complete, high-quality systems project in this empty repo, mostly autonomously. Read this whole message; it is the full context and plan.

=====================================================
0. HOW TO WORK (read first)
=====================================================
- FIRST: save this entire message verbatim as PROJECT_BRIEF.md. Create CLAUDE.md (points to PROJECT_BRIEF.md and summarizes the rules in this section) and NOTES.md (headings: Architecture, On-disk formats, Guarantees, Decisions and tradeoffs, What happens if X fails, Test and benchmark results, Bugs found by tests, Interview Q&A, Progress log). Create a Go-appropriate .gitignore. Then give me a 10-line summary of the plan and START building immediately; don't wait for my approval.
- Build phase by phase (section 6) WITHOUT stopping to ask me, except:
  (1) the DESIGN PAUSE (Phase 11): stop and wait for my design input;
  (2) anything that could cost money or needs my accounts;
  (3) anything destructive outside this repo or irreversible;
  (4) a real blocker you can't resolve after trying reasonable alternatives (explain options, recommend one).
  Every other decision: choose the best option yourself, write it and the reasons in NOTES.md, continue.
- LOCAL git commit at the end of each phase with a clear message. Never push, never add a remote.
- Update "Progress log" in NOTES.md after every phase (done, next, known issues). If the session or context resets, re-read PROJECT_BRIEF.md and NOTES.md and continue from the next unfinished task.
- Quality bar: `go build`, `go vet`, `go test -race ./...` all clean at the end of every phase; no TODO stubs left in finished phases; clear comments wherever logic is non-obvious (I will study this code afterwards).
- NEVER invent benchmark, test or crash-test numbers. Every number in docs comes from a real run, with the machine and settings recorded.
- Verify current Go version, library versions and maintenance status instead of guessing. Free tools only.
- Use the standard library wherever reasonable. The storage engine itself (WAL, memtable, SSTables, Bloom filters, cache, compaction, manifest, recovery, RESP) must be our own code, not a library. Libraries are fine for tests, CLI flags, metrics, and the benchmark comparison targets.

=====================================================
1. CONTEXT
=====================================================
I'm a 3rd-year B.Tech student (tier-3 college, India) building portfolio projects for 2027 internships and placements (backend / platform / infra roles). This is my DEPTH project: interviewers will dig deep into storage engines, so the design must be correct, clean and well documented. Other projects: FlagForge (feature-flag SaaS), CodeAtlas (code intelligence + Blast Radius), Zerodrift (payments ledger). I'm new to Go.

=====================================================
2. WHAT WE'RE BUILDING
=====================================================
NebulaDB: a crash-safe, Redis-compatible key-value database written from scratch in Go, built on an LSM tree. It runs as a single binary; the real redis-cli and Redis client libraries can talk to it.
Pitch: "A Redis-compatible LSM-tree database written from scratch in Go, proven crash-safe: thousands of randomized kill -9 crash tests with zero acknowledged writes lost."

Supported commands (RESP2): PING, ECHO, GET, SET (with EX/PX/NX/XX), DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, MGET, MSET, INCR/DECR (atomic), SCAN (cursor-based, with MATCH and COUNT), DBSIZE (approximate is fine, document it), INFO (engine stats), FLUSHDB, COMMAND (minimal, so clients connect cleanly), QUIT. Unknown commands return proper RESP errors. Also an embedded Go API (open a DB directly as a library).

=====================================================
3. ARCHITECTURE
=====================================================
- Server: TCP listener, one goroutine per connection, RESP2 parser/writer with pipelining support, connection limits, graceful shutdown (stop accepting, finish in-flight, flush, fsync, close cleanly).
- Write path: assign sequence number -> append record to WAL (format: length + CRC32C + seq + type + key len + value len + expiry + key + value) -> fsync per policy -> insert into active memtable -> acknowledge client. Group commit: batch concurrent writers into one fsync for throughput; measure the effect.
- fsync policies: always (default, strongest), every N ms, never (documented as unsafe). Document exactly what each guarantees.
- Memtable: our own skip list (concurrent reads, single writer path or appropriate locking), sorted by key then seq; tombstones for deletes; TTL stored with each entry.
- Rotation: when the memtable exceeds a size threshold, it becomes immutable, a new WAL segment and memtable start, and a background goroutine flushes the immutable memtable to an SSTable. Old WAL segments are deleted only after the SSTable is fully written, fsynced, and recorded in the MANIFEST.
- SSTable format (immutable): data blocks (prefix-compressed keys optional, checksummed), sparse index block, Bloom filter block, properties (min/max key, entry count, seq range), footer with offsets, magic number and checksum. Write to a temp file, fsync, atomic rename, fsync the directory.
- MANIFEST: append-only log of version edits (files added/removed per level, next file number, last seq); periodically rewritten as a snapshot with atomic rename + CURRENT pointer file. Startup reads it to know exactly which files are live.
- Read path: active memtable -> immutable memtables -> SSTables newest to oldest within level rules; skip files by key range and Bloom filter; LRU block cache for hot blocks; respect tombstones and TTL.
- Iterators: k-way merge iterator over memtables + SSTables (heap-based) for SCAN, compaction and range reads, with correct handling of duplicates, tombstones and expiry.
- Compaction: leveled compaction (L0 overlapping, L1+ non-overlapping, size ratio per level) in background goroutines with throttling; drop overwritten values, expired entries and tombstones when safe (no older data below). Also implement size-tiered as an option, and measure write/read/space amplification for both.
- Snapshots / consistency: reads see a consistent view (sequence-number based); compaction never deletes files that an active iterator still uses (reference counting).
- Recovery: read CURRENT + MANIFEST, open live SSTables, replay remaining WAL segments into the memtable, truncate a torn tail record (bad CRC or short record) and report it, refuse to start on mid-file corruption with a clear error (and a `nebula repair` path documented).
- Observability: INFO command + Prometheus metrics endpoint (ops/sec, latencies, memtable size, flush/compaction counts and durations, level sizes, Bloom filter useful rate, cache hit rate, WAL bytes, write stall time).
- Backpressure: write stalls/slowdowns when L0 has too many files or flushes fall behind; documented and measured.
- Admin HTTP server (separate port, localhost by default): read-only JSON + Server-Sent Events stream of engine events (put, flush start/end, compaction start/end, file created/deleted, cache hit/miss, bloom skip) for the LSM Inspector UI.

=====================================================
4. PROJECT LAYOUT AND TOOLING
=====================================================
cmd/nebuladb (server), cmd/nebula (CLI tool), internal/{resp, server, wal, memtable, sstable, bloom, cache, manifest, compaction, iterator, engine, metrics, admin}, pkg/nebuladb (embedded API), tests/{crash, fault, model, bench}, inspector/ (web UI), docs/.
Makefile targets: build, test, race, fuzz, crash, bench, inspector, lint. GitHub Actions CI (build, vet, staticcheck/golangci-lint, test -race, short fuzz run).
CLI `nebula`: stats, dump-wal, dump-sstable, dump-manifest, verify (checksums of all files), repair (salvage readable data into a new DB), compact (manual).

=====================================================
5. TESTING STRATEGY (this is what makes the project impressive)
=====================================================
- Unit tests for every package; table-driven tests; RESP test vectors including malformed and partial input.
- Property/model-based tests: random operation sequences (SET/DEL/EXPIRE/INCR/SCAN, flushes, compactions, restarts) compared against a simple in-memory oracle; deterministic seeds so failures are reproducible.
- Go native fuzzing: RESP parser, WAL reader, SSTable reader, manifest reader (must never panic or return wrong data on garbage).
- Crash test harness: a driver process launches NebulaDB, runs concurrent writers that record which writes were ACKNOWLEDGED, kills the server with SIGKILL at random moments (including during flush and compaction), restarts it, and verifies: every acknowledged write (with fsync=always) is present with the right value; nothing returns corrupted data; deleted keys stay deleted; TTLs behave. Thousands of iterations, configurable; prints a report.
- Fault injection: a filesystem abstraction that can simulate torn writes, short writes, fsync failures, ENOSPC (disk full), and bit flips; verify the engine detects problems, fails safely, and never silently returns bad data.
- Concurrency: go test -race everywhere; stress tests with many concurrent clients.
- Benchmarks: Go benchmarks per component (skip list, bloom, block cache, iterators) and end-to-end with redis-benchmark and a custom load generator: write-heavy, read-heavy, mixed, large values, range scans, with and without group commit and with each fsync policy. Compare against Redis (run in Docker, persistence settings documented) and an embedded Go LSM engine such as Pebble or BadgerDB (verify which is maintained) on the same machine. Report ops/sec, p50/p99/p999 latency, write/read/space amplification, and explain the differences honestly.
- Record every bug the tests find in NOTES.md "Bugs found by tests" (symptom, cause, fix, test added). These are interview stories.

=====================================================
6. PHASES (build in order, commit locally after each)
=====================================================
Phase 1: module, layout, Makefile, CI, filesystem abstraction (real + fault-injecting), config, logging.
Phase 2: RESP2 parser/writer with pipelining + TCP server + an in-memory engine behind an Engine interface; redis-cli works for all supported commands; graceful shutdown.
Phase 3: WAL (segments, CRC, fsync policies, group commit) + recovery by replay; crash-safe with in-memory map.
Phase 4: skip-list memtable + immutable memtables + rotation; sequence numbers; tombstones; TTL.
Phase 5: SSTable writer/reader + flush + MANIFEST/CURRENT + startup from SSTables + WAL; atomic file handling.
Phase 6: Bloom filters (tunable bits per key; measure real false-positive rate) + LRU block cache + merge iterators + SCAN.
Phase 7: leveled compaction (and size-tiered option), reference-counted files, backpressure/write stalls, consistent snapshots.
Phase 8: metrics, INFO, admin HTTP + SSE event stream, CLI tool (stats, dumps, verify, repair, compact).
Phase 9: torture testing: model tests, fuzzing, crash harness, fault injection, race/stress. Run long enough to be meaningful; fix everything found; record results and bugs.
Phase 10: benchmarks vs Redis and Pebble/BadgerDB; charts (generate SVG/PNG charts from results); analysis of where we win, lose, and why.
Phase 11: LSM INSPECTOR (functional first), then DESIGN PAUSE:
  - Build a functional, unstyled-but-clean web UI in inspector/ (React + TypeScript + Vite, or plain TS if simpler; justify) that connects to the admin SSE stream and shows, live: incoming writes, memtable fill level, immutable memtables, flushes creating SSTables, levels with files sized by bytes and positioned by key range, compactions merging files, Bloom filter skips, cache hit rate, WAL size, write stalls. Plus a "try it" panel that sends commands through a small safe proxy, and a "crash it" button (dev only) that kills and restarts the server to show recovery.
  - Also a simple project landing/docs page (can live in inspector/ as a route).
  - Keep all styling centralized (tokens + components) so the redesign is easy.
  - Take Playwright screenshots of every screen, list screens and components, then STOP and say: "Ready for design." I will come back with a design (I'm planning it separately, with references and animations). Do not start visual design before I send it.
Phase 12 (after my design input): apply the design system and animations exactly as I describe, screen by screen, with before/after screenshots; respect prefers-reduced-motion, WCAG AA contrast, keyboard access; keep the inspector smooth with high event rates (batch updates, cap animations, use requestAnimationFrame).
Phase 13 (stretch, only if Phases 1-12 are solid): replication. Leader-follower first (WAL shipping over TCP, acknowledgements, catch-up from a snapshot, follower reads), with tests that kill the leader/follower. Then optionally Raft (our own leader election + log replication per the Raft paper) with partition and kill tests verifying no acknowledged write is lost. Document exactly which guarantees hold.
Phase 14: packaging and release prep (do NOT publish anything): small Docker image, docker-compose example (NebulaDB + Inspector), release build script for Linux/macOS/Windows binaries, install docs.
Phase 15: LEARNING PACKAGE for me:
  - README: pitch, what it is (and isn't), architecture diagram (Mermaid), on-disk formats, guarantees per fsync mode, commands supported, how to run, crash/fault/fuzz results, benchmark tables and charts, limitations, Inspector screenshots placeholders.
  - docs/LEARN.md: guided tour in reading order (which files and functions first and why); each concept explained simply with the exact file/function: RESP, WAL + CRC + fsync + group commit, page cache and why fsync matters, skip list, sequence numbers and snapshots, tombstones and TTL, SSTable layout, sparse index, Bloom filter math, LRU cache, merge iterators, leveled vs size-tiered compaction, read/write/space amplification, manifest and atomic renames, recovery and torn writes, backpressure, testing strategy.
  - docs/FAILURES.md: what happens on crash at every step of write, flush and compaction; disk full; corruption; slow disk.
  - docs/INTERVIEW.md: 50 questions with answers based on THIS code, easy to very hard; a 2-minute pitch; a 10-minute deep-dive whiteboard outline.
  - Resume bullets using ONLY measured numbers.
  - Final message: what was built, what's verified, limitations, and what I should study first.
