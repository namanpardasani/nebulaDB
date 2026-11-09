# Interview guide: NebulaDB

Everything below is answerable from this repository. Numbers are from real runs on one machine
(i7-1165G7, 8 threads, Linux under WSL2, Go 1.27.3; see [BENCHMARKS.md](BENCHMARKS.md) and
[NOTES.md](../NOTES.md)). If an interviewer pushes on a number, the honest reply is "single run
on a laptop; here is the caveat", and the docs already say so. Difficulty: **E** easy, **M** medium,
**H** hard, **VH** very hard.

## The 2-minute pitch

"NebulaDB is a Redis-compatible key-value database I wrote from scratch in Go. It speaks RESP, so
`redis-cli` works, and underneath it is an LSM tree: writes go to a write-ahead log and an in-memory
skip list, full memtables are flushed to immutable sorted files with Bloom filters, and background
compaction merges them. The part I care most about is that it is *tested by killing it*. A harness
runs the real server binary with eight concurrent writers and `SIGKILL`s it at random moments and
specifically during flushes and compactions; after every restart it checks every acknowledged write.
3,000 iterations, zero violations, with 374 kills landing mid-flush and 829 mid-compaction. A
fault-injecting filesystem covers disk-full, failed fsyncs, torn writes, bit flips and simulated
power loss, and that found real bugs, like an acknowledged write lost because the log rotated in the
middle of a group commit. Group commit makes durable writes about ten times faster under concurrency,
and on a read-heavy workload with durable writes it beats Redis with `appendfsync always` by 4.4x,
because Redis stalls reads behind its inline fsync. It also loses in places, like large values, and I
documented exactly why. It has leader/follower replication, a Docker image, and a live Inspector UI
that shows flushes and compactions as they happen, including a crash button that kills the server and
shows it recover."

## The 10-minute whiteboard outline

1. **(1 min) The problem.** Disks like sequential appends; random in-place updates are slow. Draw
   `client -> server -> [WAL | memtable]  ->  L0 -> L1 -> ... Ln`.
2. **(2 min) Write path.** Stripe lock, WAL append, fsync (group commit: leader batches the queue, one
   fsync), apply to skip list, publish the sequence number. Say what each step buys: the WAL is
   durability, the skip list is sorted recent data, the sequence number is snapshot visibility.
3. **(1 min) Flush.** Seal memtable, new WAL segment, write SSTable (`.tmp`, fsync, rename, dir fsync),
   MANIFEST edit, only then delete the old WAL. Draw the crash windows between steps.
4. **(2 min) Read path.** Memtables newest-first, then L0 newest-first, then one file per deeper level
   by key range; per file: Bloom filter ("definitely not here"), sparse index to one block, block
   cache. Tombstones and TTL hide older versions.
5. **(2 min) Compaction.** Why it exists (read amplification, space, tombstones). Leveled vs tiered
   with the measured numbers (write amp 5.07 vs 3.92; space amp 1.64 vs 2.74). When a tombstone may
   be dropped.
6. **(1 min) Recovery.** CURRENT -> MANIFEST gives the file set, delete orphans, replay WAL after the
   boundary, torn tail vs corruption rule.
7. **(1 min) How I know it works.** Crash campaign, fault-injecting FS, model tests, fuzzing, the bugs
   they found. Then limits (power loss simulated, single flusher, no consensus).

---

## Fundamentals

**1. (E) What is an LSM tree and why use one?**
It turns random writes into sequential ones. Writes are appended to a log and inserted in memory; when
memory fills, it is written out as an immutable sorted file; background compaction merges files. You
trade read cost (several places to look) and background rewriting (write amplification) for fast,
sequential writes. Same family as LevelDB, RocksDB and Pebble. In this repo: `internal/engine`.

**2. (E) What is a write-ahead log and what exactly does it protect against?**
A file the change is appended to *before* the in-memory structure changes, so a restart can replay it.
It protects against losing memory contents in a crash. It is only as strong as its `fsync` policy:
`always` survives power loss, `every` can lose up to one interval, `never` can lose an unbounded amount
on power loss (but never leaves garbage, only a clean prefix). `internal/wal`.

**3. (E) What is the difference between `write()` and `fsync()`?**
`write` copies bytes to the kernel's page cache and returns; `fsync` forces them to the device. A
process crash (`kill -9`) does not lose page-cache data, a power loss does. That is why my 3,000-run
`SIGKILL` campaign proves process-crash consistency but not power-loss safety, and why I built a
filesystem wrapper that simulates power loss by discarding un-synced data.

**4. (E) What is a tombstone?**
A delete is written as a new version of the key with kind `delete`; it must exist because older
versions of the key may sit in lower levels and the reader needs something newer to hide them. It is
removed only during compaction when nothing older can still be underneath.

**5. (E) How do TTLs work?**
An absolute `expireAt` (unix ms) is stored with each value (`wal.Op.ExpireAt`, SSTable entry field).
Expiry is decided at read time (`Entry.Expired`); there is no timer deleting keys. An expired put hides
older versions exactly like a tombstone (otherwise an old value would reappear), and compaction can
rewrite it as a tombstone to reclaim the value bytes.

**6. (M) Why is the data in an SSTable sorted, and what do you get from that?**
Sorted blocks allow binary search and prefix compression, sorted files allow merging many files with
one heap, and a sparse index (one entry per block, not per key) fits in memory. A point lookup is
"search the index, read one block". `internal/sstable`.

**7. (M) Explain prefix compression and restart points.**
Consecutive sorted keys share prefixes, so each entry stores `shared` and `unshared` lengths plus only
the new suffix. A full key is stored every 16 entries (a *restart point*) so a seek can binary search
the restart keys and then scan at most 16 entries. Measured: 2,000 keys with a 36-byte common prefix
take 24,492 bytes with restarts every 16 versus 118,134 with no sharing.

**8. (M) What does a Bloom filter do here and how did you choose its size?**
Per SSTable, a bit array over user keys answers "definitely not in this file" without I/O. Default 10
bits per key, `k = 7` probes, theoretical false-positive rate 0.82%; measured 0.756% in a unit test
(20k keys, 200k absent probes). In a 3,000-key engine test 1,968 of 2,141 table checks were skipped
without reading. Each extra ~4.8 bits per key divides the rate by ten. Probes use double hashing from
one 64-bit hash, and the hash is deliberately a fixed function because the filter is persisted.

**9. (M) Why is `DBSIZE` approximate?**
An LSM cannot count live keys without merging every level (overwritten, deleted and expired entries
still occupy files). I documented it in the `Engine.ApproxSize` contract instead of faking exactness.

**10. (M) How does `SCAN` work when Redis clients want an integer cursor?**
The engine scans by *key* (resume at the first key >= start). The server keeps a bounded table (4,096)
mapping cursor ids to resume keys (`internal/server/cursor.go`); an evicted or unknown cursor returns an
error. `COUNT` bounds the keys examined and `MATCH` filters after, as in Redis, so pages can be empty.

## The write path and durability

**11. (M) Walk through a `SET` from socket to disk.**
RESP parser -> `commands` map -> `DB.Set` -> take the key's stripe lock -> `wal.Commit` queues the batch
-> the group leader writes all queued records with one `write` and one `fsync` (`always`) -> leader runs
each batch's apply callback in log order: insert into the memtable, publish `visible` -> reply `+OK`.
Rotation of the memtable happens afterwards, at the group boundary.

**12. (M) What is group commit and what did it buy?**
Concurrent writers share one `fsync`. The first to arrive becomes the leader, drains the queue (up to
4 MiB), does one write and one sync, then hands leadership on. At 32 connections: 3,948 durable writes/s
with it versus 405 without (about 10x), and p99 18 ms versus 149 ms. With one connection both are bound
by the 2.4 ms fsync (about 400/s ceiling), which is exactly what I measured (337 and 302).

**13. (H) Why do you apply to the memtable only *after* the fsync?**
So a reader can never observe a write that a crash could still erase. Visibility equals durability
(for `fsync=always`). The cost is a little latency; the benefit is that "acknowledged" and "visible"
mean the same thing, which is what the crash model relies on.

**14. (H) What happens if `fsync` fails?**
The WAL is *poisoned*: it refuses further writes until restart. After a failed `fsync` the kernel may
have dropped the dirty pages, and a retried `fsync` can report success for data that is gone
("fsyncgate"). So I never retry; restart replays what is actually on disk. `tests/fault` injects
fsync failures on the WAL, SSTables, MANIFEST and directories.

**15. (H) What happens on `ENOSPC` during an append?**
The partial record is truncated away, the error is returned to the client, and the WAL stays usable;
only if the truncation itself fails is it poisoned. Tested with 27 disk-full sizes in `tests/fault`.

**16. (H) A crash leaves a half-written record at the end of the WAL. How do you tell it from
corruption in the middle?**
A bad record is a *torn tail* only if no valid record exists anywhere after it (forward scan,
`wal.ScanSegment`/`findValidRecord`). If a valid record follows, the damage is mid-file and recovery
refuses to start with a `CorruptionError` instead of silently dropping acknowledged data. Known
ambiguity: a bit flip in the very last record looks like a torn write and is truncated and reported
(LevelDB behaves the same). Only the newest segment can have a torn tail because older segments are
fsynced at rotation.

**17. (H) Why are per-key stripe locks enough for `INCR` and `SET NX`?**
The lock covers read, decision, WAL commit and apply, so two operations on the same key are serialised
and linearizable, while operations on different stripes still reach the WAL together and share a
fsync. Multi-key ops (`MSET`) lock stripes in ascending order to avoid deadlock, and are a single WAL
record so they are all-or-nothing across a crash.

**18. (VH) You rotate the WAL and memtable inside the group leader. What can go wrong?**
It did go wrong (bug #2). A group writes many batches into segment S in one `write`, then applies them
one by one. I triggered rotation from each batch's apply callback, i.e. possibly between two batches of
the same group, so a later batch (already in S) landed in the *new* memtable. When the sealed memtable
flushed, the MANIFEST boundary let S be deleted, and that batch lived only in RAM: an acknowledged write
lost after a power loss. Fix: `wal.Options.AfterGroup` rotates only at group boundaries, plus an
invariant check in `DB.rotate` (refuse to seal unless memtable max seq equals the WAL's last seq).
Reintroducing the bug made the dedicated test fail with 339 violations and the crash campaign fail in
iteration 2.

## Reads, snapshots and the skip list

**19. (M) How do reads get a consistent view while writers run?**
Every read uses a snapshot sequence number (`DB.visible`, published only after a whole batch is in the
memtable) and ignores newer versions. Multi-key batches are therefore seen entirely or not at all, and
`MGET` uses one snapshot. No reader locks.

**20. (H) Why can the skip list be read without locks?**
Single writer; a node is fully initialised before it is published with an atomic store into its
predecessor's `next`; nodes are never modified or removed (deletes are tombstone versions), and Go atomics
give the happens-before edges. A reader may or may not see a concurrent insert, which is fine because
it filters by sequence number.

**21. (VH) Tell me about a concurrency bug you found in it.**
Bug #1: `TestBatchAtomicVisibility` saw torn `MSET` batches (a=13 b=14 c=13). `findGE` finished its
loop and then returned `x.next[0].Load()`, a *second* read of the pointer; a writer could link a newer
node (seq above the snapshot) in between, and the function returned something the loop never accepted.
Fix: return the `nx` the loop itself observed. Lesson: in lock-free code, the pointer you test must be
the pointer you use.

**22. (M) Why order the skip list by (key ascending, sequence descending)?**
The first entry at or after a key is its newest version, so a snapshot read finds the answer by
skipping entries newer than its sequence number, and iteration visits versions newest-first.

**23. (H) What does a point `Get` actually do, in order?**
Take a snapshot (version + sequence). Check the active memtable, then sealed memtables newest-first,
then L0 files ordered by max sequence (stop at the first hit), then for each deeper level the single
file whose key range covers the key: Bloom check, index binary search, one block read through the cache.
The first visible version decides; a tombstone or expired entry means "not found".

## Flush, compaction and the MANIFEST

**24. (M) Describe the flush sequence and why that order.**
Write `NNNNNN.sst.tmp`, fsync, rename, fsync the directory, open and validate, append the MANIFEST edit
(add file, new WAL boundary), swap versions in memory (readers see the memtable or the table, never
neither), then delete WAL segments below the boundary. Each step is only made visible after the previous
one is durable; a crash anywhere leaves either the old state or the new state, and leftovers are
orphans deleted at startup.

**25. (H) What is the MANIFEST and why not just list the directory?**
A CRC-framed log of edits (add/delete file, last sequence, WAL boundary) that defines which files are
live. After a crash a directory can contain half-written or no-longer-live files; the MANIFEST is the
single atomic source of truth. `CURRENT` points at it and is replaced by tmp + fsync + rename +
directory fsync. The edit log is rewritten as one snapshot edit when it grows.

**26. (VH) A MANIFEST `fsync` fails during a flush. What do you do with the new SSTable?**
Keep it (bug #5). I used to delete it on any error, but when the record was written and only the
`fsync` failed, its fate is unknown: on a process restart the page cache returns the bytes and the
MANIFEST names a file I deleted, so the database would not open. The manifest now returns
`UncertainError`; flush and compaction keep their outputs, so after restart the MANIFEST either lists
them (live) or not (orphans, deleted). Rule: after an I/O error enumerate the possible on-disk outcomes
and make each recoverable; clean up only when exactly one outcome is possible.

**27. (M) Leveled versus size-tiered compaction?**
Leveled keeps L1+ sorted and non-overlapping with a ~10x size ratio per level: low space overhead and
few files to check, more rewriting. Size-tiered merges files of similar size: less rewriting, more space
and more overlapping files. Measured (300k keys, 2M overwrites): write amplification 5.07 vs 3.92, space
amplification 1.64 vs 2.74, 28 vs 6 tables, ~1 block lookup per `Get` for both thanks to Bloom filters.

**28. (H) Define read, write and space amplification and how you measured write amplification.**
Write amp = bytes written to storage / bytes the user wrote; read amp = blocks or files touched per
lookup; space amp = bytes on disk / live data. I measured write amp two ways: the engine's own
accounting (WAL + flush + compaction output) and device bytes after a forced `sync`; they agreed within
3% for NebulaDB (5.07 vs 5.22), which is the evidence the accounting is right. Per-process `/proc/self/io`
reads zero under WSL2 and an un-synced device counter measures writeback timing; the first attempts gave
impossible numbers (below 1) and were discarded.

**29. (H) When can compaction drop a tombstone?**
Only if no file outside the compaction could still hold an older version of the key (key-range check
against deeper levels; for tiered also overlapping L0 files with a smaller sequence). Dropping it
earlier would resurrect deleted data. Older *versions* of a key can be dropped freely: readers needing
them hold pinned input files.

**30. (H) How does a long-running iterator survive a compaction deleting its files?**
Versions (the set of live tables) are immutable and reference-counted. A snapshot or iterator pins its
version; replaced tables are marked obsolete and only closed and deleted when the last version
referencing them is released.

**31. (H) How does backpressure work?**
`DB.waitForRoom`, before taking any lock: if sealed memtables >= `-max-immutables` or L0 files >= stop
trigger, writers block (error after 30 s); if L0 >= slowdown trigger, each write sleeps 1 ms; else no
delay. Counters expose stalls and slowdowns. The Inspector shows it live; under a flood with small
limits I captured the "writers stalled" state. In the benchmarks this is what limits large values: one
8 s cell had 4,384 stalls totalling 78.7 s of blocked writer time.

## Recovery and failure

**32. (M) What does startup do?**
Take the directory lock, read `CURRENT`, replay the MANIFEST to rebuild the file set (refuse if a live
table is missing), delete orphan `.sst`/`.tmp` files, then replay WAL segments from the recorded
boundary into a new memtable, truncating a torn tail. Restart-to-ready averaged 40 ms (p99 85 ms) in the
campaign, on databases under ~10 MiB; that says nothing about large databases.

**33. (H) How do you know acknowledged writes survive? What exactly does the crash test assert?**
Each of 8 writers keeps a model of what the server acknowledged and what was merely in flight
(possibly applied). After `SIGKILL` and restart, every key is read and must match an allowed state:
the last acknowledged value or a later in-flight one, never older. Counters from `INCR` must never be
lost or duplicated, `MSET` groups never half applied, TTL keys must not return after expiry. 3,000
iterations, 4,584,446 acknowledged operations, 1,320,440 verifications, 0 violations. It also checks
`SCAN` against `GET` and runs the offline verifier on the directory after each kill.

**34. (H) How do you know the test can fail?**
A mutation check: reintroducing bug #2 in a throwaway binary made the campaign fail in iteration 2.
Aiming kills matters too: random kills mostly land in idle moments, so 702 kills targeted flushes, 717
compactions and 411 WAL rotations (374 and 829 actually landed while a flush or compaction was running).

**35. (H) What does `SIGKILL` not test?**
Power loss: the OS keeps the page cache. That is covered by `vfs.Faulty.Crash()`, which discards
un-synced file data (or keeps a random torn prefix) and undoes un-fsynced creates, renames and removes.
It is a simulation; I did not pull a plug.

**36. (H) How did you test bit rot?**
`tests/fault` flips 500 random single bits across SSTables, MANIFEST and CURRENT. Every flip was
refused at open (181) or caught by a block checksum on read (319), or not on any queried path; there
were zero silently wrong or missing answers. Exhaustive tests flip every byte of a small SSTable and
every WAL byte position.

**37. (M) What do you do about reads of a corrupt block?**
Return an error (the CRC fails), never the wrong data. `nebula verify DIR` checks every checksum and
cross-reference offline; `nebula repair SRC DST` salvages readable data into a fresh database and never
modifies the source.

## Testing methodology

**38. (M) What is a model test and why use one?**
Run random operations against the real engine and a trivially correct in-memory model (my model of Redis
semantics), compare after each step, with seeds so any failure replays. 8 seeds x 3,000 operations over
SET/NX/XX/KEEPTTL, DEL, INCR, EXPIRE, MSET, MGET, SCAN, flush, compaction, restarts and snapshots, for
both compaction styles.

**39. (M) What did you fuzz?**
12 targets, 45 s each, 0 failures: RESP command and value parsers, WAL segment scan, SSTable open and
block iteration, Bloom `MayContain`, MANIFEST decode and replay, glob matching, and three "open an
engine over a garbage WAL / MANIFEST / SSTable" targets. Fuzz inputs mostly assert "no panic, no hang,
errors not garbage".

**40. (H) Why does the engine take a `vfs.FS` instead of calling `os` directly?**
So tests can substitute a faulty implementation: fail the Nth fsync, tear a write, fill the disk, flip
bits, simulate power loss. Without that seam, most of the failure analysis in `docs/FAILURES.md` could
not be tested at all.

**41. (M) Name a test-harness bug that fooled you.**
Several (all in NOTES.md). Example: the crash harness reported TTL violations because the SCAN/GET
comparison raced with expiry; the fix was bracketing the read time window. The lesson is to treat a
failing torture test as "bug in the engine *or* the harness" and prove which before changing the engine.

## Performance and benchmarking

**42. (M) Where does NebulaDB beat Redis and why?**
Read-heavy (95% GET) with durable writes at 32 connections: 41,208 vs 9,410 ops/s (4.4x) against Redis
with `appendfsync always`. Redis executes commands on one thread, so the AOF fsync for the 5% writes
stalls every read behind it; NebulaDB's reads are lock-free on the memtable and immutable files and never
wait on the WAL. With relaxed durability it also led on reads (100,456 vs 64,228 for `every` vs
`everysec`) and mixed work (74,165 vs 58,094).

**43. (M) Where does it lose, and do you know why?**
Large 16 KiB values at 32 connections: 5.6k vs 48.0k ops/s (8.6x slower). Evidence: its own INFO showed
4,384 write stalls, 78.7 s blocked, flusher ~100 MB/s; it is a flush and compaction *throughput* limit
(one flusher, one compactor, 4 MiB memtable). A 64 MiB memtable with 4 immutables gave 13.0k ops/s.
Also non-durable writes (47k vs 81k) and scans (25.7k vs 33.7k). Durable writes at 32 connections: about
20% behind Redis; I could not find the cause and did not claim one.

**44. (H) How did you make the benchmarks fair?**
Redis ran natively (Docker Desktop's port proxy halved its throughput: 41.9k vs 81.5k writes/s), the
disk and fresh directory were the same for all, compression was disabled in Pebble and Badger (a first
run with Snappy gave Pebble a space amplification of 0.65x, meaningless for engine design), durability
modes were paired honestly (`always` vs AOF always, `every` vs everysec, `never` vs none), embedded cells
are medians of 3 runs, and NebulaDB cells are preceded by a settle step because a first pass showed
cell-order effects (a write-heavy cell's compaction backlog depressed the next one). Latency is measured
closed-loop, so saturation tail latency is optimistic; I say so.

**45. (H) Describe a profile-driven optimisation you made.**
A CPU profile of a single-threaded write workload showed ~34% of time in `Reader.Get`/`memtable.findGE`:
a plain `SET` did a read-before-write to evaluate NX/XX/KEEPTTL even when none were used. `DB.Set` now
reads only when the command depends on the old value (the key lock is still taken). Embedded 1-thread
writes went from 94,875 to 147,602 ops/s (+56%) and mixed from 124,603 to 169,956 (+36%); 16-thread writes
were unchanged, which told me another limit dominates there (the WAL leader serialising `write(2)` and
inserts), and I recorded it as not yet explained instead of guessing.

**46. (H) Another finding from profiling?**
Global atomic hit/miss counters in the block cache were the bottleneck of parallel cache reads
(`BenchmarkGetHitParallel` 67.6 -> ~51 ns/op after moving counters into each shard's critical section):
a shared atomic bounces its cache line between all cores on every `Get`.

**47. (VH) Pebble is ~3x faster on non-durable writes. Why, and what would you do?**
Measured: 126k vs 437k writes/s (16 threads), scans 63k vs 203k. The profile showed part of the cost
(syscall per batch, allocations in the skip-list insert, per-write group-commit hand-offs); the rest is
Pebble's batch-oriented, allocation-light internals and a more optimised iterator stack. Next steps would
be batching writes into the WAL buffer without a syscall per batch, arena-allocating memtable nodes, and
avoiding per-entry allocation in the merge iterator. I did not do these because the project's priority was
correctness evidence.

## Design trade-offs, replication and judgement

**48. (H) How does replication work, and what does it guarantee?**
Logical WAL shipping: the engine's `OnCommit` hook gives the leader each committed batch as final
puts/deletes; followers apply it atomically with `DB.Apply` and acknowledge when durable. Catch-up is
from a bounded in-memory log or, if too far behind or a different leader, from a snapshot (the follower
wipes its data first). In sync mode (`-repl-min-acks 1`) an OK means the write is durable on the leader
and a replica: 69,513 acknowledged writes over 6 leader `SIGKILL`s, 0 missing on the replica or after
promotion. In async mode acknowledged writes can be lost when the leader dies. There is no consensus,
no automatic failover and no fencing, so two writable leaders is possible. A write whose ack times out is
applied locally but reported as an error, like a lost reply.

**49. (H) Why not Raft?**
It was optional in the plan, and consensus is only worth shipping with a partition-and-kill test suite
that proves no acknowledged write is lost; I preferred a smaller, fully tested replication feature with
precisely documented guarantees over a large, half-verified one. What I would build next: leader
election with terms, log replication with a persisted commit index, and a Jepsen-style partition test.

**50. (VH) If you had to scale this beyond one node, what breaks first?**
Single flusher and single compactor (large-value throughput), all live SSTables holding an open file
descriptor, one global WAL writer, `FLUSHDB` as a batch of per-key deletes (no range tombstone), a
bounded `SCAN` cursor table, and no sharding. I would add a descriptor cache, parallel and subcompaction,
range tombstones, a log-structured value store for large values (as Badger does), and consistent-hash or
range sharding with a replicated metadata layer.

---

## Resume bullets (every number is from a recorded run; see the linked docs)

* Built **NebulaDB**, a Redis-compatible LSM-tree key-value database from scratch in Go: RESP2 server, write-ahead log with CRC32C and group commit, lock-free skip-list memtable, prefix-compressed SSTables with Bloom filters and a sharded LRU block cache, leveled and size-tiered compaction, MANIFEST-based atomic recovery, and leader/follower replication.
* Proved crash safety by `SIGKILL`ing the real server **3,000 times** under 8 concurrent writers (4,584,446 acknowledged operations, 1,320,440 post-restart key verifications): **0 acknowledged writes lost**, with kills aimed at flushes, compactions and WAL rotations (374 and 829 landed mid-flush and mid-compaction).
* Built a fault-injecting filesystem (failed `fsync`, torn writes, disk-full, bit flips, simulated power loss) plus 12 fuzz targets; **500 random bit flips produced 0 silently wrong answers**. The tests found 6 real bugs, including an acknowledged-write loss caused by rotating the log inside a group commit, each documented with cause, fix and regression test.
* Implemented group commit that raised durable write throughput about **10x** at 32 connections (405 to 3,948 ops/s) and beat Redis (`appendfsync always`) by **4.4x** (41,208 vs 9,410 ops/s) on a read-heavy workload with durable writes; benchmarked honestly against Redis, Pebble and BadgerDB, including where it loses (8.6x slower on 16 KiB values, with the stall counters that explain why).
* Used CPU profiling to remove a read-before-write from the `SET` path (+56% single-thread write throughput, 94,875 to 147,602 ops/s) and moved block-cache counters off a contended atomic (67.6 to ~51 ns/op).
* Added synchronous replication whose acknowledgements mean "durable on a replica": **69,513 acknowledged writes verified across 6 leader `SIGKILL`s with 0 missing** after promotion.
* Built a live Inspector (React, TypeScript, Vite) that renders flushes, compactions, backpressure and a one-button `kill -9` with recovery steps reported by the restarted server; packaged as a 16.2 MB Docker image with Compose and cross-platform release scripts.
