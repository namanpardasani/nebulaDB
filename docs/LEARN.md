# A guided tour of NebulaDB's source

This is a reading order, not a reference. Each stop says *what the idea is in plain words*,
*where it lives* (file and function, so you can open it), and *what to look for*. The code itself is
heavily commented; the comments explain the "why" and are often the best text. Numbers quoted here
are from real runs recorded in [NOTES.md](../NOTES.md) and [BENCHMARKS.md](BENCHMARKS.md) (one
i7-1165G7 laptop under WSL2).

**Suggested pace:** stops 1 to 6 (one weekend) give you a working mental model of a write and a
read. Stops 7 to 12 are the storage format and compaction. Stops 13 to 17 are why you can trust it.

```
write:  client -> RESP parser -> engine -> [key lock] -> WAL (fsync) -> memtable      (stops 1-5)
flush:  memtable -> SSTable file -> MANIFEST edit -> delete old WAL                   (stops 7-9, 12)
read:   memtable -> sealed memtables -> SSTables (Bloom, index, block, cache)         (stops 6-10)
merge:  compaction rewrites overlapping files into fewer, bigger, sorted ones         (stop 11)
crash:  reopen -> read MANIFEST -> replay WAL after the last flush                    (stop 13)
```

---

## 1. The wire protocol: RESP

**Idea.** Redis clients speak RESP: a command is an array of bulk strings, e.g.
`*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n`. Lengths come first, so a parser never has to guess where
a value ends and values may contain any byte. Pipelining is just sending the next command before
reading the reply.

**Read:** `internal/resp/reader.go` (`ReadCommand`, `readBulkBody`) and `writer.go`.
**Look for:** hard limits (bulk <= 64 MiB, array <= 1M, line <= 64 KiB, nesting <= 32) and
`readBulkBody` reading in 64 KiB chunks, so a client that *claims* a 1 GiB value cannot make the
server allocate 1 GiB. Protocol errors close the connection because framing cannot be re-synchronised.
`FuzzReadCommand` in `resp_test.go` hammers exactly this.

## 2. From bytes to commands: the server

**Idea.** One goroutine per connection reads commands, runs them against the engine, buffers replies
and flushes only when no more input is waiting; that single rule *is* pipelining.

**Read:** `internal/server/server.go` (`handle`, `dispatch`) and `commands.go` (the `commands`
map, `engErr`). **Look for:** the deferred `w.Flush()` in `handle`, the `recover` around each command
(a panic becomes `-ERR internal error`, not a dead server), and graceful shutdown (read deadline in
the past wakes idle readers while in-flight commands finish). `SCAN` is in `cursor.go`: clients want
an integer cursor, an LSM wants a resume *key*, so the server keeps a bounded id-to-key table.

## 3. The engine interface and why every command is atomic inside it

**Idea.** The server never takes locks. `engine.Engine` (`internal/engine/engine.go`) is the seam:
each method (`Set` with NX/XX, `IncrBy`, `MSet`) is individually atomic, so `INCR` really is a
read-modify-write with no lost updates.

**Read:** the `Engine` interface, then `internal/engine/locks.go` and `DB.beginWriteMode` /
`DB.Set` / `DB.IncrBy` in `db.go`. **Look for:** 256 *stripe locks* hashed from the key. A write
holds its key's stripe across "read old value, decide, append to WAL, apply". Writers on different
stripes proceed in parallel, which is what lets them share one `fsync` (next stop). Multi-key
operations take stripes in ascending order (no deadlock). `engine/enginetest` is a conformance suite
that both the in-memory map engine and the LSM engine must pass.

## 4. The write-ahead log: durability

**Idea.** Memory is lost in a crash, so before changing memory we append the change to a file
(*write-ahead*). On restart we replay the file. A record is `len | crc32c | payload`; the checksum
detects a write that was cut short or damaged.

**Why `fsync` matters (page cache).** `write(2)` only copies bytes into the kernel's *page cache*;
they reach the disk later. If the *process* dies (`kill -9`) the kernel still has the bytes, so
nothing is lost. If the *machine* dies, unsynced bytes vanish. `fsync` forces them to the device and
costs milliseconds (2.4 ms p50 on this machine), so an engine that syncs per write is capped near
400 writes/s. That is the number to keep in mind for stops 4 and 5.

**Read:** `internal/wal/record.go` (`appendRecord`, `decodeRecord`) then `wal.go` (`Open`,
`createSegment`, `syncLocked`). Policies are `SyncPolicy`: `always`, `every`, `never`.
**Look for:** `Open` returns recovery info and calls a `replay` callback per batch; segment files are
numbered and only the *newest* can have a torn tail because the old one is fsynced when rotated.

## 5. Group commit: making fsync affordable

**Idea.** If 32 clients each need a durable write, doing 32 `fsync`s serially is 32x slower than
writing all 32 records and syncing once. Whoever arrives first becomes the *leader*, takes everything
queued, does one write and one `fsync`, then wakes the others. Measured: **3,948 vs 405 durable
writes/s at 32 connections (about 10x)**, and p99 latency 18 ms instead of 149 ms.

**Read:** `wal.go`: `Commit` -> `submit` -> `commitGroup` -> `writeGroup`. **Look for:** the leader
runs each request's `apply` callback *in log order* after the sync (so readers never see a write that
is not durable), sequence numbers are assigned by the WAL in log order, and `AfterGroup`/`Barrier`
exist so the engine can rotate the log only at a group boundary (bug #2 in NOTES.md was a lost write
caused by rotating in the middle of a group).

## 6. The memtable: a skip list

**Idea.** Recent writes live in a sorted in-memory structure. A skip list is a linked list with
random "express lane" levels; search is O(log n) without rebalancing. Because nodes are fully built
before being published with one atomic pointer store and never changed afterwards, **readers need no
lock at all**. There is one writer at a time.

**Read:** `internal/memtable/memtable.go` (`Add`, `findGE`, `Get`, `Iterator`), the package comment
first. **Look for:** ordering is `(user key ascending, sequence number descending)`, so the first
entry at or after a key is its *newest* version. Deletes are not removals: they are new entries of
kind `delete` (a *tombstone*), which is the only way a later layer can learn the key is gone.

## 7. Sequence numbers, snapshots and visibility

**Idea.** Every operation gets an increasing sequence number. A read picks a number S and ignores
anything newer. That gives a consistent view without locking readers out, and it makes `MGET` see a
multi-key `MSET` entirely or not at all.

**Read:** `DB.visible` and `DB.commit` in `engine/db.go` (the number is published only after a whole
batch is in the memtable), `internal/iterator/iterator.go` (`View.settle`), `engine/snapshot.go`.
**Look for:** `view.settle` walks versions of a key newest-first, skips those with `seq > snapshot`,
and treats a tombstone **or an expired put** as hiding every older version (otherwise an expired key
would resurrect an older value from a lower level). `Snapshot` pins both a sequence number and the set
of files, so compaction cannot delete a file an open iterator still needs (reference-counted
versions in `engine/tables.go`).

## 8. TTL and tombstones

**Idea.** TTL is an absolute `expireAt` (unix ms) stored with the value; "expired" is decided at read
time (`Entry.Expired`), not by a timer. A deleted or expired key is physically removed only when
compaction proves nothing older can resurface (stop 11).

**Read:** `iterator.Entry`, `DB.SetExpiry`, `DB.Persist`, and `compaction.canDropTombstone`.
`SET ... KEEPTTL` and `INCR` preserve an existing expiry; look at how `IncrBy` carries `ExpireAt`.

## 9. SSTables: the sorted files on disk

**Idea.** When the memtable reaches `-memtable-size` it is sealed (becomes read-only, new WAL
segment) and a background flusher writes it as an immutable *sorted string table*.

```
data blocks | Bloom filter | index block | properties | 64-byte footer (magic "NEBULSST")
```

- **Blocks** (~4 KiB) hold prefix-compressed entries with a *restart point* every 16 entries (full key
  there, shared-prefix deltas between) and a CRC32C. With a 36-byte common prefix, 2,000 keys take
  24,492 bytes with restarts every 16 vs 118,134 with a restart at every entry (measured).
- **Sparse index:** one entry per *block* (its last key and where it is), not per key, so the whole
  index stays in memory and a lookup is "binary search the index, read one block".
- **Footer last:** a file cut short by a crash has no valid footer and is rejected.

**Read:** `internal/sstable/format.go` (layout comment), `writer.go` (`Add`, `Finish`), `block.go`
(`blockBuilder`, `blockIter.seek`), `reader.go` (`Get`, `findBlock`, `Verify`). Then
`DB.flushOne` and `DB.writeTable` in `engine/db.go`.

## 10. Bloom filters and the block cache: avoiding disk reads

**Bloom filter math.** A point read may have to check every L0 file plus one file per deeper level.
Most do not contain the key. A Bloom filter is a bit array plus `k` hash probes that answers "definitely
not here" (never wrong) or "maybe" (wrong with probability `p`). With `b` bits per key and the best
`k = b * ln 2`: `p ~ (1 - e^(-k/b))^k`. At 10 bits per key `k = 7` and `p ~ 0.82%`; every extra ~4.8 bits
divides `p` by ten. Measured here (20k keys, 200k absent probes): 6 bits 5.67% (theory 5.61), 10 bits
0.756% (0.819), 16 bits 0.040% (0.046). In a 3,000-key engine test, 1,968 of 2,141 table checks were
answered "no" without any I/O. **Read:** `internal/bloom/bloom.go` (`Hash`, `NumProbes`, `Build`,
`MayContain`; probes use double hashing so one 64-bit hash makes all `k`). The hash is fixed, not
randomly seeded, because filters are *stored in files*.

**Block cache (LRU).** Recently used data blocks are kept in memory, keyed by `(file number, offset)`,
evicting the least recently used when the byte budget is exceeded. **Read:** `internal/cache/cache.go`
(16 shards, each a mutex + map + doubly linked list). Cached blocks are immutable, so readers need no
pinning. A strict LRU `Get` *writes* (moves the node to the front), so each shard's mutex serialises
its readers: that is the trade-off recorded in NOTES.md. Hot-key repeat reads measured 98% hits.

## 11. Merge iterators and compaction

**Idea.** Scans and compactions must walk *many sorted sources at once* (memtables and files) and
produce one sorted stream. `iterator.Merge` is a heap of child iterators; `iterator.View` on top
removes shadowed versions, tombstones and expired keys. Compaction is the same merge, written back to
new files.

**Read:** `internal/iterator/iterator.go` (`Merge`, `View`), then `internal/engine/compaction.go`:
`pickCompaction` -> `pickLeveled` / `pickTiered`, `runCompaction`, `canDropTombstone`.

- **Leveled:** L0 files may overlap (they are raw flushes); L1 and deeper are sorted, non-overlapping
  and ~10x larger per level. A compaction takes one file and the next-level files it overlaps. Lower
  space use, more rewriting.
- **Size-tiered:** merge files of similar size together. Less rewriting, more space, more overlap.
- **Measured trade-off** (300k keys, 2M random overwrites): leveled write amplification **5.07**, space
  amplification **1.64**, 28 tables; size-tiered **3.92**, **2.74**, 6 tables. Read amplification was ~1
  block lookup per `Get` in both thanks to the Bloom filters.
- **Amplification, defined:** *write* = bytes written to disk / bytes the user wrote; *space* = bytes
  on disk / live data; *read* = blocks or files touched per lookup.
- **What may be dropped:** all versions of a key but the newest (readers needing older ones hold the
  pinned input files), and a tombstone only when no file *outside* the compaction could still hold an
  older version of that key.

## 12. The MANIFEST: changing the file set atomically

**Idea.** Which files are live? Directory listings are not trustworthy after a crash (a half-written
file may exist). The MANIFEST is a log of *edits* (add file X at level L, delete file Y, WAL boundary,
last sequence), each CRC-framed. A flush or compaction becomes real at the instant its single MANIFEST
record is durable. `CURRENT` names the live MANIFEST and is replaced with the classic recipe: write
temp file, `fsync`, `rename`, `fsync` the directory (`vfs.WriteFileAtomic`).

**Read:** `internal/manifest/manifest.go` (`Open`, `LogAndApply`, `replay`, `Rewrite`) and the flush
order in NOTES.md (decision P5): write `.sst.tmp`, fsync, rename, fsync dir, open and validate, *then*
the MANIFEST edit, *then* swap in memory, *then* delete the old WAL. A crash anywhere leaves either the
old state or the new one. Files not named by the MANIFEST are orphans and deleted at startup
(`DB.removeOrphans`). `UncertainError` marks the one case where an append may or may not have reached
disk; the engine then refuses to delete files the MANIFEST might name (bug #5).

## 13. Recovery and torn writes

**Idea.** On start: lock the directory, read `CURRENT` and replay the MANIFEST to get the file set,
delete orphans, then replay WAL segments from the recorded boundary into a fresh memtable.

**Torn tail vs corruption.** A crash can leave the last record half written (a *torn tail*). Truncate
it and carry on. But a bad record *in the middle* is real damage and silently dropping everything
after it would lose acknowledged data. Rule (`wal.ScanSegment`, `findValidRecord`): a bad record is a
torn tail **only if no valid record follows it**; otherwise recovery refuses to start with a
`CorruptionError`. (Known corner, documented: a bit flip in the very last record looks like a torn
write; LevelDB behaves the same.)

**Read:** `engine.Open` in `db.go` (follow it top to bottom: `lockDir`, `manifest.Open`, open tables,
`removeOrphans`, `wal.Open(... replay)`), `wal.Open`, and `docs/FAILURES.md`, which tabulates what a
crash at each step leaves behind and names the test for each row.

## 14. Backpressure

**Idea.** If writes arrive faster than flush and compaction can absorb, memory or the L0 file count
grows without bound and reads slow down. So writers are slowed, then stopped.

**Read:** `DB.waitForRoom` in `engine/db.go` (three regimes in its comment): sealed memtables >=
`-max-immutables` or L0 files >= `-l0-stop` => writers block (fail with `ErrWriteStall` after 30 s);
L0 files >= `-l0-slowdown` => 1 ms delay per write. The Inspector's backpressure panel is this, live,
and its "writers stalled" screenshot is a real flood against small limits. In the benchmarks this is the
mechanism behind the large-value result: 4,384 stalls and 78.7 s of blocked writer time in one 8 s cell.

## 15. Files go through one interface: `vfs`

**Idea.** Every file operation in the engine goes through `internal/vfs.FS`. In production it is the
OS; in tests it is `vfs.Faulty`, which can fail the Nth `fsync`, tear a write in half, fill the disk,
flip bits on read, and simulate **power loss** by dropping everything that was not `fsync`ed
(including un-synced directory entries, which is why the recipe needs the directory fsync).
**Read:** `vfs/vfs.go`, `vfs/faulty.go` (`Crash`, `AddRule`, `SetDiskLimit`, `FlipOnRead`). This is the
single most useful testing idea in the project.

## 16. The testing strategy (why you can trust it)

Read these in order; each answers a different question.

1. **Unit tests per package**: formats, edge cases (every-byte truncation of a WAL, every single-bit
   flip of an SSTable is detected).
2. **Model tests** (`tests/model`): random operation sequences (SET/NX/XX, DEL, INCR, EXPIRE, MSET,
   SCAN, flush, compaction, restart) checked against a trivial in-memory model of Redis, with seeds so
   failures replay.
3. **Fault tests** (`tests/fault`): the filesystem misbehaves on purpose; the engine must either work
   or fail *loudly*, never return wrong data (500 random bit flips: 181 refused at open, 319 caught on
   read, 0 wrong answers).
4. **Fuzzing**: 12 targets over every parser and every "open a directory of garbage" path.
5. **The crash campaign** (`tests/crash`): the *real binary*, `SIGKILL` at random moments and aimed at
   flushes, compactions and rotations, then check every acknowledged write. 3,000 iterations, 0
   violations. The harness records, per key, what was acknowledged and what was merely in flight, so
   "maybe applied" and "definitely applied" are told apart. A mutation check (re-introducing bug #2)
   makes it fail in iteration 2: a test that cannot fail proves nothing.
6. **Replication kill tests** (`tests/repl`): same idea across two processes.
Bugs these found are in NOTES.md ("Bugs found by tests"); reading the six of them is a fast way to
learn where LSM engines go wrong.

## 17. Replication (optional reading)

**Idea.** Ship the log. The engine's `OnCommit` hook hands every committed batch (as final puts and
deletes) to `internal/repl`, which keeps the recent ones in memory and streams them to followers
(`leader.go`). A follower applies each with `DB.Apply` as one atomic batch and acknowledges when it is
durable (`follower.go`). A follower that is too far behind (or a different leader) gets a snapshot
first. With `-repl-min-acks 1` the leader replies OK only after a replica has the write (`engines.go`,
`SyncEngine`); with 0 it does not wait. **Read:** the package comment in `proto.go`, then the
guarantees list in NOTES.md, which says exactly what is and is not promised (no consensus, manual
failover).

## 18. The Inspector (optional reading)

**Idea.** The admin server (`internal/admin`) exposes JSON endpoints and a Server-Sent-Events stream:
engine lifecycle events (flush, compaction, file created/deleted, stall, recovery) with ids so a
reconnecting browser can replay what it missed, plus a 250 ms `tick` of aggregated rates so 100k
writes/s costs about 4 messages/s. The React UI (`inspector/src`) funnels everything through one pure
reducer (`lib/reducer.ts`), batched once per animation frame. The core-sample panel is *diff-driven*:
when `/api/levels` shows a new file it drops in, when one disappears it is pressed flat (see
`StrataPanel.tsx`, `lib/strata.ts`). The design is documented in NOTES.md ("Design system").

---

## Exercises (to check you understood)

1. Start the server with `-memtable-size 65536`, write 1 MB of keys, and watch `ls data/` and the
   Inspector: find the point where a `.wal` segment disappears. Which stop explains why that is safe?
2. `kill -9` the server in the middle of `redis-cli --pipe` writes; restart with
   `-log-level debug`. How many WAL batches were replayed? Is there a "truncated torn tail" message?
3. Run `nebula dump-wal data/` and `nebula dump-sstable data/000007.sst` (pick any file): match the
   output to the formats in stop 4 and stop 9.
4. In `engine/compaction.go`, find where a tombstone is kept. Write a test where it must be kept
   (an older version of the key sits in a lower level) and one where it may be dropped.
5. Set `-fsync never`, write, `kill -9`, restart: the data is there. Now explain, using the page cache,
   why that does *not* mean `never` is safe (see the table in `docs/FAILURES.md`), and how `vfs.Faulty.Crash()` models exactly the difference.
