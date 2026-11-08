# What happens when things fail

This document walks through every step of the write, flush and compaction paths
and says what a crash (or an I/O error) at that exact point leaves behind and how
recovery handles it. Each claim names the test that exercises it.

Two kinds of "crash" matter and are tested differently:

| | process crash (`kill -9`, panic, OOM kill) | power loss / kernel crash |
|---|---|---|
| memory | lost | lost |
| data already `write()`n into the OS page cache | **survives** | lost unless fsynced |
| tested by | `tests/crash` (real binary, SIGKILL) | `vfs.Faulty.Crash()` in `internal/*` and `tests/fault` |

The rule behind every recovery decision: *a write may be acknowledged to a client
only after the bytes that make it recoverable are on stable storage (fsync
policy `always`), and every file the database creates becomes visible to recovery
through exactly one atomic step.*

---

## 1. The write path (`SET k v`)

```
 S1  take key-stripe lock; read current value (for NX/XX/INCR)
 S2  wal.Commit: assign sequence number, queue for the group leader
 S3  leader: one write() of every queued record into the WAL segment
 S4  leader: fsync the WAL segment            (policy "always")
 S5  leader: apply batch to the memtable      (skip list insert)
 S6  leader: publish `visible` sequence number
 S7  leader: if the memtable is full, rotate (see section 2)
 S8  reply +OK to the client
```

| crash / failure at | on disk afterwards | after restart | client saw |
|---|---|---|---|
| before S3 | nothing | write absent | no reply (connection died) |
| during S3 (partial write) | torn record at the end of the newest WAL segment | startup finds a bad final record with **no valid record after it** -> truncates it, logs `torn_tail=true` | no reply |
| after S3, before S4 completes | full record in page cache only | process crash: record **present** (page cache survived). Power loss: record absent or torn -> truncated | no reply: either outcome is legal |
| after S4 (fsync done), before S8 | full durable record | **present** | no reply, but the write is durable. A client must treat a lost reply as "unknown" |
| after S8 | durable | present | `+OK` -> **guaranteed present** |

Tests: `internal/wal` `TestTornTailEveryOffset` (cuts the log at every byte of the
last record), `TestCrashAlwaysLosesNothingAcked`, `internal/engine`
`TestPowerLossDuringFlushLosesNothingAcked`, and `tests/crash` (kills a real
server mid-operation thousands of times and checks that the single in-flight
operation is either fully applied or not at all).

**Group commit and acknowledgement.** Many writers share S3/S4, but each is
acknowledged only after the shared fsync returns. A reader cannot observe a write
before S4 completes (S5/S6 come after it), so a value can never be visible and
then vanish in a crash.

**Multi-key atomicity.** `MSET` is one WAL record (one CRC), so after any crash
all of its keys are present or none are (`tests/crash` writes 3-key `MSET` groups
and asserts they are never half applied; `TestBatchAtomicVisibility` asserts
readers never see half).

### fsync policies

| policy | `kill -9` | power loss |
|---|---|---|
| `always` (default) | acknowledged writes safe | acknowledged writes safe |
| `every` (N ms) | safe | up to N ms of acknowledged writes may be lost; the log is still a clean prefix |
| `never` | safe | an unbounded amount may be lost (log is still a clean prefix; never garbage) |

`kill -9` safety for *all three* policies is verified by `tests/crash`
(`-fsync mix`). The "clean prefix" property for `never` is `TestCrashNeverYieldsPrefix`.

---

## 2. Memtable rotation and flush

```
 R1  (WAL leader, at a group boundary) wal.Rotate(): fsync old segment, create new segment, fsync directory
 R2  swap memtable: the full one becomes immutable, a new one starts
 F1  flush worker: write NNNNNN.sst.tmp from the immutable memtable
 F2  fsync the temp file                      (inside sstable.Writer.Finish)
 F3  rename NNNNNN.sst.tmp -> NNNNNN.sst
 F4  fsync the directory                      (rename is now durable)
 F5  reopen the table and validate its footer/index
 F6  append edit {add L0 file, logNumber = new WAL segment, lastSeq} to the MANIFEST; fsync   <- THE COMMIT POINT
 F7  install the new version in memory and drop the immutable memtable (one lock: readers see the memtable or the table, never neither)
 F8  delete WAL segments older than logNumber; fsync directory
```

| crash at | on disk | recovery |
|---|---|---|
| R1 (new segment created, memtable not swapped) | an empty extra WAL segment | replayed (empty); harmless |
| R2..F4 | `.sst.tmp` or an unreferenced `.sst` | **orphan**: not in the MANIFEST, deleted at startup (`removed orphan file` in the log). All the data is still in the WAL, which is replayed |
| after F6, before F8 | table + MANIFEST edit + old WAL segments | MANIFEST says `logNumber = B`; startup deletes WAL segments `< B` without replaying them (`RemovedStale`), the table supplies their data |
| after F8 | clean | nothing to do |

F6 is the commit point: before it the WAL is the only copy, after it the table is.
The WAL segment is deleted only *after* F6, never before, and the order
`fsync table -> rename -> fsync dir -> MANIFEST fsync` guarantees the MANIFEST
never names a table that is not fully durable.

Tests: `TestFlushFailureStagesAreSafe` (fault at create, write, fsync, rename and
MANIFEST write), `tests/crash` kills during flush (thousands of kills, a
measured share of them landing while a flush was running), `tests/fault`
`TestFsyncFailureByFileClass`.

### Two subtle bugs this section's tests found

* **Rotation inside a group commit (`NOTES.md` bug 2).** Rotating between two
  batches of the same group put a batch whose record was in the *old* segment into
  the *new* memtable; flushing the old memtable then deleted that segment and the
  batch survived only in RAM. Fix: rotate only at group boundaries.
* **Uncertain MANIFEST append (`NOTES.md` bug 5).** If the MANIFEST write
  succeeds but its fsync fails, the edit may nevertheless be durable. Deleting the
  table "because the edit failed" made the next startup refuse to open. Fix: on an
  *uncertain* failure keep the files and let the next startup decide.

---

## 3. Compaction

```
 C1  merge input tables -> new output tables NNNNNN.sst.tmp (drop shadowed versions, droppable tombstones, expired entries)
 C2  fsync each output; rename to NNNNNN.sst
 C3  fsync the directory once for all renames
 C4  append ONE MANIFEST edit: add outputs + delete inputs; fsync        <- THE COMMIT POINT
 C5  install the new version in memory; inputs become *obsolete*
 C6  inputs are closed and deleted when the last version/iterator/snapshot using them is released
```

| crash at | recovery |
|---|---|
| C1-C3 | outputs are orphans, deleted at startup; inputs still live and complete |
| C4 (record torn) | MANIFEST tail truncated; same as above |
| after C4, before C6 | inputs are orphans (not in the MANIFEST), deleted at startup; outputs are live |

The single edit is what makes "outputs appear and inputs disappear" atomic. A
tombstone (or an expired value) is dropped only if no table outside the
compaction could still hold an older version of the key (`TestTombstoneDroppedOnlyWhenSafe`):
dropping it otherwise would resurrect the old value after the next read.

Snapshots and iterators pin a *version*; a table replaced by compaction stays on
disk until they are closed (`TestSnapshotPinsFilesAcrossCompaction`: 5 files
while pinned, 1 after release).

Tests: `TestCompactionFailureStages`, `TestPowerLossDuringCompaction`,
`tests/crash` (kills while a compaction is running), `TestLeveledCompactionModel`.

---

## 4. MANIFEST and CURRENT

* Each edit is one CRC'd record; a torn tail is truncated, mid-file damage
  (a bad record with a valid one after it) is refused with a clear error and
  `nebula repair` as the way out (`TestMidFileCorruptionRefused`).
* The log is rewritten as a snapshot into `MANIFEST-N+1`, fsynced, then `CURRENT`
  is replaced with *temp file -> fsync -> rename -> directory fsync*. A crash at
  any point leaves either the old or the new manifest complete. If the `CURRENT`
  swap itself fails ambiguously the manifest poisons itself rather than append
  to a file `CURRENT` may no longer name (`TestFailedCurrentSwapPoisons`).
* `CURRENT` missing while `.sst` files exist => refuse to start
  (`ErrMissingCurrent`): starting empty would silently orphan the data.
* A live table named by the MANIFEST that is missing or fails its footer check
  => refuse to start, naming the file.

---

## 5. Disk full (ENOSPC)

| where | behaviour | test |
|---|---|---|
| WAL append | the partial record is truncated away, the write returns `ENOSPC` (never acknowledged), the WAL stays usable; if the truncate fails too the WAL poisons itself (read-only) | `TestDiskFullIsRecoverable`, `TestShortWriteAtEveryLength` (every partial length 0..70) |
| flush / compaction output | the temp file is removed, the error is counted (`flush_errors`/`compaction_errors`) and retried every 500 ms; memory fills up to `MaxImmutables`, then **writers stall** and fail with `write stalled` after `StallTimeout` instead of growing memory | `TestWriteStallAndRecovery`, `tests/fault` `TestDiskFullSweep` (27 different disk sizes) |
| MANIFEST append | rolled back by truncate; transient, flush retries | `TestAppendFailureRollbackAndFsyncPoison` |
| space freed | everything resumes by itself, no restart needed; acknowledged writes survive a restart | `TestDiskFullSweep` |

## 6. fsync failure

After a failed `fsync` Linux may drop the dirty pages and mark them clean, so a
retry "succeeds" without the data being on disk (the "fsyncgate" problem).
Retrying is therefore not safe, and NebulaDB does not retry:

* **WAL fsync fails:** the write is **not** acknowledged, and the WAL becomes
  read-only for the life of the process (`wal_failed` in `INFO`, `nebuladb_wal_failed 1`
  in Prometheus). Reads keep working. A restart re-establishes the truth from what is
  really on disk (`TestFsyncFailureByFileClass`).
* **SSTable / MANIFEST fsync fails:** flush or compaction fails and retries (new
  file number); a MANIFEST fsync failure additionally poisons the manifest until
  restart, and, because the edit may have reached the page cache, the output
  files are kept (bug 5).
* **Directory fsync fails:** the operation fails; nothing is considered durable.

## 7. Corruption (bit rot, bad sectors, stray writes)

| damage | detected by | outcome |
|---|---|---|
| a data block of an SSTable | per-block CRC32C | reads of keys in that block return an **error** (never wrong data); `nebula verify` names the file and block; `nebula repair` salvages the rest |
| SSTable footer / index / filter / properties | footer CRC, per-block CRC, magic number | table refused at open => startup refuses and names the file |
| MANIFEST record | per-record CRC | torn tail truncated; damage followed by valid records refused |
| WAL mid-file | per-record CRC + "valid record follows" rule | startup refuses (`CorruptionError`) |
| WAL final record | CRC | indistinguishable from a torn write, so it is truncated and reported. This is a documented ambiguity (the same one LevelDB has) |
| anything | `tests/fault` `TestBitFlipsNeverCauseSilentCorruption`: 500 random single-bit flips in SSTables/MANIFEST: every one either refused at open, caught on read, or not on any queried path; **zero** silent wrong or missing answers (counts in `NOTES.md`) | |

`nebula repair SRC DST` never modifies `SRC`. It salvages whatever is readable
into a fresh database; with a readable MANIFEST it respects it (so deleted keys
stay deleted), without one it takes every table and deleted keys may reappear.

## 8. Slow disk

* **Group commit** turns N concurrent writers into one fsync; a slow fsync makes
  groups larger, not latency worse per writer beyond one fsync
  (`TestGroupCommitBatchesFsyncs`: 480 commits needed 30 fsyncs on a simulated
  2 ms disk, 480 without group commit).
* **Flush falls behind:** after `MaxImmutables` sealed memtables, writers wait (a
  *stall*) rather than allocate without bound.
* **Compaction falls behind:** L0 files accumulate; at the slowdown trigger every
  write is delayed 1 ms (smoothing), at the stop trigger writers block, both
  bounded by `StallTimeout`, and all of it is visible as
  `write_slowdowns`, `write_stalls`, `write_stall_seconds` (`TestL0BackpressureSlowdownStopRecovery`).
* **Throttling** (`-compaction-rate`) caps compaction bandwidth so background
  merges do not starve foreground I/O (`TestCompactionRateLimit`).
* `fsync=every` bounds loss to the interval and removes the fsync from the
  request path; `fsync=never` removes it entirely (unsafe).

## 9. What is *not* protected against

* Data lost at the storage layer *after* a successful fsync (a disk that lies
  about flushing, firmware bugs): only checksums on read can notice, and only for
  data that is read or verified.
* A write acknowledged under `fsync=every|never` followed by power loss.
* Two processes opening one directory on a filesystem without `flock` support
  (the lock file prevents it on local Linux/macOS filesystems; there is none on Windows).
* A lost *reply*: if the connection dies after the server applied a write, the
  client cannot tell. Use idempotent commands or read back.
