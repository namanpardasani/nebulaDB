# Benchmarks

Every number in this document comes from a real run on the machine below; the raw
per-run results are in `tests/bench/results/*.jsonl` and the full tables in
[`docs/charts/results-tables.md`](charts/results-tables.md). Charts (SVG, light
and dark) are in [`docs/charts/`](charts/). Reproduce with the commands at the end.

**Read this first: what these numbers are and are not.** They compare design
choices on one laptop-class machine, with a virtualised disk whose fsync is slow.
They are *not* a claim that NebulaDB is faster or slower than Redis, Pebble or
BadgerDB in general. Single network cells are 8 s long, one run each; on this
machine differences below roughly 15% are noise (see "Variance").

## Environment

| | |
|---|---|
| CPU / RAM | 11th Gen Intel i7-1165G7 @ 2.80 GHz, 8 hardware threads, 7.8 GiB RAM |
| OS | Linux 6.18 under WSL2 (Windows 11 host), ext4 on a virtual disk (`/dev/sdd`) |
| Go | 1.27.3 |
| **fsync cost** | a 100-byte append + `fsync`: **p50 2.4 ms, p99 6.7 ms, max 8.6 ms** (n=300). Every `fsync=always` number below is dominated by this. |
| NebulaDB | this repository, default settings unless stated (4 MiB memtable, 32 MiB block cache, 10-bit Bloom filters, leveled compaction) |
| Redis | 8.10.3, the binary shipped in the `redis:8` Docker image, extracted and run **natively** (see below); single-threaded |
| Pebble | `github.com/cockroachdb/pebble/v2` v2.1.7 (released 2026-08-24, maintained) |
| BadgerDB | `github.com/dgraph-io/badger/v4` v4.9.6 (released 2026-08-05, maintained) |

`goleveldb` was rejected as a comparison target: last release 2019.

## Method and the fairness decisions behind it

* **Redis runs natively, not in Docker, for the headline numbers.** This machine
  uses Docker Desktop, which publishes container ports through a proxy. Measured
  on the same workload, `redis-docker-nopersist` reached 4.4k / 41.9k writes/s
  (1 / 32 connections) versus 10.6k / 81.5k for the *same* Redis binary run
  natively: the proxy halves throughput and would have benchmarked Docker, not
  Redis. The Docker row is kept as a labelled data point. (The binary was taken
  from the image with `docker create` / `docker cp`; persistence flags below.)
* **Persistence settings, stated exactly.** Redis: `appendonly yes appendfsync
  always`, `appendonly yes appendfsync everysec`, and `appendonly no` (no
  persistence), all with `save ""`. NebulaDB: `-fsync always` (group commit on),
  `-fsync always -group-commit=false`, `-fsync every -fsync-interval 1s`, `-fsync never`.
  The honest pairings are: *always* ↔ AOF always, *every* ↔ AOF everysec,
  *never* ↔ no persistence.
* **Same disk, fresh directory per target**, 50,000-key space, 100-byte values
  (16 KiB for the "large" workload), uniformly random keys, preloaded and
  unmeasured. 2 s warm-up, 8 s measured. 1 and 32 connections.
* **Closed-loop clients with exact latency.** Every operation is timed and the
  list is sorted (no histogram buckets). Closed-loop means a stalled server also
  stalls the client, so tail latency under saturation is *optimistic* (no
  coordinated-omission correction). The same for every target.
* **Settling between cells (NebulaDB).** A first full pass ran the cells back to
  back and showed `fsync=never` *reading slower* than `fsync=every`: the
  write-heavy cell before it had left a compaction backlog that depressed the next
  cell. That is a real LSM property, but it made results depend on cell order, so
  the harness now waits for flush/compaction to go quiet before each cell and
  reports how long that took. All tables here use the settled run.
* **Embedded comparison:** each engine in its own process, same workloads, 32 MiB
  cache and ~4 MiB memtable for all, **compression disabled in Pebble and Badger**
  (NebulaDB has none; a first run with default Snappy gave Pebble a space
  amplification of 0.65× on my compressible values, which says nothing about
  engine design), Badger's value threshold raised to 512 KiB so the 100-byte values
  stay in its LSM tree. Durable mode = `pebble.Sync`, Badger `SyncWrites`,
  NebulaDB `fsync=always`. Embedded cells are the **median of 3 runs**, with the
  min–max in the full tables.
* **Write amplification** is measured two ways because one is not enough: the
  engine's own accounting (WAL + flush + compaction output ÷ user bytes; NebulaDB
  and Pebble expose it, Badger does not) and bytes written to the block device
  after a forced `sync` ÷ user bytes (the kernel may skip short-lived files, so
  this can undercut the first). For NebulaDB the two agree within 3%, which is the
  evidence the accounting is right. (Per-process `/proc/self/io` reads zero under
  WSL2 and a no-`sync` device reading mostly measures kernel writeback timing;
  the first attempts at both produced impossible results, e.g. a write
  amplification below 1, and were discarded.)

## Headline results: network (RESP over TCP)

Single runs; ops/s (higher is better) and p99 latency (lower is better).
32 connections unless noted. Full table with p50/p99.9: `docs/charts/results-tables.md`.

### Durable writes: `fsync always` vs AOF `always`

| | 1 conn: ops/s | 1 conn: p99 | 32 conns: ops/s | 32 conns: p99 |
|---|---|---|---|---|
| NebulaDB, group commit | 337 | 6.6 ms | 3,948 | 18.1 ms |
| NebulaDB, **no** group commit | 302 | 8.0 ms | **405** | **149 ms** |
| Redis AOF always | 337 | 6.3 ms | 4,797 | 12.4 ms |

* With one connection, every durable write needs its own fsync, so both systems
  are bound by the disk: ≈ 1 / 2.4 ms ≈ 400 ops/s ceiling, and both measure ≈ 337.
* **Group commit is worth ~10×** (3,948 vs 405 ops/s; the first settled run had
  4,814 vs 375, so 10–13×): without it 32 writers queue behind 32 serial fsyncs
  and p99 reaches 149 ms.
* Redis AOF-always batches too (it fsyncs once per event-loop iteration), and is
  ~20% ahead of NebulaDB at 32 connections here, with a lower p99 (12 vs 18 ms).
  I did not find the cause; plausible suspects are my per-batch WAL bookkeeping
  and goroutine hand-offs in the group-commit leader hand-off, which are not
  measured in isolation. I am not claiming a reason I have not measured.

### Read-heavy (95% GET / 5% SET) with durable writes

| | 32 conns: ops/s | p99 |
|---|---|---|
| NebulaDB `fsync=always` | **41,208** | 8.8 ms |
| Redis AOF always | 9,410 | 9.0 ms |

A real architectural difference: Redis executes commands on one thread and the
AOF fsync for the 5% writes blocks that thread, stalling every read behind it.
NebulaDB's reads (lock-free memtable, immutable table files) never wait on the
WAL fsync. 4.4× here.

### Non-durable and relaxed durability

| workload (32 conns) | NebulaDB `every` | NebulaDB `never` | Redis AOF everysec | Redis no persistence |
|---|---|---|---|---|
| write: ops/s | 47,627 | 46,941 | 54,516 | **81,461** |
| write: p99 | 2.2 ms | 2.2 ms | 1.7 ms | 0.9 ms |
| read (95/5): ops/s | 100,456 | **104,060** | 64,228 | 83,393 |
| mixed 50/50: ops/s | 74,165 | 72,390 | 58,094 | 52,354 |
| scan (100 keys/call): ops/s | 23,109 | 25,711 | 31,258 | 33,701 |
| large 16 KiB writes: ops/s | 5,274 | 5,586 | 16,900 | **48,010** |

At 1 connection Redis is ahead everywhere (e.g. read 11.4k vs 7.6k ops/s, write
10.6k vs 5.8k): per-request latency is lower in a C event loop than through a Go
goroutine + channels + syscalls. NebulaDB overtakes it on reads and mixed work at
32 connections because it uses all 8 cores while Redis uses one.

### Where NebulaDB loses clearly, and why

1. **Large values (16 KiB), 32 connections: 5.6k vs 48.0k ops/s (8.6× slower).**
   Evidence from NebulaDB's own `INFO` at the end of the first settled run (before the
   plain-SET optimisation; later runs overwrote that file): 4,384 write
   stalls totalling 78.7 s of blocked writer time and 28k L0 slowdowns, with the
   flusher averaging 38 ms per 4 MiB memtable (~100 MB/s). It is a flush and
   compaction *throughput* limit (one flusher, one compactor, 4 MiB memtable, 2
   immutables) interacting with the backpressure policy, not a format or protocol
   cost. Ablation: `-memtable-size 64MiB -max-immutables 4` gives **13.0k ops/s**
   (2.3×); in the first run of that ablation (11.2k ops/s) `INFO` reported zero
   write stalls. The measured window is only 8 s, so part of the gain is just the
   bigger buffer absorbing the burst; sustained throughput will be lower. Still
   3.7× below Redis.
2. **Scans:** 25.7k vs 33.7k ops/s. Different work: Redis walks a hash table with
   a bucket cursor; NebulaDB merges memtables and every table in key order and
   maps its key-based resume point to an integer cursor.
3. **Non-durable writes at 32 connections:** 47k vs Redis 81k. Redis spends almost
   nothing per command; NebulaDB pays the write-ahead log syscall, key-stripe
   locks, a skip-list insert with several allocations, and group-commit hand-offs
   per write.

## Headline results: embedded (in-process library), medians of 3 runs

100-byte values, 50,000 keys, 16 goroutines unless noted. ops/s; min–max in the
full tables.

| workload | NebulaDB | Pebble v2 | BadgerDB v4 |
|---|---|---|---|
| write, no fsync, 1 thread | 158,135 | **358,366** | 77,862 |
| write, no fsync, 16 threads | 126,480 | **437,050** | 82,550 |
| write, **durable**, 1 thread | 307 | 323 | **524** |
| write, **durable**, 16 threads | **2,379** | 2,473 | 627 |
| read (all hits, memory-resident), 1 thread | **629,422** | 390,044 | 243,269 |
| read, 16 threads | **1,461,317** (0.92–2.22 M) | 885,150 | 316,731 |
| mixed 50/50, 16 threads | 159,228 | **450,987** | 119,870 |
| scan (100 keys), 16 threads | 63,329 | **203,387** | 80,790 |

* **Durable writes: parity.** NebulaDB and Pebble are within noise (2,379 vs 2,473
  at 16 threads): both are bounded by the 2.4 ms fsync and both group-commit.
  Badger is slower at 16 threads (627) because each write is its own transaction
  with its own sync handling, and faster at 1 thread (524 vs ~310); I did not
  investigate why.
* **Reads: NebulaDB is fastest here**, but the data set (5 MB) is entirely in
  memtable/cache, so this measures lookup-path overhead, not disk behaviour. The
  16-thread read cell has a wide range (0.92–2.22 M ops/s across the three runs
  and 1.44–1.96 M across five isolated repeats): treat it as "about 1–2 M".
* **Non-durable writes and scans: Pebble is about 3× faster** (437k vs 126k writes/s;
  203k vs 63k scans/s; mixed 451k vs 159k). A CPU profile of the NebulaDB write path explains part of
  it (next section); the rest is Pebble's batch-oriented, allocation-light
  internals and a far more optimised iterator stack, which I did not replicate.

### One optimisation this produced

The profile of a single-threaded write workload showed **~34% of CPU in
`Reader.Get`/`memtable.findGE`**: a plain `SET` did a read-before-write (to
evaluate NX/XX/KEEPTTL) even when none of those options was used. Fixing it
(`DB.Set` reads only when the command depends on the old value; the key lock is
still taken) changed, with everything else unchanged:

| embedded NebulaDB, no fsync | before | after |
|---|---|---|
| write, 1 thread | 94,875 | 147,602 (+56%) (3-run median 158,135) |
| mixed, 1 thread | 124,603 | 169,956 (+36%) |
| write, 16 threads | 108,988 | 107,807 (unchanged: another limit dominates) |

The remaining cost at 16 threads is not yet explained; the WAL's single group
leader serialises `write(2)` and memtable inserts, and the profile's other big
item is that syscall (~23% of CPU at 1 thread).

## Amplification (300,000 keys, 2,000,000 random overwrites, no per-write fsync)

32 MB of live data. Space = allocated directory size (like `du`) ÷ live bytes.

| engine | write amp, engine-accounted | write amp, device after sync | space amp | read amp (NebulaDB: block lookups / disk reads per Get) |
|---|---|---|---|---|
| NebulaDB **leveled** | 5.07 | 5.22 | 1.64 | 0.99 / 0.475 |
| NebulaDB **size-tiered** | **3.92** | 4.00 | 2.74 | 1.00 / 0.532 |
| Pebble v2 (leveled) | 4.51 | 3.97 | 1.57 | n/a |
| BadgerDB v4 | n/a | 4.36 | 1.86 | n/a |

* **Leveled vs size-tiered, the textbook trade, measured:** tiered writes 23% less
  to disk (3.92 vs 5.07) but occupies 67% more space (2.74 vs 1.64) and ends with
  fewer, larger overlapping files (6 vs 28 tables). Read amplification measured
  as block lookups per Get is ~1 for both because of the Bloom filters; the disk
  reads per Get (0.475 vs 0.532) are a little higher for tiered, since more
  tables overlap each key.
* NebulaDB's leveled write amplification is in the same range as Pebble's
  (5.07 vs 4.51 engine-accounted); there is no evidence of a design defect there.
* Caveat on all three: one run each; the 300k-key workload fits comfortably in
  memory, so the number of levels is small (L0–L2). A larger data set would show
  more levels and higher amplification.

## Variance, and what I would not trust

* Repeated embedded cells differ by 5–30% run to run (e.g. 16-thread writes
  107.8k–149.3k; reads 0.92–2.22 M). The Windows host runs other work, and WSL2
  gives no way to pin cores.
* Network cells are a single 8 s run. Two settled passes of the NebulaDB rows
  exist (before and after the SET optimisation; see
  `tests/bench/results/before-set-optimization/`): e.g. `fsync always` writes at
  32 connections gave 4,814 and 3,948 ops/s. Treat a gap under ~15–20% between
  two systems as a tie.
* The Redis rows are from one pass and were not re-measured after the NebulaDB
  rows were.
* Latency is server-side + loopback TCP on one machine; a real network adds RTT
  for every system equally.

## What these results say about the design

* Durability costs are dominated by fsync, and **group commit** is what makes
  `fsync=always` usable under concurrency (10× here); the relaxed modes remove the
  fsync from the request path and are about 12× faster than `always` at 32 connections
  (47.6k vs 3.9k writes/s).
* An LSM with an LSM-style write path keeps **reads independent of write
  durability**, unlike a single-threaded event loop that fsyncs inline (4.4× on the
  read-heavy durable workload).
* The costs of the design show up exactly where expected: **flush/compaction
  throughput** bounds sustained large-value writes, and **compaction policy** trades
  write amplification against space amplification and file counts.

## Reproduce

```sh
go build -o bin/nebuladb ./cmd/nebuladb
# Redis binary (once): docker create --name rx redis:8 && docker cp rx:/usr/local/bin/redis-server bin/redis/ && docker rm rx
tests/bench/run_network.sh        # ~25 min; network.jsonl, redis-benchmark/all.csv, INFO dumps
tests/bench/run_embedded.sh       # ~20 min per repetition; embedded.jsonl and amp.jsonl
EMB=tests/bench/results/embedded-r2.jsonl AMP=0 tests/bench/run_embedded.sh   # repetitions
(cd tests/bench && go run ./charts -results results -out ../../docs/charts)    # SVG charts + tables
```

The benchmark code is a separate Go module (`tests/bench`) so Pebble and Badger
never become dependencies of NebulaDB itself.
