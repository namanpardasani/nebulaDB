# Crash test report: PASS

- Machine: linux/amd64, 8 CPUs
- Seed: 20261108   Writers: 8   Run window per iteration: 60ms..450ms
- Iterations completed: 3000 (wall time 16m36s)
- Operations sent: 4606774, acknowledged: 4584446
- Keys checked after restarts: 1320440 key-verifications
- In-flight operation outcomes at kill time: 946 turned out durable, 21382 were not applied (both legal)
- Kills while a flush was running: 374; while a compaction was running: 829
- Flushes / compactions completed (observed via event stream): 68334 / 69203
- Restarts that found and truncated a torn WAL tail: 39
- Graceful (SIGTERM) stops mixed in: 130
- `nebula verify`-equivalent warnings on post-kill files (torn tails, orphans): 2586, problems: 0
- Largest data directory: 9500 KiB
- Restart-to-ready time (recovery included): mean 40 ms, p50 39 ms, p99 85 ms, max 185 ms
- Kill schedule: map[compaction:717 flush:702 graceful:130 random:1040 rotate:411]
- fsync policies: map[always:1848 every:581 never:571]
- compaction styles: map[leveled:1475 size-tiered:1525]
