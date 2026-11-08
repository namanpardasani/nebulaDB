# NebulaDB Notes

## Architecture

## On-disk formats

## Guarantees

## Decisions and tradeoffs
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
  KNOWN ISSUE: `go test -race` cannot run on this dev machine: no gcc (cgo required) and no passwordless sudo. Race runs are therefore NOT yet verified locally (CI runs them). Need either `sudo apt install gcc` or permission to use a user-space compiler (zig cc, checksum-verified tarball already unpacked at ~/.local/zig). Awaiting user decision.
  Next: Phase 2 (RESP2 + server + in-memory engine).
