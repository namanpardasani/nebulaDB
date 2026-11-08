// Command crash is NebulaDB's kill -9 torture test.
//
// It starts the real server binary against a persistent data directory, runs
// concurrent writers that record exactly which operations were ACKNOWLEDGED,
// SIGKILLs the server at a random moment (or, deliberately, while it is flushing,
// compacting or rotating, detected from the live event stream), restarts it, and
// verifies against the model of acknowledged operations:
//
//   - every acknowledged write is present with exactly the right bytes (values
//     carry a checksum and are regenerated deterministically);
//   - the single operation that was in flight is either fully applied or not at
//     all (MSET groups of three keys are never half applied);
//   - acknowledged INCRs are neither lost nor duplicated;
//   - deleted keys stay deleted; nothing unwritten appears (SCAN cross-check);
//   - TTLs survive restarts and expired keys do not come back;
//   - the files left on disk after the kill pass `nebula verify` (torn WAL tail
//     tolerated), and read-your-writes holds while flush/compaction run.
//
// Note what this does and does not prove. SIGKILL destroys the process but not
// the OS page cache, so it tests crash consistency of the engine's own logic
// under all three fsync policies; it cannot test losing data that was never
// fsynced. That (power loss) is covered by the fault-injecting filesystem tests.
//
// Usage: make crash   (or: go run ./tests/crash -bin bin/nebuladb -iters 2000)
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func main() {
	var cfg Config
	var report, keep string
	flag.StringVar(&cfg.Bin, "bin", "bin/nebuladb", "path to the nebuladb binary")
	flag.StringVar(&cfg.Dir, "dir", "", "data directory (default: a fresh temp dir)")
	flag.IntVar(&cfg.Iterations, "iters", 200, "number of kill/restart iterations")
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano()&0xffffffff, "random seed (printed; reuse to replay the schedule)")
	flag.IntVar(&cfg.Writers, "writers", 8, "concurrent writers")
	flag.DurationVar(&cfg.MinRun, "min-run", 60*time.Millisecond, "minimum time writers run before the kill")
	flag.DurationVar(&cfg.MaxRun, "max-run", 450*time.Millisecond, "maximum time writers run before the kill")
	flag.StringVar(&cfg.Fsync, "fsync", "mix", "fsync policy: always | every | never | mix")
	flag.StringVar(&cfg.Style, "style", "mix", "compaction style: leveled | size-tiered | mix")
	flag.BoolVar(&cfg.Verbose, "v", false, "log every iteration")
	flag.StringVar(&report, "report", "", "write a markdown report to this file")
	flag.StringVar(&keep, "keep", "", "keep the data directory on failure (default: always kept when -dir is given)")
	flag.Parse()
	cfg.Log = os.Stderr
	if cfg.MaxRun < cfg.MinRun {
		cfg.MaxRun = cfg.MinRun
	}
	if _, err := os.Stat(cfg.Bin); err != nil {
		fmt.Fprintf(os.Stderr, "server binary %q not found (run `make build` first): %v\n", cfg.Bin, err)
		os.Exit(2)
	}
	tmp := cfg.Dir == ""
	if tmp {
		d, err := os.MkdirTemp("", "nebula-crash-*")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.Dir = filepath.Join(d, "db")
	}
	abs, _ := filepath.Abs(cfg.Bin)
	cfg.Bin = abs
	fmt.Fprintf(os.Stderr, "crash test: %d iterations, %d writers, seed=%d, fsync=%s, style=%s, dir=%s\n", cfg.Iterations, cfg.Writers, cfg.Seed, cfg.Fsync, cfg.Style, cfg.Dir)

	rep, err := Run(cfg)
	text := render(cfg, rep, err)
	fmt.Println(text)
	if report != "" {
		os.WriteFile(report, []byte(text), 0o644)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nFAILED. Data directory kept at %s (server log: %s.server.log)\n", cfg.Dir, cfg.Dir)
		os.Exit(1)
	}
	if tmp {
		os.RemoveAll(filepath.Dir(cfg.Dir))
	}
}

func render(cfg Config, r *Report, runErr error) string {
	var b strings.Builder
	status := "PASS"
	if runErr != nil || len(r.Violations) > 0 {
		status = "FAIL"
	}
	fmt.Fprintf(&b, "# Crash test report: %s\n\n", status)
	fmt.Fprintf(&b, "- Machine: %s/%s, %d CPUs\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Fprintf(&b, "- Seed: %d   Writers: %d   Run window per iteration: %v..%v\n", r.Seed, cfg.Writers, cfg.MinRun, cfg.MaxRun)
	fmt.Fprintf(&b, "- Iterations completed: %d (wall time %v)\n", r.Iterations, r.Duration.Round(time.Second))
	fmt.Fprintf(&b, "- Operations sent: %d, acknowledged: %d\n", r.OpsAttempted, r.OpsAcked)
	fmt.Fprintf(&b, "- Keys checked after restarts: %d key-verifications\n", r.KeysVerified)
	fmt.Fprintf(&b, "- In-flight operation outcomes at kill time: %d turned out durable, %d were not applied (both legal)\n", r.PendingApplied, r.PendingDiscarded)
	fmt.Fprintf(&b, "- Kills while a flush was running: %d; while a compaction was running: %d\n", r.KilledDuringFlush, r.KilledDuringCompaction)
	fmt.Fprintf(&b, "- Flushes / compactions completed (observed via event stream): %d / %d\n", r.FlushesSeen, r.CompactionsSeen)
	fmt.Fprintf(&b, "- Restarts that found and truncated a torn WAL tail: %d\n", r.TornTails)
	fmt.Fprintf(&b, "- Graceful (SIGTERM) stops mixed in: %d\n", r.GracefulStops)
	fmt.Fprintf(&b, "- `nebula verify`-equivalent warnings on post-kill files (torn tails, orphans): %d, problems: 0\n", r.OnDiskVerifyWarn)
	fmt.Fprintf(&b, "- Largest data directory: %d KiB\n", r.LargestDBBytes/1024)
	if n := len(r.StartupMs); n > 0 {
		s := append([]float64(nil), r.StartupMs...)
		sort.Float64s(s)
		var sum float64
		for _, v := range s {
			sum += v
		}
		fmt.Fprintf(&b, "- Restart-to-ready time (recovery included): mean %.0f ms, p50 %.0f ms, p99 %.0f ms, max %.0f ms\n", sum/float64(n), s[n/2], s[(n*99)/100], s[n-1])
	}
	fmt.Fprintf(&b, "- Kill schedule: %v\n- fsync policies: %v\n- compaction styles: %v\n", r.KillMode, r.Policy, r.Style)
	if len(r.Violations) > 0 {
		fmt.Fprintf(&b, "\n## Violations (%d)\n\n", len(r.Violations))
		for i, v := range r.Violations {
			if i >= 20 {
				fmt.Fprintf(&b, "... %d more\n", len(r.Violations)-i)
				break
			}
			fmt.Fprintf(&b, "- %s\n", v)
		}
	}
	if runErr != nil {
		fmt.Fprintf(&b, "\nError: %v\n", runErr)
	}
	return b.String()
}
