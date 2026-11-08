package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// A short run of the real harness (build the server, kill it a few times) so
// `go test ./...` keeps the harness itself working. The long campaign is `make crash`.
func TestHarnessSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	bin := filepath.Join(t.TempDir(), "nebuladb")
	build := exec.Command("go", "build", "-o", bin, "github.com/naman/nebuladb/cmd/nebuladb")
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	rep, err := Run(Config{
		Bin: bin, Dir: filepath.Join(t.TempDir(), "db"), Iterations: 6, Seed: 42, Writers: 4,
		MinRun: 40 * time.Millisecond, MaxRun: 150 * time.Millisecond, Fsync: "mix", Style: "mix", Log: os.Stderr,
	})
	if err != nil {
		t.Fatalf("%v\nviolations: %v", err, rep.Violations)
	}
	if rep.OpsAcked == 0 || rep.KeysVerified == 0 {
		t.Fatalf("harness did no work: %+v", rep)
	}
	t.Logf("smoke: %d ops acked, %d keys verified, %d iterations", rep.OpsAcked, rep.KeysVerified, rep.Iterations)
}
