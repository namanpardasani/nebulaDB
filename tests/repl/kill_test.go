// Replication kill tests with REAL processes: the leader and follower are the
// actual nebuladb binary, killed with SIGKILL.
//
//	go test ./tests/repl -count=1 -v
//
// Guarantees tested (see NOTES.md "Replication"):
//   - sync mode (-repl-min-acks 1): every write the client saw acknowledged
//     is on the follower, also after the leader is SIGKILLed, and after the
//     follower is promoted (restarted without -replicaof).
//   - async mode: acknowledged writes MAY be missing on the follower after a
//     leader crash; the test only reports how many (it asserts nothing).
//   - a follower killed with SIGKILL catches up and converges.
package repl_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/resp"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "nebula-repl-bin")
	if err != nil {
		panic(err)
	}
	binPath = filepath.Join(dir, "nebuladb")
	out, err := exec.Command("go", "build", "-o", binPath, "github.com/naman/nebuladb/cmd/nebuladb").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.Exit(2)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	var ls []net.Listener
	var out []string
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
		out = append(out, l.Addr().String())
	}
	for _, l := range ls {
		l.Close()
	}
	return out
}

type proc struct {
	cmd  *exec.Cmd
	addr string
	done chan struct{}
}

func start(t *testing.T, dir, addr string, extra ...string) *proc {
	t.Helper()
	logf, err := os.OpenFile(dir+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"-dir", dir, "-addr", addr, "-admin-addr", "", "-fsync", "always", "-memtable-size", "65536", "-log-level", "info"}, extra...)
	cmd := exec.Command(binPath, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, addr: addr, done: make(chan struct{})}
	go func() { cmd.Wait(); logf.Close(); close(p.done) }()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := dial(addr); err == nil {
			v, err := c.do("PING")
			c.close()
			if err == nil && v.Kind == resp.KindSimple {
				t.Cleanup(func() { p.kill() })
				return p
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server %s did not start; log: %s.log", addr, dir)
	return nil
}

func (p *proc) kill() {
	p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.done
}

type client struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func dial(addr string) (*client, error) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	return &client{c, resp.NewReader(c), resp.NewWriter(c)}, nil
}
func (c *client) close() { c.c.Close() }
func (c *client) do(args ...string) (resp.Value, error) {
	c.c.SetDeadline(time.Now().Add(10 * time.Second))
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	c.w.WriteCommand(bs...)
	if err := c.w.Flush(); err != nil {
		return resp.Value{}, err
	}
	return c.r.ReadValue()
}

// get returns (value, found).
func get(t *testing.T, addr, key string) (string, bool) {
	t.Helper()
	c, err := dial(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()
	v, err := c.do("GET", key)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if v.Nil {
		return "", false
	}
	return string(v.Str), true
}

// writers runs n concurrent clients doing SET w<i>:<seq> until stop is closed
// or the connection fails; it returns, per writer, the highest acknowledged seq
// (acknowledged = the client saw +OK; keys are written in order 1,2,3...).
func writers(addr string, n int, stop <-chan struct{}) []int {
	acked := make([]int, n)
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := dial(addr)
			if err != nil {
				return
			}
			defer c.close()
			for i := 1; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				v, err := c.do("SET", fmt.Sprintf("w%d:%d", w, i), strconv.Itoa(i))
				if err != nil || v.Kind != resp.KindSimple || string(v.Str) != "OK" {
					return // the reply is lost or an error: NOT acknowledged
				}
				acked[w] = i
			}
		}(w)
	}
	wg.Wait()
	return acked
}

func waitFollowing(t *testing.T, leaderAddr, followerAddr string) {
	t.Helper()
	c, err := dial(leaderAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		k, v := fmt.Sprintf("marker:%d", i), "1"
		c.do("SET", k, v) // async: may or may not be acked on a follower yet
		if got, ok := get(t, followerAddr, k); ok && got == v {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("follower never started following")
}

func verify(t *testing.T, label, addr string, acked []int) (checked, missing int) {
	t.Helper()
	for w, hi := range acked {
		for i := 1; i <= hi; i++ {
			checked++
			if v, ok := get(t, addr, fmt.Sprintf("w%d:%d", w, i)); !ok || v != strconv.Itoa(i) {
				missing++
			}
		}
	}
	return
}

func TestSyncReplicationLeaderKillLosesNothingAcknowledged(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	totalChecked := 0
	for round := 0; round < 6; round++ {
		root := t.TempDir()
		a := freeAddrs(t, 3)
		leader := start(t, filepath.Join(root, "leader"), a[0], "-repl-listen", a[1], "-repl-min-acks", "1", "-repl-ack-timeout", "3s")
		follower := start(t, filepath.Join(root, "follower"), a[2], "-replicaof", a[1])
		waitFollowing(t, a[0], a[2])

		stop := make(chan struct{})
		go func() { time.Sleep(time.Duration(500+round*300) * time.Millisecond); leader.kill(); close(stop) }() // SIGKILL mid-write
		acked := writers(a[0], 4, stop)

		// 1. the follower, still running as a replica, has every acknowledged write
		checked, missing := verify(t, "replica", a[2], acked)
		if missing != 0 {
			t.Fatalf("round %d: %d of %d acknowledged writes are missing on the replica after the leader was SIGKILLed", round, missing, checked)
		}
		// 2. promote it: restart WITHOUT -replicaof from the same directory (SIGKILL: no clean shutdown either)
		follower.kill()
		promoted := start(t, filepath.Join(root, "follower"), a[2])
		if _, missing := verify(t, "promoted", a[2], acked); missing != 0 {
			t.Fatalf("round %d: %d acknowledged writes missing after promotion", round, missing)
		}
		c, _ := dial(a[2])
		if v, err := c.do("SET", "after:promotion", "1"); err != nil || string(v.Str) != "OK" {
			t.Fatalf("promoted node does not accept writes: %v %v", v, err)
		}
		c.close()
		promoted.kill()
		totalChecked += checked
		t.Logf("round %d: %d acknowledged writes, 0 missing on the replica and after promotion", round, checked)
	}
	t.Logf("TOTAL: %d acknowledged writes verified across 6 leader SIGKILLs", totalChecked)
}

func TestAsyncReplicationLeaderKillReport(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	totalChecked, totalMissing := 0, 0
	for round := 0; round < 4; round++ {
		root := t.TempDir()
		a := freeAddrs(t, 3)
		leader := start(t, filepath.Join(root, "leader"), a[0], "-repl-listen", a[1])
		start(t, filepath.Join(root, "follower"), a[2], "-replicaof", a[1])
		waitFollowing(t, a[0], a[2])
		stop := make(chan struct{})
		go func() { time.Sleep(time.Duration(500+round*300) * time.Millisecond); leader.kill(); close(stop) }()
		acked := writers(a[0], 4, stop)
		time.Sleep(300 * time.Millisecond) // the follower drains whatever was already in flight
		checked, missing := verify(t, "replica", a[2], acked)
		totalChecked += checked
		totalMissing += missing
		t.Logf("async round %d: %d acknowledged writes, %d missing on the replica", round, checked, missing)
	}
	t.Logf("ASYNC TOTAL: %d acknowledged, %d missing on the replica after the leader was SIGKILLed (allowed by the async contract)", totalChecked, totalMissing)
}

func TestReplicaRejectsWritesWithREADONLY(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	root := t.TempDir()
	a := freeAddrs(t, 3)
	start(t, filepath.Join(root, "leader"), a[0], "-repl-listen", a[1])
	start(t, filepath.Join(root, "follower"), a[2], "-replicaof", a[1])
	waitFollowing(t, a[0], a[2])
	c, err := dial(a[2])
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	for _, cmd := range [][]string{{"SET", "k", "v"}, {"DEL", "k"}, {"INCR", "n"}, {"MSET", "a", "b"}, {"EXPIRE", "k", "10"}} {
		v, err := c.do(cmd...)
		if err != nil || v.Kind != resp.KindError || len(v.Str) < 8 || string(v.Str[:8]) != "READONLY" {
			t.Fatalf("%v on a replica: want -READONLY, got %v %v", cmd, v, err)
		}
	}
	if v, err := c.do("PING"); err != nil || string(v.Str) != "PONG" {
		t.Fatalf("reads must still work: %v %v", v, err)
	}
}

func TestFollowerKillCatchesUpAndConverges(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	root := t.TempDir()
	a := freeAddrs(t, 3)
	start(t, filepath.Join(root, "leader"), a[0], "-repl-listen", a[1])
	fdir := filepath.Join(root, "follower")
	f := start(t, fdir, a[2], "-replicaof", a[1])
	waitFollowing(t, a[0], a[2])

	stop := make(chan struct{})
	res := make(chan []int, 1)
	go func() { res <- writers(a[0], 4, stop) }()
	for i := 0; i < 3; i++ { // SIGKILL the follower three times while the leader keeps taking writes
		time.Sleep(400 * time.Millisecond)
		f.kill()
		time.Sleep(200 * time.Millisecond)
		f = start(t, fdir, a[2], "-replicaof", a[1])
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	acked := <-res

	deadline := time.Now().Add(30 * time.Second)
	var checked, missing int
	for time.Now().Before(deadline) {
		checked, missing = verify(t, "replica", a[2], acked)
		if missing == 0 {
			t.Logf("follower converged after 3 SIGKILLs: %d writes present", checked)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("follower did not converge: %d of %d writes missing", missing, checked)
}
