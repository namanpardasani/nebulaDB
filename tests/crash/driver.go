package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/internal/tools"
)

// Config is the crash-test configuration.
type Config struct {
	Bin        string // path to the nebuladb binary
	Dir        string // data directory (persists across iterations)
	Iterations int
	Seed       int64
	Writers    int
	MinRun     time.Duration // writers run at least this long before the kill
	MaxRun     time.Duration
	Fsync      string // always | every | never | mix
	Style      string // leveled | size-tiered | mix
	Verbose    bool
	Log        io.Writer
}

// Report accumulates results.
type Report struct {
	Iterations             int
	Violations             []string
	OpsAttempted           uint64
	OpsAcked               uint64
	PendingApplied         int // in-flight op turned out to be durable
	PendingDiscarded       int
	KilledDuringFlush      int
	KilledDuringCompaction int
	KillMode               map[string]int
	Policy                 map[string]int
	Style                  map[string]int
	GracefulStops          int
	TornTails              int
	FlushesSeen            int
	CompactionsSeen        int
	StartupMs              []float64
	KeysVerified           uint64
	LargestDBBytes         int64
	OnDiskVerifyWarn       int
	Duration               time.Duration
	Seed                   int64
}

type server struct {
	cmd      *exec.Cmd
	addr     string
	admin    string
	logPath  string
	started  time.Time
	policy   string
	style    string
	ready    time.Duration
	waitDone chan error
}

// freePorts reserves n distinct loopback ports. All listeners are held open
// until every port is chosen: closing each one immediately lets the kernel hand
// the same ephemeral port out twice (this bit the first long campaign).
func freePorts(n int) ([]string, error) {
	var ls []net.Listener
	defer func() {
		for _, l := range ls {
			l.Close()
		}
	}()
	var addrs []string
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		ls = append(ls, l)
		addrs = append(addrs, l.Addr().String())
	}
	return addrs, nil
}

type client struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func dial(addr string) (*client, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &client{c, resp.NewReader(c), resp.NewWriter(c)}, nil
}

func (c *client) do(args ...string) (resp.Value, error) {
	c.c.SetDeadline(time.Now().Add(15 * time.Second))
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

// ---------------------------------------------------------------- run

// Run executes the crash test and returns the report.
func Run(cfg Config) (*Report, error) {
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	logf := func(f string, a ...any) { fmt.Fprintf(cfg.Log, f+"\n", a...) }
	rep := &Report{KillMode: map[string]int{}, Policy: map[string]int{}, Style: map[string]int{}, Seed: cfg.Seed}
	rng := rand.New(rand.NewSource(cfg.Seed))
	models := make([]*writerModel, cfg.Writers)
	for i := range models {
		models[i] = newWriterModel(i)
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return rep, err
	}
	t0 := time.Now()
	defer func() { rep.Duration = time.Since(t0) }()

	for iter := 1; iter <= cfg.Iterations; iter++ {
		rep.Iterations = iter
		policy := cfg.Fsync
		if policy == "mix" {
			policy = []string{"always", "always", "always", "every", "never"}[rng.Intn(5)]
		}
		style := cfg.Style
		if style == "mix" {
			style = []string{"leveled", "size-tiered"}[rng.Intn(2)]
		}
		rep.Policy[policy]++
		rep.Style[style]++

		srv, err := startServer(cfg, policy, style, rng)
		if err != nil {
			return rep, fmt.Errorf("iteration %d: %w", iter, err)
		}
		rep.StartupMs = append(rep.StartupMs, float64(srv.ready.Microseconds())/1000)
		if tail := countTornTails(srv.logPath); tail > 0 {
			rep.TornTails += tail
		}

		// 1. Verify what the previous incarnation left behind.
		vc, err := dial(srv.addr)
		if err != nil {
			srv.kill()
			return rep, err
		}
		viols, applied, discarded, nkeys := verifyAll(vc, models, time.Now())
		rep.PendingApplied += applied
		rep.PendingDiscarded += discarded
		rep.KeysVerified += nkeys
		vc.c.Close()
		if len(viols) > 0 {
			srv.kill()
			for _, v := range viols {
				rep.Violations = append(rep.Violations, fmt.Sprintf("iteration %d (seed %d): %s", iter, cfg.Seed, v))
			}
			return rep, fmt.Errorf("iteration %d: %d violation(s)", iter, len(viols))
		}

		// 2. Run writers; 3. kill.
		runFor := cfg.MinRun + time.Duration(rng.Int63n(int64(cfg.MaxRun-cfg.MinRun)+1))
		graceful := rng.Intn(100) < 4
		mode := "random"
		if !graceful {
			switch r := rng.Intn(100); {
			case r < 35:
				mode = "random"
			case r < 60:
				mode = "flush"
			case r < 85:
				mode = "compaction"
			default:
				mode = "rotate"
			}
		} else {
			mode = "graceful"
		}
		rep.KillMode[mode]++

		ev := watchEvents(srv.admin)
		var att, ack atomic.Uint64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for _, m := range models {
			wg.Add(1)
			go func(m *writerModel) {
				defer wg.Done()
				wrng := rand.New(rand.NewSource(cfg.Seed ^ int64(iter)<<20 ^ int64(m.ID)))
				runWriter(srv.addr, m, wrng, stop, &att, &ack)
			}(m)
		}
		killAt := time.Now().Add(runFor)
		switch mode {
		case "flush", "compaction", "rotate":
			// Wait for the target event (after a short warm-up), then strike
			// within a few ms; give up after runFor+1s and kill anyway.
			deadline := time.Now().Add(runFor + time.Second)
			time.Sleep(30 * time.Millisecond)
			if !ev.waitFor(mode, deadline) {
				mode += "(timeout)"
				rep.KillMode[mode]++
			}
			time.Sleep(time.Duration(rng.Intn(15)) * time.Millisecond)
		default:
			time.Sleep(time.Until(killAt))
		}
		inFlush, inComp, flushes, comps := ev.snapshot()
		if graceful {
			srv.terminate()
			rep.GracefulStops++
		} else {
			srv.kill() // SIGKILL: no flush, no fsync, no cleanup
		}
		close(stop)
		wg.Wait()
		ev.close()
		rep.OpsAttempted += att.Load()
		rep.OpsAcked += ack.Load()
		rep.FlushesSeen += flushes
		rep.CompactionsSeen += comps
		if inFlush && !graceful {
			rep.KilledDuringFlush++
		}
		if inComp && !graceful {
			rep.KilledDuringCompaction++
		}

		// 4. The files left behind must verify (torn WAL tail etc. are warnings).
		var vb bytes.Buffer
		vr, _ := tools.Verify(&vb, cfg.Dir)
		if !vr.OK() {
			rep.Violations = append(rep.Violations, fmt.Sprintf("iteration %d (seed %d): on-disk verification failed after kill:\n%s", iter, cfg.Seed, vb.String()))
			return rep, fmt.Errorf("iteration %d: on-disk state invalid after kill", iter)
		}
		rep.OnDiskVerifyWarn += len(vr.Warnings)
		if sz := dirSize(cfg.Dir); sz > rep.LargestDBBytes {
			rep.LargestDBBytes = sz
		}
		if cfg.Verbose || iter%50 == 0 || iter == cfg.Iterations {
			logf("iter %4d/%d  policy=%-6s style=%-11s kill=%-18s acked=%d pending(applied/discarded)=%d/%d tornTails=%d db=%dKB",
				iter, cfg.Iterations, policy, style, mode, rep.OpsAcked, rep.PendingApplied, rep.PendingDiscarded, rep.TornTails, dirSize(cfg.Dir)/1024)
		}
	}
	// Final incarnation: verify the last kill too.
	srv, err := startServer(cfg, "always", "leveled", rng)
	if err != nil {
		return rep, err
	}
	defer srv.kill()
	vc, err := dial(srv.addr)
	if err != nil {
		return rep, err
	}
	viols, applied, discarded, nkeys := verifyAll(vc, models, time.Now())
	rep.PendingApplied += applied
	rep.PendingDiscarded += discarded
	rep.KeysVerified += nkeys
	for _, v := range viols {
		rep.Violations = append(rep.Violations, "final: "+v)
	}
	if len(viols) > 0 {
		return rep, fmt.Errorf("final verification: %d violation(s)", len(viols))
	}
	return rep, nil
}

func dirSize(dir string) (n int64) {
	filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			n += fi.Size()
		}
		return nil
	})
	return
}

// ---------------------------------------------------------------- server process

func startServer(cfg Config, policy, style string, rng *rand.Rand) (*server, error) {
	ports, err := freePorts(2)
	if err != nil {
		return nil, err
	}
	addr, admin := ports[0], ports[1]
	memtable := []int{32 << 10, 64 << 10, 128 << 10, 256 << 10}[rng.Intn(4)]
	l0 := []int{2, 3, 4}[rng.Intn(3)]
	l1 := []int{128 << 10, 512 << 10}[rng.Intn(2)]
	logPath := filepath.Join(cfg.Dir, "..", filepath.Base(cfg.Dir)+".server.log")
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// Mark where this incarnation's log starts so we only parse its lines.
	fmt.Fprintf(logf, "=== incarnation policy=%s style=%s memtable=%d l0=%d l1=%d\n", policy, style, memtable, l0, l1)
	cmd := exec.Command(cfg.Bin,
		"-dir", cfg.Dir, "-addr", addr, "-admin-addr", admin,
		"-fsync", policy, "-fsync-interval", "20ms",
		"-memtable-size", strconv.Itoa(memtable), "-l0-compact", strconv.Itoa(l0),
		"-l0-slowdown", "8", "-l0-stop", "16", "-l1-bytes", strconv.Itoa(l1),
		"-target-file-size", "65536", "-level-ratio", "4", "-compaction", style,
		"-cache-size", "4194304", "-log-level", "info")
	cmd.Stderr, cmd.Stdout = logf, logf
	t0 := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &server{cmd: cmd, addr: addr, admin: admin, logPath: logPath, started: t0, policy: policy, style: style, waitDone: make(chan error, 1)}
	go func() { s.waitDone <- cmd.Wait(); logf.Close() }()
	// Wait until it answers PING.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-s.waitDone:
			return nil, fmt.Errorf("server exited during startup (recovery failed?): %v\n%s", err, tailLog(logPath, 15))
		default:
		}
		if c, err := dial(addr); err == nil {
			if v, err := c.do("PING"); err == nil && v.Kind == resp.KindSimple {
				c.c.Close()
				s.ready = time.Since(t0)
				return s, nil
			}
			c.c.Close()
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.kill()
	return nil, fmt.Errorf("server did not become ready\n%s", tailLog(logPath, 15))
}

func (s *server) kill() {
	s.cmd.Process.Signal(syscall.SIGKILL)
	select {
	case <-s.waitDone:
	case <-time.After(10 * time.Second):
	}
}

func (s *server) terminate() {
	s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.waitDone:
	case <-time.After(15 * time.Second):
		s.cmd.Process.Kill()
	}
}

func tailLog(path string, n int) string {
	b, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// countTornTails counts "torn_tail=true" in the last incarnation's recovery line.
func countTornTails(path string) int {
	b, _ := os.ReadFile(path)
	i := bytes.LastIndex(b, []byte("=== incarnation"))
	if i < 0 {
		return 0
	}
	return strings.Count(string(b[i:]), "torn_tail=true")
}

// ---------------------------------------------------------------- event watcher

type eventWatcher struct {
	mu          sync.Mutex
	cond        *sync.Cond
	flushing    bool
	compacting  bool
	sawFlush    bool
	sawCompact  bool
	sawRotate   bool
	flushes     int
	compactions int
	resp        *http.Response
	cancel      context.CancelFunc
}

func watchEvents(adminAddr string) *eventWatcher {
	w := &eventWatcher{}
	w.cond = sync.NewCond(&w.mu)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go func() {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+adminAddr+"/api/events", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev struct {
				Type string `json:"type"`
			}
			if json.Unmarshal([]byte(line[6:]), &ev) != nil {
				continue
			}
			w.mu.Lock()
			switch ev.Type {
			case "flush_start":
				w.flushing, w.sawFlush = true, true
			case "flush_end":
				w.flushing = false
				w.flushes++
			case "compaction_start":
				w.compacting, w.sawCompact = true, true
			case "compaction_end":
				w.compacting = false
				w.compactions++
			case "memtable_sealed", "wal_rotated":
				w.sawRotate = true
			}
			w.cond.Broadcast()
			w.mu.Unlock()
		}
	}()
	return w
}

// waitFor blocks until the requested event type has been seen (or the deadline passes).
func (w *eventWatcher) waitFor(kind string, deadline time.Time) bool {
	done := make(chan struct{})
	go func() {
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		select {
		case <-t.C:
			w.mu.Lock()
			w.cond.Broadcast()
			w.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		switch kind {
		case "flush":
			if w.flushing {
				return true
			}
		case "compaction":
			if w.compacting {
				return true
			}
		case "rotate":
			if w.sawRotate {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		w.cond.Wait()
	}
}

func (w *eventWatcher) snapshot() (flushing, compacting bool, flushes, compactions int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushing, w.compacting, w.flushes, w.compactions
}

func (w *eventWatcher) close() { w.cancel() }

// ---------------------------------------------------------------- writers

func runWriter(addr string, m *writerModel, rng *rand.Rand, stop <-chan struct{}, att, ack *atomic.Uint64) {
	c, err := dial(addr)
	if err != nil {
		return
	}
	defer c.c.Close()
	for {
		select {
		case <-stop:
			return
		default:
		}
		now := time.Now()
		p := &pendingOp{SentAt: now, Seq: m.NextSeq}
		m.NextSeq++
		var args []string
		switch r := rng.Intn(100); {
		case r < 40:
			p.Kind, p.Keys = opSet, []string{m.key(rng.Intn(keysPerWriter))}
			args = []string{"SET", p.Keys[0], makeValue(m.ID, p.Seq)}
		case r < 52:
			p.Kind, p.Keys = opDel, []string{m.key(rng.Intn(keysPerWriter))}
			args = []string{"DEL", p.Keys[0]}
		case r < 68:
			p.Kind, p.Keys = opIncr, []string{m.ctrKey()}
			args = []string{"INCR", m.ctrKey()}
		case r < 80:
			p.Kind = opMSet
			args = []string{"MSET"}
			for i := 0; i < groupSize; i++ {
				p.Keys = append(p.Keys, m.groupKey(i))
				args = append(args, m.groupKey(i), makeValue(m.ID, p.Seq))
			}
		case r < 92:
			p.Kind, p.Keys = opSetShortTTL, []string{m.ttlKey(rng.Intn(ttlKeys))}
			args = []string{"SET", p.Keys[0], makeValue(m.ID, p.Seq), "PX", strconv.Itoa(shortTTLms)}
		default:
			p.Kind, p.Keys = opSetLongTTL, []string{m.key(rng.Intn(keysPerWriter))}
			args = []string{"SET", p.Keys[0], makeValue(m.ID, p.Seq), "PX", strconv.Itoa(longTTLms)}
		}
		m.Pending = p
		att.Add(1)
		v, err := c.do(args...)
		if err != nil {
			return // killed mid-operation: pending stays unresolved
		}
		if v.Kind == resp.KindError {
			// An error reply is never expected in this test (no faults injected).
			m.Pending = nil
			m.NextSeq++
			m.Anomaly = fmt.Sprintf("unexpected error reply to %v: %s", args[:2], v.Str)
			return
		}
		if p.Kind == opIncr && v.Int != m.Counter+1 {
			m.Anomaly = fmt.Sprintf("INCR returned %d, expected %d (lost or duplicated increment)", v.Int, m.Counter+1)
			return
		}
		m.ack(p, time.Now())
		m.Pending = nil
		ack.Add(1)
		// Read-your-writes while the engine is busy flushing/compacting.
		if p.Kind == opSet && rng.Intn(4) == 0 {
			g, err := c.do("GET", p.Keys[0])
			if err != nil {
				return
			}
			if g.Kind != resp.KindBulk || g.Nil || string(g.Str) != makeValue(m.ID, p.Seq) {
				m.Anomaly = fmt.Sprintf("read-your-write violated for %s (seq %d)", p.Keys[0], p.Seq)
				return
			}
		}
	}
}

// ---------------------------------------------------------------- verification

type observed struct {
	present bool
	val     string
	pttl    int64
	err     string
}

func fetchAll(c *client, keys []string) (map[string]observed, error) {
	out := map[string]observed{}
	for i := 0; i < len(keys); i += 64 {
		end := i + 64
		if end > len(keys) {
			end = len(keys)
		}
		args := append([]string{"MGET"}, keys[i:end]...)
		v, err := c.do(args...)
		if err != nil {
			return nil, err
		}
		if v.Kind != resp.KindArray || len(v.Elems) != end-i {
			return nil, fmt.Errorf("bad MGET reply")
		}
		for j, e := range v.Elems {
			o := observed{}
			if !(e.Kind == resp.KindBulk && e.Nil) {
				o.present, o.val = true, string(e.Str)
			}
			out[keys[i+j]] = o
		}
	}
	return out, nil
}

// matchCandidate decides whether an observation is consistent with candidate
// state cs at time now, given the window in which a TTL could have fired.
func matchCandidate(o observed, cs keyState, now time.Time, ackBound time.Time) bool {
	if !cs.Present {
		return !o.present
	}
	valid := func() bool {
		if !o.present {
			return false
		}
		_, seq, err := parseValue(o.val)
		return err == nil && seq == cs.Seq
	}
	if cs.TTLms > 0 && cs.TTLms <= shortTTLms {
		lo := cs.SentAt.Add(time.Duration(cs.TTLms)*time.Millisecond - 25*time.Millisecond)
		hi := ackBound.Add(time.Duration(cs.TTLms)*time.Millisecond + 25*time.Millisecond)
		switch {
		case now.Before(lo):
			return valid()
		case now.After(hi):
			return !o.present
		default:
			return valid() || !o.present
		}
	}
	return valid()
}

// verifyAll checks every tracked key against the model, resolves pending
// operations by adopting what the database actually holds, and cross-checks SCAN.
func verifyAll(c *client, models []*writerModel, now time.Time) (viol []string, applied, discarded int, nkeys uint64) {
	var all []string
	owner := map[string]*writerModel{}
	for _, m := range models {
		for _, k := range m.allKeys() {
			all = append(all, k)
			owner[k] = m
		}
	}
	obs, err := fetchAll(c, all)
	if err != nil {
		return []string{"verification read failed: " + err.Error()}, 0, 0, 0
	}
	nkeys = uint64(len(all))
	for _, m := range models {
		if m.Anomaly != "" {
			viol = append(viol, fmt.Sprintf("writer %d: %s", m.ID, m.Anomaly))
			m.Anomaly = ""
		}
		// Value integrity of everything returned, acked or not.
		for _, k := range m.allKeys() {
			o := obs[k]
			if o.present && k != m.ctrKey() {
				if w, _, err := parseValue(o.val); err != nil {
					viol = append(viol, fmt.Sprintf("%s: %v", k, err))
				} else if w != m.ID {
					viol = append(viol, fmt.Sprintf("%s holds a value written by writer %d", k, w))
				}
			}
		}
		p := m.Pending
		// --- counter
		ctr := obs[m.ctrKey()]
		var got int64
		if ctr.present {
			got, _ = strconv.ParseInt(ctr.val, 10, 64)
		}
		switch {
		case got == m.Counter:
			if p != nil && p.Kind == opIncr {
				discarded++
			}
		case got == m.Counter+1 && p != nil && p.Kind == opIncr:
			applied++
			m.Counter = got
		default:
			viol = append(viol, fmt.Sprintf("%s: counter is %d, acknowledged increments=%d (pending incr=%v): acknowledged INCR lost or duplicated", m.ctrKey(), got, m.Counter, p != nil && p.Kind == opIncr))
		}
		// --- MSET group atomicity
		var gk []string
		for i := 0; i < groupSize; i++ {
			gk = append(gk, m.groupKey(i))
		}
		ackGrp := m.Acked[gk[0]]
		pendGrp := p != nil && p.Kind == opMSet
		matchAll := func(cs keyState) bool {
			for _, k := range gk {
				if !matchCandidate(obs[k], cs, now, now) {
					return false
				}
			}
			return true
		}
		switch {
		case matchAll(ackGrp):
			if pendGrp {
				discarded++
			}
		case pendGrp && matchAll(keyState{Present: true, Seq: p.Seq}):
			applied++
			m.ack(p, now)
		default:
			var d []string
			for _, k := range gk {
				d = append(d, fmt.Sprintf("%s=%s", k, short(obs[k])))
			}
			viol = append(viol, fmt.Sprintf("MSET group of writer %d is not atomic or lost an acked write: acked seq=%d pending=%v observed [%s]", m.ID, ackGrp.Seq, pendGrp, strings.Join(d, " ")))
		}
		// --- single keys
		for _, k := range m.allKeys() {
			if k == m.ctrKey() || strings.Contains(k, ":g") {
				continue
			}
			o := obs[k]
			cands := []keyState{m.Acked[k]}
			var pendCand *keyState
			if p != nil && len(p.Keys) == 1 && p.Keys[0] == k && p.Kind != opIncr {
				var cs keyState
				switch p.Kind {
				case opDel:
					cs = keyState{}
				case opSet:
					cs = keyState{Present: true, Seq: p.Seq}
				case opSetShortTTL:
					cs = keyState{Present: true, Seq: p.Seq, TTLms: shortTTLms, SentAt: p.SentAt}
				case opSetLongTTL:
					cs = keyState{Present: true, Seq: p.Seq, TTLms: longTTLms, SentAt: p.SentAt}
				}
				pendCand = &cs
			}
			switch {
			case matchCandidate(o, cands[0], now, orNow(cands[0].AckAt, now)):
				if pendCand != nil {
					discarded++
				}
				// adopt (TTL keys that expired become absent)
				if !o.present {
					m.Acked[k] = keyState{}
				}
			case pendCand != nil && matchCandidate(o, *pendCand, now, now):
				applied++
				if o.present {
					m.Acked[k] = *pendCand
				} else {
					m.Acked[k] = keyState{}
				}
			default:
				viol = append(viol, fmt.Sprintf("%s: observed %s but acknowledged state is %s (pending=%s): acknowledged write lost, resurrected or corrupted",
					k, short(o), describe(m.Acked[k]), describePending(pendCand)))
			}
			// long TTL must survive the restart with a sane remaining lifetime
			if st := m.Acked[k]; st.Present && st.TTLms == longTTLms && o.present {
				v, err := c.do("PTTL", k)
				if err == nil {
					elapsed := now.Sub(st.SentAt).Milliseconds()
					if v.Int <= 0 || v.Int > longTTLms || v.Int < longTTLms-elapsed-1500 {
						viol = append(viol, fmt.Sprintf("%s: PTTL=%d after restart (set %dms ago with PX %d): TTL lost or wrong", k, v.Int, elapsed, longTTLms))
					}
				}
			}
		}
		m.Pending = nil
	}
	// --- SCAN must agree with point reads and contain no foreign keys.
	known := map[string]bool{}
	for _, k := range all {
		known[k] = true
	}
	seen := map[string]bool{}
	cursor := "0"
	prevPage := ""
	for guard := 0; guard < 100000; guard++ {
		v, err := c.do("SCAN", cursor, "COUNT", "300")
		if err != nil || v.Kind != resp.KindArray || len(v.Elems) != 2 {
			viol = append(viol, fmt.Sprintf("SCAN failed: %v", err))
			break
		}
		cursor = string(v.Elems[0].Str)
		page := ""
		for _, e := range v.Elems[1].Elems {
			k := string(e.Str)
			if seen[k] {
				viol = append(viol, "SCAN returned duplicate key "+k)
			}
			seen[k] = true
			if !known[k] {
				viol = append(viol, "SCAN returned a key nobody wrote: "+k)
			}
			if page != "" && k <= page {
				viol = append(viol, "SCAN page out of order at "+k)
			}
			page = k
		}
		if page != "" && prevPage != "" && page <= prevPage {
			viol = append(viol, "SCAN pages out of order")
		}
		if page != "" {
			prevPage = page
		}
		if cursor == "0" {
			break
		}
	}
	// SCAN ran first, the GETs run a little later. A key GET can see must have
	// been visible to SCAN; the reverse (SCAN saw it, GET did not) is only legal
	// for short-TTL keys, which may expire in between.
	for _, k := range all {
		o2, _ := fetchOne(c, k)
		isShortTTL := strings.Contains(k, ":t")
		switch {
		case o2.present && !seen[k]:
			viol = append(viol, fmt.Sprintf("SCAN missed %s although GET returns it", k))
		case !o2.present && seen[k] && !isShortTTL:
			viol = append(viol, fmt.Sprintf("SCAN returned %s but GET says it is absent (key has no short TTL)", k))
		}
	}
	sort.Strings(viol)
	return
}

func fetchOne(c *client, k string) (observed, error) {
	m, err := fetchAll(c, []string{k})
	return m[k], err
}

func orNow(t, now time.Time) time.Time {
	if t.IsZero() {
		return now
	}
	return t
}

func short(o observed) string {
	if !o.present {
		return "<absent>"
	}
	if len(o.val) > 24 {
		return fmt.Sprintf("%q…(%d bytes)", o.val[:24], len(o.val))
	}
	return strconv.Quote(o.val)
}

func describe(k keyState) string {
	if !k.Present {
		return "absent"
	}
	return fmt.Sprintf("seq %d", k.Seq)
}

func describePending(k *keyState) string {
	if k == nil {
		return "none"
	}
	return describe(*k)
}
