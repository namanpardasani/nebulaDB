package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/server"
	"github.com/naman/nebuladb/internal/wal"
)

type env struct {
	db  *engine.DB
	srv *server.Server
	br  *Broker
	ts  *httptest.Server
	adm *Server
}

func setup(t *testing.T, dev bool) *env {
	t.Helper()
	br := NewBroker(500)
	db, _, err := engine.Open(engine.Options{
		Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true, MemtableSize: 8 << 10, BloomBits: 10,
		OnEvent: br.Publish, Compaction: engine.CompactionOptions{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(db, server.Options{})
	adm := New(Options{Source: db, Dev: db, Server: srv, Broker: br, DevMode: dev, TickEvery: 30 * time.Millisecond})
	ts := httptest.NewServer(adm.Handler())
	t.Cleanup(func() { ts.Close(); adm.Shutdown(nil); db.Close() })
	return &env{db, srv, br, ts, adm}
}

func (e *env) do(t *testing.T, method, path string, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func TestGuard(t *testing.T) {
	e := setup(t, false)
	if r, _ := e.do(t, "GET", "/api/health", "", nil); r.StatusCode != 200 {
		t.Fatalf("health: %d", r.StatusCode)
	}
	if r, _ := e.do(t, "GET", "/api/health", "", map[string]string{"Host": "evil.example.com"}); r.StatusCode != 403 {
		t.Fatalf("DNS-rebinding style Host must be rejected, got %d", r.StatusCode)
	}
	if r, _ := e.do(t, "GET", "/api/stats", "", map[string]string{"Origin": "https://evil.example.com"}); r.StatusCode != 403 {
		t.Fatalf("foreign Origin must be rejected, got %d", r.StatusCode)
	}
	r, _ := e.do(t, "GET", "/api/stats", "", map[string]string{"Origin": "http://localhost:5173"})
	if r.StatusCode != 200 || r.Header.Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Fatalf("loopback origin should get CORS: %d %v", r.StatusCode, r.Header)
	}
	req, _ := http.NewRequest("POST", e.ts.URL+"/api/cmd", strings.NewReader(`{"args":["PING"]}`))
	req.Header.Set("Content-Type", "text/plain")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 415 {
		t.Fatalf("non-JSON POST must be rejected (CSRF), got %d", resp.StatusCode)
	}
	if r, _ := e.do(t, "OPTIONS", "/api/cmd", "", map[string]string{"Origin": "http://127.0.0.1:5173"}); r.StatusCode != 204 {
		t.Fatalf("preflight: %d", r.StatusCode)
	}
}

func TestStatsLevelsAndProxy(t *testing.T) {
	e := setup(t, false)
	post := func(args ...string) (int, map[string]any) {
		b, _ := json.Marshal(map[string]any{"args": args})
		r, body := e.do(t, "POST", "/api/cmd", string(b), nil)
		var m map[string]any
		json.Unmarshal(body, &m)
		return r.StatusCode, m
	}
	if c, m := post("SET", "hello", "world"); c != 200 || m["reply"] != "OK" {
		t.Fatalf("%d %v", c, m)
	}
	if c, m := post("GET", "hello"); c != 200 || m["reply"] != "world" {
		t.Fatalf("%d %v", c, m)
	}
	if _, m := post("GET", "missing"); m["reply"] != nil {
		t.Fatalf("nil reply expected: %v", m)
	}
	if _, m := post("INCR", "hello"); m["reply"].(map[string]any)["error"] == nil {
		t.Fatalf("error reply should be reported: %v", m)
	}
	if _, m := post("MGET", "hello", "missing"); len(m["reply"].([]any)) != 2 {
		t.Fatalf("%v", m)
	}
	for _, bad := range [][]string{{"FLUSHDB"}, {"SHUTDOWN"}, {"CONFIG", "SET", "x", "y"}, {"CLIENT", "KILL"}} {
		if c, _ := post(bad...); c != 403 {
			t.Fatalf("%v must be forbidden through the proxy, got %d", bad, c)
		}
	}
	if r, _ := e.do(t, "POST", "/api/cmd", `{"args":[]}`, nil); r.StatusCode != 400 {
		t.Fatalf("empty args: %d", r.StatusCode)
	}

	for i := 0; i < 400; i++ {
		e.db.Set([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 100), engine.SetOptions{})
	}
	e.db.WaitForFlush()
	_, body := e.do(t, "GET", "/api/stats", "", nil)
	var st struct {
		Server struct {
			TotalCommands uint64 `json:"total_commands"`
			PerCommand    map[string]uint64
		} `json:"server"`
		Engine engine.EngineMetrics `json:"engine"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("%v\n%s", err, body)
	}
	if st.Server.TotalCommands < 5 || st.Engine.Flushes == 0 || st.Engine.TableFiles == 0 || st.Engine.LastSeq < 400 {
		t.Fatalf("stats look wrong: %+v", st)
	}
	_, body = e.do(t, "GET", "/api/levels", "", nil)
	var lv struct {
		Levels [][]engine.FileInfo `json:"levels"`
		Memory engine.MemInfo      `json:"memory"`
	}
	if err := json.Unmarshal(body, &lv); err != nil || len(lv.Levels) != 7 || lv.Memory.Threshold != 8<<10 {
		t.Fatalf("%v %s", err, body)
	}
	n := 0
	for _, l := range lv.Levels {
		n += len(l)
	}
	if n != st.Engine.TableFiles {
		t.Fatalf("levels lists %d files, stats says %d", n, st.Engine.TableFiles)
	}
}

func TestPrometheusFormat(t *testing.T) {
	e := setup(t, false)
	for i := 0; i < 300; i++ {
		e.srv.Exec([][]byte{[]byte("SET"), []byte(fmt.Sprint("k", i)), make([]byte, 80)})
	}
	e.srv.Exec([][]byte{[]byte("GET"), []byte("k1")})
	e.db.WaitForFlush()
	r, body := e.do(t, "GET", "/metrics", "", nil)
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("content type %q", r.Header.Get("Content-Type"))
	}
	out := string(body)
	for _, want := range []string{
		"# TYPE nebuladb_commands_total counter", `nebuladb_commands_total{command="set"} 300`,
		"# TYPE nebuladb_command_duration_seconds histogram", `nebuladb_command_duration_seconds_count{class="write"} 300`,
		"nebuladb_memtable_bytes ", "nebuladb_flushes_total ", `nebuladb_level_files{level="0"}`,
		"nebuladb_wal_bytes_total ", "nebuladb_write_stall_seconds_total ", "nebuladb_bloom_useful_ratio ",
		"nebuladb_block_cache_hit_ratio ", "nebuladb_ops_per_second ", "nebuladb_write_amplification ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	// Every sample line must be: name[{labels}] value
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 {
			t.Fatalf("malformed sample line: %q", line)
		}
		var v float64
		if _, err := fmt.Sscan(f[1], &v); err != nil && f[1] != "+Inf" {
			t.Fatalf("bad value in %q", line)
		}
	}
}

// readSSE parses the stream until pred says stop or the deadline passes.
func readSSE(t *testing.T, url string, lastID string, pred func(id string, data map[string]any) bool) {
	t.Helper()
	req, _ := http.NewRequest("GET", url+"/api/events", nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	ctx, cancel := contextWithTimeout(5 * time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content type %q", resp.Header.Get("Content-Type"))
	}
	sc := bufio.NewScanner(resp.Body)
	id := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			var m map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m); err != nil {
				t.Fatalf("bad event json: %v", err)
			}
			if pred(id, m) {
				return
			}
			id = ""
		}
	}
	t.Fatalf("stream ended before the expected event (%v)", sc.Err())
}

func TestSSEEventsTicksAndReplay(t *testing.T) {
	e := setup(t, true)
	// Generate lifecycle events: fill memtables, flush.
	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 500; i++ {
			e.db.Set([]byte(fmt.Sprintf("k%05d", i)), make([]byte, 100), engine.SetOptions{})
		}
		e.db.FlushMemtable()
	}()
	seen := map[string]bool{}
	var lastID string
	readSSE(t, e.ts.URL, "", func(id string, m map[string]any) bool {
		typ, _ := m["type"].(string)
		seen[typ] = true
		if id != "" {
			lastID = id
		}
		if typ == "tick" {
			for _, k := range []string{"ops_per_sec", "writes_per_sec", "memory", "levels", "cache_hit_rate", "recent_writes"} {
				if _, ok := m[k]; !ok {
					t.Fatalf("tick missing %q: %v", k, m)
				}
			}
		}
		return seen["tick"] && seen["flush_end"] && seen["file_created"] && seen["memtable_sealed"]
	})
	if lastID == "" {
		t.Fatal("persisted events must carry ids")
	}
	// Reconnect with Last-Event-ID: older events are not replayed, newer ones are.
	e.db.Set([]byte("late"), []byte("x"), engine.SetOptions{})
	e.db.FlushMemtable()
	replayed := false
	readSSE(t, e.ts.URL, lastID, func(id string, m map[string]any) bool {
		if id != "" {
			var a, b uint64
			fmt.Sscan(id, &a)
			fmt.Sscan(lastID, &b)
			if a <= b {
				t.Fatalf("replayed an event (%s) that the client already had (%s)", id, lastID)
			}
			replayed = true
			return true
		}
		return false
	})
	if !replayed {
		t.Fatal("nothing replayed")
	}
}

func TestBrokerDropsSlowSubscribers(t *testing.T) {
	b := NewBroker(10)
	sub, _ := b.Subscribe(0)
	for i := 0; i < 2000; i++ { // never read: must not block the publisher
		b.Publish(engine.Event{Type: engine.EventFlushEnd, File: uint64(i)})
	}
	if !sub.Dropped() || b.Subscribers() != 0 {
		t.Fatal("slow subscriber must be dropped")
	}
	// Ring keeps only the newest events.
	_, backlog := b.Subscribe(0)
	if len(backlog) != 10 {
		t.Fatalf("backlog %d", len(backlog))
	}
}

func TestDevEndpoints(t *testing.T) {
	e := setup(t, false)
	for _, p := range []string{"/api/dev/crash", "/api/dev/flush", "/api/dev/compact"} {
		if r, _ := e.do(t, "POST", p, "{}", nil); r.StatusCode != 403 {
			t.Fatalf("%s must be forbidden outside dev mode, got %d", p, r.StatusCode)
		}
	}
	d := setup(t, true)
	if r, _ := d.do(t, "POST", "/api/dev/crash", "{}", nil); r.StatusCode != 409 {
		t.Fatalf("crash without a supervisor must be refused (409), got %d", r.StatusCode)
	}
	d.db.Set([]byte("a"), []byte("1"), engine.SetOptions{})
	if r, _ := d.do(t, "POST", "/api/dev/flush", "{}", nil); r.StatusCode != 200 {
		t.Fatalf("flush: %d", r.StatusCode)
	}
	if r, _ := d.do(t, "POST", "/api/dev/compact", "{}", nil); r.StatusCode != 200 {
		t.Fatalf("compact: %d", r.StatusCode)
	}
	// In dev mode FLUSHDB is allowed through the proxy.
	if r, _ := d.do(t, "POST", "/api/cmd", `{"args":["FLUSHDB"]}`, nil); r.StatusCode != 200 {
		t.Fatalf("FLUSHDB in dev mode: %d", r.StatusCode)
	}
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
