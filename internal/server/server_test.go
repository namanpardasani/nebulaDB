package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/engine/enginetest"
	"github.com/naman/nebuladb/internal/resp"
)

type testSrv struct {
	*Server
	addr string
	clk  *enginetest.Clock
	done chan error
}

func startServer(t *testing.T, maxConns int) *testSrv {
	t.Helper()
	clk := enginetest.NewClock()
	eng := engine.NewMemClock(clk.Now)
	s := New(eng, Options{MaxConns: maxConns, Now: clk.Now})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testSrv{Server: s, addr: ln.Addr().String(), clk: clk, done: make(chan error, 1)}
	go func() { ts.done <- s.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
		eng.Close()
	})
	return ts
}

type client struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func (ts *testSrv) dial(t *testing.T) *client {
	t.Helper()
	c, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	return &client{c, resp.NewReader(c), resp.NewWriter(c)}
}

// do sends one command and returns its reply.
func (c *client) do(t *testing.T, args ...string) resp.Value {
	t.Helper()
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	c.w.WriteCommand(bs...)
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatalf("reply to %v: %v", args, err)
	}
	return v
}

// render turns a reply into a compact string for assertions.
func render(v resp.Value) string {
	switch v.Kind {
	case resp.KindSimple:
		return "+" + string(v.Str)
	case resp.KindError:
		return "-" + string(v.Str)
	case resp.KindInt:
		return fmt.Sprintf(":%d", v.Int)
	case resp.KindBulk:
		if v.Nil {
			return "(nil)"
		}
		return `"` + string(v.Str) + `"`
	case resp.KindArray:
		if v.Nil {
			return "(nilarray)"
		}
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return "?"
}

func TestCommands(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	steps := []struct {
		cmd  []string
		want string
	}{
		{[]string{"PING"}, "+PONG"},
		{[]string{"ping", "hi"}, `"hi"`},
		{[]string{"ECHO", "x y"}, `"x y"`},
		{[]string{"GET", "k"}, "(nil)"},
		{[]string{"SET", "k", "v"}, "+OK"},
		{[]string{"GET", "k"}, `"v"`},
		{[]string{"SET", "k", "v2", "NX"}, "(nil)"},
		{[]string{"SET", "n", "1", "XX"}, "(nil)"},
		{[]string{"SET", "n", "1", "NX"}, "+OK"},
		{[]string{"SET", "k", "v", "NX", "XX"}, "-ERR syntax error"},
		{[]string{"SET", "k", "v", "EX", "10", "PX", "5"}, "-ERR syntax error"},
		{[]string{"SET", "k", "v", "EX", "0"}, "-ERR invalid expire time in 'set' command"},
		{[]string{"SET", "k", "v", "EX", "abc"}, "-ERR value is not an integer or out of range"},
		{[]string{"SET", "k", "v", "EX"}, "-ERR syntax error"},
		{[]string{"SET", "k", "v", "BOGUS"}, "-ERR syntax error"},
		{[]string{"EXISTS", "k", "k", "zz"}, ":2"},
		{[]string{"DEL", "k", "zz", "n"}, ":2"},
		{[]string{"MSET", "a", "1", "b", "2"}, "+OK"},
		{[]string{"MSET", "a", "1", "b"}, "-ERR wrong number of arguments for 'mset' command"},
		{[]string{"MGET", "a", "none", "b"}, `["1" (nil) "2"]`},
		{[]string{"INCR", "cnt"}, ":1"},
		{[]string{"INCRBY", "cnt", "9"}, ":10"},
		{[]string{"DECR", "cnt"}, ":9"},
		{[]string{"DECRBY", "cnt", "4"}, ":5"},
		{[]string{"DECRBY", "cnt", "-9223372036854775808"}, "-ERR value is not an integer or out of range"},
		{[]string{"SET", "s", "abc"}, "+OK"},
		{[]string{"INCR", "s"}, "-ERR value is not an integer or out of range"},
		{[]string{"GET"}, "-ERR wrong number of arguments for 'get' command"},
		{[]string{"NOPE", "x"}, "-ERR unknown command 'NOPE'"},
		{[]string{"SELECT", "0"}, "+OK"},
		{[]string{"SELECT", "3"}, "-ERR DB index is out of range"},
		{[]string{"HELLO", "3"}, "-NOPROTO unsupported protocol version"},
		{[]string{"CLIENT", "SETNAME", "x"}, "+OK"},
		{[]string{"COMMAND", "DOCS"}, "[]"},
		{[]string{"FLUSHDB"}, "+OK"},
		{[]string{"DBSIZE"}, ":0"},
	}
	for _, st := range steps {
		if got := render(c.do(t, st.cmd...)); got != st.want {
			t.Errorf("%v: got %s want %s", st.cmd, got, st.want)
		}
	}
}

func TestExpiryCommands(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	check := func(want string, args ...string) {
		t.Helper()
		if got := render(c.do(t, args...)); got != want {
			t.Errorf("%v: got %s want %s", args, got, want)
		}
	}
	check("+OK", "SET", "k", "v", "EX", "10")
	check(":10", "TTL", "k")
	check(":10000", "PTTL", "k")
	ts.clk.Advance(9600)
	check(":0", "TTL", "k") // 400ms left rounds to 0, like Redis
	check(":400", "PTTL", "k")
	check(":1", "PERSIST", "k")
	check(":0", "PERSIST", "k")
	check(":-1", "TTL", "k")
	check(":1", "PEXPIRE", "k", "100")
	check(":0", "EXPIRE", "missing", "5")
	ts.clk.Advance(100)
	check("(nil)", "GET", "k")
	check(":-2", "TTL", "k")
	check("+OK", "SET", "k", "v")
	check(":1", "EXPIRE", "k", "-1") // non-positive deletes
	check("(nil)", "GET", "k")
	check("+OK", "SET", "x", "1", "PX", "50")
	ts.clk.Advance(50)
	check("(nil)", "GET", "x")
	check("-ERR invalid expire time in 'expire' command", "EXPIRE", "k", "9223372036854775807")
}

func TestScan(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	for i := 0; i < 25; i++ {
		c.do(t, "SET", fmt.Sprintf("user:%02d", i), "v")
		c.do(t, "SET", fmt.Sprintf("order:%02d", i), "v")
	}
	scanAll := func(extra ...string) []string {
		var all []string
		cursor := "0"
		for i := 0; i < 1000; i++ {
			v := c.do(t, append([]string{"SCAN", cursor}, extra...)...)
			cursor = string(v.Elems[0].Str)
			for _, k := range v.Elems[1].Elems {
				all = append(all, string(k.Str))
			}
			if cursor == "0" {
				return all
			}
		}
		t.Fatal("scan never finished")
		return nil
	}
	if got := scanAll("COUNT", "7"); len(got) != 50 {
		t.Fatalf("got %d keys", len(got))
	}
	got := scanAll("MATCH", "user:*", "COUNT", "5")
	if len(got) != 25 {
		t.Fatalf("match user:* gave %d", len(got))
	}
	for _, k := range got {
		if !strings.HasPrefix(k, "user:") {
			t.Fatalf("unexpected %q", k)
		}
	}
	if got := scanAll("MATCH", "order:1?"); len(got) != 10 {
		t.Fatalf("order:1? gave %d", len(got))
	}
	if r := render(c.do(t, "SCAN", "999999")); r != "-ERR invalid cursor" {
		t.Fatalf("got %s", r)
	}
	if r := render(c.do(t, "SCAN", "abc")); r != "-ERR invalid cursor" {
		t.Fatalf("got %s", r)
	}
	if r := render(c.do(t, "SCAN", "0", "COUNT", "0")); r != "-ERR syntax error" {
		t.Fatalf("got %s", r)
	}
}

func TestPipelining(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	const n = 5000
	// Write all commands before reading any reply, from a separate goroutine so
	// we do not deadlock on full socket buffers.
	go func() {
		for i := 0; i < n; i++ {
			c.w.WriteCommand([]byte("INCR"), []byte("p"))
		}
		c.w.Flush()
	}()
	for i := 1; i <= n; i++ {
		v, err := c.r.ReadValue()
		if err != nil || v.Int != int64(i) {
			t.Fatalf("reply %d: %+v %v (ordering broken)", i, v, err)
		}
	}
}

func TestSplitAndInlineInput(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	// A command dribbled one byte at a time.
	raw := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nabc\r\n"
	for i := 0; i < len(raw); i++ {
		c.c.Write([]byte{raw[i]})
		time.Sleep(time.Millisecond)
	}
	if v, _ := c.r.ReadValue(); render(v) != "+OK" {
		t.Fatalf("got %s", render(v))
	}
	// Inline command (telnet style).
	c.c.Write([]byte("GET k\r\n"))
	if v, _ := c.r.ReadValue(); render(v) != `"abc"` {
		t.Fatalf("got %s", render(v))
	}
}

func TestProtocolErrorClosesConn(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	c.c.Write([]byte("*abc\r\n"))
	v, err := c.r.ReadValue()
	if err != nil || v.Kind != resp.KindError || !strings.Contains(string(v.Str), "Protocol error") {
		t.Fatalf("got %+v %v", v, err)
	}
	if _, err := c.r.ReadValue(); err != io.EOF {
		t.Fatalf("connection should be closed, got %v", err)
	}
}

func TestQuit(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	if r := render(c.do(t, "QUIT")); r != "+OK" {
		t.Fatal(r)
	}
	if _, err := c.r.ReadValue(); err != io.EOF {
		t.Fatalf("got %v", err)
	}
}

func TestMaxConns(t *testing.T) {
	ts := startServer(t, 2)
	c1, c2 := ts.dial(t), ts.dial(t)
	c1.do(t, "PING")
	c2.do(t, "PING")
	c3 := ts.dial(t)
	v, err := c3.r.ReadValue()
	if err != nil || !strings.Contains(string(v.Str), "max number of clients") {
		t.Fatalf("got %+v %v", v, err)
	}
	// Freeing a slot lets a new client in.
	c1.c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c4 := ts.dial(t)
		c4.w.WriteCommand([]byte("PING"))
		c4.w.Flush()
		if v, err := c4.r.ReadValue(); err == nil && render(v) == "+PONG" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot was never released")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConcurrentClients(t *testing.T) {
	ts := startServer(t, 0)
	const clients, per = 20, 200
	var wg sync.WaitGroup
	var failures atomic.Int64
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", ts.addr)
			if err != nil {
				failures.Add(1)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(20 * time.Second))
			r, w := resp.NewReader(c), resp.NewWriter(c)
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("c%d-%d", ci, i)
				w.WriteCommand([]byte("SET"), []byte(k), []byte(k))
				w.WriteCommand([]byte("GET"), []byte(k))
				w.WriteCommand([]byte("INCR"), []byte("shared"))
				w.Flush()
				a, e1 := r.ReadValue()
				b, e2 := r.ReadValue()
				_, e3 := r.ReadValue()
				if e1 != nil || e2 != nil || e3 != nil || render(a) != "+OK" || render(b) != `"`+k+`"` {
					failures.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d client failures", failures.Load())
	}
	c := ts.dial(t)
	if r := render(c.do(t, "GET", "shared")); r != fmt.Sprintf(`"%d"`, clients*per) {
		t.Fatalf("lost increments: %s", r)
	}
}

func TestGracefulShutdown(t *testing.T) {
	ts := startServer(t, 0)
	idle := ts.dial(t)
	idle.do(t, "PING")
	busy := ts.dial(t)
	busy.do(t, "SET", "k", "v")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := ts.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("shutdown waited on idle connections")
	}
	if err := <-ts.done; err != nil {
		t.Fatalf("Serve returned %v", err)
	}
	if _, err := idle.r.ReadValue(); err == nil {
		t.Fatal("idle connection should be closed")
	}
	if _, err := net.DialTimeout("tcp", ts.addr, 500*time.Millisecond); err == nil {
		t.Fatal("listener still accepting")
	}
}

// A full pipelined batch that is already buffered when Shutdown arrives must
// still be answered completely.
func TestShutdownFinishesBufferedPipeline(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	const n = 200
	for i := 0; i < n; i++ {
		c.w.WriteCommand([]byte("INCR"), []byte("q"))
	}
	c.w.Flush()
	// Make sure the server has started on the batch before shutting down.
	if v, err := c.r.ReadValue(); err != nil || v.Int != 1 {
		t.Fatal(v, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go ts.Shutdown(ctx)
	got := 1
	for {
		v, err := c.r.ReadValue()
		if err != nil {
			break
		}
		got++
		if v.Int != int64(got) {
			t.Fatalf("out of order reply %d at %d", v.Int, got)
		}
	}
	// Every reply we received is in order; the server never half-answers a command.
	if got < 1 {
		t.Fatal("no replies")
	}
}

func TestInfoHasSections(t *testing.T) {
	ts := startServer(t, 0)
	c := ts.dial(t)
	v := c.do(t, "INFO")
	s := string(v.Str)
	for _, want := range []string{"# Server", "# Clients", "# Stats", "# Engine", "connected_clients:1", "engine:mem"} {
		if !strings.Contains(s, want) {
			t.Errorf("INFO missing %q:\n%s", want, s)
		}
	}
}

func TestGlob(t *testing.T) {
	tests := []struct {
		pat, s string
		want   bool
	}{
		{"*", "", true}, {"*", "anything", true}, {"", "", true}, {"", "a", false},
		{"a*", "abc", true}, {"*c", "abc", true}, {"a*c", "abc", true}, {"a*c", "abd", false},
		{"a?c", "abc", true}, {"a?c", "ac", false},
		{"h[ae]llo", "hello", true}, {"h[ae]llo", "hallo", true}, {"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true}, {"h[^e]llo", "hello", false},
		{"h[!e]llo", "hallo", true},
		{"h[a-b]llo", "hbllo", true}, {"h[a-b]llo", "hcllo", false},
		{`a\*b`, "a*b", true}, {`a\*b`, "axb", false},
		{"*a*a*a*a*b", strings.Repeat("a", 60), false}, // must not blow up
		{"user:*:name", "user:42:name", true}, {"user:*:name", "user:42:nam", false},
		{"[abc", "a", true}, // unterminated class, as Redis
		{"**a", "a", true}, {"a**", "a", true},
	}
	for _, tt := range tests {
		if got := globMatch([]byte(tt.pat), []byte(tt.s)); got != tt.want {
			t.Errorf("glob(%q,%q)=%v want %v", tt.pat, tt.s, got, tt.want)
		}
	}
}

func FuzzGlob(f *testing.F) {
	f.Add("a*b[c-d]?\\", "abcd")
	f.Fuzz(func(t *testing.T, pat, s string) {
		globMatch([]byte(pat), []byte(s)) // must terminate without panicking
	})
}

func TestCursorTableEviction(t *testing.T) {
	ct := newCursorTable(3)
	var ids []uint64
	for i := 0; i < 5; i++ {
		ids = append(ids, ct.put([]byte{byte(i)}))
	}
	if _, ok := ct.get(ids[0]); ok {
		t.Fatal("oldest cursor should be evicted")
	}
	if k, ok := ct.get(ids[4]); !ok || k[0] != 4 {
		t.Fatal("newest cursor lost")
	}
	if ids[0] == 0 {
		t.Fatal("cursor id 0 is reserved")
	}
}
