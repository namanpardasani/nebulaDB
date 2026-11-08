// Package model holds the model-based (property) tests: long, seeded, random
// operation sequences are applied to NebulaDB and to a trivially correct
// in-memory oracle, and every observable result must agree. The seed is part of
// every failure message, so any failure replays exactly.
//
// Scale with environment variables:
//
//	NEBULA_MODEL_SEEDS  number of seeds   (default 8)
//	NEBULA_MODEL_STEPS  operations / seed (default 3000)
package model

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/internal/server"
	"github.com/naman/nebuladb/internal/wal"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

type clock struct{ ms atomic.Int64 }

func newClock() *clock       { c := &clock{}; c.ms.Store(5_000_000); return c }
func (c *clock) Now() int64  { return c.ms.Load() }
func (c *clock) Add(d int64) { c.ms.Add(d) }

type entry struct {
	v   string
	exp int64
}

// oracle is the reference model: a map plus Redis semantics.
type oracle struct {
	m   map[string]entry
	clk *clock
}

func newOracle(c *clock) *oracle { return &oracle{m: map[string]entry{}, clk: c} }

func (o *oracle) live(k string) (entry, bool) {
	e, ok := o.m[k]
	if ok && e.exp != 0 && e.exp <= o.clk.Now() {
		return e, false
	}
	return e, ok
}

func (o *oracle) liveKeys() []string {
	var ks []string
	for k := range o.m {
		if _, ok := o.live(k); ok {
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	return ks
}

func (o *oracle) clone(at int64) map[string]entry {
	c := map[string]entry{}
	for k, e := range o.m {
		if e.exp == 0 || e.exp > at {
			c[k] = e
		}
	}
	return c
}

// ------------------------------------------------------------------ engine-level model

func TestEngineModel(t *testing.T) {
	seeds, steps := envInt("NEBULA_MODEL_SEEDS", 8), envInt("NEBULA_MODEL_STEPS", 3000)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { runEngineModel(t, seed, steps) })
	}
}

func runEngineModel(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	clk := newClock()
	dir := t.TempDir()
	style := []string{engine.StyleLeveled, engine.StyleSizeTiered}[seed%2]
	o := engine.Options{
		Dir: dir, Sync: wal.SyncNever, GroupCommit: true, Now: clk.Now,
		MemtableSize: 3 << 10, BlockSize: 512, BloomBits: 10,
		Compaction: engine.CompactionOptions{Style: style, L0CompactTrigger: 2, L0SlowdownTrigger: 6, L0StopTrigger: 12, LevelSizeRatio: 4, L1MaxBytes: 12 << 10, TargetFileBytes: 4 << 10},
	}
	open := func() *engine.DB {
		d, _, err := engine.Open(o)
		if err != nil {
			t.Fatalf("seed %d: open: %v", seed, err)
		}
		return d
	}
	d := open()
	defer func() { d.Close() }()
	or := newOracle(clk)

	fail := func(step int, f string, a ...any) {
		t.Helper()
		t.Fatalf("seed=%d style=%s step=%d: %s", seed, style, step, fmt.Sprintf(f, a...))
	}
	key := func() string { return fmt.Sprintf("key%02d", rng.Intn(70)) }

	type snapshot struct {
		s    *engine.Snapshot
		want map[string]entry
		at   int
	}
	var snaps []snapshot
	checkSnap := func(sn snapshot, step int) {
		for i := 0; i < 70; i++ {
			k := fmt.Sprintf("key%02d", i)
			got, ok, err := sn.s.Get([]byte(k))
			if err != nil {
				fail(step, "snapshot (taken at step %d) read error: %v", sn.at, err)
			}
			w, wok := sn.want[k]
			if ok != wok || (ok && string(got) != w.v) {
				fail(step, "snapshot taken at step %d: %s = %q ok=%v, expected %q ok=%v (a later write leaked in, or compaction damaged a pinned view)", sn.at, k, got, ok, w.v, wok)
			}
		}
		it := sn.s.NewIterator()
		n := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if w, ok := sn.want[string(it.Key())]; !ok || w.v != string(it.Value()) {
				fail(step, "snapshot iterator (taken at step %d) returned %s=%q", sn.at, it.Key(), it.Value())
			}
			n++
		}
		if it.Err() != nil || n != len(sn.want) {
			fail(step, "snapshot iterator (taken at step %d): %d keys, want %d, err %v", sn.at, n, len(sn.want), it.Err())
		}
		it.Close()
	}

	for step := 0; step < steps; step++ {
		k := key()
		switch op := rng.Intn(100); {
		case op < 22: // plain SET with optional TTL
			v := fmt.Sprintf("v%d", rng.Intn(100000))
			var exp int64
			if rng.Intn(5) == 0 {
				exp = clk.Now() + int64(1+rng.Intn(400))
			}
			ok, err := d.Set([]byte(k), []byte(v), engine.SetOptions{ExpireAtMs: exp})
			if err != nil || !ok {
				fail(step, "SET: ok=%v err=%v", ok, err)
			}
			or.m[k] = entry{v, exp}
		case op < 28: // SET NX / XX
			v := fmt.Sprintf("c%d", rng.Intn(1000))
			nx := rng.Intn(2) == 0
			ok, err := d.Set([]byte(k), []byte(v), engine.SetOptions{NX: nx, XX: !nx})
			_, exists := or.live(k)
			want := (nx && !exists) || (!nx && exists)
			if err != nil || ok != want {
				fail(step, "SET nx=%v on %s: ok=%v want %v err=%v", nx, k, ok, want, err)
			}
			if want {
				or.m[k] = entry{v, 0}
			}
		case op < 32: // SET KEEPTTL
			v := fmt.Sprintf("kt%d", rng.Intn(1000))
			old, exists := or.live(k)
			ok, err := d.Set([]byte(k), []byte(v), engine.SetOptions{KeepTTL: true})
			if err != nil || !ok {
				fail(step, "KEEPTTL: %v %v", ok, err)
			}
			exp := int64(0)
			if exists {
				exp = old.exp
			}
			or.m[k] = entry{v, exp}
		case op < 40: // DEL (multi-key, with duplicates)
			k2 := key()
			n, err := d.Del([][]byte{[]byte(k), []byte(k2), []byte(k)})
			want := 0
			seen := map[string]bool{}
			for _, x := range []string{k, k2, k} {
				if _, ok := or.live(x); ok && !seen[x] {
					want++
				}
				seen[x] = true
			}
			if err != nil || n != want {
				fail(step, "DEL %s %s: n=%d want %d err=%v", k, k2, n, want, err)
			}
			delete(or.m, k)
			delete(or.m, k2)
		case op < 48: // INCRBY
			delta := int64(rng.Intn(21) - 10)
			e, ok := or.live(k)
			cur, isInt := int64(0), true
			if ok {
				c, err := strconv.ParseInt(e.v, 10, 64)
				cur, isInt = c, err == nil
			}
			got, err := d.IncrBy([]byte(k), delta)
			switch {
			case !isInt && err == nil:
				fail(step, "INCRBY on non-integer %q succeeded", e.v)
			case isInt && (err != nil || got != cur+delta):
				fail(step, "INCRBY %s: got %d err %v want %d", k, got, err, cur+delta)
			case isInt:
				ne := entry{strconv.FormatInt(cur+delta, 10), 0}
				if ok {
					ne.exp = e.exp
				}
				or.m[k] = ne
			}
		case op < 54: // EXPIRE / PERSIST
			if rng.Intn(3) == 0 {
				ok, err := d.Persist([]byte(k))
				e, was := or.live(k)
				if err != nil || ok != (was && e.exp != 0) {
					fail(step, "PERSIST %s: ok=%v", k, ok)
				}
				if ok {
					e.exp = 0
					or.m[k] = e
				}
			} else {
				at := clk.Now() + int64(1+rng.Intn(300))
				ok, err := d.SetExpiry([]byte(k), at)
				e, was := or.live(k)
				if err != nil || ok != was {
					fail(step, "SetExpiry %s: ok=%v want %v", k, ok, was)
				}
				if ok {
					e.exp = at
					or.m[k] = e
				}
			}
		case op < 59: // MSET (atomic)
			k2, k3 := key(), key()
			err := d.MSet([][]byte{[]byte(k), []byte("m1"), []byte(k2), []byte("m2"), []byte(k3), []byte("m3")})
			if err != nil {
				fail(step, "MSET: %v", err)
			}
			or.m[k], or.m[k2], or.m[k3] = entry{"m1", 0}, entry{"m2", 0}, entry{"m3", 0}
		case op < 64: // advance the clock (expires keys)
			clk.Add(int64(rng.Intn(150)))
		case op < 70: // paged SCAN must equal the oracle exactly
			page := []int{1, 5, 17, 1000}[rng.Intn(4)]
			var got []string
			var next []byte
			for {
				ks, n, err := d.Scan(next, page)
				if err != nil {
					fail(step, "SCAN: %v", err)
				}
				for _, kk := range ks {
					got = append(got, string(kk))
				}
				if n == nil {
					break
				}
				next = n
			}
			want := or.liveKeys()
			if fmt.Sprint(got) != fmt.Sprint(want) {
				fail(step, "SCAN (page %d) differs:\n got %v\nwant %v", page, got, want)
			}
		case op < 73: // MGet from one snapshot
			ks := [][]byte{[]byte(k), []byte(key()), []byte(key())}
			vals, found, err := d.MGet(ks)
			if err != nil {
				fail(step, "MGET: %v", err)
			}
			for i, kk := range ks {
				e, ok := or.live(string(kk))
				if found[i] != ok || (ok && string(vals[i]) != e.v) {
					fail(step, "MGET %s = %q found=%v, want %q ok=%v", kk, vals[i], found[i], e.v, ok)
				}
			}
		case op < 76: // flush memtable now
			if err := d.FlushMemtable(); err != nil {
				fail(step, "FlushMemtable: %v", err)
			}
		case op < 78: // full compaction
			if err := d.CompactAll(); err != nil {
				fail(step, "CompactAll: %v", err)
			}
		case op < 81: // clean restart: replay + manifest must reproduce the exact state
			for _, sn := range snaps {
				sn.s.Close()
			}
			snaps = nil
			d.Close()
			d = open()
		case op < 84: // take a snapshot, to be verified later
			if len(snaps) < 3 {
				s, err := d.NewSnapshot()
				if err != nil {
					fail(step, "snapshot: %v", err)
				}
				snaps = append(snaps, snapshot{s, or.clone(clk.Now()), step})
			}
		case op < 87: // verify and release a snapshot
			if len(snaps) > 0 {
				i := rng.Intn(len(snaps))
				checkSnap(snaps[i], step)
				snaps[i].s.Close()
				snaps = append(snaps[:i], snaps[i+1:]...)
			}
		default: // point read
		}
		// The touched key must always agree.
		got, ok, err := d.Get([]byte(k))
		e, want := or.live(k)
		if err != nil || ok != want || (ok && string(got) != e.v) {
			fail(step, "GET %s = %q ok=%v err=%v; oracle %q ok=%v", k, got, ok, err, e.v, want)
		}
		if step%250 == 249 { // periodic full comparison
			for i := 0; i < 70; i++ {
				kk := fmt.Sprintf("key%02d", i)
				got, ok, _ := d.Get([]byte(kk))
				e, want := or.live(kk)
				if ok != want || (ok && string(got) != e.v) {
					fail(step, "periodic check %s = %q ok=%v; oracle %q ok=%v", kk, got, ok, e.v, want)
				}
			}
		}
	}
	for _, sn := range snaps {
		checkSnap(sn, steps)
		sn.s.Close()
	}
}

// ------------------------------------------------------------------ protocol-level model

type tclient struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

func render(v resp.Value) string {
	switch v.Kind {
	case resp.KindSimple:
		return "+" + string(v.Str)
	case resp.KindError:
		return "-" + string(v.Str)
	case resp.KindInt:
		return ":" + strconv.FormatInt(v.Int, 10)
	case resp.KindBulk:
		if v.Nil {
			return "(nil)"
		}
		return strconv.Quote(string(v.Str))
	case resp.KindArray:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return "?"
}

// TestProtocolModel drives a real RESP server with random pipelined command
// batches and compares every reply with a model of Redis semantics.
func TestProtocolModel(t *testing.T) {
	seeds, steps := envInt("NEBULA_MODEL_SEEDS", 8), envInt("NEBULA_MODEL_STEPS", 3000)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { runProtocolModel(t, seed, steps) })
	}
}

func runProtocolModel(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed * 7919))
	clk := newClock()
	db, _, err := engine.Open(engine.Options{
		Dir: t.TempDir(), Sync: wal.SyncNever, GroupCommit: true, Now: clk.Now, MemtableSize: 4 << 10, BloomBits: 10,
		Compaction: engine.CompactionOptions{L0CompactTrigger: 2, L1MaxBytes: 16 << 10, TargetFileBytes: 4 << 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(db, server.Options{Now: clk.Now})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(ln)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		db.Close()
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &tclient{conn, resp.NewReader(conn), resp.NewWriter(conn)}
	or := newOracle(clk)

	key := func() string { return fmt.Sprintf("p%02d", rng.Intn(40)) }
	ttlReply := func(k string, ms bool) string {
		e, ok := or.live(k)
		switch {
		case !ok:
			return ":-2"
		case e.exp == 0:
			return ":-1"
		}
		rem := e.exp - clk.Now()
		if !ms {
			rem = (rem + 500) / 1000
		}
		return ":" + strconv.FormatInt(rem, 10)
	}

	// Build one batch of 1..12 commands with their expected replies, then send
	// them pipelined and compare in order.
	for step := 0; step < steps/4; step++ {
		type cmd struct {
			args []string
			want string
		}
		var batch []cmd
		var advance int64 // time passes only BETWEEN batches, never inside one
		for n := 1 + rng.Intn(12); n > 0; n-- {
			k := key()
			var cm cmd
			switch r := rng.Intn(100); {
			case r < 18:
				v := strconv.Itoa(rng.Intn(1000))
				cm.args = []string{"SET", k, v}
				cm.want = "+OK"
				or.m[k] = entry{v, 0}
			case r < 26:
				v := strconv.Itoa(rng.Intn(1000))
				unit := []string{"EX", "PX"}[rng.Intn(2)]
				n := 1 + rng.Intn(5)
				ms := int64(n)
				if unit == "EX" {
					ms *= 1000
				}
				cm.args = []string{"SET", k, v, unit, strconv.Itoa(n)}
				cm.want = "+OK"
				or.m[k] = entry{v, clk.Now() + ms}
			case r < 32:
				v := strconv.Itoa(rng.Intn(1000))
				flag := []string{"NX", "XX"}[rng.Intn(2)]
				_, ex := or.live(k)
				cm.args = []string{"SET", k, v, flag}
				if (flag == "NX") != ex {
					cm.want = "+OK"
					or.m[k] = entry{v, 0}
				} else {
					cm.want = "(nil)"
				}
			case r < 42:
				cm.args = []string{"GET", k}
				if e, ok := or.live(k); ok {
					cm.want = strconv.Quote(e.v)
				} else {
					cm.want = "(nil)"
				}
			case r < 48:
				k2 := key()
				cm.args = []string{"DEL", k, k2}
				n := 0
				if _, ok := or.live(k); ok {
					n++
				}
				if _, ok := or.live(k2); ok && k2 != k {
					n++
				}
				cm.want = ":" + strconv.Itoa(n)
				delete(or.m, k)
				delete(or.m, k2)
			case r < 52:
				k2 := key()
				cm.args = []string{"EXISTS", k, k2, k}
				n := 0
				for _, x := range []string{k, k2, k} {
					if _, ok := or.live(x); ok {
						n++
					}
				}
				cm.want = ":" + strconv.Itoa(n)
			case r < 60:
				cm.args = []string{"INCR", k}
				if e, ok := or.live(k); ok {
					c, err := strconv.ParseInt(e.v, 10, 64)
					if err != nil {
						cm.want = "-ERR value is not an integer or out of range"
					} else {
						e.v = strconv.FormatInt(c+1, 10)
						or.m[k] = e
						cm.want = ":" + e.v
					}
				} else {
					or.m[k] = entry{"1", 0}
					cm.want = ":1"
				}
			case r < 64:
				cm.args = []string{"DECRBY", k, "5"}
				if e, ok := or.live(k); ok {
					c, err := strconv.ParseInt(e.v, 10, 64)
					if err != nil {
						cm.want = "-ERR value is not an integer or out of range"
					} else {
						e.v = strconv.FormatInt(c-5, 10)
						or.m[k] = e
						cm.want = ":" + e.v
					}
				} else {
					or.m[k] = entry{"-5", 0}
					cm.want = ":-5"
				}
			case r < 70:
				secs := rng.Intn(6) - 1 // -1, 0 delete the key (Redis semantics)
				cm.args = []string{"EXPIRE", k, strconv.Itoa(secs)}
				e, ok := or.live(k)
				switch {
				case !ok:
					cm.want = ":0"
				case secs <= 0:
					cm.want = ":1"
					delete(or.m, k)
				default:
					cm.want = ":1"
					e.exp = clk.Now() + int64(secs)*1000
					or.m[k] = e
				}
			case r < 73:
				cm.args = []string{"PERSIST", k}
				if e, ok := or.live(k); ok && e.exp != 0 {
					e.exp = 0
					or.m[k] = e
					cm.want = ":1"
				} else {
					cm.want = ":0"
				}
			case r < 77:
				cm.args = []string{"TTL", k}
				cm.want = ttlReply(k, false)
			case r < 80:
				cm.args = []string{"PTTL", k}
				cm.want = ttlReply(k, true)
			case r < 85:
				k2 := key()
				cm.args = []string{"MGET", k, k2}
				val := func(x string) string {
					if e, ok := or.live(x); ok {
						return strconv.Quote(e.v)
					}
					return "(nil)"
				}
				cm.want = "[" + val(k) + " " + val(k2) + "]"
			case r < 88:
				k2 := key()
				cm.args = []string{"MSET", k, "a", k2, "b"}
				cm.want = "+OK"
				or.m[k] = entry{"a", 0}
				or.m[k2] = entry{"b", 0}
			case r < 91:
				advance += int64(rng.Intn(1500))
				continue // not a command; time passes after this batch has run
			case r < 96:
				// SCAN with MATCH, fully paged; compare the resulting key set.
				pattern := []string{"p0*", "p1?", "p[23]*", "*"}[rng.Intn(4)]
				cm.args = nil // handled after the batch is flushed
				_ = pattern
				continue
			default:
				cm.args = []string{"PING"}
				cm.want = "+PONG"
			}
			batch = append(batch, cm)
		}
		for _, cm := range batch {
			bs := make([][]byte, len(cm.args))
			for i, a := range cm.args {
				bs[i] = []byte(a)
			}
			c.w.WriteCommand(bs...)
		}
		if err := c.w.Flush(); err != nil {
			t.Fatal(err)
		}
		for i, cm := range batch {
			c.c.SetReadDeadline(time.Now().Add(10 * time.Second))
			v, err := c.r.ReadValue()
			if err != nil {
				t.Fatalf("seed %d step %d cmd %d %v: %v", seed, step, i, cm.args, err)
			}
			if got := render(v); got != cm.want {
				t.Fatalf("seed %d step %d (batch pos %d): %v\n  got  %s\n  want %s", seed, step, i, cm.args, got, cm.want)
			}
		}
		clk.Add(advance)
		// Full SCAN after the batch, via cursors, with a random MATCH.
		if step%7 == 0 {
			pattern := []string{"p0*", "p1?", "p[23]*", "*"}[rng.Intn(4)]
			var got []string
			cursor := "0"
			for guard := 0; guard < 1000; guard++ {
				c.w.WriteCommand([]byte("SCAN"), []byte(cursor), []byte("MATCH"), []byte(pattern), []byte("COUNT"), []byte("7"))
				c.w.Flush()
				v, err := c.r.ReadValue()
				if err != nil || v.Kind != resp.KindArray {
					t.Fatalf("seed %d: SCAN: %v %v", seed, err, v)
				}
				cursor = string(v.Elems[0].Str)
				for _, e := range v.Elems[1].Elems {
					got = append(got, string(e.Str))
				}
				if cursor == "0" {
					break
				}
			}
			var want []string
			for _, kk := range or.liveKeys() {
				if globMatch(pattern, kk) {
					want = append(want, kk)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("seed %d step %d: SCAN MATCH %s differs:\n got %v\nwant %v", seed, step, pattern, got, want)
			}
		}
	}
}

// globMatch is an independent, deliberately naive recursive implementation used
// as the oracle for the server's iterative matcher (patterns here use only
// literal characters, '*', '?' and one [..] class).
func globMatch(p, s string) bool {
	if p == "" {
		return s == ""
	}
	switch p[0] {
	case '*':
		for i := 0; i <= len(s); i++ {
			if globMatch(p[1:], s[i:]) {
				return true
			}
		}
		return false
	case '?':
		return s != "" && globMatch(p[1:], s[1:])
	case '[':
		end := strings.IndexByte(p, ']')
		if s == "" || end < 0 {
			return false
		}
		if strings.IndexByte(p[1:end], s[0]) < 0 {
			return false
		}
		return globMatch(p[end+1:], s[1:])
	}
	return s != "" && s[0] == p[0] && globMatch(p[1:], s[1:])
}
