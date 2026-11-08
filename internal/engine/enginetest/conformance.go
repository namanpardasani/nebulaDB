// Package enginetest holds the behavioural contract tests every
// engine.Engine implementation must pass (Mem now, the LSM engine later).
package enginetest

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/naman/nebuladb/internal/engine"
)

// Factory builds a fresh empty engine reading time from now (unix ms).
type Factory func(t *testing.T, now func() int64) engine.Engine

// Clock is a manually advanced clock.
type Clock struct{ ms atomic.Int64 }

// NewClock returns a clock starting at a fixed nonzero time.
func NewClock() *Clock {
	c := &Clock{}
	c.ms.Store(1_000_000)
	return c
}

// Now returns the current fake unix-ms time.
func (c *Clock) Now() int64 { return c.ms.Load() }

// Advance moves the clock forward by d milliseconds.
func (c *Clock) Advance(d int64) { c.ms.Add(d) }

func b(s string) []byte { return []byte(s) }

// Run executes the whole suite against engines from f.
func Run(t *testing.T, f Factory) {
	cases := []struct {
		name string
		fn   func(*testing.T, engine.Engine, *Clock)
	}{
		{"SetGetDel", setGetDel},
		{"NXXX", nxxx},
		{"TTL", ttl},
		{"KeepTTL", keepTTL},
		{"Incr", incr},
		{"MSet", mset},
		{"Scan", scan},
		{"FlushAndSize", flushAndSize},
		{"BinaryKeys", binaryKeys},
		{"Concurrent", concurrent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clk := NewClock()
			e := f(t, clk.Now)
			defer e.Close()
			c.fn(t, e, clk)
		})
	}
}

func get(t *testing.T, e engine.Engine, k string) (string, bool) {
	t.Helper()
	v, ok, err := e.Get(b(k))
	if err != nil {
		t.Fatal(err)
	}
	return string(v), ok
}

func set(t *testing.T, e engine.Engine, k, v string, o engine.SetOptions) bool {
	t.Helper()
	ok, err := e.Set(b(k), b(v), o)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func setGetDel(t *testing.T, e engine.Engine, _ *Clock) {
	if _, ok := get(t, e, "a"); ok {
		t.Fatal("missing key found")
	}
	set(t, e, "a", "1", engine.SetOptions{})
	set(t, e, "a", "2", engine.SetOptions{}) // overwrite
	if v, ok := get(t, e, "a"); !ok || v != "2" {
		t.Fatalf("got %q %v", v, ok)
	}
	set(t, e, "empty", "", engine.SetOptions{})
	if v, ok := get(t, e, "empty"); !ok || v != "" {
		t.Fatalf("empty value: %q %v", v, ok)
	}
	n, err := e.Del([][]byte{b("a"), b("a"), b("nope"), b("empty")})
	if err != nil || n != 2 {
		t.Fatalf("del n=%d err=%v (duplicates and missing must not count)", n, err)
	}
	if ok, _ := e.Exists(b("a")); ok {
		t.Fatal("deleted key exists")
	}
	// Returned slices belong to the caller: mutating them must not corrupt the store.
	set(t, e, "k", "abc", engine.SetOptions{})
	v, _, _ := e.Get(b("k"))
	v[0] = 'X'
	if got, _ := get(t, e, "k"); got != "abc" {
		t.Fatalf("store aliased caller memory: %q", got)
	}
}

func nxxx(t *testing.T, e engine.Engine, _ *Clock) {
	if set(t, e, "k", "v", engine.SetOptions{XX: true}) {
		t.Fatal("XX on missing key applied")
	}
	if !set(t, e, "k", "v1", engine.SetOptions{NX: true}) {
		t.Fatal("NX on missing key rejected")
	}
	if set(t, e, "k", "v2", engine.SetOptions{NX: true}) {
		t.Fatal("NX on existing key applied")
	}
	if !set(t, e, "k", "v3", engine.SetOptions{XX: true}) {
		t.Fatal("XX on existing key rejected")
	}
	if v, _ := get(t, e, "k"); v != "v3" {
		t.Fatalf("got %q", v)
	}
}

func ttl(t *testing.T, e engine.Engine, clk *Clock) {
	if ms, _ := e.TTL(b("none")); ms != -2 {
		t.Fatalf("missing ttl=%d", ms)
	}
	set(t, e, "p", "v", engine.SetOptions{})
	if ms, _ := e.TTL(b("p")); ms != -1 {
		t.Fatalf("persistent ttl=%d", ms)
	}
	set(t, e, "e", "v", engine.SetOptions{ExpireAtMs: clk.Now() + 1000})
	if ms, _ := e.TTL(b("e")); ms != 1000 {
		t.Fatalf("ttl=%d", ms)
	}
	clk.Advance(999)
	if _, ok := get(t, e, "e"); !ok {
		t.Fatal("expired 1ms early")
	}
	clk.Advance(1) // exactly at expiry: expired
	if _, ok := get(t, e, "e"); ok {
		t.Fatal("key alive at its expiry time")
	}
	if ok, _ := e.Exists(b("e")); ok {
		t.Fatal("Exists true for expired key")
	}
	// SetExpiry / Persist
	if ok, _ := e.SetExpiry(b("missing"), clk.Now()+5); ok {
		t.Fatal("SetExpiry on missing key")
	}
	if ok, _ := e.SetExpiry(b("p"), clk.Now()+500); !ok {
		t.Fatal("SetExpiry failed")
	}
	if ok, _ := e.Persist(b("p")); !ok {
		t.Fatal("Persist failed")
	}
	if ok, _ := e.Persist(b("p")); ok {
		t.Fatal("Persist on persistent key returned true")
	}
	clk.Advance(10_000)
	if _, ok := get(t, e, "p"); !ok {
		t.Fatal("persisted key expired")
	}
	// Overwriting without KeepTTL clears the TTL.
	set(t, e, "o", "1", engine.SetOptions{ExpireAtMs: clk.Now() + 100})
	set(t, e, "o", "2", engine.SetOptions{})
	clk.Advance(1000)
	if _, ok := get(t, e, "o"); !ok {
		t.Fatal("plain SET must clear the old TTL")
	}
	// A key that expired can be re-created with NX.
	set(t, e, "n", "1", engine.SetOptions{ExpireAtMs: clk.Now() + 10})
	clk.Advance(10)
	if !set(t, e, "n", "2", engine.SetOptions{NX: true}) {
		t.Fatal("NX must succeed on an expired key")
	}
	if n, _ := e.Del([][]byte{b("n")}); n != 1 {
		t.Fatal("del of recreated key")
	}
}

func keepTTL(t *testing.T, e engine.Engine, clk *Clock) {
	set(t, e, "k", "1", engine.SetOptions{ExpireAtMs: clk.Now() + 100})
	set(t, e, "k", "2", engine.SetOptions{KeepTTL: true})
	clk.Advance(100)
	if _, ok := get(t, e, "k"); ok {
		t.Fatal("KEEPTTL lost the expiry")
	}
}

func incr(t *testing.T, e engine.Engine, clk *Clock) {
	for i, want := range []int64{1, 2, 3} {
		got, err := e.IncrBy(b("c"), 1)
		if err != nil || got != want {
			t.Fatalf("step %d got %d err %v", i, got, err)
		}
	}
	if got, _ := e.IncrBy(b("c"), -10); got != -7 {
		t.Fatalf("got %d", got)
	}
	set(t, e, "s", "abc", engine.SetOptions{})
	if _, err := e.IncrBy(b("s"), 1); !errors.Is(err, engine.ErrNotInteger) {
		t.Fatalf("want ErrNotInteger, got %v", err)
	}
	set(t, e, "max", "9223372036854775807", engine.SetOptions{})
	if _, err := e.IncrBy(b("max"), 1); !errors.Is(err, engine.ErrNotInteger) {
		t.Fatalf("overflow: got %v", err)
	}
	set(t, e, "min", "-9223372036854775808", engine.SetOptions{})
	if _, err := e.IncrBy(b("min"), -1); !errors.Is(err, engine.ErrNotInteger) {
		t.Fatalf("underflow: got %v", err)
	}
	// INCR keeps the TTL.
	set(t, e, "t", "5", engine.SetOptions{ExpireAtMs: clk.Now() + 100})
	e.IncrBy(b("t"), 1)
	clk.Advance(100)
	if _, ok := get(t, e, "t"); ok {
		t.Fatal("INCR must not clear TTL")
	}
	// INCR on an expired key starts from 0.
	if got, _ := e.IncrBy(b("t"), 1); got != 1 {
		t.Fatalf("incr on expired: %d", got)
	}
}

func mset(t *testing.T, e engine.Engine, _ *Clock) {
	if err := e.MSet([][]byte{b("a"), b("1"), b("b"), b("2")}); err != nil {
		t.Fatal(err)
	}
	if v, _ := get(t, e, "b"); v != "2" {
		t.Fatal("mset lost b")
	}
}

func scan(t *testing.T, e engine.Engine, clk *Clock) {
	var want []string
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%03d", i)
		want = append(want, k)
		set(t, e, k, "v", engine.SetOptions{})
	}
	set(t, e, "dead", "v", engine.SetOptions{ExpireAtMs: clk.Now() + 1})
	clk.Advance(5)
	var got []string
	var start []byte
	for iter := 0; ; iter++ {
		if iter > 100 {
			t.Fatal("scan did not terminate")
		}
		keys, next, err := e.Scan(start, 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) > 7 {
			t.Fatalf("page of %d > limit", len(keys))
		}
		for _, k := range keys {
			got = append(got, string(k))
		}
		if next == nil {
			break
		}
		start = next
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("scan order/content mismatch:\n got %v\nwant %v", got, want)
	}
}

func flushAndSize(t *testing.T, e engine.Engine, _ *Clock) {
	for i := 0; i < 10; i++ {
		set(t, e, fmt.Sprintf("k%d", i), "v", engine.SetOptions{})
	}
	if n, _ := e.ApproxSize(); n < 10 {
		t.Fatalf("size %d < 10 live keys", n)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, ok := get(t, e, "k3"); ok {
		t.Fatal("key survived Flush")
	}
	keys, _, _ := e.Scan(nil, 100)
	if len(keys) != 0 {
		t.Fatalf("scan after flush: %q", keys)
	}
	set(t, e, "after", "1", engine.SetOptions{})
	if _, ok := get(t, e, "after"); !ok {
		t.Fatal("write after Flush lost")
	}
}

func binaryKeys(t *testing.T, e engine.Engine, _ *Clock) {
	k := []byte{0, 1, 255, '\r', '\n', 0}
	v := []byte{255, 0, 0, '\n'}
	e.Set(k, v, engine.SetOptions{})
	e.Set([]byte{}, []byte("emptykey"), engine.SetOptions{})
	got, ok, _ := e.Get(k)
	if !ok || !bytes.Equal(got, v) {
		t.Fatalf("binary round trip: %v %v", got, ok)
	}
	got, ok, _ = e.Get([]byte{})
	if !ok || string(got) != "emptykey" {
		t.Fatalf("empty key: %q %v", got, ok)
	}
}

func concurrent(t *testing.T, e engine.Engine, _ *Clock) {
	const workers, per = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				e.IncrBy(b("ctr"), 1)
				k := fmt.Sprintf("w%d-%d", w, i)
				e.Set(b(k), b(k), engine.SetOptions{})
				e.Get(b(k))
			}
		}()
	}
	wg.Wait()
	if v, _ := get(t, e, "ctr"); v != fmt.Sprint(workers*per) {
		t.Fatalf("lost increments: %s", v)
	}
}
