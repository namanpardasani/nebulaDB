package repl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func openDB(t *testing.T, dir string, onCommit func(uint64, []wal.Op)) *engine.DB {
	t.Helper()
	db, _, err := engine.Open(engine.Options{FS: vfs.Default, Dir: dir, Sync: wal.SyncNever, GroupCommit: true,
		MemtableSize: 64 << 10, BloomBits: 10, OnCommit: onCommit, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type leaderNode struct {
	db   *engine.DB
	l    *Leader
	addr string
	ln   net.Listener
}

func startLeader(t *testing.T, dir string, backlog int64, addr string) *leaderNode {
	t.Helper()
	l, err := NewLeader(dir, backlog, quiet)
	if err != nil {
		t.Fatal(err)
	}
	db := openDB(t, dir, l.OnCommit)
	l.Attach(db)
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go l.Serve(ln)
	return &leaderNode{db: db, l: l, addr: ln.Addr().String(), ln: ln}
}

func (n *leaderNode) stop(t *testing.T) {
	t.Helper()
	n.l.Close()
	if err := n.db.Close(); err != nil {
		t.Fatal(err)
	}
}

type followerNode struct {
	db     *engine.DB
	f      *Follower
	cancel context.CancelFunc
}

func startFollower(t *testing.T, dir, leaderAddr string) *followerNode {
	t.Helper()
	db := openDB(t, dir, nil)
	f, err := NewFollower(db, dir, leaderAddr, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go f.Run(ctx)
	return &followerNode{db: db, f: f, cancel: cancel}
}

func (n *followerNode) stop(t *testing.T) {
	t.Helper()
	n.cancel()
	n.f.Wait()
	if err := n.db.Close(); err != nil {
		t.Fatal(err)
	}
}

type kv struct {
	v   string
	exp int64
}

func dump(t *testing.T, db *engine.DB) map[string]kv {
	t.Helper()
	s, err := db.NewSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	it := s.NewIterator()
	defer it.Close()
	out := map[string]kv{}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out[string(it.Key())] = kv{string(it.Value()), it.ExpireAt()}
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func waitConverged(t *testing.T, leader, follower *engine.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var a, b map[string]kv
	for time.Now().Before(deadline) {
		a, b = dump(t, leader), dump(t, follower)
		if len(a) == len(b) {
			same := true
			for k, v := range a {
				if b[k] != v {
					same = false
					break
				}
			}
			if same {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("not converged: key %q leader=%v follower=%v (leader has %d keys, follower %d)", k, v, b[k], len(a), len(b))
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			t.Fatalf("not converged: follower has extra key %q", k)
		}
	}
	t.Fatal("not converged")
}

func write(t *testing.T, db *engine.DB, rng *rand.Rand, n int) {
	t.Helper()
	far := time.Now().Add(time.Hour).UnixMilli()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key:%05d", rng.Intn(3000)))
		switch rng.Intn(10) {
		case 0:
			if _, err := db.Del([][]byte{k}); err != nil {
				t.Fatal(err)
			}
		case 1:
			if _, err := db.Set(k, []byte(fmt.Sprint(rng.Int())), engine.SetOptions{ExpireAtMs: far}); err != nil {
				t.Fatal(err)
			}
		case 2:
			if _, err := db.IncrBy([]byte("counter"), 1); err != nil {
				t.Fatal(err)
			}
		case 3:
			if err := db.MSet([][]byte{k, []byte("a"), []byte(fmt.Sprintf("key:%05d", rng.Intn(3000))), []byte("b")}); err != nil {
				t.Fatal(err)
			}
		default:
			if _, err := db.Set(k, bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, 20+rng.Intn(200)), engine.SetOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSnapshotCatchUpThenStream(t *testing.T) {
	root := t.TempDir()
	ld := startLeader(t, filepath.Join(root, "l"), 64<<20, "")
	defer ld.stop(t)
	rng := rand.New(rand.NewSource(1))
	write(t, ld.db, rng, 4000) // before any follower exists: must arrive via snapshot (ring only has recent batches... all of them here)

	fl := startFollower(t, filepath.Join(root, "f"), ld.addr)
	defer fl.stop(t)
	waitConverged(t, ld.db, fl.db)
	write(t, ld.db, rng, 2000) // streamed
	waitConverged(t, ld.db, fl.db)
	if got := fl.f.Stats(); len(got) == 0 {
		t.Fatal("no stats")
	}
}

func TestFollowerRestartContinuesWithoutResync(t *testing.T) {
	root := t.TempDir()
	ld := startLeader(t, filepath.Join(root, "l"), 64<<20, "")
	defer ld.stop(t)
	fdir := filepath.Join(root, "f")
	rng := rand.New(rand.NewSource(2))
	write(t, ld.db, rng, 500)
	fl := startFollower(t, fdir, ld.addr)
	waitConverged(t, ld.db, fl.db)
	fl.stop(t)

	write(t, ld.db, rng, 1500) // while the follower is down
	fl2 := startFollower(t, fdir, ld.addr)
	defer fl2.stop(t)
	waitConverged(t, ld.db, fl2.db)
	if n := fl2.f.resyncs.Load(); n != 0 {
		t.Fatalf("restart caused %d full resyncs; expected to continue from the saved position", n)
	}
	if ld.l.conts.Load() == 0 {
		t.Fatal("leader never served a partial resync")
	}
}

func TestBacklogOverflowForcesResyncAndRemovesStaleKeys(t *testing.T) {
	root := t.TempDir()
	ld := startLeader(t, filepath.Join(root, "l"), 4<<10, "") // tiny backlog
	defer ld.stop(t)
	fdir := filepath.Join(root, "f")
	rng := rand.New(rand.NewSource(3))
	write(t, ld.db, rng, 300)
	fl := startFollower(t, fdir, ld.addr)
	waitConverged(t, ld.db, fl.db)
	fl.stop(t)

	// While the follower is away: delete everything it knows and write much more than the backlog holds.
	for k := range dump(t, ld.db) {
		if _, err := ld.db.Del([][]byte{[]byte(k)}); err != nil {
			t.Fatal(err)
		}
	}
	write(t, ld.db, rng, 1500)
	fl2 := startFollower(t, fdir, ld.addr)
	defer fl2.stop(t)
	waitConverged(t, ld.db, fl2.db) // also proves keys deleted on the leader are gone on the follower
	if fl2.f.resyncs.Load() == 0 {
		t.Fatal("expected a full resync after the backlog was exceeded")
	}
}

func TestLeaderRestartKeepsIdentityAndFollowerCatchesUp(t *testing.T) {
	root := t.TempDir()
	ldir := filepath.Join(root, "l")
	ld := startLeader(t, ldir, 64<<20, "")
	addr := ld.addr
	rng := rand.New(rand.NewSource(4))
	write(t, ld.db, rng, 800)
	fl := startFollower(t, filepath.Join(root, "f"), addr)
	defer fl.stop(t)
	waitConverged(t, ld.db, fl.db)
	id := ld.l.id
	ld.stop(t)

	ld2 := startLeader(t, ldir, 64<<20, addr) // same dir, same port
	defer ld2.stop(t)
	if ld2.l.id != id {
		t.Fatal("leader identity changed across a restart")
	}
	write(t, ld2.db, rng, 800)
	waitConverged(t, ld2.db, fl.db)
}

func TestNewLeaderDirectoryForcesResyncAndWipesOldData(t *testing.T) {
	root := t.TempDir()
	ld := startLeader(t, filepath.Join(root, "l1"), 64<<20, "")
	rng := rand.New(rand.NewSource(5))
	write(t, ld.db, rng, 600)
	fdir := filepath.Join(root, "f")
	fl := startFollower(t, fdir, ld.addr)
	waitConverged(t, ld.db, fl.db)
	fl.stop(t)
	addr := ld.addr
	ld.stop(t)

	// A different leader (fresh directory => new identity) appears at the same address.
	ld2 := startLeader(t, filepath.Join(root, "l2"), 64<<20, addr)
	defer ld2.stop(t)
	if err := ld2.db.MSet([][]byte{[]byte("only"), []byte("here")}); err != nil {
		t.Fatal(err)
	}
	fl2 := startFollower(t, fdir, addr)
	defer fl2.stop(t)
	waitConverged(t, ld2.db, fl2.db)
	if len(dump(t, fl2.db)) != 1 {
		t.Fatal("old leader's keys survived on the follower")
	}
}

func TestSyncAcksMeanDurableOnReplicaBeforeReply(t *testing.T) {
	root := t.TempDir()
	ld := startLeader(t, filepath.Join(root, "l"), 64<<20, "")
	defer ld.stop(t)
	se := &SyncEngine{Engine: ld.db, L: ld.l, MinAcks: 1, Wait: 300 * time.Millisecond}

	// No follower: the write applies locally, the wait fails.
	if _, err := se.Set([]byte("orphan"), []byte("1"), engine.SetOptions{}); !errors.Is(err, ErrNotEnoughAcks) {
		t.Fatalf("want ErrNotEnoughAcks without followers, got %v", err)
	}
	if v, ok, _ := ld.db.Get([]byte("orphan")); !ok || string(v) != "1" {
		t.Fatal("documented behaviour: the write must be applied locally even if the ack wait fails")
	}

	fl := startFollower(t, filepath.Join(root, "f"), ld.addr)
	defer fl.stop(t)
	waitConverged(t, ld.db, fl.db)
	se.Wait = 5 * time.Second
	// Concurrent writers: every OK reply must already be applied on the follower.
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				k := []byte(fmt.Sprintf("w%d:%d", w, i))
				if _, err := se.Set(k, []byte("v"), engine.SetOptions{}); err != nil {
					t.Error(err)
					return
				}
				if _, ok, _ := fl.db.Get(k); !ok { // read immediately, NO waiting
					t.Errorf("acknowledged write %s not on the replica when the reply returned", k)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestReadOnlyReplicaRejectsWrites(t *testing.T) {
	root := t.TempDir()
	db := openDB(t, filepath.Join(root, "f"), nil)
	defer db.Close()
	ro := ReadOnly{Engine: db, F: &Follower{db: db, log: quiet, done: make(chan struct{})}}
	if _, err := ro.Set([]byte("k"), []byte("v"), engine.SetOptions{}); !errors.Is(err, engine.ErrReadOnly) {
		t.Fatalf("got %v", err)
	}
	if _, err := ro.IncrBy([]byte("k"), 1); !errors.Is(err, engine.ErrReadOnly) {
		t.Fatalf("got %v", err)
	}
	if _, _, err := ro.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolRejectsCorruptionAndGarbage(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if err := writeFrame(w, frameBatch, encodeBatch(7, []wal.Op{{Type: wal.OpPut, Key: []byte("k"), Value: []byte("v"), ExpireAt: 5}})); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	good := append([]byte(nil), buf.Bytes()...)
	typ, p, err := readFrame(bufio.NewReader(bytes.NewReader(good)))
	if err != nil || typ != frameBatch {
		t.Fatal(err)
	}
	first, ops, err := decodeBatch(p)
	if err != nil || first != 7 || len(ops) != 1 || ops[0].ExpireAt != 5 || string(ops[0].Value) != "v" {
		t.Fatalf("round trip: %v %v %v", first, ops, err)
	}
	for i := range good { // every single-bit flip must be detected
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x10
		if _, _, err := readFrame(bufio.NewReader(bytes.NewReader(bad))); err == nil {
			t.Fatalf("bit flip at byte %d went undetected", i)
		}
	}
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 2000; i++ { // garbage never panics
		b := make([]byte, rng.Intn(80))
		rng.Read(b)
		_, _, _ = decodeBatch(b)
	}
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add(encodeBatch(1, []wal.Op{{Type: wal.OpPut, Key: []byte("a"), Value: []byte("b")}}))
	f.Fuzz(func(t *testing.T, b []byte) { _, _, _ = decodeBatch(b) })
}
