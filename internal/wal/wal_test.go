package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/naman/nebuladb/internal/vfs"
)

const dir = "/wal"

// memDir returns a Faulty FS rooted in a real temp dir.
func newFS(t *testing.T) (*vfs.Faulty, string) {
	t.Helper()
	d := t.TempDir()
	return vfs.NewFaulty(vfs.Default), d
}

func opts(fs vfs.FS, d string, p SyncPolicy) Options {
	return Options{FS: fs, Dir: d, Policy: p, GroupCommit: true, Interval: 5 * time.Millisecond}
}

func put(k, v string) *Batch {
	return &Batch{Ops: []Op{{Type: OpPut, Key: []byte(k), Value: []byte(v)}}}
}

type rec struct {
	seq uint64
	k   string
	v   string
	t   OpType
	exp int64
}

// collect opens the WAL, gathering replayed ops, and returns it.
func collect(t *testing.T, o Options) (*WAL, RecoveryInfo, []rec) {
	t.Helper()
	var got []rec
	w, info, err := Open(o, func(b *Batch) error {
		for i, op := range b.Ops {
			got = append(got, rec{b.Seq + uint64(i), string(op.Key), string(op.Value), op.Type, op.ExpireAt})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w, info, got
}

func TestRoundTrip(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, info, got := collect(t, o)
	if len(got) != 0 || info.Batches != 0 {
		t.Fatal("fresh wal not empty")
	}
	var applied []uint64
	commit := func(b *Batch) {
		if err := w.Commit(b, func(s uint64) { applied = append(applied, s) }); err != nil {
			t.Fatal(err)
		}
	}
	commit(put("a", "1"))
	commit(&Batch{Ops: []Op{
		{Type: OpPut, Key: []byte("b"), Value: []byte("2"), ExpireAt: 12345},
		{Type: OpDelete, Key: []byte("a")},
		{Type: OpPut, Key: []byte{0, 255, '\n'}, Value: nil},
	}})
	commit(put("", "emptykey"))
	if fmt.Sprint(applied) != "[1 2 5]" {
		t.Fatalf("apply seqs %v", applied)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, info, got := collect(t, o)
	defer w2.Close()
	want := []rec{
		{1, "a", "1", OpPut, 0},
		{2, "b", "2", OpPut, 12345},
		{3, "a", "", OpDelete, 0},
		{4, "\x00\xff\n", "", OpPut, 0},
		{5, "", "emptykey", OpPut, 0},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
	if info.LastSeq != 5 || info.TornTail || w2.LastSeq() != 5 {
		t.Fatalf("info %+v", info)
	}
	// Sequence numbers continue after recovery.
	var s uint64
	w2.Commit(put("z", "z"), func(x uint64) { s = x })
	if s != 6 {
		t.Fatalf("next seq %d", s)
	}
}

// Cut the log at every possible byte offset inside the final record: recovery
// must always yield exactly the earlier records, flag the torn tail (unless the
// cut is on a record boundary), and allow appending afterwards.
func TestTornTailEveryOffset(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	for i := 0; i < 3; i++ {
		w.Commit(put(fmt.Sprintf("k%d", i), fmt.Sprintf("value-%d", i)), nil)
	}
	size := w.Size()
	// Size of the first two records:
	b1, _ := appendRecord(nil, &Batch{Seq: 1, Ops: put("k0", "value-0").Ops})
	b2, _ := appendRecord(nil, &Batch{Seq: 2, Ops: put("k1", "value-1").Ops})
	boundary := int64(len(b1) + len(b2))
	w.Close()
	full, _ := os.ReadFile(SegmentPath(d, 1))
	if int64(len(full)) != size {
		t.Fatal("size mismatch")
	}

	for cut := boundary; cut <= size; cut++ {
		if err := os.WriteFile(SegmentPath(d, 1), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		w, info, got := collect(t, o)
		wantRecs := 2
		if cut == size {
			wantRecs = 3
		}
		if len(got) != wantRecs {
			t.Fatalf("cut=%d: replayed %d records, want %d", cut, len(got), wantRecs)
		}
		torn := cut != boundary && cut != size
		if info.TornTail != torn {
			t.Fatalf("cut=%d: TornTail=%v want %v", cut, info.TornTail, torn)
		}
		// Appending after recovery works and survives another restart.
		if err := w.Commit(put("after", "x"), nil); err != nil {
			t.Fatal(err)
		}
		w.Close()
		w, _, got = collect(t, o)
		if got[len(got)-1].k != "after" {
			t.Fatalf("cut=%d: record appended after recovery lost: %v", cut, got)
		}
		w.Close()
	}
}

func TestZeroedTailIsTorn(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	w.Commit(put("a", "1"), nil)
	w.Close()
	f, _ := os.OpenFile(SegmentPath(d, 1), os.O_APPEND|os.O_WRONLY, 0)
	f.Write(make([]byte, 100)) // filesystems may extend a file with zeros on crash
	f.Close()
	w, info, got := collect(t, o)
	defer w.Close()
	if len(got) != 1 || !info.TornTail || info.TruncatedBytes != 100 {
		t.Fatalf("got=%v info=%+v", got, info)
	}
}

func TestMidFileCorruptionRefused(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	for i := 0; i < 5; i++ {
		w.Commit(put(fmt.Sprintf("key%d", i), "some value here"), nil)
	}
	w.Close()
	data, _ := os.ReadFile(SegmentPath(d, 1))
	rl := len(data) / 5

	// Flip a bit in each byte of record #2 (a middle record): header or payload,
	// it must never be silently accepted nor treated as a torn tail.
	for off := 2 * rl; off < 3*rl; off++ {
		bad := append([]byte(nil), data...)
		bad[off] ^= 0x10
		os.WriteFile(SegmentPath(d, 1), bad, 0o644)
		_, _, err := Open(o, nil)
		var ce *CorruptionError
		if !errors.As(err, &ce) {
			t.Fatalf("flip at byte %d: want CorruptionError, got %v", off, err)
		}
	}
	// The same flip in the LAST record is indistinguishable from a torn write and
	// is truncated (with a report), never replayed wrong.
	bad := append([]byte(nil), data...)
	bad[len(bad)-3] ^= 0xff
	os.WriteFile(SegmentPath(d, 1), bad, 0o644)
	w, info, got := collect(t, o)
	defer w.Close()
	if len(got) != 4 || !info.TornTail {
		t.Fatalf("got %d recs, info %+v", len(got), info)
	}
}

func TestTornTailInOldSegmentIsCorruption(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	w.Commit(put("a", "1"), nil)
	w.Commit(put("b", "2"), nil)
	w.Rotate()
	w.Commit(put("c", "3"), nil)
	w.Close()
	data, _ := os.ReadFile(SegmentPath(d, 1))
	os.WriteFile(SegmentPath(d, 1), data[:len(data)-4], 0o644)
	_, _, err := Open(o, nil)
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Segment != 1 {
		t.Fatalf("got %v", err)
	}
}

func TestSequenceRegressionDetected(t *testing.T) {
	r1, _ := appendRecord(nil, &Batch{Seq: 5, Ops: put("a", "1").Ops})
	r2, _ := appendRecord(nil, &Batch{Seq: 3, Ops: put("b", "1").Ops})
	_, err := ScanSegment(append(r1, r2...), 0, func(*Batch) error { return nil })
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("got %v", err)
	}
}

func TestRotateAndRemove(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncNever)
	w, _, _ := collect(t, o)
	w.Commit(put("a", "1"), nil)
	n, err := w.Rotate()
	if err != nil || n != 2 || w.ActiveSegment() != 2 {
		t.Fatalf("rotate: %d %v", n, err)
	}
	w.Commit(put("b", "2"), nil)
	n, _ = w.Rotate()
	w.Commit(put("c", "3"), nil)
	if segs, _ := ListSegments(fs, d); fmt.Sprint(segs) != "[1 2 3]" {
		t.Fatalf("segs %v", segs)
	}
	if err := w.RemoveBefore(3); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if segs, _ := ListSegments(fs, d); fmt.Sprint(segs) != "[3]" {
		t.Fatalf("segs %v", segs)
	}
	// MinSegment drops obsolete files on open, and replay skips them.
	w, _, _ = collect(t, o)
	w.Rotate()
	w.Commit(put("d", "4"), nil)
	w.Close()
	o.MinSegment = 4
	w, info, got := collect(t, o)
	defer w.Close()
	if fmt.Sprint(info.RemovedStale) != "[3]" || len(got) != 1 || got[0].k != "d" {
		t.Fatalf("info %+v got %v", info, got)
	}
	// Sequence numbers must continue above the pre-existing ones even though
	// segment 3 (seq 3) is gone.
	var s uint64
	w.Commit(put("e", "5"), func(x uint64) { s = x })
	if s != 5 { // a=1 b=2 c=3 d=4 -> e=5, even though segment 3 (seq 3) was deleted
		t.Fatalf("seq %d", s)
	}
}

func TestStartSeqRaisesNextSeq(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	o.StartSeq = 100
	w, _, _ := collect(t, o)
	defer w.Close()
	var s uint64
	w.Commit(put("a", "1"), func(x uint64) { s = x })
	if s != 101 {
		t.Fatalf("seq %d", s)
	}
}

func TestGroupCommitBatchesFsyncs(t *testing.T) {
	run := func(group bool) (commits, syncs int, maxGroup uint64) {
		fs, d := newFS(t)
		fs.SetSyncDelay(2 * time.Millisecond) // slow disk
		o := opts(fs, d, SyncAlways)
		o.GroupCommit = group
		w, _, _ := collect(t, o)
		const writers, per = 32, 15
		var wg sync.WaitGroup
		var mu sync.Mutex
		var order []uint64
		for g := 0; g < writers; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < per; i++ {
					err := w.Commit(put(fmt.Sprintf("g%d-%d", g, i), "v"), func(s uint64) {
						mu.Lock() // apply callbacks are serialised, but be explicit for -race
						order = append(order, s)
						mu.Unlock()
					})
					if err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		for i := 1; i < len(order); i++ {
			if order[i] <= order[i-1] {
				t.Fatalf("apply out of log order: %v", order[i-1:i+1])
			}
		}
		st := w.Stats()
		w.Close()
		// Everything must be recoverable.
		w2, info, _ := collect(t, o)
		w2.Close()
		if info.Batches != writers*per {
			t.Fatalf("recovered %d batches, want %d", info.Batches, writers*per)
		}
		return writers * per, int(st.Syncs), st.MaxGroup
	}
	c, s, mg := run(true)
	t.Logf("group commit ON : %d commits, %d fsyncs, largest group %d", c, s, mg)
	if s >= c/2 {
		t.Fatalf("group commit did not batch: %d fsyncs for %d commits", s, c)
	}
	c, s, mg = run(false)
	t.Logf("group commit OFF: %d commits, %d fsyncs, largest group %d", c, s, mg)
	if s != c || mg != 1 {
		t.Fatalf("without group commit expected 1 fsync per commit, got %d for %d (max group %d)", s, c, mg)
	}
}

func TestFsyncFailurePoisons(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	if err := w.Commit(put("ok", "1"), nil); err != nil {
		t.Fatal(err)
	}
	fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Count: 1})
	applied := false
	err := w.Commit(put("lost", "2"), func(uint64) { applied = true })
	if err == nil || applied {
		t.Fatalf("fsync failure must fail the commit without applying: err=%v applied=%v", err, applied)
	}
	// Even though the fault was one-shot, the log must stay failed (fsyncgate).
	if err := w.Commit(put("later", "3"), nil); err == nil {
		t.Fatal("commit after fsync failure must fail")
	}
	if w.Err() == nil {
		t.Fatal("Err() should report poisoning")
	}
	w.Close()
	// Restart recovers a consistent log; the unacked write may or may not be there.
	w2, _, got := collect(t, o)
	defer w2.Close()
	if len(got) < 1 || got[0].k != "ok" {
		t.Fatalf("got %v", got)
	}
	for _, r := range got {
		if r.k == "later" {
			t.Fatal("write after poison reached disk")
		}
	}
}

func TestDiskFullIsRecoverable(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	w.Commit(put("a", "1"), nil)
	fs.SetDiskLimit(w.Size() + 30) // room for a header but not a whole record
	err := w.Commit(put("big", string(bytes.Repeat([]byte("x"), 200))), nil)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	if w.Err() != nil {
		t.Fatalf("ENOSPC with a clean rollback must not poison: %v", w.Err())
	}
	fs.SetDiskLimit(-1) // space freed
	var s uint64
	if err := w.Commit(put("b", "2"), func(x uint64) { s = x }); err != nil {
		t.Fatal(err)
	}
	if s != 2 {
		t.Fatalf("sequence number leaked by failed write: got %d want 2", s)
	}
	w.Close()
	w2, info, got := collect(t, o)
	defer w2.Close()
	if info.TornTail || len(got) != 2 || got[1].k != "b" {
		t.Fatalf("info %+v got %v", info, got)
	}
}

func TestShortWriteWithBrokenTruncatePoisons(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncAlways)
	w, _, _ := collect(t, o)
	w.Commit(put("a", "1"), nil)
	fs.AddRule(vfs.Rule{Op: vfs.OpWrite, Partial: 5})
	fs.AddRule(vfs.Rule{Op: vfs.OpTruncate, Count: -1})
	if err := w.Commit(put("b", "2"), nil); err == nil {
		t.Fatal("want error")
	}
	if w.Err() == nil {
		t.Fatal("unrepairable tail must poison the log")
	}
	w.Close()
	fs.ClearRules()
	w2, info, got := collect(t, o)
	defer w2.Close()
	if len(got) != 1 || !info.TornTail {
		t.Fatalf("got %v info %+v", got, info)
	}
}

// Crash-safety property for fsync=always: after a simulated power loss, every
// ACKNOWLEDGED commit is recovered, in order, in every iteration.
func TestCrashAlwaysLosesNothingAcked(t *testing.T) {
	for _, mode := range []vfs.CrashMode{vfs.CrashDropUnsynced, vfs.CrashTornTail} {
		for seed := int64(1); seed <= 25; seed++ {
			rng := rand.New(rand.NewSource(seed))
			fs, d := newFS(t)
			o := opts(fs, d, SyncAlways)
			w, _, _ := collect(t, o)
			var acked []string
			var mu sync.Mutex
			var wg sync.WaitGroup
			n := 20 + rng.Intn(40)
			for g := 0; g < 4; g++ {
				wg.Add(1)
				grng := rand.New(rand.NewSource(seed*100 + int64(g))) // rand.Rand is not goroutine-safe
				go func() {
					defer wg.Done()
					for i := 0; i < n; i++ {
						k := fmt.Sprintf("g%d-%d", g, i)
						if err := w.Commit(put(k, k), nil); err == nil {
							mu.Lock()
							acked = append(acked, k)
							mu.Unlock()
						}
						if g == 0 && grng.Intn(10) == 0 {
							w.Rotate()
						}
					}
				}()
			}
			wg.Wait()
			// A write in flight at the crash instant: appended but never synced.
			fs.AddRule(vfs.Rule{Op: vfs.OpSync, Match: ".wal", Count: -1})
			w.Commit(put("inflight", "x"), nil)
			if err := fs.Crash(mode); err != nil {
				t.Fatal(err)
			}
			fs.ClearRules()
			w2, _, got := collect(t, o)
			have := map[string]bool{}
			for _, r := range got {
				have[r.k] = true
			}
			for _, k := range acked {
				if !have[k] {
					t.Fatalf("mode=%d seed=%d: acknowledged write %q lost", mode, seed, k)
				}
			}
			w2.Close()
		}
	}
}

// With fsync=never a power loss may drop recent writes, but what remains must
// be a clean PREFIX of the commit order (never a hole, never garbage).
func TestCrashNeverYieldsPrefix(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		fs, d := newFS(t)
		o := opts(fs, d, SyncNever)
		w, _, _ := collect(t, o)
		const n = 60
		for i := 0; i < n; i++ {
			w.Commit(put(fmt.Sprintf("k%03d", i), "value"), nil)
		}
		fs.Crash(vfs.CrashTornTail)
		w2, _, got := collect(t, o)
		for i, r := range got {
			if r.k != fmt.Sprintf("k%03d", i) {
				t.Fatalf("seed %d: recovered log is not a prefix at %d: %v", seed, i, r)
			}
		}
		w2.Close()
	}
}

func TestSyncEveryEventuallySyncs(t *testing.T) {
	fs, d := newFS(t)
	o := opts(fs, d, SyncEvery)
	w, _, _ := collect(t, o)
	defer w.Close()
	w.Commit(put("a", "1"), nil)
	if w.Stats().Syncs != 0 {
		t.Fatal("SyncEvery must not fsync on the commit path")
	}
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().Syncs == 0 {
		if time.Now().After(deadline) {
			t.Fatal("background fsync never ran")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchTooLarge(t *testing.T) {
	fs, d := newFS(t)
	w, _, _ := collect(t, opts(fs, d, SyncNever))
	defer w.Close()
	if err := w.Commit(&Batch{}, nil); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("empty batch: %v", err)
	}
}

func FuzzScanSegment(f *testing.F) {
	r1, _ := appendRecord(nil, &Batch{Seq: 1, Ops: put("key", "value").Ops})
	r2, _ := appendRecord(nil, &Batch{Seq: 2, Ops: []Op{{Type: OpDelete, Key: []byte("k")}}})
	f.Add(append(append([]byte(nil), r1...), r2...))
	f.Add(r1[:len(r1)-1])
	f.Add([]byte{})
	f.Add(make([]byte, 64))
	f.Fuzz(func(t *testing.T, data []byte) {
		var seen []rec
		res, err := ScanSegment(data, 0, func(b *Batch) error {
			for i, op := range b.Ops {
				if op.Type != OpPut && op.Type != OpDelete {
					t.Fatalf("invalid op type %d decoded", op.Type)
				}
				seen = append(seen, rec{b.Seq + uint64(i), string(op.Key), string(op.Value), op.Type, op.ExpireAt})
			}
			return nil
		})
		if res.ValidEnd < 0 || res.ValidEnd > int64(len(data)) {
			t.Fatalf("ValidEnd %d out of range", res.ValidEnd)
		}
		if err == nil {
			// Whatever the scanner accepted must re-encode to exactly the same bytes.
			var again []byte
			off := 0
			for off < int(res.ValidEnd) {
				b, n, reason := decodeRecord(data[off:])
				if reason != "" {
					t.Fatalf("accepted prefix does not re-decode: %s", reason)
				}
				again, _ = appendRecord(again, &b)
				off += n
			}
			if !bytes.Equal(again, data[:res.ValidEnd]) {
				t.Fatal("round trip mismatch")
			}
		}
	})
}
