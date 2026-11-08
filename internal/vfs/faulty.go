package vfs

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Op identifies a filesystem operation that a fault rule can target.
type Op int

const (
	OpCreate Op = iota
	OpOpen
	OpWrite
	OpSync
	OpRead
	OpRename
	OpRemove
	OpSyncDir
	OpTruncate
)

func (o Op) String() string {
	return [...]string{"create", "open", "write", "sync", "read", "rename", "remove", "syncdir", "truncate"}[o]
}

// ErrInjected is the default error returned by a rule with a nil Err.
var ErrInjected = errors.New("vfs: injected fault")

// ErrCrashed is returned by file handles that were open when Crash() was called
// (the "process" that owned them is dead).
var ErrCrashed = errors.New("vfs: file handle invalidated by simulated crash")

// Rule describes one injected fault.
//
// A rule matches a call when Op equals the call's op and the file path
// contains Match (empty Match matches everything). The first Skip matching
// calls pass through untouched; the next Count matching calls fault
// (Count==0 means exactly one; Count<0 means forever — a "sticky" failure such
// as a dead disk).
type Rule struct {
	Op    Op
	Match string
	Skip  int
	Count int
	Err   error // nil => ErrInjected
	// Partial applies to OpWrite only: that many bytes of the buffer are really
	// written before the error is returned, simulating a short/torn write.
	// Partial<=0 means nothing is written.
	Partial int

	seen  int
	fired int
}

type flipRule struct {
	match  string
	offset int64
	mask   byte
}

type dirOpKind int

const (
	dirCreate dirOpKind = iota
	dirRename
	dirRemove
)

// pendingDirOp records a namespace change that has not yet been made durable by
// SyncDir. Crash() undoes these, because on real hardware an unsynced
// create/rename/remove may or may not survive power loss; undoing is the
// adversarial choice.
type pendingDirOp struct {
	kind      dirOpKind
	dir       string
	name      string // created / removed / rename-destination path
	oldName   string // rename source path
	backup    []byte // previous content of the overwritten/removed file (nil if none)
	hadBkp    bool
	srcSize   int64 // for rename: tracked sizes of the moved file
	srcSynced int64
}

type fileState struct {
	size   int64 // bytes currently in the file (as the OS would report)
	synced int64 // prefix known durable (last fsync)
}

// Faulty wraps an FS with fault injection and power-loss simulation.
// It is safe for concurrent use.
type Faulty struct {
	inner FS

	mu      sync.Mutex
	rules   []*Rule
	flips   []flipRule
	files   map[string]*fileState
	pending []pendingDirOp
	gen     int // incremented by Crash; stale handles error out

	limit    int64 // disk capacity in bytes; <0 = unlimited
	syncs    int
	writes   int
	crashRng *rand.Rand
}

// NewFaulty wraps inner. Files that already exist are treated as durable.
func NewFaulty(inner FS) *Faulty {
	return &Faulty{inner: inner, files: map[string]*fileState{}, limit: -1, crashRng: rand.New(rand.NewSource(1))}
}

// AddRule installs a fault rule.
func (f *Faulty) AddRule(r Rule) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rc := r
	f.rules = append(f.rules, &rc)
}

// ClearRules removes every rule, flip and the disk limit (the "disk recovered").
func (f *Faulty) ClearRules() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
	f.flips = nil
	f.limit = -1
}

// SetDiskLimit caps the total size of all tracked files; writes beyond it are
// short and fail with ENOSPC. Pass a negative number to lift the cap.
func (f *Faulty) SetDiskLimit(bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limit = bytes
}

// FlipOnRead flips bits (XOR mask) in the byte at absolute file offset whenever
// a read of a file whose path contains match covers that offset. The bytes on
// "disk" are untouched, so this models a bad read/cable rather than bad media.
func (f *Faulty) FlipOnRead(match string, offset int64, mask byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flips = append(f.flips, flipRule{match, offset, mask})
}

// SyncCount / WriteCount report how many fsyncs and writes went through, so
// tests can assert group commit really batches.
func (f *Faulty) SyncCount() int  { f.mu.Lock(); defer f.mu.Unlock(); return f.syncs }
func (f *Faulty) WriteCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.writes }

// match returns the fault (if any) for an operation. Caller holds f.mu.
func (f *Faulty) match(op Op, name string) *Rule {
	for _, r := range f.rules {
		if r.Op != op || (r.Match != "" && !strings.Contains(name, r.Match)) {
			continue
		}
		r.seen++
		if r.seen <= r.Skip {
			continue
		}
		limit := r.Count
		if limit == 0 {
			limit = 1
		}
		if limit > 0 && r.fired >= limit {
			continue
		}
		r.fired++
		return r
	}
	return nil
}

func ruleErr(r *Rule) error {
	if r.Err != nil {
		return r.Err
	}
	return ErrInjected
}

func (f *Faulty) usedLocked() int64 {
	var n int64
	for _, st := range f.files {
		n += st.size
	}
	return n
}

// track registers a file that exists on the inner FS but is not yet tracked.
// Caller holds f.mu.
func (f *Faulty) track(name string) *fileState {
	if st, ok := f.files[name]; ok {
		return st
	}
	var size int64
	if fi, err := f.inner.Stat(name); err == nil {
		size = fi.Size()
	}
	st := &fileState{size: size, synced: size}
	f.files[name] = st
	return st
}

// ---- FS methods ----

func (f *Faulty) Create(name string) (File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.match(OpCreate, name); r != nil {
		return nil, ruleErr(r)
	}
	var backup []byte
	_, existed := f.inner.Stat(name)
	hadBkp := existed == nil
	if hadBkp {
		backup, _ = ReadFile(f.inner, name)
	}
	in, err := f.inner.Create(name)
	if err != nil {
		return nil, err
	}
	f.files[name] = &fileState{}
	f.pending = append(f.pending, pendingDirOp{kind: dirCreate, dir: filepath.Dir(name), name: name, backup: backup, hadBkp: hadBkp})
	return &faultyFile{fs: f, inner: in, name: name, gen: f.gen}, nil
}

func (f *Faulty) Open(name string) (File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *Faulty) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.match(OpOpen, name); r != nil {
		return nil, ruleErr(r)
	}
	_, statErr := f.inner.Stat(name)
	existed := statErr == nil
	in, err := f.inner.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if flag&os.O_TRUNC != 0 && existed {
		f.files[name] = &fileState{}
	} else {
		f.track(name)
	}
	if !existed && flag&os.O_CREATE != 0 {
		f.files[name] = &fileState{}
		f.pending = append(f.pending, pendingDirOp{kind: dirCreate, dir: filepath.Dir(name), name: name})
	}
	return &faultyFile{fs: f, inner: in, name: name, gen: f.gen, append_: flag&os.O_APPEND != 0}, nil
}

func (f *Faulty) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.match(OpRemove, name); r != nil {
		return ruleErr(r)
	}
	var backup []byte
	_, statErr := f.inner.Stat(name)
	had := statErr == nil
	if had {
		backup, _ = ReadFile(f.inner, name)
	}
	if err := f.inner.Remove(name); err != nil {
		return err
	}
	st := f.files[name]
	delete(f.files, name)
	op := pendingDirOp{kind: dirRemove, dir: filepath.Dir(name), name: name, backup: backup, hadBkp: had}
	if st != nil {
		op.srcSize, op.srcSynced = st.size, st.synced
	}
	f.pending = append(f.pending, op)
	return nil
}

func (f *Faulty) Rename(oldname, newname string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.match(OpRename, newname); r != nil {
		return ruleErr(r)
	}
	var backup []byte
	_, statErr := f.inner.Stat(newname)
	had := statErr == nil
	if had {
		backup, _ = ReadFile(f.inner, newname)
	}
	if err := f.inner.Rename(oldname, newname); err != nil {
		return err
	}
	op := pendingDirOp{kind: dirRename, dir: filepath.Dir(newname), name: newname, oldName: oldname, backup: backup, hadBkp: had}
	if st := f.files[oldname]; st != nil {
		op.srcSize, op.srcSynced = st.size, st.synced
		f.files[newname] = st
		delete(f.files, oldname)
	}
	f.pending = append(f.pending, op)
	return nil
}

func (f *Faulty) MkdirAll(dir string, perm os.FileMode) error { return f.inner.MkdirAll(dir, perm) }
func (f *Faulty) List(dir string) ([]string, error)           { return f.inner.List(dir) }
func (f *Faulty) Stat(name string) (os.FileInfo, error)       { return f.inner.Stat(name) }

func (f *Faulty) SyncDir(dir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.match(OpSyncDir, dir); r != nil {
		return ruleErr(r)
	}
	if err := f.inner.SyncDir(dir); err != nil {
		return err
	}
	kept := f.pending[:0]
	for _, op := range f.pending {
		if op.dir != dir {
			kept = append(kept, op)
		}
	}
	f.pending = kept
	return nil
}

// CrashMode selects how much unsynced data survives a simulated power loss.
type CrashMode int

const (
	// CrashDropUnsynced truncates every file to its last fsynced length.
	CrashDropUnsynced CrashMode = iota
	// CrashTornTail keeps a random prefix of the unsynced tail of each file
	// (disks persist pages in arbitrary order/prefix), which produces torn
	// records for recovery to cope with.
	CrashTornTail
)

// Crash simulates sudden power loss: unsynced file data is lost per mode,
// unsynced create/rename/remove operations are undone, and all open handles
// become invalid. The FS can be reused afterwards to "reboot" the engine.
func (f *Faulty) Crash(mode CrashMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen++
	// 1. Roll back unsynced namespace operations, newest first.
	for i := len(f.pending) - 1; i >= 0; i-- {
		op := f.pending[i]
		switch op.kind {
		case dirCreate:
			f.inner.Remove(op.name)
			delete(f.files, op.name)
			if op.hadBkp { // Create had truncated an existing file
				f.restore(op.name, op.backup)
			}
		case dirRename:
			f.inner.Rename(op.name, op.oldName)
			f.files[op.oldName] = &fileState{size: op.srcSize, synced: op.srcSynced}
			delete(f.files, op.name)
			if op.hadBkp {
				f.restore(op.name, op.backup)
			}
		case dirRemove:
			f.restore(op.name, op.backup)
			if op.hadBkp {
				f.files[op.name] = &fileState{size: op.srcSize, synced: op.srcSynced}
			}
		}
	}
	f.pending = nil
	// 2. Lose unsynced data.
	for name, st := range f.files {
		keep := st.synced
		if mode == CrashTornTail && st.size > st.synced {
			keep += f.crashRng.Int63n(st.size - st.synced + 1)
		}
		if keep < st.size {
			in, err := f.inner.OpenFile(name, os.O_RDWR, 0)
			if err != nil {
				continue
			}
			err = in.Truncate(keep)
			in.Close()
			if err != nil {
				return fmt.Errorf("vfs: crash truncate %s: %w", name, err)
			}
		}
		st.size = keep
		st.synced = keep
	}
	return nil
}

// restore rewrites a file's pre-change content on the inner FS. Caller holds f.mu.
func (f *Faulty) restore(name string, data []byte) {
	in, err := f.inner.Create(name)
	if err != nil {
		return
	}
	in.Write(data)
	in.Close()
	f.files[name] = &fileState{size: int64(len(data)), synced: int64(len(data))}
}

// ---- file handle ----

type faultyFile struct {
	fs      *Faulty
	inner   File
	name    string
	gen     int
	append_ bool
}

func (h *faultyFile) Name() string { return h.name }

func (h *faultyFile) stale() bool { return h.gen != h.fs.gen }

func (h *faultyFile) Write(p []byte) (int, error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.stale() {
		return 0, ErrCrashed
	}
	f.writes++
	st := f.track(h.name)
	var faultErr error
	n := len(p)
	if r := f.match(OpWrite, h.name); r != nil {
		faultErr = ruleErr(r)
		n = r.Partial
		if n < 0 {
			n = 0
		}
		if n > len(p) {
			n = len(p)
		}
	}
	// ENOSPC: let through only what fits.
	if f.limit >= 0 && faultErr == nil {
		free := f.limit - f.usedLocked()
		if free < int64(n) {
			if free < 0 {
				free = 0
			}
			n = int(free)
			faultErr = syscall.ENOSPC
		}
	}
	if n > 0 {
		w, err := h.inner.Write(p[:n])
		if w > 0 {
			// Extend tracked size (position-based for non-append is close
			// enough: our engine only appends or writes sequentially).
			pos, _ := h.inner.Seek(0, io.SeekCurrent)
			if pos > st.size {
				st.size = pos
			}
		}
		if err != nil {
			return w, err
		}
		n = w
	}
	if faultErr != nil {
		return n, faultErr
	}
	return n, nil
}

func (h *faultyFile) Sync() error {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.stale() {
		return ErrCrashed
	}
	f.syncs++
	if r := f.match(OpSync, h.name); r != nil {
		return ruleErr(r)
	}
	if err := h.inner.Sync(); err != nil {
		return err
	}
	st := f.track(h.name)
	st.synced = st.size
	return nil
}

func (h *faultyFile) Truncate(size int64) error {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.stale() {
		return ErrCrashed
	}
	if r := f.match(OpTruncate, h.name); r != nil {
		return ruleErr(r)
	}
	if err := h.inner.Truncate(size); err != nil {
		return err
	}
	st := f.track(h.name)
	st.size = size
	if st.synced > size {
		st.synced = size
	}
	return nil
}

func (h *faultyFile) Read(p []byte) (int, error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.stale() {
		return 0, ErrCrashed
	}
	if r := f.match(OpRead, h.name); r != nil {
		return 0, ruleErr(r)
	}
	pos, _ := h.inner.Seek(0, io.SeekCurrent)
	n, err := h.inner.Read(p)
	f.applyFlips(h.name, p[:n], pos)
	return n, err
}

func (h *faultyFile) ReadAt(p []byte, off int64) (int, error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if h.stale() {
		return 0, ErrCrashed
	}
	if r := f.match(OpRead, h.name); r != nil {
		return 0, ruleErr(r)
	}
	n, err := h.inner.ReadAt(p, off)
	f.applyFlips(h.name, p[:n], off)
	return n, err
}

// applyFlips corrupts buf, which holds file bytes starting at absolute offset
// base. Caller holds f.mu.
func (f *Faulty) applyFlips(name string, buf []byte, base int64) {
	for _, fl := range f.flips {
		if !strings.Contains(name, fl.match) {
			continue
		}
		if fl.offset >= base && fl.offset < base+int64(len(buf)) {
			buf[fl.offset-base] ^= fl.mask
		}
	}
}

func (h *faultyFile) Seek(offset int64, whence int) (int64, error) {
	return h.inner.Seek(offset, whence)
}
func (h *faultyFile) Stat() (os.FileInfo, error) { return h.inner.Stat() }
func (h *faultyFile) Close() error               { return h.inner.Close() }
