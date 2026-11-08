package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/naman/nebuladb/internal/vfs"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	currentName    = "CURRENT"
	manifestPrefix = "MANIFEST-"
	maxEditSize    = 64 << 20
	headerSize     = 8
)

// ErrMissingCurrent is returned when table files exist but CURRENT does not:
// starting fresh would silently orphan data.
var ErrMissingCurrent = errors.New("manifest: CURRENT file is missing but SSTable files exist (refusing to start; restore CURRENT or run `nebula repair`)")

// CorruptionError reports damage in the middle of a manifest.
type CorruptionError struct {
	File   string
	Offset int64
	Reason string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("manifest: corruption in %s at offset %d: %s (refusing to start; run `nebula repair`)", e.File, e.Offset, e.Reason)
}

// Options configures Open.
type Options struct {
	// RewriteThreshold: when the live manifest exceeds this many bytes it is
	// rewritten as a compact snapshot. Default 1 MiB.
	RewriteThreshold int64
}

// Info reports what Open found.
type Info struct {
	ManifestNum    uint64
	Edits          int
	Fresh          bool // no database existed; one was created
	TornTail       bool
	TruncatedBytes int64
	Removed        []string // stale manifest files deleted
}

// Manifest is the live, appendable manifest.
type Manifest struct {
	fs   vfs.FS
	dir  string
	opts Options

	mu     sync.Mutex
	f      vfs.File
	num    uint64
	size   int64
	cur    *Version
	failed error
}

func manifestFile(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d", manifestPrefix, num))
}

func parseManifestName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, manifestPrefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(name, manifestPrefix), 10, 64)
	return n, err == nil
}

func appendRecord(dst, payload []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(payload, crcTable))
	return append(dst, payload...)
}

func decodeRecord(data []byte) (payload []byte, n int, reason string) {
	if len(data) < headerSize {
		return nil, 0, "short header"
	}
	l := binary.LittleEndian.Uint32(data)
	if l == 0 || l > maxEditSize {
		return nil, 0, fmt.Sprintf("implausible record length %d", l)
	}
	if uint64(len(data)) < headerSize+uint64(l) {
		return nil, 0, "record extends past end of file"
	}
	p := data[headerSize : headerSize+int(l)]
	if crc32.Checksum(p, crcTable) != binary.LittleEndian.Uint32(data[4:]) {
		return nil, 0, "checksum mismatch"
	}
	return p, headerSize + int(l), ""
}

// replay rebuilds the Version from a manifest file's bytes. Torn-tail rule is
// the same as the WAL's: a bad record is a torn tail iff no valid record
// follows it anywhere; otherwise it is corruption.
func replay(name string, data []byte, onEdit func(*Edit)) (v *Version, validEnd int64, edits int, torn bool, err error) {
	v = &Version{NextFileNum: 1}
	off := 0
	for off < len(data) {
		p, n, reason := decodeRecord(data[off:])
		if reason != "" {
			for i := off + 1; i+headerSize < len(data); i++ {
				if _, _, r := decodeRecord(data[i:]); r == "" {
					return nil, 0, 0, false, &CorruptionError{name, int64(off), reason + ", but a valid record follows at offset " + strconv.Itoa(i)}
				}
			}
			return v, int64(off), edits, true, nil
		}
		e, derr := DecodeEdit(p)
		if derr != nil {
			return nil, 0, 0, false, &CorruptionError{name, int64(off), derr.Error()}
		}
		if aerr := v.Apply(&e); aerr != nil {
			return nil, 0, 0, false, &CorruptionError{name, int64(off), aerr.Error()}
		}
		if onEdit != nil {
			onEdit(&e)
		}
		edits++
		off += n
	}
	return v, int64(off), edits, false, nil
}

func readCurrent(fs vfs.FS, dir string) (uint64, bool, error) {
	b, err := vfs.ReadFile(fs, filepath.Join(dir, currentName))
	if err != nil {
		if _, serr := fs.Stat(filepath.Join(dir, currentName)); serr != nil {
			return 0, false, nil // does not exist
		}
		return 0, false, err
	}
	name := strings.TrimSpace(string(b))
	n, ok := parseManifestName(name)
	if !ok || !strings.HasSuffix(string(b), "\n") {
		return 0, false, fmt.Errorf("manifest: CURRENT is malformed: %q", string(b))
	}
	return n, true, nil
}

// Open reads CURRENT and the manifest it names, repairs a torn tail, deletes
// stale manifest files and returns the live Manifest. For a brand-new directory
// it creates MANIFEST-000001 and CURRENT.
func Open(fs vfs.FS, dir string, opts Options) (*Manifest, Info, error) {
	if opts.RewriteThreshold <= 0 {
		opts.RewriteThreshold = 1 << 20
	}
	var info Info
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return nil, info, err
	}
	m := &Manifest{fs: fs, dir: dir, opts: opts}
	num, ok, err := readCurrent(fs, dir)
	if err != nil {
		return nil, info, err
	}
	if !ok {
		names, err := fs.List(dir)
		if err != nil {
			return nil, info, err
		}
		for _, n := range names {
			if strings.HasSuffix(n, ".sst") {
				return nil, info, ErrMissingCurrent
			}
		}
		info.Fresh = true
		if err := m.createFresh(); err != nil {
			return nil, info, err
		}
		info.ManifestNum = m.num
		m.removeStale(&info)
		return m, info, nil
	}

	path := manifestFile(dir, num)
	data, err := vfs.ReadFile(fs, path)
	if err != nil {
		return nil, info, fmt.Errorf("manifest: CURRENT names %s but it cannot be read: %w", filepath.Base(path), err)
	}
	v, validEnd, edits, torn, err := replay(filepath.Base(path), data, nil)
	if err != nil {
		return nil, info, err
	}
	f, err := fs.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, info, err
	}
	if torn {
		info.TornTail, info.TruncatedBytes = true, int64(len(data))-validEnd
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			return nil, info, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, info, err
		}
	}
	if _, err := f.Seek(validEnd, 0); err != nil {
		f.Close()
		return nil, info, err
	}
	m.f, m.num, m.size, m.cur = f, num, validEnd, v
	info.ManifestNum, info.Edits = num, edits
	m.removeStale(&info)
	return m, info, nil
}

// removeStale deletes manifest files other than the live one and leftover temp
// files (outputs of an interrupted rewrite).
func (m *Manifest) removeStale(info *Info) {
	names, err := m.fs.List(m.dir)
	if err != nil {
		return
	}
	removed := false
	for _, n := range names {
		if num, ok := parseManifestName(n); (ok && num != m.num) || n == currentName+".tmp" {
			if m.fs.Remove(filepath.Join(m.dir, n)) == nil {
				info.Removed = append(info.Removed, n)
				removed = true
			}
		}
	}
	if removed {
		m.fs.SyncDir(m.dir)
	}
}

// createFresh makes a new manifest holding an empty version and points CURRENT at it.
func (m *Manifest) createFresh() error {
	m.num = 1
	m.cur = &Version{NextFileNum: 2}
	_, err := m.writeSnapshotFile(m.num)
	return err
}

// writeSnapshotFile writes MANIFEST-num containing a snapshot of m.cur, fsyncs
// it, then atomically repoints CURRENT. Caller holds mu (or is Open).
//
// swapStarted reports whether the CURRENT switch was attempted. A failure
// before it leaves the old manifest authoritative and is harmless; a failure
// during it is ambiguous (the rename may or may not be durable), so the caller
// must stop using the old file.
func (m *Manifest) writeSnapshotFile(num uint64) (swapStarted bool, err error) {
	path := manifestFile(m.dir, num)
	f, err := m.fs.Create(path)
	if err != nil {
		return false, err
	}
	fail := func(err error) (bool, error) {
		f.Close()
		m.fs.Remove(path)
		return false, err
	}
	rec := appendRecord(nil, m.cur.Snapshot().Encode())
	if _, err := f.Write(rec); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	// Both the new file's directory entry and the CURRENT swap must be durable
	// before we consider the switch done; WriteFileAtomic fsyncs the directory.
	if err := vfs.WriteFileAtomic(m.fs, filepath.Join(m.dir, currentName), []byte(filepath.Base(path)+"\n")); err != nil {
		f.Close()
		return true, err
	}
	if m.f != nil {
		m.f.Close()
	}
	m.f, m.size = f, int64(len(rec))
	return true, nil
}

// Current returns a copy of the live version.
func (m *Manifest) Current() *Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur.Clone()
}

// NewFileNumber reserves a file number. It becomes durable with the next edit
// that is logged; numbers lost to a crash are simply skipped.
func (m *Manifest) NewFileNumber() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.cur.NextFileNum
	m.cur.NextFileNum++
	return n
}

// LogAndApply validates e against the current version, appends it durably
// (write + fsync) and only then installs the new version. If it returns nil the
// change survives any crash; if it returns an error the in-memory version is
// unchanged and the manifest is poisoned (a failed append may have left a torn
// record, and appending after it would hide later edits).
func (m *Manifest) LogAndApply(e *Edit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed != nil {
		return m.failed
	}
	next := m.cur.Clone()
	e.HasNextFileNum, e.NextFileNum = true, m.cur.NextFileNum
	if err := next.Apply(e); err != nil {
		return err // invalid edit: nothing written
	}
	rec := appendRecord(nil, e.Encode())
	if n, err := m.f.Write(rec); err != nil || n != len(rec) {
		if err == nil {
			err = errors.New("short write")
		}
		// A partial record may now sit at the tail. If we can cut it off the
		// manifest stays usable (typical for ENOSPC, which clears when space
		// is freed); if not, appending after garbage would hide later edits.
		if terr := m.f.Truncate(m.size); terr == nil {
			if _, serr := m.f.Seek(m.size, 0); serr == nil {
				return fmt.Errorf("manifest: append failed (rolled back): %w", err)
			}
		}
		m.failed = fmt.Errorf("manifest: append failed and tail could not be repaired, manifest is now read-only: %w", err)
		return m.failed
	}
	if err := m.f.Sync(); err != nil {
		m.failed = fmt.Errorf("manifest: fsync failed, manifest is now read-only: %w", err)
		return m.failed
	}
	m.size += int64(len(rec))
	m.cur = next
	if m.size > m.opts.RewriteThreshold {
		if err := m.rewriteLocked(); err != nil {
			// The edit itself is durable; only compaction of the log failed.
			// Keep running on the old (longer) manifest.
			return nil
		}
	}
	return nil
}

// rewriteLocked compacts the manifest into a fresh snapshot file.
func (m *Manifest) rewriteLocked() error {
	oldNum := m.num
	newNum := m.cur.NextFileNum
	m.cur.NextFileNum++
	swapStarted, err := m.writeSnapshotFile(newNum)
	if err != nil {
		if swapStarted {
			// CURRENT may now name the new file while we still hold the old one;
			// appending to the old file would lose edits after a restart.
			m.failed = fmt.Errorf("manifest: CURRENT switch failed, manifest is now read-only: %w", err)
		}
		return err
	}
	m.num = newNum
	m.fs.Remove(manifestFile(m.dir, oldNum)) // best effort; Open cleans stragglers
	m.fs.SyncDir(m.dir)
	return nil
}

// Rewrite forces a snapshot rewrite (used by tests and `nebula compact`).
func (m *Manifest) Rewrite() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rewriteLocked()
}

// Size returns the live manifest's size in bytes.
func (m *Manifest) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

// Number returns the live manifest's file number.
func (m *Manifest) Number() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.num
}

// Close closes the manifest file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return nil
	}
	err := m.f.Close()
	m.f = nil
	return err
}

// Dump reads the live manifest without modifying anything and returns every
// edit in order plus the resulting version. Used by `nebula dump-manifest`.
func Dump(fs vfs.FS, dir string) (edits []Edit, v *Version, info Info, err error) {
	num, ok, err := readCurrent(fs, dir)
	if err != nil || !ok {
		if err == nil {
			err = errors.New("manifest: no CURRENT file")
		}
		return nil, nil, info, err
	}
	name := fmt.Sprintf("%s%06d", manifestPrefix, num)
	data, err := vfs.ReadFile(fs, filepath.Join(dir, name))
	if err != nil {
		return nil, nil, info, err
	}
	v, validEnd, n, torn, err := replay(name, data, func(e *Edit) { edits = append(edits, *e) })
	info.ManifestNum, info.Edits, info.TornTail = num, n, torn
	if torn {
		info.TruncatedBytes = int64(len(data)) - validEnd
	}
	return edits, v, info, err
}
