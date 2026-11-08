// Package vfs is the filesystem abstraction every NebulaDB component does its
// I/O through. Two implementations exist:
//
//   - OS:      a thin wrapper over package os (production).
//   - Faulty:  wraps any FS and injects failures (tests): short/torn writes,
//     fsync errors, ENOSPC, bit flips on read, and a simulated power loss that
//     discards everything that was never fsynced.
//
// Keeping all I/O behind this interface is what lets the fault-injection tests
// (tests/fault) prove the engine fails safely instead of hoping it does.
package vfs

import (
	"io"
	"os"
	"path/filepath"
)

// File is the subset of *os.File the engine needs.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.Seeker
	io.Closer
	// Sync flushes the file's data to stable storage (fsync).
	Sync() error
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	Name() string
}

// FS is the filesystem interface.
type FS interface {
	// Create creates or truncates a file for read+write.
	Create(name string) (File, error)
	// Open opens a file read-only.
	Open(name string) (File, error)
	// OpenFile is the general form (flags as in os.OpenFile).
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Remove(name string) error
	// Rename atomically replaces newname with oldname (POSIX rename semantics).
	Rename(oldname, newname string) error
	MkdirAll(dir string, perm os.FileMode) error
	// List returns the base names of the entries in dir, sorted.
	List(dir string) ([]string, error)
	Stat(name string) (os.FileInfo, error)
	// SyncDir fsyncs a directory so that creates/renames/removes inside it are
	// durable. Without this, a file can vanish after power loss even though its
	// own contents were fsynced.
	SyncDir(dir string) error
}

// OS is the real filesystem.
type OS struct{}

// Default is the real filesystem singleton.
var Default FS = OS{}

func (OS) Create(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}
func (OS) Open(name string) (File, error) { return os.Open(name) }
func (OS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}
func (OS) Remove(name string) error                    { return os.Remove(name) }
func (OS) Rename(oldname, newname string) error        { return os.Rename(oldname, newname) }
func (OS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (OS) Stat(name string) (os.FileInfo, error)       { return os.Stat(name) }

func (OS) List(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir) // already sorted by filename
	if err != nil {
		return nil, err
	}
	out := make([]string, len(ents))
	for i, e := range ents {
		out[i] = e.Name()
	}
	return out, nil
}

func (OS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	// Directory fsync is a no-op or unsupported on some platforms (Windows);
	// ignore errors from Sync there but surface them on POSIX systems.
	err = d.Sync()
	cerr := d.Close()
	if err != nil {
		return err
	}
	return cerr
}

// WriteFileAtomic writes data to name using the crash-safe recipe used for all
// small replace-in-place files (CURRENT, manifest snapshots, repair output):
//
//  1. write to name+".tmp"
//  2. fsync the temp file
//  3. rename over name (atomic on POSIX)
//  4. fsync the parent directory so the rename itself is durable
//
// After a crash readers see either the complete old file or the complete new
// one, never a mixture.
func WriteFileAtomic(fs FS, name string, data []byte) error {
	tmp := name + ".tmp"
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, name); err != nil {
		return err
	}
	return fs.SyncDir(filepath.Dir(name))
}

// ReadFile reads a whole file through the FS.
func ReadFile(fs FS, name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
