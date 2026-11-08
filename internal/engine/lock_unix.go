//go:build unix

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive advisory lock on <dir>/LOCK so that two processes
// (for example a running server and `nebula compact`) can never open the same
// database directory at once, which would corrupt it. The lock is released by
// the returned function, or automatically by the kernel if the process dies.
func lockDir(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("database %s is in use by another process (LOCK held)", dir)
		}
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
