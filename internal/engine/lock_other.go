//go:build !unix

package engine

// lockDir is a no-op on platforms without flock(2); running two processes on one
// directory is unsupported there.
func lockDir(dir string) (func(), error) { return func() {}, nil }
