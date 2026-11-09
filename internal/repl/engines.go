package repl

import (
	"time"

	"github.com/naman/nebuladb/internal/engine"
)

// SyncEngine wraps the leader's engine. With minAcks > 0 a write command is not
// acknowledged to the client until that many replicas have durably applied it.
// With minAcks == 0 it only adds replication stats (asynchronous replication).
type SyncEngine struct {
	engine.Engine
	L       *Leader
	MinAcks int
	Wait    time.Duration
}

func (s *SyncEngine) wait() error {
	if s.MinAcks <= 0 {
		return nil
	}
	// The write has been applied, so the leader's newest seq is >= its seq.
	return s.L.WaitAcks(s.L.LastSeq(), s.MinAcks, s.Wait)
}

func (s *SyncEngine) Set(k, v []byte, o engine.SetOptions) (bool, error) {
	ok, err := s.Engine.Set(k, v, o)
	if err != nil || !ok {
		return ok, err
	}
	return ok, s.wait()
}
func (s *SyncEngine) MSet(p [][]byte) error {
	if err := s.Engine.MSet(p); err != nil {
		return err
	}
	return s.wait()
}
func (s *SyncEngine) Del(k [][]byte) (int, error) {
	n, err := s.Engine.Del(k)
	if err != nil || n == 0 {
		return n, err
	}
	return n, s.wait()
}
func (s *SyncEngine) SetExpiry(k []byte, at int64) (bool, error) {
	ok, err := s.Engine.SetExpiry(k, at)
	if err != nil || !ok {
		return ok, err
	}
	return ok, s.wait()
}
func (s *SyncEngine) Persist(k []byte) (bool, error) {
	ok, err := s.Engine.Persist(k)
	if err != nil || !ok {
		return ok, err
	}
	return ok, s.wait()
}
func (s *SyncEngine) IncrBy(k []byte, d int64) (int64, error) {
	n, err := s.Engine.IncrBy(k, d)
	if err != nil {
		return n, err
	}
	return n, s.wait()
}
func (s *SyncEngine) Flush() error {
	if err := s.Engine.Flush(); err != nil {
		return err
	}
	return s.wait()
}
func (s *SyncEngine) Stats() []engine.Stat { return append(s.Engine.Stats(), s.L.Stats()...) }

// ReadOnly wraps a replica's engine: reads work, every write command fails with
// the Redis READONLY error.
type ReadOnly struct {
	engine.Engine
	F *Follower
}

func (ReadOnly) Set(_, _ []byte, _ engine.SetOptions) (bool, error) { return false, engine.ErrReadOnly }
func (ReadOnly) MSet([][]byte) error                                { return engine.ErrReadOnly }
func (ReadOnly) Del([][]byte) (int, error)                          { return 0, engine.ErrReadOnly }
func (ReadOnly) SetExpiry([]byte, int64) (bool, error)              { return false, engine.ErrReadOnly }
func (ReadOnly) Persist([]byte) (bool, error)                       { return false, engine.ErrReadOnly }
func (ReadOnly) IncrBy([]byte, int64) (int64, error)                { return 0, engine.ErrReadOnly }
func (ReadOnly) Flush() error                                       { return engine.ErrReadOnly }
func (r ReadOnly) Stats() []engine.Stat                             { return append(r.Engine.Stats(), r.F.Stats()...) }
