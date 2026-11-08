// Package server is NebulaDB's RESP2 TCP front end.
//
// Model: one goroutine per connection. Each goroutine loops
//
//	read command -> execute against the Engine -> buffer reply -> (flush)
//
// Pipelining: a client may send many commands without waiting for replies. We
// keep executing while the read buffer still holds bytes and flush all the
// replies in one write when it runs dry (see Reader.Buffered). Replies are
// always in command order because a connection is handled by exactly one
// goroutine.
//
// Graceful shutdown (Shutdown): stop accepting; set a read deadline in the past
// on every connection so goroutines blocked in Read wake up; a goroutine that is
// in the middle of executing a command finishes it and flushes its reply before
// noticing. The caller then closes the Engine (which flushes and fsyncs).
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/resp"
)

// Version is reported by INFO and HELLO.
const Version = "0.1.0-dev"

// Options configures a Server.
type Options struct {
	MaxConns int          // 0 = unlimited
	Logger   *slog.Logger // nil = discard
	Now      func() int64 // unix ms clock for EX/PX/EXPIRE; nil = wall clock
}

// Server serves RESP over TCP.
type Server struct {
	eng engine.Engine
	log *slog.Logger
	now func() int64

	sem chan struct{} // connection slots; nil = unlimited

	mu      sync.Mutex
	ln      net.Listener
	conns   map[net.Conn]struct{}
	closing bool
	wg      sync.WaitGroup

	cursors *cursorTable
	started time.Time

	// counters (also surfaced through INFO)
	totalConns    atomic.Int64
	rejectedConns atomic.Int64
	totalCmds     atomic.Int64
	activeConns   atomic.Int64
}

// New creates a Server over eng.
func New(eng engine.Engine, opt Options) *Server {
	s := &Server{
		eng:     eng,
		log:     opt.Logger,
		now:     opt.Now,
		conns:   map[net.Conn]struct{}{},
		cursors: newCursorTable(4096),
		started: time.Now(),
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.now == nil {
		s.now = func() int64 { return time.Now().UnixMilli() }
	}
	if opt.MaxConns > 0 {
		s.sem = make(chan struct{}, opt.MaxConns)
	}
	return s
}

// Serve accepts connections on ln until Shutdown. It returns nil after a clean
// shutdown and the accept error otherwise.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()

	var backoff time.Duration
	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			// Transient errors (EMFILE etc.): back off and retry.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if backoff == 0 {
					backoff = 5 * time.Millisecond
				} else if backoff < time.Second {
					backoff *= 2
				}
				s.log.Warn("accept error, retrying", "err", err, "backoff", backoff)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		s.totalConns.Add(1)
		if s.sem != nil {
			select {
			case s.sem <- struct{}{}:
			default:
				s.rejectedConns.Add(1)
				c.SetWriteDeadline(time.Now().Add(time.Second))
				c.Write([]byte("-ERR max number of clients reached\r\n"))
				c.Close()
				continue
			}
		}
		if !s.register(c) {
			s.release()
			c.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.activeConns.Add(1)
			defer s.activeConns.Add(-1)
			defer s.release()
			defer s.unregister(c)
			s.handle(c)
		}()
	}
}

func (s *Server) release() {
	if s.sem != nil {
		<-s.sem
	}
}

func (s *Server) register(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) unregister(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.Close()
}

// Shutdown stops accepting, lets in-flight commands finish and waits for all
// connection goroutines. If ctx expires first, remaining connections are
// force-closed. It does not close the Engine.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	// Wake goroutines blocked in Read. An in-progress command is unaffected
	// because the deadline only applies to socket reads.
	for _, c := range conns {
		c.SetReadDeadline(time.Unix(1, 0))
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

// handle runs one connection.
func (s *Server) handle(c net.Conn) {
	r := resp.NewReader(c)
	w := resp.NewWriter(c)
	// Flush whatever is buffered no matter how we leave (shutdown, EOF...).
	defer w.Flush()
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if resp.IsProtocolError(err) {
				w.Error("ERR " + err.Error())
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		s.totalCmds.Add(1)
		quit := s.dispatch(w, args)
		// Flush only at the end of a pipelined batch (or on QUIT).
		if quit || r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

// dispatch executes one command and reports whether the connection should close.
func (s *Server) dispatch(w *resp.Writer, args [][]byte) (quit bool) {
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("panic in command", "cmd", string(args[0]), "panic", p)
			w.Error("ERR internal error")
		}
	}()
	return s.execute(w, args)
}
