// Command nebuladb is the NebulaDB server.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naman/nebuladb/internal/config"
	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/logx"
	"github.com/naman/nebuladb/internal/server"
	"github.com/naman/nebuladb/internal/vfs"
	"github.com/naman/nebuladb/internal/wal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "nebuladb:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Parse(os.Args[1:], os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log, err := logx.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	var eng engine.Engine
	if cfg.InMemory {
		eng = engine.NewMem()
		log.Warn("running WITHOUT persistence (-in-memory): data is lost on exit")
	} else {
		policy := wal.SyncAlways
		switch cfg.Fsync {
		case config.FsyncEvery:
			policy = wal.SyncEvery
		case config.FsyncNever:
			policy = wal.SyncNever
			log.Warn("fsync=never: acknowledged writes can be lost on power failure")
		}
		db, info, err := engine.Open(engine.Options{
			FS: vfs.Default, Dir: cfg.Dir,
			Sync: policy, SyncEvery: cfg.FsyncInterval, GroupCommit: cfg.GroupCommit,
			MemtableSize: cfg.MemtableSize, Logger: log,
		})
		if err != nil {
			return fmt.Errorf("recovery failed: %w", err)
		}
		eng = db
		log.Info("recovery complete", "dir", cfg.Dir, "wal_segments", len(info.Segments), "batches", info.Batches,
			"last_seq", info.LastSeq, "torn_tail", info.TornTail, "truncated_bytes", info.TruncatedBytes)
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	srv := server.New(eng, server.Options{MaxConns: cfg.MaxConns, Logger: log})
	log.Info("nebuladb listening", "addr", ln.Addr().String(), "engine", "lsm", "fsync", cfg.Fsync, "group_commit", cfg.GroupCommit)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("shutdown requested", "signal", s.String())
	case err := <-serveErr:
		if err != nil {
			return err
		}
	}

	// Graceful shutdown: stop accepting, finish in-flight commands, then close
	// the engine (which flushes and fsyncs).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Warn("forced connection close after timeout", "err", err)
	}
	if err := eng.Close(); err != nil {
		return fmt.Errorf("closing engine: %w", err)
	}
	log.Info("shutdown complete")
	return nil
}
