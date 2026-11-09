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

	"github.com/naman/nebuladb/internal/admin"
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
	if cfg.Supervise && os.Getenv(superviseEnv) != "1" {
		return supervise(os.Args[1:])
	}
	supervised := os.Getenv(superviseEnv) == "1"
	log, err := logx.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	broker := admin.NewBroker(2000) // engine events -> SSE stream

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
		cacheBytes := cfg.BlockCacheSize
		if cacheBytes == 0 {
			cacheBytes = -1 // -cache-size=0 disables the cache
		}
		db, info, err := engine.Open(engine.Options{
			FS: vfs.Default, Dir: cfg.Dir,
			Sync: policy, SyncEvery: cfg.FsyncInterval, GroupCommit: cfg.GroupCommit,
			MemtableSize: cfg.MemtableSize, MaxImmutables: cfg.MaxImmutables, OnEvent: broker.Publish,
			BlockSize: cfg.BlockSize, BloomBits: cfg.BloomBitsKey, BlockCacheBytes: cacheBytes, Logger: log,
			Compaction: engine.CompactionOptions{
				Style: cfg.Compaction, L0CompactTrigger: cfg.L0CompactTrigger, L0SlowdownTrigger: cfg.L0SlowdownTrigger,
				L0StopTrigger: cfg.L0StopTrigger, LevelSizeRatio: cfg.LevelSizeRatio, L1MaxBytes: cfg.L1MaxBytes,
				TargetFileBytes: cfg.TargetFileSize, RateBytesPerSec: cfg.CompactionRate,
			},
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

	var adm *admin.Server
	if cfg.AdminAddr != "" {
		aopts := admin.Options{Server: srv, Broker: broker, ServerAddr: ln.Addr().String(), DevMode: cfg.DevMode || supervised, Supervised: supervised}
		if db, ok := eng.(*engine.DB); ok {
			aopts.Source, aopts.Dev = db, db
		}
		host, _, _ := net.SplitHostPort(cfg.AdminAddr)
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			aopts.AllowAnyHost = true
			log.Warn("admin server is bound to a non-loopback address: it exposes a command proxy"+
				" and (with -dev) maintenance endpoints without authentication", "addr", cfg.AdminAddr)
		}
		adm = admin.New(aopts)
		aln, err := net.Listen("tcp", cfg.AdminAddr)
		if err != nil {
			return fmt.Errorf("admin listener: %w", err)
		}
		log.Info("admin server listening", "addr", aln.Addr().String(), "dev", aopts.DevMode, "supervised", supervised)
		go func() {
			if err := adm.Serve(aln); err != nil {
				log.Error("admin server stopped", "err", err)
			}
		}()
	}

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
	if adm != nil {
		adm.Shutdown(ctx)
	}
	if err := eng.Close(); err != nil {
		return fmt.Errorf("closing engine: %w", err)
	}
	log.Info("shutdown complete")
	return nil
}
