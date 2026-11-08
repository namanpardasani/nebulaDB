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

	// Phase 2: in-memory engine. Replaced by the LSM engine in later phases.
	eng := engine.NewMem()

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	srv := server.New(eng, server.Options{MaxConns: cfg.MaxConns, Logger: log})
	log.Info("nebuladb listening", "addr", ln.Addr().String(), "engine", "mem", "fsync", cfg.Fsync)

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
