// Command nebuladb is the NebulaDB server.
package main

import (
	"fmt"
	"os"

	"github.com/naman/nebuladb/internal/config"
	"github.com/naman/nebuladb/internal/logx"
)

func main() {
	cfg, err := config.Parse(os.Args[1:], os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log, err := logx.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log.Info("nebuladb starting", "dir", cfg.Dir, "addr", cfg.Addr, "fsync", cfg.Fsync)
}
