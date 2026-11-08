// Package config holds NebulaDB's server configuration: defaults, flag
// parsing and validation. Engine-level options are derived from it by
// cmd/nebuladb; the engine itself never imports this package.
package config

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

// FsyncPolicy controls when the WAL is flushed to stable storage.
type FsyncPolicy string

const (
	// FsyncAlways fsyncs before acknowledging every write (batched across
	// concurrent writers by group commit). Survives power loss and kill -9:
	// an acknowledged write is never lost.
	FsyncAlways FsyncPolicy = "always"
	// FsyncEvery fsyncs on a timer (FsyncInterval). Survives kill -9 of the
	// process (data is in the OS page cache) but a power loss can lose up to
	// one interval of acknowledged writes.
	FsyncEvery FsyncPolicy = "every"
	// FsyncNever never calls fsync; the OS flushes when it likes. Survives
	// kill -9 only; power loss can lose an unbounded amount. Unsafe.
	FsyncNever FsyncPolicy = "never"
)

// Compaction styles.
const (
	CompactLeveled    = "leveled"
	CompactSizeTiered = "size-tiered"
)

// Config is the complete server configuration.
type Config struct {
	Dir       string // data directory
	Addr      string // RESP listen address
	AdminAddr string // admin HTTP listen address ("" disables)
	MaxConns  int    // max simultaneous client connections
	InMemory  bool   // volatile engine, no WAL (testing / cache use)

	Fsync         FsyncPolicy
	FsyncInterval time.Duration // for FsyncEvery
	GroupCommit   bool          // batch concurrent writers into one fsync

	MemtableSize   int64 // bytes before the memtable is rotated
	MaxImmutables  int   // flush backlog allowed before writers stall
	BlockSize      int   // SSTable data block size (bytes)
	BloomBitsKey   int   // Bloom filter bits per key (0 disables)
	BlockCacheSize int64 // bytes of cache for decoded SSTable blocks

	Compaction        string // leveled | size-tiered
	L0CompactTrigger  int    // L0 file count that triggers compaction
	L0SlowdownTrigger int    // L0 file count where writes are delayed
	L0StopTrigger     int    // L0 file count where writes stop
	LevelSizeRatio    int    // size multiplier between levels
	L1MaxBytes        int64  // target size of level 1

	LogLevel  string // debug | info | warn | error
	LogFormat string // text | json
	DevMode   bool   // enables dev-only admin endpoints (crash button)
}

// Default returns the default configuration.
func Default() Config {
	return Config{
		Dir:       "./data",
		Addr:      "127.0.0.1:6380",
		AdminAddr: "127.0.0.1:7380",
		MaxConns:  10000,

		Fsync:         FsyncAlways,
		FsyncInterval: 100 * time.Millisecond,
		GroupCommit:   true,

		MemtableSize:   4 << 20,
		MaxImmutables:  2,
		BlockSize:      4 << 10,
		BloomBitsKey:   10,
		BlockCacheSize: 64 << 20,

		Compaction:        CompactLeveled,
		L0CompactTrigger:  4,
		L0SlowdownTrigger: 8,
		L0StopTrigger:     12,
		LevelSizeRatio:    10,
		L1MaxBytes:        16 << 20,

		LogLevel:  "info",
		LogFormat: "text",
	}
}

// RegisterFlags binds the config's fields to a FlagSet.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Dir, "dir", c.Dir, "data directory")
	fs.StringVar(&c.Addr, "addr", c.Addr, "RESP listen address")
	fs.StringVar(&c.AdminAddr, "admin-addr", c.AdminAddr, "admin HTTP address (empty disables)")
	fs.BoolVar(&c.InMemory, "in-memory", c.InMemory, "run without persistence (data lost on exit)")
	fs.IntVar(&c.MaxConns, "max-conns", c.MaxConns, "maximum concurrent client connections")

	fs.Func("fsync", "WAL fsync policy: always | every | never (default always)", func(s string) error {
		c.Fsync = FsyncPolicy(s)
		return nil
	})
	fs.DurationVar(&c.FsyncInterval, "fsync-interval", c.FsyncInterval, "interval for -fsync=every")
	fs.BoolVar(&c.GroupCommit, "group-commit", c.GroupCommit, "batch concurrent writers into one fsync")

	fs.Int64Var(&c.MemtableSize, "memtable-size", c.MemtableSize, "memtable rotation threshold in bytes")
	fs.IntVar(&c.MaxImmutables, "max-immutables", c.MaxImmutables, "immutable memtables allowed before writers stall")
	fs.IntVar(&c.BlockSize, "block-size", c.BlockSize, "SSTable data block size in bytes")
	fs.IntVar(&c.BloomBitsKey, "bloom-bits", c.BloomBitsKey, "Bloom filter bits per key (0 disables)")
	fs.Int64Var(&c.BlockCacheSize, "cache-size", c.BlockCacheSize, "block cache size in bytes")

	fs.StringVar(&c.Compaction, "compaction", c.Compaction, "compaction style: leveled | size-tiered")
	fs.IntVar(&c.L0CompactTrigger, "l0-compact", c.L0CompactTrigger, "L0 file count that triggers compaction")
	fs.IntVar(&c.L0SlowdownTrigger, "l0-slowdown", c.L0SlowdownTrigger, "L0 file count that slows writes")
	fs.IntVar(&c.L0StopTrigger, "l0-stop", c.L0StopTrigger, "L0 file count that stops writes")
	fs.IntVar(&c.LevelSizeRatio, "level-ratio", c.LevelSizeRatio, "size ratio between levels")
	fs.Int64Var(&c.L1MaxBytes, "l1-bytes", c.L1MaxBytes, "target size of level 1 in bytes")

	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "debug | info | warn | error")
	fs.StringVar(&c.LogFormat, "log-format", c.LogFormat, "text | json")
	fs.BoolVar(&c.DevMode, "dev", c.DevMode, "enable dev-only admin endpoints")
}

// Parse parses args (without the program name) into a validated Config.
func Parse(args []string, out io.Writer) (Config, error) {
	c := Default()
	fs := flag.NewFlagSet("nebuladb", flag.ContinueOnError)
	fs.SetOutput(out)
	c.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	return c, c.Validate()
}

// Validate checks every field and returns a descriptive error for the first
// problem found.
func (c *Config) Validate() error {
	switch c.Fsync {
	case FsyncAlways, FsyncEvery, FsyncNever:
	default:
		return fmt.Errorf("config: -fsync must be always|every|never, got %q", c.Fsync)
	}
	switch c.Compaction {
	case CompactLeveled, CompactSizeTiered:
	default:
		return fmt.Errorf("config: -compaction must be leveled|size-tiered, got %q", c.Compaction)
	}
	switch strings.ToLower(c.LogFormat) {
	case "text", "json":
	default:
		return fmt.Errorf("config: -log-format must be text|json, got %q", c.LogFormat)
	}
	if c.Dir == "" {
		return fmt.Errorf("config: -dir must not be empty")
	}
	if c.Fsync == FsyncEvery && c.FsyncInterval <= 0 {
		return fmt.Errorf("config: -fsync-interval must be > 0 with -fsync=every")
	}
	if c.MaxConns < 1 {
		return fmt.Errorf("config: -max-conns must be >= 1")
	}
	if c.MemtableSize < 1<<10 {
		return fmt.Errorf("config: -memtable-size must be >= 1KiB")
	}
	if c.MaxImmutables < 1 {
		return fmt.Errorf("config: -max-immutables must be >= 1")
	}
	if c.BlockSize < 256 {
		return fmt.Errorf("config: -block-size must be >= 256")
	}
	if c.BloomBitsKey < 0 || c.BloomBitsKey > 64 {
		return fmt.Errorf("config: -bloom-bits must be in [0,64]")
	}
	if c.BlockCacheSize < 0 {
		return fmt.Errorf("config: -cache-size must be >= 0")
	}
	if c.L0CompactTrigger < 1 || c.L0SlowdownTrigger < c.L0CompactTrigger || c.L0StopTrigger < c.L0SlowdownTrigger {
		return fmt.Errorf("config: need 1 <= l0-compact <= l0-slowdown <= l0-stop")
	}
	if c.LevelSizeRatio < 2 {
		return fmt.Errorf("config: -level-ratio must be >= 2")
	}
	if c.L1MaxBytes < 1<<10 {
		return fmt.Errorf("config: -l1-bytes must be >= 1KiB")
	}
	return nil
}
