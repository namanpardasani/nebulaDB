// Command nebula is the NebulaDB maintenance CLI.
//
//	nebula stats          [-dir DIR | -addr HOST:PORT]
//	nebula dump-wal       [-n N] PATH        (a segment file or a database directory)
//	nebula dump-sstable   [-n N] FILE
//	nebula dump-manifest  DIR
//	nebula verify         DIR
//	nebula repair         SRC_DIR DST_DIR
//	nebula compact        DIR                (the server must not be running)
//
// Everything except `compact` and the destination of `repair` is read-only.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/internal/tools"
)

const usage = `usage: nebula <command> [flags] [args]

commands:
  stats [-dir DIR | -addr HOST:PORT]   summary of a data directory, or INFO from a running server
  dump-wal [-n N] PATH                 print WAL records of a segment file or every segment in a directory
  dump-sstable [-n N] FILE             print a table's properties and entries
  dump-manifest DIR                    print the MANIFEST edit log and the live file set
  verify DIR                           check every checksum and cross-reference (read-only); exit 1 on problems
  repair SRC DST                       salvage readable data from SRC into a new database in DST
  compact DIR                          flush and fully compact (the server must be stopped)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	if err := run(cmd, args); err != nil {
		fmt.Fprintln(os.Stderr, "nebula:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	switch cmd {
	case "stats":
		dir := fs.String("dir", "", "database directory (offline summary)")
		addr := fs.String("addr", "", "running server address (prints INFO)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		switch {
		case *dir != "":
			return tools.Stats(os.Stdout, *dir)
		case *addr != "":
			return liveInfo(*addr)
		}
		return fmt.Errorf("stats: give -dir or -addr")
	case "dump-wal":
		n := fs.Int("n", 0, "max batches per segment (0 = all)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("dump-wal: need exactly one PATH")
		}
		return tools.DumpWAL(os.Stdout, fs.Arg(0), *n)
	case "dump-sstable":
		n := fs.Int("n", 50, "max entries to print (0 = all)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("dump-sstable: need exactly one FILE")
		}
		return tools.DumpSSTable(os.Stdout, fs.Arg(0), *n)
	case "dump-manifest":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("dump-manifest: need DIR")
		}
		return tools.DumpManifest(os.Stdout, fs.Arg(0))
	case "verify":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("verify: need DIR")
		}
		rep, err := tools.Verify(os.Stdout, fs.Arg(0))
		if err != nil {
			return err
		}
		if !rep.OK() {
			os.Exit(1)
		}
		return nil
	case "repair":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return fmt.Errorf("repair: need SRC and DST")
		}
		_, err := tools.Repair(os.Stdout, fs.Arg(0), fs.Arg(1))
		return err
	case "compact":
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("compact: need DIR")
		}
		return tools.Compact(os.Stdout, fs.Arg(0))
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	}
	fmt.Fprint(os.Stderr, usage)
	return fmt.Errorf("unknown command %q", cmd)
}

// liveInfo sends INFO to a running server and prints the reply.
func liveInfo(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	w := resp.NewWriter(c)
	w.WriteCommand([]byte("INFO"))
	if err := w.Flush(); err != nil {
		return err
	}
	v, err := resp.NewReader(bufio.NewReader(c)).ReadValue()
	if err != nil {
		return err
	}
	if v.Kind == resp.KindError {
		return fmt.Errorf("server: %s", v.Str)
	}
	fmt.Print(strings.ReplaceAll(string(v.Str), "\r\n", "\n"))
	return nil
}
