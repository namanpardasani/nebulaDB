// Command nebula is the NebulaDB maintenance CLI (stats, dumps, verify, repair, compact).
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "usage: nebula <stats|dump-wal|dump-sstable|dump-manifest|verify|repair|compact> [args]")
	os.Exit(2)
}
