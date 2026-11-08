package config

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestDefaultIsValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Fsync != FsyncAlways || !c.GroupCommit {
		t.Fatal("defaults must be the safest settings")
	}
}

func TestParse(t *testing.T) {
	c, err := Parse([]string{"-dir", "/x", "-fsync", "every", "-fsync-interval", "50ms",
		"-memtable-size", "65536", "-compaction", "size-tiered", "-group-commit=false"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.Dir != "/x" || c.Fsync != FsyncEvery || c.FsyncInterval != 50*time.Millisecond ||
		c.MemtableSize != 65536 || c.Compaction != CompactSizeTiered || c.GroupCommit {
		t.Fatalf("unexpected: %+v", c)
	}
}

func TestValidateErrors(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"-fsync", "sometimes"}, "-fsync"},
		{[]string{"-compaction", "x"}, "-compaction"},
		{[]string{"-memtable-size", "10"}, "memtable-size"},
		{[]string{"-bloom-bits", "99"}, "bloom-bits"},
		{[]string{"-l0-compact", "9", "-l0-slowdown", "8"}, "l0-compact"},
		{[]string{"-fsync", "every", "-fsync-interval", "0s"}, "fsync-interval"},
		{[]string{"-log-format", "xml"}, "log-format"},
		{[]string{"-dir", ""}, "-dir"},
		{[]string{"-max-conns", "0"}, "max-conns"},
		{[]string{"-level-ratio", "1"}, "level-ratio"},
	}
	for _, tt := range tests {
		_, err := Parse(tt.args, io.Discard)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("args %v: err=%v, want mention of %q", tt.args, err, tt.want)
		}
	}
}
