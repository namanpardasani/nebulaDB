package logx

import (
	"bytes"
	"strings"
	"testing"
)

func TestLevels(t *testing.T) {
	var b bytes.Buffer
	l, err := New(&b, "warn", "text")
	if err != nil {
		t.Fatal(err)
	}
	l.Info("hidden")
	l.Warn("shown")
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), "shown") {
		t.Fatalf("got %q", b.String())
	}
	if _, err := New(&b, "loud", "text"); err == nil {
		t.Fatal("want error for bad level")
	}
}

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	l, _ := New(&b, "info", "json")
	l.Info("hi", "k", 1)
	if !strings.HasPrefix(b.String(), "{") {
		t.Fatalf("not json: %q", b.String())
	}
}
