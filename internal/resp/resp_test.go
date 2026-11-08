package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func cmdOf(t *testing.T, in string) ([][]byte, error) {
	t.Helper()
	return NewReader(strings.NewReader(in)).ReadCommand()
}

func strs(a [][]byte) []string {
	out := make([]string, len(a))
	for i := range a {
		out[i] = string(a[i])
	}
	return out
}

func TestReadCommandVectors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"get", "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", []string{"GET", "k"}},
		{"empty bulk", "*2\r\n$3\r\nSET\r\n$0\r\n\r\n", []string{"SET", ""}},
		{"binary safe", "*1\r\n$4\r\na\r\nb\r\n", []string{"a\r\nb"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline spaces", "  SET   a   b \n", []string{"SET", "a", "b"}},
		{"blank inline", "\r\n", nil},
		{"star zero", "*0\r\n", nil},
		{"star negative", "*-1\r\n", nil},
	}
	for _, tt := range tests {
		got, err := cmdOf(t, tt.in)
		if err != nil {
			t.Errorf("%s: %v", tt.name, err)
			continue
		}
		if g := strs(got); strings.Join(g, "|") != strings.Join(tt.want, "|") || len(g) != len(tt.want) {
			t.Errorf("%s: got %q want %q", tt.name, g, tt.want)
		}
	}
}

func TestReadCommandMalformed(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		proto bool // expect *ProtocolError (else unexpected EOF)
	}{
		{"bad array len", "*abc\r\n", true},
		{"huge array", "*99999999999\r\n", true},
		{"array not bulk", "*1\r\n:5\r\n", true},
		{"neg bulk in cmd", "*1\r\n$-1\r\n", true},
		{"huge bulk", "*1\r\n$99999999999\r\n", true},
		{"bulk bad terminator", "*1\r\n$3\r\nabcXY", true},
		{"bare LF in multibulk header", "*1\n", true},
		{"plus sign length", "*+1\r\n", true},
		{"truncated header", "*2\r\n$3\r\nGET\r\n", false},
		{"truncated body", "*1\r\n$10\r\nabc", false},
		{"truncated line", "*1", false},
		{"too long line", "*" + strings.Repeat("1", MaxInlineLen+10), true},
	}
	for _, tt := range tests {
		_, err := cmdOf(t, tt.in)
		if err == nil {
			t.Errorf("%s: want error", tt.name)
			continue
		}
		if tt.proto != IsProtocolError(err) {
			t.Errorf("%s: err=%v protocol=%v want %v", tt.name, err, IsProtocolError(err), tt.proto)
		}
		if !tt.proto && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%s: want ErrUnexpectedEOF, got %v", tt.name, err)
		}
	}
}

func TestCleanEOF(t *testing.T) {
	if _, err := cmdOf(t, ""); err != io.EOF {
		t.Fatalf("got %v", err)
	}
}

// A byte-at-a-time reader proves the parser copes with arbitrary TCP segmentation.
func TestPartialReads(t *testing.T) {
	in := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\nPING\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(in)))
	a, err := r.ReadCommand()
	if err != nil || strings.Join(strs(a), " ") != "SET k hello" {
		t.Fatalf("%v %q", err, strs(a))
	}
	b, err := r.ReadCommand()
	if err != nil || string(b[0]) != "PING" {
		t.Fatalf("%v %q", err, strs(b))
	}
}

func TestPipelineBuffered(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(strings.NewReader(in))
	r.ReadCommand()
	if r.Buffered() == 0 {
		t.Fatal("second pipelined command should be buffered")
	}
	r.ReadCommand()
	if r.Buffered() != 0 {
		t.Fatal("buffer should be drained")
	}
}

func TestLargeBulkChunked(t *testing.T) {
	big := strings.Repeat("x", 3*bulkChunk+17)
	in := "*1\r\n$" + "196625" + "\r\n" + big + "\r\n"
	if len(big) != 196625 {
		t.Fatal("test constant out of date")
	}
	a, err := cmdOf(t, in)
	if err != nil || string(a[0]) != big {
		t.Fatalf("err=%v len=%d", err, len(a[0]))
	}
	// Declared huge but only a little data sent: must fail without allocating the declared size.
	_, err = cmdOf(t, "*1\r\n$60000000\r\nabc")
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", err)
	}
}

func TestReadValue(t *testing.T) {
	in := "+OK\r\n-ERR boom\r\n:-7\r\n$3\r\nabc\r\n$-1\r\n*2\r\n:1\r\n*1\r\n$1\r\nx\r\n*-1\r\n"
	r := NewReader(strings.NewReader(in))
	var vs []Value
	for {
		v, err := r.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
	}
	if len(vs) != 7 {
		t.Fatalf("got %d values", len(vs))
	}
	if string(vs[0].Str) != "OK" || vs[1].Kind != KindError || vs[2].Int != -7 ||
		string(vs[3].Str) != "abc" || !vs[4].Nil || len(vs[5].Elems) != 2 ||
		string(vs[5].Elems[1].Elems[0].Str) != "x" || !vs[6].Nil {
		t.Fatalf("bad parse: %+v", vs)
	}
}

func TestReadValueMalformed(t *testing.T) {
	for _, in := range []string{"?x\r\n", ":12a\r\n", "$-5\r\n", "*-5\r\n", "\r\n", strings.Repeat("*1\r\n", maxDepth+3)} {
		if _, err := NewReader(strings.NewReader(in)).ReadValue(); err == nil {
			t.Errorf("input %q: want error", in)
		}
	}
}

func TestWriter(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	w.OK()
	w.SimpleString("PONG")
	w.Error("ERR bad\r\nthing")
	w.Integer(-3)
	w.Bulk([]byte("hi"))
	w.BulkString("")
	w.Null()
	w.NullArray()
	w.ArrayHeader(2)
	w.Integer(1)
	w.Integer(2)
	if b.Len() != 0 {
		t.Fatal("writer must buffer until Flush")
	}
	w.Flush()
	want := "+OK\r\n+PONG\r\n-ERR bad  thing\r\n:-3\r\n$2\r\nhi\r\n$0\r\n\r\n$-1\r\n*-1\r\n*2\r\n:1\r\n:2\r\n"
	if b.String() != want {
		t.Fatalf("got %q", b.String())
	}
}

func TestWriteCommandRoundTrip(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	w.WriteCommand([]byte("SET"), []byte("k\r\n"), []byte(""))
	w.Flush()
	got, err := NewReader(&b).ReadCommand()
	if err != nil || strings.Join(strs(got), "|") != "SET|k\r\n|" {
		t.Fatalf("%v %q", err, strs(got))
	}
}

func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*1\r\n$-1\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 1000; i++ {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}

func FuzzReadValue(f *testing.F) {
	f.Add([]byte("*2\r\n:1\r\n$1\r\nx\r\n"))
	f.Add([]byte("-ERR\r\n"))
	f.Add([]byte("*1\r\n*1\r\n*1\r\n:1\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 1000; i++ {
			if _, err := r.ReadValue(); err != nil {
				return
			}
		}
	})
}
