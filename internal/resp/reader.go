// Package resp implements the Redis serialization protocol, version 2 (RESP2):
// a streaming parser (server side: commands; client side: any reply value) and
// a buffered writer.
//
// Wire format recap (every line ends in \r\n):
//
//	+OK                     simple string
//	-ERR message            error
//	:42                     integer
//	$5\r\nhello             bulk string (length-prefixed, binary safe); $-1 is nil
//	*2\r\n$3\r\nGET\r\n$1\r\nk   array of the above; *-1 is nil array
//
// Clients send commands as an array of bulk strings. For telnet convenience a
// line that does not start with '*' is an "inline command" split on spaces.
//
// The parser is hardened against hostile input: every length is bounds-checked
// before allocating, huge bulk bodies are read incrementally so a client cannot
// make us allocate memory it never sends, and nesting depth is capped. It never
// panics (see FuzzReadCommand / FuzzReadValue).
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Limits for hostile-input protection.
const (
	MaxBulkLen     = 64 << 20 // largest accepted bulk string (Redis allows 512MB)
	MaxArrayLen    = 1 << 20  // largest accepted array element count
	MaxInlineLen   = 64 << 10 // longest accepted line
	maxDepth       = 32       // nested array depth for ReadValue
	bulkChunk      = 64 << 10 // bulk bodies above this are read in chunks
	readerBufBytes = 64 << 10
)

// ProtocolError is a malformed-input error. The server replies with
// "-ERR Protocol error: ..." and closes the connection, since after a framing
// error the byte stream cannot be re-synchronised.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErr(format string, a ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, a...)}
}

// IsProtocolError reports whether err is a *ProtocolError.
func IsProtocolError(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

// Reader parses RESP from a stream. It is not safe for concurrent use.
type Reader struct {
	br *bufio.Reader
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readerBufBytes)}
}

// Buffered returns the number of bytes already read from the connection but
// not yet parsed. The server uses it to detect a pipelined batch: while more
// input is buffered it keeps executing commands and only flushes replies when
// the batch is exhausted.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// readLine returns one line without its terminator. The returned slice is only
// valid until the next read. If strictCRLF, a bare \n is a protocol error.
func (r *Reader) readLine(strictCRLF bool) ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("too big line")
		}
		if err == io.EOF && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if len(line) > MaxInlineLen {
		return nil, protoErr("too big line")
	}
	line = line[:len(line)-1] // drop \n
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	} else if strictCRLF {
		return nil, protoErr("expected CRLF line terminator")
	}
	return line, nil
}

// parseInt parses a decimal integer, rejecting anything strconv would accept
// that RESP does not (leading '+', spaces, empty).
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 || len(b) > 20 {
		return 0, protoErr("invalid integer")
	}
	if b[0] == '+' {
		return 0, protoErr("invalid integer")
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, protoErr("invalid integer")
	}
	return n, nil
}

// readBulkBody reads exactly n bytes plus the trailing CRLF.
func (r *Reader) readBulkBody(n int) ([]byte, error) {
	var buf []byte
	if n <= bulkChunk {
		buf = make([]byte, n)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, unexpected(err)
		}
	} else {
		// Grow as data really arrives instead of trusting the declared length.
		buf = make([]byte, 0, bulkChunk)
		for len(buf) < n {
			want := n - len(buf)
			if want > bulkChunk {
				want = bulkChunk
			}
			start := len(buf)
			buf = append(buf, make([]byte, want)...)
			if _, err := io.ReadFull(r.br, buf[start:]); err != nil {
				return nil, unexpected(err)
			}
		}
	}
	var crlf [2]byte
	if _, err := io.ReadFull(r.br, crlf[:]); err != nil {
		return nil, unexpected(err)
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf, nil
}

// unexpected converts EOF in the middle of a frame to ErrUnexpectedEOF so
// callers can tell a clean disconnect from a truncated message.
func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// ReadCommand reads one client command as a list of arguments. It returns
// io.EOF only on a clean end of stream between commands. An empty (nil, nil)
// result means an empty command (blank inline line or "*0"); callers skip it.
// The returned argument slices are freshly allocated and may be retained.
func (r *Reader) ReadCommand() ([][]byte, error) {
	b, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] != '*' {
		return r.readInline()
	}
	line, err := r.readLine(true)
	if err != nil {
		return nil, unexpected(err)
	}
	n, err := parseInt(line[1:])
	if err != nil {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxArrayLen {
		return nil, protoErr("invalid multibulk length")
	}
	// Don't trust n for the initial capacity beyond a small bound.
	args := make([][]byte, 0, min(int(n), 64))
	for i := int64(0); i < n; i++ {
		line, err := r.readLine(true)
		if err != nil {
			return nil, unexpected(err)
		}
		if len(line) == 0 || line[0] != '$' {
			c := byte(0)
			if len(line) > 0 {
				c = line[0]
			}
			return nil, protoErr("expected '$', got '%c'", c)
		}
		l, err := parseInt(line[1:])
		if err != nil || l < 0 || l > MaxBulkLen {
			return nil, protoErr("invalid bulk length")
		}
		body, err := r.readBulkBody(int(l))
		if err != nil {
			return nil, err
		}
		args = append(args, body)
	}
	return args, nil
}

func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine(false)
	if err != nil {
		return nil, err
	}
	fields := bytes.Fields(line)
	if len(fields) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(fields))
	for i, f := range fields {
		out[i] = append([]byte(nil), f...)
	}
	return out, nil
}

// Kind is the RESP type of a Value.
type Kind byte

const (
	KindSimple Kind = '+'
	KindError  Kind = '-'
	KindInt    Kind = ':'
	KindBulk   Kind = '$'
	KindArray  Kind = '*'
)

// Value is a parsed RESP value (used by clients, tests and the load generator).
type Value struct {
	Kind  Kind
	Str   []byte // simple, error and bulk payload
	Int   int64
	Elems []Value
	Nil   bool // nil bulk ($-1) or nil array (*-1)
}

// ReadValue reads one complete RESP value of any type.
func (r *Reader) ReadValue() (Value, error) { return r.readValue(0) }

func (r *Reader) readValue(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, protoErr("nesting too deep")
	}
	line, err := r.readLine(true)
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protoErr("empty line")
	}
	kind, rest := Kind(line[0]), line[1:]
	switch kind {
	case KindSimple, KindError:
		return Value{Kind: kind, Str: append([]byte(nil), rest...)}, nil
	case KindInt:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindInt, Int: n}, nil
	case KindBulk:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: KindBulk, Nil: true}, nil
		}
		if n < 0 || n > MaxBulkLen {
			return Value{}, protoErr("invalid bulk length")
		}
		body, err := r.readBulkBody(int(n))
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBulk, Str: body}, nil
	case KindArray:
		n, err := parseInt(rest)
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Kind: KindArray, Nil: true}, nil
		}
		if n < 0 || n > MaxArrayLen {
			return Value{}, protoErr("invalid multibulk length")
		}
		elems := make([]Value, 0, min(int(n), 64))
		for i := int64(0); i < n; i++ {
			v, err := r.readValue(depth + 1)
			if err != nil {
				return Value{}, unexpected(err)
			}
			elems = append(elems, v)
		}
		return Value{Kind: KindArray, Elems: elems}, nil
	default:
		return Value{}, protoErr("unknown type byte '%c'", line[0])
	}
}
