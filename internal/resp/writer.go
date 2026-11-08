package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes RESP replies into a buffer. Nothing reaches the socket until
// Flush, which is how pipelined replies get coalesced into few syscalls.
// Errors are sticky: after the first failed write all methods become no-ops and
// Flush returns the error.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

// NewWriter wraps w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16<<10), scratch: make([]byte, 0, 32)}
}

// Flush sends buffered replies.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered returns the number of unflushed bytes.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

func (w *Writer) line(prefix byte, s string) {
	w.bw.WriteByte(prefix)
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) num(prefix byte, n int64) {
	w.scratch = append(w.scratch[:0], prefix)
	w.scratch = strconv.AppendInt(w.scratch, n, 10)
	w.scratch = append(w.scratch, '\r', '\n')
	w.bw.Write(w.scratch)
}

// SimpleString writes +s. s must not contain CR or LF.
func (w *Writer) SimpleString(s string) { w.line('+', s) }

// OK writes +OK.
func (w *Writer) OK() { w.line('+', "OK") }

// Error writes -msg. msg conventionally starts with an upper-case code such as
// "ERR" or "WRONGTYPE". CR/LF are replaced so a message can never break framing.
func (w *Writer) Error(msg string) {
	b := []byte(msg)
	for i, c := range b {
		if c == '\r' || c == '\n' {
			b[i] = ' '
		}
	}
	w.line('-', string(b))
}

// Integer writes :n.
func (w *Writer) Integer(n int64) { w.num(':', n) }

// Bulk writes a binary-safe bulk string.
func (w *Writer) Bulk(b []byte) {
	w.num('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// BulkString is Bulk for a string.
func (w *Writer) BulkString(s string) {
	w.num('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Null writes the RESP2 nil bulk string ($-1).
func (w *Writer) Null() { w.bw.WriteString("$-1\r\n") }

// NullArray writes the RESP2 nil array (*-1).
func (w *Writer) NullArray() { w.bw.WriteString("*-1\r\n") }

// ArrayHeader starts an array of n elements; the caller then writes n values.
func (w *Writer) ArrayHeader(n int) { w.num('*', int64(n)) }

// WriteCommand encodes a client command (array of bulk strings). Used by the
// load generator and tests.
func (w *Writer) WriteCommand(args ...[]byte) {
	w.ArrayHeader(len(args))
	for _, a := range args {
		w.Bulk(a)
	}
}
