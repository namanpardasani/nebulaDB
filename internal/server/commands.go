package server

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/resp"
)

type handler func(s *Server, w *resp.Writer, args [][]byte)

type command struct {
	// arity follows Redis: n>0 means exactly n args including the command name,
	// n<0 means at least -n.
	arity int
	fn    handler
}

var commands map[string]command

func init() {
	commands = map[string]command{
		"PING":     {-1, cmdPing},
		"ECHO":     {2, cmdEcho},
		"GET":      {2, cmdGet},
		"SET":      {-3, cmdSet},
		"DEL":      {-2, cmdDel},
		"EXISTS":   {-2, cmdExists},
		"EXPIRE":   {3, func(s *Server, w *resp.Writer, a [][]byte) { cmdExpire(s, w, a, 1000) }},
		"PEXPIRE":  {3, func(s *Server, w *resp.Writer, a [][]byte) { cmdExpire(s, w, a, 1) }},
		"TTL":      {2, func(s *Server, w *resp.Writer, a [][]byte) { cmdTTL(s, w, a, true) }},
		"PTTL":     {2, func(s *Server, w *resp.Writer, a [][]byte) { cmdTTL(s, w, a, false) }},
		"PERSIST":  {2, cmdPersist},
		"MGET":     {-2, cmdMGet},
		"MSET":     {-3, cmdMSet},
		"INCR":     {2, func(s *Server, w *resp.Writer, a [][]byte) { cmdIncr(s, w, a, 1, false) }},
		"DECR":     {2, func(s *Server, w *resp.Writer, a [][]byte) { cmdIncr(s, w, a, -1, false) }},
		"INCRBY":   {3, func(s *Server, w *resp.Writer, a [][]byte) { cmdIncr(s, w, a, 1, true) }},
		"DECRBY":   {3, func(s *Server, w *resp.Writer, a [][]byte) { cmdIncr(s, w, a, -1, true) }},
		"SCAN":     {-2, cmdScan},
		"DBSIZE":   {1, cmdDBSize},
		"INFO":     {-1, cmdInfo},
		"FLUSHDB":  {-1, cmdFlush},
		"FLUSHALL": {-1, cmdFlush},
		"COMMAND":  {-1, cmdCommand},
		"SELECT":   {2, cmdSelect},
		"HELLO":    {-1, cmdHello},
		"CLIENT":   {-2, cmdClient},
		// QUIT is handled in execute() because it closes the connection.
	}
}

const (
	errSyntax  = "ERR syntax error"
	errNotInt  = "ERR value is not an integer or out of range"
	errBadExp  = "ERR invalid expire time in '%s' command"
	errWrongOk = "ERR wrong number of arguments for '%s' command"
)

// execute looks up and runs a command. Returns true for QUIT.
func (s *Server) execute(w *resp.Writer, args [][]byte) bool {
	name := strings.ToUpper(string(args[0]))
	if name == "QUIT" {
		s.m.perCmd["QUIT"].Inc()
		w.OK()
		return true
	}
	cmd, ok := commands[name]
	if !ok {
		s.m.unknown.Inc()
		w.Error(fmt.Sprintf("ERR unknown command '%s'", truncate(string(args[0]), 64)))
		return false
	}
	if (cmd.arity > 0 && len(args) != cmd.arity) || (cmd.arity < 0 && len(args) < -cmd.arity) {
		w.Error(fmt.Sprintf(errWrongOk, strings.ToLower(name)))
		return false
	}
	s.m.perCmd[name].Inc()
	t0 := time.Now()
	cmd.fn(s, w, args)
	s.m.latency[commandClass[name]].Observe(time.Since(t0))
	return false
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// engErr translates an engine error into a RESP error reply.
func engErr(w *resp.Writer, err error) {
	if errors.Is(err, engine.ErrNotInteger) {
		w.Error(errNotInt)
		return
	}
	if errors.Is(err, engine.ErrReadOnly) {
		w.Error(err.Error()) // Redis' own wording and prefix, so client libraries recognise it
		return
	}
	w.Error("ERR " + err.Error())
}

// parseInt64 parses a strict base-10 int64 argument.
func parseInt64(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

// absExpire converts a relative expiry (n units of unitMs) to an absolute unix
// ms time, returning false on overflow or non-positive n.
func absExpire(now, n, unitMs int64) (int64, bool) {
	if n <= 0 || n > (math.MaxInt64-now)/unitMs {
		return 0, false
	}
	return now + n*unitMs, true
}

func cmdPing(_ *Server, w *resp.Writer, a [][]byte) {
	switch len(a) {
	case 1:
		w.SimpleString("PONG")
	case 2:
		w.Bulk(a[1])
	default:
		w.Error(fmt.Sprintf(errWrongOk, "ping"))
	}
}

func cmdEcho(_ *Server, w *resp.Writer, a [][]byte) { w.Bulk(a[1]) }

func cmdGet(s *Server, w *resp.Writer, a [][]byte) {
	v, ok, err := s.eng.Get(a[1])
	switch {
	case err != nil:
		engErr(w, err)
	case !ok:
		w.Null()
	default:
		w.Bulk(v)
	}
}

func cmdSet(s *Server, w *resp.Writer, a [][]byte) {
	var opt engine.SetOptions
	var haveEx bool
	var exN, exUnit int64
	for i := 3; i < len(a); i++ {
		switch strings.ToUpper(string(a[i])) {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "KEEPTTL":
			opt.KeepTTL = true
		case "EX", "PX":
			if haveEx || i+1 >= len(a) {
				w.Error(errSyntax)
				return
			}
			n, ok := parseInt64(a[i+1])
			if !ok {
				w.Error(errNotInt)
				return
			}
			haveEx, exN = true, n
			exUnit = 1
			if strings.EqualFold(string(a[i]), "EX") {
				exUnit = 1000
			}
			i++
		default:
			w.Error(errSyntax)
			return
		}
	}
	if (opt.NX && opt.XX) || (haveEx && opt.KeepTTL) {
		w.Error(errSyntax)
		return
	}
	if haveEx {
		at, ok := absExpire(s.now(), exN, exUnit)
		if !ok {
			w.Error(fmt.Sprintf(errBadExp, "set"))
			return
		}
		opt.ExpireAtMs = at
	}
	applied, err := s.eng.Set(a[1], a[2], opt)
	switch {
	case err != nil:
		engErr(w, err)
	case applied:
		w.OK()
	default:
		w.Null() // NX/XX condition not met
	}
}

func cmdDel(s *Server, w *resp.Writer, a [][]byte) {
	n, err := s.eng.Del(a[1:])
	if err != nil {
		engErr(w, err)
		return
	}
	w.Integer(int64(n))
}

func cmdExists(s *Server, w *resp.Writer, a [][]byte) {
	var n int64
	for _, k := range a[1:] { // repeated keys count repeatedly, as in Redis
		ok, err := s.eng.Exists(k)
		if err != nil {
			engErr(w, err)
			return
		}
		if ok {
			n++
		}
	}
	w.Integer(n)
}

func cmdExpire(s *Server, w *resp.Writer, a [][]byte, unitMs int64) {
	n, ok := parseInt64(a[2])
	if !ok {
		w.Error(errNotInt)
		return
	}
	if n <= 0 {
		// Redis semantics: a non-positive timeout deletes the key immediately.
		removed, err := s.eng.Del(a[1:2])
		if err != nil {
			engErr(w, err)
			return
		}
		w.Integer(int64(removed))
		return
	}
	at, ok := absExpire(s.now(), n, unitMs)
	if !ok {
		w.Error(fmt.Sprintf(errBadExp, strings.ToLower(string(a[0]))))
		return
	}
	set, err := s.eng.SetExpiry(a[1], at)
	if err != nil {
		engErr(w, err)
		return
	}
	if set {
		w.Integer(1)
	} else {
		w.Integer(0)
	}
}

func cmdTTL(s *Server, w *resp.Writer, a [][]byte, seconds bool) {
	ms, err := s.eng.TTL(a[1])
	if err != nil {
		engErr(w, err)
		return
	}
	if ms >= 0 && seconds {
		ms = (ms + 500) / 1000 // Redis rounds to nearest second
	}
	w.Integer(ms)
}

func cmdPersist(s *Server, w *resp.Writer, a [][]byte) {
	ok, err := s.eng.Persist(a[1])
	if err != nil {
		engErr(w, err)
		return
	}
	if ok {
		w.Integer(1)
	} else {
		w.Integer(0)
	}
}

func cmdMGet(s *Server, w *resp.Writer, a [][]byte) {
	keys := a[1:]
	vals := make([][]byte, len(keys))
	found := make([]bool, len(keys))
	var err error
	if mg, ok := s.eng.(engine.MultiGetter); ok {
		// One snapshot for all keys: MGET never observes half of an MSET.
		vals, found, err = mg.MGet(keys)
	} else {
		for i, k := range keys {
			if vals[i], found[i], err = s.eng.Get(k); err != nil {
				break
			}
		}
	}
	// Reply only after all reads succeeded, so an engine error yields one clean
	// error rather than a half-written array.
	if err != nil {
		engErr(w, err)
		return
	}
	w.ArrayHeader(len(vals))
	for i := range vals {
		if found[i] {
			w.Bulk(vals[i])
		} else {
			w.Null()
		}
	}
}

func cmdMSet(s *Server, w *resp.Writer, a [][]byte) {
	if len(a)%2 != 1 { // name + pairs => odd total
		w.Error(fmt.Sprintf(errWrongOk, "mset"))
		return
	}
	if err := s.eng.MSet(a[1:]); err != nil {
		engErr(w, err)
		return
	}
	w.OK()
}

func cmdIncr(s *Server, w *resp.Writer, a [][]byte, sign int64, hasDelta bool) {
	delta := int64(1)
	if hasDelta {
		var ok bool
		if delta, ok = parseInt64(a[2]); !ok {
			w.Error(errNotInt)
			return
		}
	}
	if sign < 0 {
		if delta == math.MinInt64 { // cannot be negated
			w.Error(errNotInt)
			return
		}
		delta = -delta
	}
	n, err := s.eng.IncrBy(a[1], delta)
	if err != nil {
		engErr(w, err)
		return
	}
	w.Integer(n)
}

func cmdScan(s *Server, w *resp.Writer, a [][]byte) {
	cur, ok := parseUint64(a[1])
	if !ok {
		w.Error("ERR invalid cursor")
		return
	}
	var start []byte
	if cur != 0 {
		k, found := s.cursors.get(cur)
		if !found {
			w.Error("ERR invalid cursor")
			return
		}
		start = k
	}
	count := 10
	var pattern []byte
	for i := 2; i < len(a); i += 2 {
		if i+1 >= len(a) {
			w.Error(errSyntax)
			return
		}
		switch strings.ToUpper(string(a[i])) {
		case "MATCH":
			pattern = a[i+1]
		case "COUNT":
			n, ok := parseInt64(a[i+1])
			if !ok {
				w.Error(errNotInt)
				return
			}
			if n < 1 {
				w.Error(errSyntax)
				return
			}
			if n > 10000 {
				n = 10000 // cap the work done per call
			}
			count = int(n)
		case "TYPE":
			// All values are strings; accept and ignore for client compatibility.
		default:
			w.Error(errSyntax)
			return
		}
	}
	keys, next, err := s.eng.Scan(start, count)
	if err != nil {
		engErr(w, err)
		return
	}
	// As in Redis, COUNT bounds keys *examined*, and MATCH filters afterwards,
	// so a page can legitimately be empty while the scan is not finished.
	out := keys[:0:0]
	for _, k := range keys {
		if pattern == nil || globMatch(pattern, k) {
			out = append(out, k)
		}
	}
	cursor := uint64(0)
	if next != nil {
		cursor = s.cursors.put(next)
	}
	w.ArrayHeader(2)
	w.BulkString(strconv.FormatUint(cursor, 10))
	w.ArrayHeader(len(out))
	for _, k := range out {
		w.Bulk(k)
	}
}

func parseUint64(b []byte) (uint64, bool) {
	n, err := strconv.ParseUint(string(b), 10, 64)
	return n, err == nil
}

func cmdDBSize(s *Server, w *resp.Writer, _ [][]byte) {
	n, err := s.eng.ApproxSize()
	if err != nil {
		engErr(w, err)
		return
	}
	w.Integer(n)
}

func cmdInfo(s *Server, w *resp.Writer, _ [][]byte) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "# Server\r\nnebuladb_version:%s\r\nuptime_in_seconds:%d\r\n",
		Version, int64(time.Since(s.started).Seconds()))
	fmt.Fprintf(&b, "\r\n# Clients\r\nconnected_clients:%d\r\ntotal_connections_received:%d\r\nrejected_connections:%d\r\n",
		s.activeConns.Load(), s.totalConns.Load(), s.rejectedConns.Load())
	sn := s.Snapshot()
	fmt.Fprintf(&b, "\r\n# Stats\r\ntotal_commands_processed:%d\r\ninstantaneous_ops_per_sec:%.0f\r\nunknown_commands:%d\r\n",
		sn.TotalCommands, sn.OpsPerSec, sn.UnknownCmds)
	fmt.Fprintf(&b, "\r\n# Latency (server-side command processing, bucketed estimates)\r\n%s", latencyReport(sn))
	fmt.Fprintf(&b, "\r\n# Engine\r\n")
	for _, st := range s.eng.Stats() {
		fmt.Fprintf(&b, "%s:%s\r\n", st.Name, st.Value)
	}
	w.Bulk(b.Bytes())
}

func cmdFlush(s *Server, w *resp.Writer, _ [][]byte) {
	if err := s.eng.Flush(); err != nil {
		engErr(w, err)
		return
	}
	w.OK()
}

// cmdCommand answers just enough for clients that probe on connect
// (redis-cli sends COMMAND DOCS). Everything but COUNT returns an empty array.
func cmdCommand(_ *Server, w *resp.Writer, a [][]byte) {
	if len(a) >= 2 && strings.EqualFold(string(a[1]), "COUNT") {
		w.Integer(int64(len(commands) + 1)) // +1 for QUIT
		return
	}
	w.ArrayHeader(0)
}

func cmdSelect(_ *Server, w *resp.Writer, a [][]byte) {
	if string(a[1]) != "0" {
		w.Error("ERR DB index is out of range") // single keyspace
		return
	}
	w.OK()
}

// cmdHello supports RESP2 only. Clients that try HELLO 3 get the NOPROTO error
// Redis itself sends, and fall back to RESP2.
func cmdHello(_ *Server, w *resp.Writer, a [][]byte) {
	if len(a) >= 2 {
		if v, ok := parseInt64(a[1]); !ok || v != 2 {
			w.Error("NOPROTO unsupported protocol version")
			return
		}
	}
	w.ArrayHeader(14)
	for _, kv := range [][2]string{{"server", "nebuladb"}, {"version", Version}} {
		w.BulkString(kv[0])
		w.BulkString(kv[1])
	}
	w.BulkString("proto")
	w.Integer(2)
	w.BulkString("id")
	w.Integer(1)
	w.BulkString("mode")
	w.BulkString("standalone")
	w.BulkString("role")
	w.BulkString("master")
	w.BulkString("modules")
	w.ArrayHeader(0)
}

func cmdClient(_ *Server, w *resp.Writer, a [][]byte) {
	switch strings.ToUpper(string(a[1])) {
	case "SETNAME", "SETINFO", "NO-EVICT", "NO-TOUCH":
		w.OK()
	case "ID":
		w.Integer(1)
	default:
		w.Error(fmt.Sprintf("ERR unknown subcommand '%s'", truncate(string(a[1]), 64)))
	}
}
