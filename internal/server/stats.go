package server

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/naman/nebuladb/internal/metrics"
	"github.com/naman/nebuladb/internal/resp"
)

// Class groups commands for latency accounting.
type Class int

const (
	ClassRead Class = iota
	ClassWrite
	ClassOther
	numClasses
)

func (c Class) String() string { return [...]string{"read", "write", "other"}[c] }

var commandClass = map[string]Class{
	"GET": ClassRead, "EXISTS": ClassRead, "TTL": ClassRead, "PTTL": ClassRead, "MGET": ClassRead, "SCAN": ClassRead, "DBSIZE": ClassRead,
	"SET": ClassWrite, "DEL": ClassWrite, "EXPIRE": ClassWrite, "PEXPIRE": ClassWrite, "PERSIST": ClassWrite, "MSET": ClassWrite,
	"INCR": ClassWrite, "DECR": ClassWrite, "INCRBY": ClassWrite, "DECRBY": ClassWrite, "FLUSHDB": ClassWrite, "FLUSHALL": ClassWrite,
}

// serverMetrics holds the command-level counters and histograms.
type serverMetrics struct {
	latency   [numClasses]metrics.Histogram
	perCmd    map[string]*metrics.Counter // fixed key set after init: lock-free reads
	unknown   metrics.Counter
	total     metrics.Counter
	opsPerSec atomic.Uint64 // float64 bits, updated by the sampler
}

func newServerMetrics() *serverMetrics {
	m := &serverMetrics{perCmd: map[string]*metrics.Counter{}}
	for name := range commands {
		m.perCmd[name] = &metrics.Counter{}
	}
	m.perCmd["QUIT"] = &metrics.Counter{}
	return m
}

// Snapshot is a copy of the server's counters for INFO, Prometheus and the admin API.
type Snapshot struct {
	Uptime        time.Duration
	ActiveConns   int64
	TotalConns    int64
	RejectedConns int64
	TotalCommands uint64
	UnknownCmds   uint64
	OpsPerSec     float64
	PerCommand    map[string]uint64
	Latency       [numClasses]metrics.HistSnapshot
}

// Snapshot returns the current counters.
func (s *Server) Snapshot() Snapshot {
	sn := Snapshot{
		Uptime:        time.Since(s.started),
		ActiveConns:   s.activeConns.Load(),
		TotalConns:    s.totalConns.Load(),
		RejectedConns: s.rejectedConns.Load(),
		TotalCommands: uint64(s.totalCmds.Load()),
		UnknownCmds:   s.m.unknown.Value(),
		OpsPerSec:     s.opsPerSec(),
		PerCommand:    map[string]uint64{},
	}
	for name, c := range s.m.perCmd {
		if v := c.Value(); v > 0 {
			sn.PerCommand[name] = v
		}
	}
	for i := range sn.Latency {
		sn.Latency[i] = s.m.latency[i].Snapshot()
	}
	return sn
}

// ClassNames returns the latency class names in index order.
func ClassNames() []Class { return []Class{ClassRead, ClassWrite, ClassOther} }

func (s *Server) opsPerSec() float64 {
	return float64frombits(s.m.opsPerSec.Load())
}

// sampleLoop keeps an exponentially smoothed ops/sec figure (like Redis's
// instantaneous_ops_per_sec) until stop is closed.
func (s *Server) sampleLoop(stop <-chan struct{}) {
	const interval = 250 * time.Millisecond
	t := time.NewTicker(interval)
	defer t.Stop()
	last := s.totalCmds.Load()
	rate := 0.0
	for {
		select {
		case <-t.C:
			cur := s.totalCmds.Load()
			inst := float64(cur-last) / interval.Seconds()
			last = cur
			rate = 0.6*rate + 0.4*inst
			s.m.opsPerSec.Store(float64bits(rate))
		case <-stop:
			return
		}
	}
}

// Exec runs one command in-process (no socket) and returns the parsed reply.
// It is the engine behind the admin "try it" proxy; the caller is responsible
// for deciding which commands are allowed.
func (s *Server) Exec(args [][]byte) (resp.Value, error) {
	if len(args) == 0 {
		return resp.Value{}, nil
	}
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	s.totalCmds.Add(1)
	s.dispatch(w, args)
	if err := w.Flush(); err != nil {
		return resp.Value{}, err
	}
	return resp.NewReader(&buf).ReadValue()
}

// latencyReport renders p50/p99/p99.9 per class for INFO.
func latencyReport(sn Snapshot) string {
	var b strings.Builder
	classes := ClassNames()
	sort.Slice(classes, func(i, j int) bool { return classes[i] < classes[j] })
	for _, c := range classes {
		h := sn.Latency[c]
		b.WriteString(strings.ToLower(c.String()))
		b.WriteString("_commands:")
		b.WriteString(itoa(h.Count))
		b.WriteString("\r\n")
		for _, q := range []struct {
			name string
			q    float64
		}{{"p50", 0.5}, {"p99", 0.99}, {"p999", 0.999}} {
			b.WriteString(strings.ToLower(c.String()) + "_latency_" + q.name + "_us:")
			b.WriteString(itoa(uint64(h.Quantile(q.q) / time.Microsecond)))
			b.WriteString("\r\n")
		}
	}
	return b.String()
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

func float64bits(f float64) uint64     { return math.Float64bits(f) }
func float64frombits(b uint64) float64 { return math.Float64frombits(b) }
