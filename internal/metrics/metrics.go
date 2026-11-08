// Package metrics provides the few observability primitives NebulaDB needs,
// implemented on the standard library: lock-free counters, a fixed-bucket
// latency histogram (with quantile estimates for INFO) and a writer for the
// Prometheus text exposition format (version 0.0.4).
//
// Not using the Prometheus client library keeps the dependency list empty and
// the hot path to one or two atomic adds per observation.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// bucketBounds are histogram upper bounds in microseconds: roughly 1-2.5-5
// steps from 5 µs to 10 s. Observations above the last bound land in +Inf.
var bucketBounds = [...]float64{
	5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
	100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000,
}

const numBuckets = len(bucketBounds) + 1 // + overflow

// Histogram records durations into fixed buckets. Observe is wait-free: two
// atomic adds. Quantiles are estimates by linear interpolation inside the
// bucket, so they are only as precise as the bucket width (benchmarks that need
// exact percentiles record raw samples instead).
type Histogram struct {
	buckets [numBuckets]atomic.Uint64
	count   atomic.Uint64
	sumNs   atomic.Uint64
}

// Observe records one duration.
func (h *Histogram) Observe(d time.Duration) {
	us := float64(d) / float64(time.Microsecond)
	i := sort.SearchFloat64s(bucketBounds[:], us) // first bound >= us
	h.buckets[i].Add(1)
	h.count.Add(1)
	h.sumNs.Add(uint64(d))
}

// HistSnapshot is a copy of a histogram's state.
type HistSnapshot struct {
	Buckets [numBuckets]uint64
	Count   uint64
	SumNs   uint64
}

// Snapshot copies the current state.
func (h *Histogram) Snapshot() HistSnapshot {
	var s HistSnapshot
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
	}
	s.Count = h.count.Load()
	s.SumNs = h.sumNs.Load()
	return s
}

// Mean returns the mean observation.
func (s HistSnapshot) Mean() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(s.SumNs / s.Count)
}

// Quantile estimates the q-quantile (0 < q <= 1).
func (s HistSnapshot) Quantile(q float64) time.Duration {
	var total uint64
	for _, c := range s.Buckets {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := q * float64(total)
	var cum float64
	for i, c := range s.Buckets {
		if c == 0 {
			continue
		}
		if cum+float64(c) >= rank {
			lo := 0.0
			if i > 0 {
				lo = bucketBounds[i-1]
			}
			hi := lo * 2
			if i < len(bucketBounds) {
				hi = bucketBounds[i]
			}
			frac := (rank - cum) / float64(c)
			return time.Duration((lo + frac*(hi-lo)) * float64(time.Microsecond))
		}
		cum += float64(c)
	}
	return time.Duration(bucketBounds[len(bucketBounds)-1] * float64(time.Microsecond))
}

// ---------------------------------------------------------------- exposition

// Exposition writes Prometheus text format. Methods may be called in any order;
// HELP/TYPE headers are written once per metric name, before its first sample.
type Exposition struct {
	w      io.Writer
	seen   map[string]bool
	prefix string
	err    error
}

// NewExposition writes to w, prefixing every metric name with prefix.
func NewExposition(w io.Writer, prefix string) *Exposition {
	return &Exposition{w: w, seen: map[string]bool{}, prefix: prefix}
}

// Err returns the first write error.
func (e *Exposition) Err() error { return e.err }

func (e *Exposition) printf(format string, a ...any) {
	if e.err == nil {
		_, e.err = fmt.Fprintf(e.w, format, a...)
	}
}

func (e *Exposition) header(name, help, typ string) {
	if e.seen[name] {
		return
	}
	e.seen[name] = true
	e.printf("# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(help), name, typ)
}

func escapeHelp(s string) string {
	return strings.NewReplacer("\\", `\\`, "\n", `\n`).Replace(s)
}

func escapeLabel(s string) string {
	return strings.NewReplacer("\\", `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

// labels formats alternating key, value pairs as {k="v",...}.
func labels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, kv[i], escapeLabel(kv[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

func fmtFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Counter writes a counter sample.
func (e *Exposition) Counter(name, help string, v float64, kv ...string) {
	name = e.prefix + name
	e.header(name, help, "counter")
	e.printf("%s%s %s\n", name, labels(kv), fmtFloat(v))
}

// Gauge writes a gauge sample.
func (e *Exposition) Gauge(name, help string, v float64, kv ...string) {
	name = e.prefix + name
	e.header(name, help, "gauge")
	e.printf("%s%s %s\n", name, labels(kv), fmtFloat(v))
}

// Histogram writes a histogram in seconds.
func (e *Exposition) Histogram(name, help string, s HistSnapshot, kv ...string) {
	name = e.prefix + name
	e.header(name, help, "histogram")
	var cum uint64
	for i := 0; i < numBuckets; i++ {
		cum += s.Buckets[i]
		le := "+Inf"
		if i < len(bucketBounds) {
			le = fmtFloat(bucketBounds[i] / 1e6)
		}
		e.printf("%s_bucket%s %d\n", name, labels(append(append([]string(nil), kv...), "le", le)), cum)
	}
	e.printf("%s_sum%s %s\n", name, labels(kv), fmtFloat(float64(s.SumNs)/1e9))
	e.printf("%s_count%s %d\n", name, labels(kv), s.Count)
}
