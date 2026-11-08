package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistogramBasics(t *testing.T) {
	var h Histogram
	for i := 0; i < 100; i++ {
		h.Observe(80 * time.Microsecond) // bucket (50,100]
	}
	h.Observe(3 * time.Second) // tail
	s := h.Snapshot()
	if s.Count != 101 {
		t.Fatal(s.Count)
	}
	p50 := s.Quantile(0.5)
	if p50 < 50*time.Microsecond || p50 > 100*time.Microsecond {
		t.Fatalf("p50 %v outside its bucket", p50)
	}
	p999 := s.Quantile(0.999)
	if p999 < 2500*time.Millisecond || p999 > 5*time.Second {
		t.Fatalf("p99.9 %v should be in the tail bucket", p999)
	}
	if m := s.Mean(); m < 20*time.Millisecond || m > 40*time.Millisecond {
		t.Fatalf("mean %v", m)
	}
	var empty Histogram
	if empty.Snapshot().Quantile(0.99) != 0 || empty.Snapshot().Mean() != 0 {
		t.Fatal("empty histogram must report zeros")
	}
	h.Observe(time.Hour) // overflow bucket must not panic
	if h.Snapshot().Quantile(1) == 0 {
		t.Fatal("overflow lost")
	}
}

func TestHistogramConcurrent(t *testing.T) {
	var h Histogram
	var c Counter
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				h.Observe(time.Duration(i) * time.Microsecond)
				c.Inc()
			}
		}()
	}
	wg.Wait()
	s := h.Snapshot()
	var sum uint64
	for _, b := range s.Buckets {
		sum += b
	}
	if s.Count != 80000 || sum != 80000 || c.Value() != 80000 {
		t.Fatalf("count=%d bucketsum=%d counter=%d", s.Count, sum, c.Value())
	}
}

func TestExpositionFormat(t *testing.T) {
	var b bytes.Buffer
	e := NewExposition(&b, "nebula_")
	e.Counter("commands_total", "Commands processed.", 42, "cmd", "get")
	e.Counter("commands_total", "Commands processed.", 7, "cmd", `se"t`)
	e.Gauge("memtable_bytes", "Active memtable size.", 1.5e6)
	var h Histogram
	h.Observe(30 * time.Microsecond)
	h.Observe(2 * time.Millisecond)
	e.Histogram("command_seconds", "Latency.", h.Snapshot(), "class", "write")
	if e.Err() != nil {
		t.Fatal(e.Err())
	}
	out := b.String()
	if strings.Count(out, "# TYPE nebula_commands_total counter") != 1 {
		t.Fatalf("TYPE header must appear once per metric:\n%s", out)
	}
	for _, want := range []string{
		`nebula_commands_total{cmd="get"} 42`,
		`nebula_commands_total{cmd="se\"t"} 7`,
		`nebula_memtable_bytes 1.5e+06`,
		`nebula_command_seconds_bucket{class="write",le="5e-05"} 1`,
		`nebula_command_seconds_bucket{class="write",le="0.0025"} 2`,
		`nebula_command_seconds_bucket{class="write",le="+Inf"} 2`,
		`nebula_command_seconds_count{class="write"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing line %q in:\n%s", want, out)
		}
	}
	// Buckets must be cumulative and non-decreasing.
	prev := -1
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "nebula_command_seconds_bucket") {
			var n int
			fields := strings.Fields(line)
			for _, c := range fields[len(fields)-1] {
				n = n*10 + int(c-'0')
			}
			if n < prev {
				t.Fatalf("bucket counts decreased: %s", line)
			}
			prev = n
		}
	}
}

func BenchmarkObserve(b *testing.B) {
	var h Histogram
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(37 * time.Microsecond)
		}
	})
}
