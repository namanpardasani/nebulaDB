// Package benchutil holds the result record and statistics shared by the
// benchmark programs.
package benchutil

import (
	"encoding/json"
	"os"
	"runtime"
	"sort"
	"time"
)

// Result is one measured cell (one target x workload x concurrency).
type Result struct {
	Suite     string         `json:"suite"`  // network | embedded | amp
	Target    string         `json:"target"` // e.g. nebuladb-always, redis-aof-always, pebble-sync
	Workload  string         `json:"workload"`
	Conns     int            `json:"conns"`
	Seconds   float64        `json:"seconds"`
	Ops       uint64         `json:"ops"`
	Errors    uint64         `json:"errors"`
	OpsPerSec float64        `json:"ops_per_sec"`
	MeanUs    float64        `json:"mean_us"`
	P50Us     float64        `json:"p50_us"`
	P90Us     float64        `json:"p90_us"`
	P99Us     float64        `json:"p99_us"`
	P999Us    float64        `json:"p999_us"`
	MaxUs     float64        `json:"max_us"`
	Extra     map[string]any `json:"extra,omitempty"`
	GoVersion string         `json:"go_version"`
	CPUs      int            `json:"cpus"`
}

// Summarize computes the statistics from raw per-operation latencies (exact, not bucketed).
func Summarize(r *Result, lat []time.Duration, elapsed time.Duration) {
	r.GoVersion, r.CPUs = runtime.Version(), runtime.NumCPU()
	r.Seconds = elapsed.Seconds()
	r.Ops = uint64(len(lat))
	if elapsed > 0 {
		r.OpsPerSec = float64(len(lat)) / elapsed.Seconds()
	}
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	us := func(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
	q := func(p float64) float64 {
		i := int(p * float64(len(lat)))
		if i >= len(lat) {
			i = len(lat) - 1
		}
		return us(lat[i])
	}
	var sum time.Duration
	for _, d := range lat {
		sum += d
	}
	r.MeanUs = us(sum) / float64(len(lat))
	r.P50Us, r.P90Us, r.P99Us, r.P999Us, r.MaxUs = q(0.5), q(0.9), q(0.99), q(0.999), us(lat[len(lat)-1])
}

// Append writes r as one JSON line to path.
func Append(path string, r Result) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, _ := json.Marshal(r)
	_, err = f.Write(append(b, '\n'))
	return err
}

// Load reads all result lines from path.
func Load(path string) ([]Result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []Result
	dec := json.NewDecoder(bytesReader(b))
	for dec.More() {
		var r Result
		if err := dec.Decode(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
