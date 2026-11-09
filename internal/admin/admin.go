package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/naman/nebuladb/internal/engine"
	"github.com/naman/nebuladb/internal/metrics"
	"github.com/naman/nebuladb/internal/resp"
	"github.com/naman/nebuladb/internal/server"
)

// Source is the engine state the admin server reads. *engine.DB implements it.
type Source interface {
	Metrics() engine.EngineMetrics
	LevelDetails() [][]engine.FileInfo
	MemoryInfo() engine.MemInfo
	RecentWrites() []engine.WriteSample
}

// DevOps are the maintenance actions behind dev-only endpoints. *engine.DB implements it.
type DevOps interface {
	FlushMemtable() error
	CompactAll() error
}

// Options configures the admin server.
type Options struct {
	Source Source         // nil when running -in-memory (engine stats unavailable)
	Dev    DevOps         // nil unless dev mode
	Server *server.Server // RESP server (command stats, try-it proxy)
	Broker *Broker
	// DevMode enables POST /api/dev/*; Supervised reports whether a supervisor
	// will restart the process after /api/dev/crash.
	DevMode    bool
	Supervised bool
	// AllowAnyHost disables the loopback Host-header check (set when the admin
	// address is deliberately bound to a non-loopback interface).
	AllowAnyHost bool
	TickEvery    time.Duration // default 250ms
	// ServerAddr is the RESP listen address, reported by /api/health so the UI
	// can show what it is looking at.
	ServerAddr string
}

// Server is the admin HTTP server.
type Server struct {
	opts    Options
	handler http.Handler
	http    *http.Server

	tickMu   sync.Mutex
	tickStop chan struct{}
	prev     engine.EngineMetrics
	prevAt   time.Time
}

// New builds the admin server (not yet listening).
func New(o Options) *Server {
	if o.TickEvery <= 0 {
		o.TickEvery = 250 * time.Millisecond
	}
	if o.Broker == nil {
		o.Broker = NewBroker(2000)
	}
	a := &Server{opts: o}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", a.health)
	mux.HandleFunc("GET /api/stats", a.stats)
	mux.HandleFunc("GET /api/levels", a.levels)
	mux.HandleFunc("GET /api/events", a.events)
	mux.HandleFunc("GET /metrics", a.prometheus)
	mux.HandleFunc("POST /api/cmd", a.command)
	mux.HandleFunc("POST /api/dev/crash", a.devCrash)
	mux.HandleFunc("POST /api/dev/flush", a.devFlush)
	mux.HandleFunc("POST /api/dev/compact", a.devCompact)
	a.handler = a.guard(mux)
	o.Broker.onChange = a.subscribersChanged
	return a
}

// Handler returns the HTTP handler (useful with httptest).
func (a *Server) Handler() http.Handler { return a.handler }

// Serve serves on ln until Shutdown.
func (a *Server) Serve(ln net.Listener) error {
	a.http = &http.Server{Handler: a.handler, ReadHeaderTimeout: 5 * time.Second}
	err := a.http.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops the server and the tick generator.
func (a *Server) Shutdown(ctx context.Context) error {
	a.stopTicks()
	if a.http == nil {
		return nil
	}
	// SSE streams never end by themselves; Close drops them immediately.
	a.http.Close()
	return nil
}

// ---------------------------------------------------------------- guard

func isLoopbackHost(h string) bool {
	host := h
	if hh, _, err := net.SplitHostPort(h); err == nil {
		host = hh
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// guard enforces the admin server's security rules:
//   - Host must be a loopback name (defeats DNS rebinding: a hostile page that
//     re-points its DNS name at 127.0.0.1 still sends its own Host header);
//   - a browser Origin, if present, must also be loopback (so other websites
//     cannot call the API), and is echoed in CORS headers so the Inspector dev
//     server on another local port works;
//   - state-changing requests must be JSON, which forces a CORS preflight.
func (a *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.opts.AllowAnyHost && !isLoopbackHost(r.Host) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || (!a.opts.AllowAnyHost && !isLoopbackHost(u.Host)) {
				http.Error(w, "forbidden origin", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodPost && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			http.Error(w, "POST requires Content-Type: application/json", http.StatusUnsupportedMediaType)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------- read-only

func (a *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"status": "ok", "version": server.Version, "dev": a.opts.DevMode,
		"engine": a.opts.Source != nil, "supervised": a.opts.Supervised,
		"addr": a.opts.ServerAddr,
	})
}

type latencyJSON struct {
	Count uint64  `json:"count"`
	P50us float64 `json:"p50_us"`
	P99us float64 `json:"p99_us"`
	P999  float64 `json:"p999_us"`
	MeanU float64 `json:"mean_us"`
}

type serverJSON struct {
	UptimeSeconds float64                `json:"uptime_seconds"`
	ActiveConns   int64                  `json:"active_connections"`
	TotalConns    int64                  `json:"total_connections"`
	RejectedConns int64                  `json:"rejected_connections"`
	TotalCommands uint64                 `json:"total_commands"`
	UnknownCmds   uint64                 `json:"unknown_commands"`
	OpsPerSec     float64                `json:"ops_per_sec"`
	PerCommand    map[string]uint64      `json:"per_command"`
	Latency       map[string]latencyJSON `json:"latency"`
}

func serverStats(s *server.Server) serverJSON {
	sn := s.Snapshot()
	out := serverJSON{
		UptimeSeconds: sn.Uptime.Seconds(), ActiveConns: sn.ActiveConns, TotalConns: sn.TotalConns,
		RejectedConns: sn.RejectedConns, TotalCommands: sn.TotalCommands, UnknownCmds: sn.UnknownCmds,
		OpsPerSec: sn.OpsPerSec, PerCommand: sn.PerCommand, Latency: map[string]latencyJSON{},
	}
	for _, c := range server.ClassNames() {
		h := sn.Latency[c]
		us := func(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
		out.Latency[c.String()] = latencyJSON{h.Count, us(h.Quantile(0.5)), us(h.Quantile(0.99)), us(h.Quantile(0.999)), us(h.Mean())}
	}
	return out
}

func (a *Server) stats(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{}
	if a.opts.Server != nil {
		resp["server"] = serverStats(a.opts.Server)
	}
	if a.opts.Source != nil {
		resp["engine"] = a.opts.Source.Metrics()
	}
	writeJSON(w, 200, resp)
}

func (a *Server) levels(w http.ResponseWriter, r *http.Request) {
	if a.opts.Source == nil {
		writeJSON(w, 404, map[string]string{"error": "engine introspection unavailable (running -in-memory)"})
		return
	}
	writeJSON(w, 200, map[string]any{
		"levels": a.opts.Source.LevelDetails(),
		"memory": a.opts.Source.MemoryInfo(),
	})
}

// ---------------------------------------------------------------- Prometheus

func (a *Server) prometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	e := metrics.NewExposition(w, "nebuladb_")
	if s := a.opts.Server; s != nil {
		sn := s.Snapshot()
		e.Gauge("uptime_seconds", "Seconds since the server started.", sn.Uptime.Seconds())
		e.Gauge("connections", "Open client connections.", float64(sn.ActiveConns))
		e.Counter("connections_total", "Client connections accepted.", float64(sn.TotalConns))
		e.Counter("connections_rejected_total", "Connections refused at the connection limit.", float64(sn.RejectedConns))
		e.Gauge("ops_per_second", "Smoothed commands per second.", sn.OpsPerSec)
		e.Counter("unknown_commands_total", "Commands with an unknown name.", float64(sn.UnknownCmds))
		for name, n := range sn.PerCommand {
			e.Counter("commands_total", "Commands processed, by command.", float64(n), "command", strings.ToLower(name))
		}
		for _, c := range server.ClassNames() {
			e.Histogram("command_duration_seconds", "Server-side command processing time.", sn.Latency[c], "class", c.String())
		}
	}
	if a.opts.Source != nil {
		m := a.opts.Source.Metrics()
		e.Gauge("last_sequence", "Highest committed sequence number.", float64(m.LastSeq))
		e.Gauge("memtable_bytes", "Approximate size of the active memtable.", float64(m.MemtableBytes))
		e.Gauge("memtable_entries", "Entries in the active memtable.", float64(m.MemtableEntries))
		e.Gauge("immutable_memtables", "Sealed memtables awaiting flush.", float64(m.Immutables))
		e.Counter("memtable_rotations_total", "Memtables sealed.", float64(m.Rotations))
		e.Counter("flushes_total", "Memtable flushes completed.", float64(m.Flushes))
		e.Counter("flush_errors_total", "Failed flush attempts.", float64(m.FlushErrors))
		e.Counter("flush_bytes_total", "Bytes written by flushes.", float64(m.FlushBytes))
		e.Counter("flush_seconds_total", "Time spent flushing.", m.FlushSeconds)
		e.Counter("compactions_total", "Compactions completed.", float64(m.Compactions))
		e.Counter("compaction_errors_total", "Failed compactions.", float64(m.CompactionErrors))
		e.Counter("compaction_bytes_total", "Compaction I/O in bytes.", float64(m.CompactionBytesIn), "direction", "read")
		e.Counter("compaction_bytes_total", "Compaction I/O in bytes.", float64(m.CompactionBytesOut), "direction", "written")
		e.Counter("compaction_dropped_entries_total", "Entries discarded by compaction (shadowed, deleted, expired).", float64(m.CompactionDropped))
		e.Counter("compaction_seconds_total", "Time spent compacting.", m.CompactionSeconds)
		e.Counter("compaction_throttled_seconds_total", "Time compaction slept for the rate limit.", m.ThrottledSeconds)
		e.Counter("write_stalls_total", "Times a writer blocked on backpressure.", float64(m.WriteStalls))
		e.Counter("write_stall_seconds_total", "Total time writers spent blocked.", m.WriteStallSeconds)
		e.Counter("write_slowdowns_total", "Writes delayed by the L0 slowdown.", float64(m.WriteSlowdowns))
		e.Counter("write_slowdown_seconds_total", "Total delay added by the L0 slowdown.", m.WriteSlowdownSeconds)
		e.Counter("user_bytes_total", "Key+value bytes accepted from clients.", float64(m.UserBytes))
		e.Gauge("write_amplification", "Bytes written to disk / user bytes.", m.WriteAmplification)
		for lvl, l := range m.Levels {
			ls := fmt.Sprint(lvl)
			e.Gauge("level_files", "SSTable files per level.", float64(l.Files), "level", ls)
			e.Gauge("level_bytes", "SSTable bytes per level.", float64(l.Bytes), "level", ls)
		}
		e.Counter("bloom_checks_total", "Table lookups consulting a Bloom filter.", float64(m.BloomChecked))
		e.Counter("bloom_skipped_total", "Table lookups avoided by a Bloom filter.", float64(m.BloomSkipped))
		e.Counter("bloom_false_positives_total", "Filter said maybe but the table had no such key.", float64(m.BloomFalsePositive))
		e.Gauge("bloom_useful_ratio", "Fraction of filter checks that avoided I/O.", m.BloomUsefulRate)
		e.Counter("block_reads_total", "Data blocks read from disk.", float64(m.BlockReads))
		if m.CacheEnabled {
			e.Counter("block_cache_hits_total", "Block cache hits.", float64(m.CacheHits))
			e.Counter("block_cache_misses_total", "Block cache misses.", float64(m.CacheMisses))
			e.Counter("block_cache_evictions_total", "Blocks evicted.", float64(m.CacheEvictions))
			e.Gauge("block_cache_bytes", "Bytes held by the block cache.", float64(m.CacheBytes))
			e.Gauge("block_cache_hit_ratio", "Block cache hit ratio.", m.CacheHitRate)
		}
		e.Counter("wal_bytes_total", "Bytes appended to the write-ahead log.", float64(m.WALBytesWritten))
		e.Counter("wal_batches_total", "Batches committed.", float64(m.WALBatches))
		e.Counter("wal_groups_total", "Group-commit groups (write calls).", float64(m.WALGroups))
		e.Counter("wal_fsyncs_total", "WAL fsync calls.", float64(m.WALFsyncs))
		e.Counter("wal_fsync_seconds_total", "Time spent in WAL fsync.", m.WALFsyncSeconds)
		e.Gauge("wal_active_bytes", "Size of the active WAL segment.", float64(m.WALActiveBytes))
		failed := 0.0
		if m.WALFailed != "" {
			failed = 1
		}
		e.Gauge("wal_failed", "1 if the WAL is poisoned (read-only) after an I/O failure.", failed)
	}
}

// ---------------------------------------------------------------- SSE

type tickMsg struct {
	Type         string                `json:"type"`
	Time         time.Time             `json:"time"`
	OpsPerSec    float64               `json:"ops_per_sec"`
	WritesPerSec float64               `json:"writes_per_sec"`
	WriteBytesPS float64               `json:"write_bytes_per_sec"`
	Memory       engine.MemInfo        `json:"memory"`
	Levels       []engine.LevelMetrics `json:"levels"`
	CacheHitRate float64               `json:"cache_hit_rate"`
	CacheHits    uint64                `json:"cache_hits"`
	CacheMisses  uint64                `json:"cache_misses"`
	BloomChecked uint64                `json:"bloom_checked"`
	BloomSkipped uint64                `json:"bloom_skipped"`
	WALFsyncs    uint64                `json:"wal_fsyncs"`
	WALBytes     uint64                `json:"wal_bytes"`
	LastSeq      uint64                `json:"last_seq"`
	TableFiles   int                   `json:"table_files"`
	WriteAmp     float64               `json:"write_amp"`
	BloomUseful  float64               `json:"bloom_useful"` // cumulative since start
	UptimeSecs   float64               `json:"uptime_seconds"`
	Stalled      bool                  `json:"stalled"`
	Slowed       bool                  `json:"slowed"`
	Recent       []engine.WriteSample  `json:"recent_writes"`
}

func (a *Server) subscribersChanged(n int) {
	if n > 0 {
		a.startTicks()
	} else {
		a.stopTicks()
	}
}

func (a *Server) startTicks() {
	a.tickMu.Lock()
	defer a.tickMu.Unlock()
	if a.tickStop != nil || a.opts.Source == nil {
		return
	}
	a.tickStop = make(chan struct{})
	a.prev, a.prevAt = a.opts.Source.Metrics(), time.Now()
	go a.tickLoop(a.tickStop)
}

func (a *Server) stopTicks() {
	a.tickMu.Lock()
	defer a.tickMu.Unlock()
	if a.tickStop != nil {
		close(a.tickStop)
		a.tickStop = nil
	}
}

// tickLoop publishes one aggregated message per interval instead of one event
// per operation, so a 100k writes/s workload costs the UI ~4 messages/s.
func (a *Server) tickLoop(stop <-chan struct{}) {
	t := time.NewTicker(a.opts.TickEvery)
	defer t.Stop()
	lastSeq := uint64(0)
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			m := a.opts.Source.Metrics()
			dt := now.Sub(a.prevAt).Seconds()
			if dt <= 0 {
				continue
			}
			var opsps, uptime float64
			if a.opts.Server != nil {
				sn := a.opts.Server.Snapshot()
				opsps, uptime = sn.OpsPerSec, sn.Uptime.Seconds()
			}
			msg := tickMsg{
				Type: "tick", Time: now, OpsPerSec: opsps, UptimeSecs: uptime,
				WritesPerSec: float64(m.LastSeq-a.prev.LastSeq) / dt,
				WriteBytesPS: float64(m.UserBytes-a.prev.UserBytes) / dt,
				Memory:       a.opts.Source.MemoryInfo(),
				LastSeq:      m.LastSeq, TableFiles: m.TableFiles, WriteAmp: m.WriteAmplification, BloomUseful: m.BloomUsefulRate,
				Levels:       m.Levels[:],
				CacheHitRate: m.CacheHitRate,
				CacheHits:    m.CacheHits - a.prev.CacheHits, CacheMisses: m.CacheMisses - a.prev.CacheMisses,
				BloomChecked: m.BloomChecked - a.prev.BloomChecked, BloomSkipped: m.BloomSkipped - a.prev.BloomSkipped,
				WALFsyncs: m.WALFsyncs - a.prev.WALFsyncs, WALBytes: m.WALBytesWritten - a.prev.WALBytesWritten,
				Stalled: m.WriteStallSeconds > a.prev.WriteStallSeconds,
				Slowed:  m.WriteSlowdowns > a.prev.WriteSlowdowns,
			}
			for _, s := range a.opts.Source.RecentWrites() {
				if s.Seq > lastSeq {
					msg.Recent = append(msg.Recent, s)
				}
			}
			for _, s := range msg.Recent {
				if s.Seq > lastSeq {
					lastSeq = s.Seq
				}
			}
			a.prev, a.prevAt = m, now
			a.opts.Broker.PublishEphemeral(msg)
		}
	}
}

func (a *Server) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	var last uint64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		fmt.Sscanf(v, "%d", &last)
	}
	sub, backlog := a.opts.Broker.Subscribe(last)
	defer sub.Close()
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	write := func(e Envelope) error {
		var b bytes.Buffer
		if e.ID != 0 {
			fmt.Fprintf(&b, "id: %d\n", e.ID)
		}
		fmt.Fprintf(&b, "data: %s\n\n", e.Data)
		_, err := w.Write(b.Bytes())
		return err
	}
	fmt.Fprint(w, "retry: 1000\n\n")
	for _, e := range backlog {
		if write(e) != nil {
			return
		}
	}
	fl.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case e, ok := <-sub.C:
			if !ok {
				return // dropped as a slow consumer: client reconnects with Last-Event-ID
			}
			if write(e) != nil {
				return
			}
			// Drain whatever else is ready before flushing: fewer syscalls.
			for more := true; more; {
				select {
				case e2, ok2 := <-sub.C:
					if !ok2 {
						fl.Flush()
						return
					}
					if write(e2) != nil {
						return
					}
				default:
					more = false
				}
			}
			fl.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// ---------------------------------------------------------------- try-it proxy

// safeCommands may be run through POST /api/cmd. Everything that destroys data
// in bulk (FLUSHDB) additionally needs dev mode.
var safeCommands = map[string]bool{
	"PING": true, "ECHO": true, "GET": true, "SET": true, "DEL": true, "EXISTS": true, "EXPIRE": true, "PEXPIRE": true,
	"TTL": true, "PTTL": true, "PERSIST": true, "MGET": true, "MSET": true, "INCR": true, "DECR": true, "INCRBY": true,
	"DECRBY": true, "SCAN": true, "DBSIZE": true, "INFO": true,
}

func replyJSON(v resp.Value) any {
	switch v.Kind {
	case resp.KindSimple:
		return string(v.Str)
	case resp.KindError:
		return map[string]string{"error": string(v.Str)}
	case resp.KindInt:
		return v.Int
	case resp.KindBulk:
		if v.Nil {
			return nil
		}
		return string(v.Str)
	case resp.KindArray:
		if v.Nil {
			return nil
		}
		out := make([]any, len(v.Elems))
		for i, e := range v.Elems {
			out[i] = replyJSON(e)
		}
		return out
	}
	return nil
}

func (a *Server) command(w http.ResponseWriter, r *http.Request) {
	if a.opts.Server == nil {
		writeJSON(w, 503, map[string]string{"error": "no command server"})
		return
	}
	var req struct {
		Args []string `json:"args"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || json.Unmarshal(body, &req) != nil || len(req.Args) == 0 {
		writeJSON(w, 400, map[string]string{"error": `body must be {"args":["COMMAND", ...]}`})
		return
	}
	name := strings.ToUpper(req.Args[0])
	if !safeCommands[name] && !(a.opts.DevMode && (name == "FLUSHDB" || name == "FLUSHALL")) {
		writeJSON(w, 403, map[string]string{"error": fmt.Sprintf("command %q is not allowed through the admin proxy", name)})
		return
	}
	args := make([][]byte, len(req.Args))
	for i, s := range req.Args {
		args[i] = []byte(s)
	}
	v, err := a.opts.Server.Exec(args)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"reply": replyJSON(v)})
}

// ---------------------------------------------------------------- dev only

func (a *Server) requireDev(w http.ResponseWriter) bool {
	if !a.opts.DevMode {
		writeJSON(w, 403, map[string]string{"error": "dev endpoints are disabled (start with -dev)"})
		return false
	}
	return true
}

// devCrash SIGKILLs the whole process: no flush, no fsync, no cleanup. It is how
// the Inspector demonstrates crash recovery, and only works when a supervisor
// (`nebuladb -supervise`) will start the server again.
func (a *Server) devCrash(w http.ResponseWriter, r *http.Request) {
	if !a.requireDev(w) {
		return
	}
	if !a.opts.Supervised {
		writeJSON(w, 409, map[string]string{"error": "refusing to crash an unsupervised server; start with -supervise so it is restarted"})
		return
	}
	writeJSON(w, 202, map[string]string{"status": "killing process with SIGKILL"})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		// Process.Kill is SIGKILL on Unix and TerminateProcess on Windows: no
		// deferred functions, no flush, no fsync.
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			p.Kill()
		}
	}()
}

func (a *Server) devFlush(w http.ResponseWriter, r *http.Request) {
	if !a.requireDev(w) || a.opts.Dev == nil {
		if a.opts.Dev == nil && a.opts.DevMode {
			writeJSON(w, 503, map[string]string{"error": "engine unavailable"})
		}
		return
	}
	if err := a.opts.Dev.FlushMemtable(); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "flushed"})
}

func (a *Server) devCompact(w http.ResponseWriter, r *http.Request) {
	if !a.requireDev(w) || a.opts.Dev == nil {
		if a.opts.Dev == nil && a.opts.DevMode {
			writeJSON(w, 503, map[string]string{"error": "engine unavailable"})
		}
		return
	}
	if err := a.opts.Dev.CompactAll(); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "compacted"})
}
