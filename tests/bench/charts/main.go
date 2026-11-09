// Command charts renders the benchmark results as static, self-contained SVG
// files (no JavaScript, no external fonts) plus a markdown table view of the same
// numbers. Colours follow one rule: colour = system family, in a fixed order
// (NebulaDB, Redis, Pebble, Badger); variants of a system (fsync mode,
// compaction style) are distinguished by their direct labels, never by hue.
// Light and dark palettes are both defined and chosen by the viewer's
// prefers-color-scheme.
package main

import (
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/naman/nebuladb/tests/bench/benchutil"
)

type bar struct {
	label  string
	value  float64
	family int // 0 nebula, 1 redis, 2 pebble, 3 badger
	text   string
}

func family(target string) int {
	switch {
	case strings.HasPrefix(target, "nebuladb"):
		return 0
	case strings.HasPrefix(target, "redis"):
		return 1
	case strings.HasPrefix(target, "pebble"):
		return 2
	case strings.HasPrefix(target, "badger"):
		return 3
	}
	return 0
}

const style = `
 svg{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
 .bg{fill:var(--surface)} .t1{fill:var(--text1)} .t2{fill:var(--text2)} .t3{fill:var(--text3)}
 .axis{stroke:var(--axis);stroke-width:1}
 .s0{fill:var(--s0)} .s1{fill:var(--s1)} .s2{fill:var(--s2)} .s3{fill:var(--s3)}
 :root{--surface:#fcfcfb;--text1:#0b0b0b;--text2:#52514e;--text3:#6f6e69;--axis:#cfcec8;
       --s0:#2a78d6;--s1:#eb6834;--s2:#1baf7a;--s3:#eda100}
 @media (prefers-color-scheme:dark){:root{--surface:#1a1a19;--text1:#f0efec;--text2:#c3c2b7;--text3:#a3a29a;--axis:#4a4944;
       --s0:#3987e5;--s1:#d95926;--s2:#199e70;--s3:#c98500}}
`

func fmtNum(v float64, unit string) string {
	switch {
	case unit == "us" && v >= 1000:
		return fmt.Sprintf("%.1f ms", v/1000)
	case unit == "us":
		return fmt.Sprintf("%.0f µs", v)
	case v >= 100000:
		return fmt.Sprintf("%.0fk", v/1000)
	case v >= 10000:
		return fmt.Sprintf("%.1fk", v/1000)
	case v >= 1000:
		return fmt.Sprintf("%.2fk", v/1000)
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

// panel draws one horizontal bar chart. The axis starts at zero (bars encode
// length), bars are thin with a 4px rounded data end, and every bar carries its
// value as a direct label so no gridlines are needed.
func panel(b *strings.Builder, x, y, w float64, title, sub string, bars []bar, unit string, ascending bool) float64 {
	sort.SliceStable(bars, func(i, j int) bool {
		if ascending {
			return bars[i].value < bars[j].value
		}
		return bars[i].value > bars[j].value
	})
	const labelW, rowH, barH, pad = 188.0, 26.0, 14.0, 8.0
	var max float64
	for _, bb := range bars {
		if bb.value > max {
			max = bb.value
		}
	}
	if max == 0 {
		max = 1
	}
	barW := w - labelW - 70
	fmt.Fprintf(b, `<text x="%.0f" y="%.0f" class="t1" font-size="14" font-weight="600">%s</text>`, x, y+14, html.EscapeString(title))
	if sub != "" {
		fmt.Fprintf(b, `<text x="%.0f" y="%.0f" class="t2" font-size="11">%s</text>`, x, y+30, html.EscapeString(sub))
	}
	top := y + 44
	for i, bb := range bars {
		cy := top + float64(i)*rowH
		bw := bb.value / max * barW
		if bw < 2 {
			bw = 2
		}
		fmt.Fprintf(b, `<text x="%.0f" y="%.0f" class="t2" font-size="12" text-anchor="end">%s</text>`, x+labelW-8, cy+barH-2, html.EscapeString(bb.label))
		// bar with a rounded data end and a square baseline end
		r := 4.0
		if bw < 2*r {
			r = bw / 2
		}
		fmt.Fprintf(b, `<path class="s%d" d="M%.1f %.1f h%.1f a%.1f %.1f 0 0 1 %.1f %.1f v%.1f a%.1f %.1f 0 0 1 -%.1f %.1f h-%.1f z"><title>%s: %s</title></path>`,
			bb.family, x+labelW, cy, bw-r, r, r, r, r, barH-2*r, r, r, r, r, bw-r, html.EscapeString(bb.label), html.EscapeString(bb.text))
		fmt.Fprintf(b, `<text x="%.1f" y="%.0f" class="t1" font-size="12">%s</text>`, x+labelW+bw+6, cy+barH-2, html.EscapeString(bb.text))
	}
	fmt.Fprintf(b, `<line class="axis" x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f"/>`, x+labelW, top-4, x+labelW, top+float64(len(bars))*rowH)
	return top + float64(len(bars))*rowH + pad - y
}

func legend(b *strings.Builder, x, y float64, names []string, fams []int) {
	for i, n := range names {
		cx := x + float64(i)*150
		fmt.Fprintf(b, `<rect class="s%d" x="%.0f" y="%.0f" width="12" height="12" rx="3"/><text x="%.0f" y="%.0f" class="t2" font-size="12">%s</text>`, fams[i], cx, y-10, cx+18, y, html.EscapeString(n))
	}
}

func svgDoc(title, desc string, w, h float64, body string) string {
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" width="%.0f" role="img" aria-labelledby="t d"><title id="t">%s</title><desc id="d">%s</desc><style>%s</style><rect class="bg" width="%.0f" height="%.0f" rx="8"/>%s</svg>`,
		w, h, w, html.EscapeString(title), html.EscapeString(desc), style, w, h, body)
}

func get(rs []benchutil.Result, f func(benchutil.Result) bool) []benchutil.Result {
	var o []benchutil.Result
	for _, r := range rs {
		if f(r) {
			o = append(o, r)
		}
	}
	return o
}

func main() {
	res := flag.String("results", "tests/bench/results", "results directory")
	out := flag.String("out", "docs/charts", "output directory for SVG files")
	flag.Parse()
	os.MkdirAll(*out, 0o755)
	var md strings.Builder

	// ---- network + embedded: per workload, 4 panels (throughput / p99 at two concurrency levels)
	for _, suite := range []struct{ file, name, unitWord string }{{"network.jsonl", "network", "connections"}, {"embedded.jsonl", "embedded", "threads"}} {
		rs, err := benchutil.Load(filepath.Join(*res, suite.file))
		if err != nil {
			fmt.Fprintln(os.Stderr, "skip", suite.file, ":", err)
			continue
		}
		workloads := map[string]bool{}
		for _, r := range rs {
			workloads[r.Workload] = true
		}
		var ws []string
		for w := range workloads {
			ws = append(ws, w)
		}
		sort.Strings(ws)
		fmt.Fprintf(&md, "\n### %s benchmark: full results\n\n| workload | target | %s | ops/s | mean | p50 | p99 | p99.9 |\n|---|---|---|---|---|---|---|---|\n", suite.name, suite.unitWord)
		for _, w := range ws {
			cs := map[int]bool{}
			for _, r := range get(rs, func(r benchutil.Result) bool { return r.Workload == w }) {
				cs[r.Conns] = true
			}
			var conns []int
			for c := range cs {
				conns = append(conns, c)
			}
			sort.Ints(conns)
			var body strings.Builder
			y := 70.0
			colW := 540.0
			var famsSeen = map[int]string{}
			maxH := 0.0
			for ci, c := range conns {
				cell := get(rs, func(r benchutil.Result) bool { return r.Workload == w && r.Conns == c })
				var thr, lat []bar
				for _, r := range cell {
					f := family(r.Target)
					famsSeen[f] = [...]string{"NebulaDB", "Redis 8", "Pebble v2", "BadgerDB v4"}[f]
					thr = append(thr, bar{r.Target, r.OpsPerSec, f, fmtNum(r.OpsPerSec, "")})
					lat = append(lat, bar{r.Target, r.P99Us, f, fmtNum(r.P99Us, "us")})
					fmt.Fprintf(&md, "| %s | %s | %d | %.0f | %.0f µs | %.0f µs | %.0f µs | %.0f µs |\n", w, r.Target, c, r.OpsPerSec, r.MeanUs, r.P50Us, r.P99Us, r.P999Us)
				}
				x := 24 + float64(ci)*colW
				h1 := panel(&body, x, y, colW-24, fmt.Sprintf("Throughput, %d %s", c, suite.unitWord), "operations per second (higher is better)", thr, "", false)
				h2 := panel(&body, x, y+h1+20, colW-24, fmt.Sprintf("p99 latency, %d %s", c, suite.unitWord), "lower is better", lat, "us", true)
				if y+h1+h2+20 > maxH {
					maxH = y + h1 + h2 + 20
				}
			}
			var names []string
			var fams []int
			for f := 0; f < 4; f++ {
				if n, ok := famsSeen[f]; ok {
					names = append(names, n)
					fams = append(fams, f)
				}
			}
			var hdr strings.Builder
			fmt.Fprintf(&hdr, `<text x="24" y="30" class="t1" font-size="17" font-weight="700">%s: %s workload</text>`, strings.Title(suite.name), w)
			legend(&hdr, 24, 52, names, fams)
			width := 24 + float64(len(conns))*colW
			doc := svgDoc(fmt.Sprintf("%s benchmark, %s workload", suite.name, w),
				"Throughput and p99 latency per target at each concurrency level; full numbers in the results table.", width, maxH+16, hdr.String()+body.String())
			os.WriteFile(filepath.Join(*out, fmt.Sprintf("%s-%s.svg", suite.name, w)), []byte(doc), 0o644)
		}
	}

	// ---- amplification
	if rs, err := benchutil.Load(filepath.Join(*res, "amp.jsonl")); err == nil && len(rs) > 0 {
		fmt.Fprintf(&md, "\n### Write / space / read amplification\n\n| target | write amp (OS bytes written / user bytes) | space amp (directory / live data) | blocks looked up per get | disk blocks read per get | tables | compactions |\n|---|---|---|---|---|---|---|\n")
		var wa, sa, ra []bar
		for _, r := range rs {
			f := family(r.Target)
			w, _ := r.Extra["write_amp_os"].(float64)
			s, _ := r.Extra["space_amp"].(float64)
			wa = append(wa, bar{r.Target, w, f, fmt.Sprintf("%.1f×", w)})
			sa = append(sa, bar{r.Target, s, f, fmt.Sprintf("%.2f×", s)})
			rd, rok := r.Extra["read_amp_blocks_per_get"].(float64)
			rdd, _ := r.Extra["read_amp_disk_blocks_per_get"].(float64)
			if rok {
				ra = append(ra, bar{r.Target, rd, f, fmt.Sprintf("%.2f", rd)})
			}
			fmt.Fprintf(&md, "| %s | %.2f | %.2f | %s | %s | %v | %v |\n", r.Target, w, s,
				ifOK(rok, fmt.Sprintf("%.2f", rd)), ifOK(rok, fmt.Sprintf("%.3f", rdd)), r.Extra["table_files"], r.Extra["compactions"])
		}
		var body strings.Builder
		h1 := panel(&body, 24, 70, 540, "Write amplification", "bytes the process wrote to disk ÷ bytes of user data (lower is better)", wa, "", true)
		panel(&body, 24, 70+h1+20, 540, "Space amplification", "directory size ÷ logical live data (lower is better)", sa, "", true)
		if len(ra) > 0 {
			panel(&body, 580, 70, 540, "Read amplification (NebulaDB)", "data-block lookups per Get (lower is better)", ra, "", true)
		}
		var hdr strings.Builder
		fmt.Fprintf(&hdr, `<text x="24" y="30" class="t1" font-size="17" font-weight="700">Amplification after a random-overwrite workload</text>`)
		legend(&hdr, 24, 52, []string{"NebulaDB", "Pebble v2", "BadgerDB v4"}, []int{0, 2, 3})
		os.WriteFile(filepath.Join(*out, "amplification.svg"), []byte(svgDoc("Amplification", "Write, space and read amplification per engine", 1130, 70+h1*2+60, hdr.String()+body.String())), 0o644)
	}
	os.WriteFile(filepath.Join(*out, "results-tables.md"), []byte("# Benchmark result tables\n\nGenerated by `tests/bench/charts` from `tests/bench/results/*.jsonl`.\n"+md.String()), 0o644)
	fmt.Println("charts written to", *out)
}

func ifOK(ok bool, s string) string {
	if ok {
		return s
	}
	return "n/a"
}
