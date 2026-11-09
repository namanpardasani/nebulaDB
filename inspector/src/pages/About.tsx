const REPO = 'https://github.com/namanpardasani/nebulaDB'
const doc = (f: string) => `${REPO}/blob/main/${f}`

// Every figure on this page is copied from a real run recorded in the repo:
// crash campaign + fuzzing + bugs from NOTES.md, benchmark numbers from
// docs/BENCHMARKS.md (machine and settings are recorded there).

function Core() {
  return (
    <svg className="core" viewBox="0 0 360 440" role="img" aria-label="A core sample: writes drip into the memtable, then layers L0 to L3 get denser with depth">
      <defs><pattern id="hatch" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="2" height="7" fill="rgba(0,0,0,.14)" /></pattern></defs>
      <circle className="drip" cx="120" cy="40" r="4" fill="var(--amber)" />
      <circle className="drip d2" cx="150" cy="40" r="3.5" fill="var(--amber)" />
      <circle className="drip d3" cx="132" cy="40" r="3" fill="var(--amber)" />
      <rect x="70" y="96" width="140" height="56" rx="8" fill="var(--amber-soft)" stroke="var(--amber)" strokeWidth="1.4" />
      <path d="M70 116 q17-7 35 0 t35 0 t35 0 t35 0" fill="none" stroke="var(--amber)" strokeWidth="1.4" />
      {[['--l0', 160, 44], ['--l1', 208, 56], ['--l2', 268, 70], ['--l3', 342, 80]].map(([c, y, h]) => (
        <g key={c as string}>
          <rect x="70" y={y as number} width="140" height={h as number} rx="4" fill={`var(${c})`} />
          <rect x="70" y={y as number} width="140" height={h as number} rx="4" fill="url(#hatch)" />
        </g>
      ))}
      <rect x="66" y="92" width="148" height="334" rx="10" fill="none" stroke="var(--text-2)" strokeWidth="1.2" />
      <g fontFamily="IBM Plex Mono, monospace" fontSize="11" fill="var(--text-2)">
        {[[50, 'writes arrive'], [124, 'memtable · memory'], [182, 'L0 · fresh'], [236, 'L1'], [303, 'L2'], [382, 'L3 · compacted']].map(([y, t]) => (
          <g key={t as string}><path d={`M214 ${y}h24`} stroke="var(--text-2)" /><text x="244" y={(y as number) + 4}>{t}</text></g>
        ))}
        <text x="8" y="160" fill="var(--muted)">newer</text><text x="8" y="426" fill="var(--muted)">older</text>
        <path d="M28 170v236" stroke="var(--muted)" strokeDasharray="3 4" /><path d="M24 400l4 8 4-8" fill="none" stroke="var(--muted)" />
      </g>
    </svg>
  )
}

export function About() {
  return (
    <main className="about">
      <div className="ab-hero">
        <div>
          <div className="ab-kicker">redis-compatible · lsm tree · written from scratch in go</div>
          <h1>a database<br />you can <em>kill.</em></h1>
          <p>NebulaDB stores your keys like rock stores time: in layers. Fresh writes settle on top, older data is pressed down and compacted. We killed it with <span className="mono">kill -9</span> three thousand times. It never lost a write it said OK to.</p>
          <div className="ab-cta">
            <a className="btn-amber" href="#/">open the inspector</a>
            <a className="btn-line" href={doc('docs/FAILURES.md')}>read the failure analysis →</a>
          </div>
        </div>
        <Core />
      </div>

      <div className="proof">
        <div><b>3,000</b><span>kill -9 crash iterations, 8 concurrent writers</span></div>
        <div><b>0</b><span>acknowledged writes lost (0 violations)</span></div>
        <div><b>12</b><span>fuzz targets, 0 failures (45 s each)</span></div>
        <div><b>6</b><span>engine bugs found by the tests, each written up in NOTES.md</span></div>
      </div>

      <div className="ab-sec">
        <div><h2>How a write travels</h2><p className="lead">From your command to rock, in five steps.</p></div>
        <div className="steps">
          <div className="step"><i style={{ background: 'var(--amber)' }} /><b>01 · wal</b><p>Appended to the write-ahead log with a CRC32C checksum, then fsynced. Now it survives a crash.</p></div>
          <div className="step"><i style={{ background: 'var(--amber)', opacity: 0.6 }} /><b>02 · memtable</b><p>Inserted into a sorted skip list in memory. Reads see it instantly.</p></div>
          <div className="step"><i style={{ background: 'var(--l0)' }} /><b>03 · flush</b><p>A full memtable is written out as an immutable, sorted SSTable in L0.</p></div>
          <div className="step"><i style={{ background: 'var(--l1)' }} /><b>04 · compaction</b><p>Overlapping files are merged downward; old versions and deleted keys are dropped.</p></div>
          <div className="step"><i style={{ background: 'var(--l3)' }} /><b>05 · read</b><p>Newest first. Bloom filters skip files that can’t hold the key; hot blocks stay cached.</p></div>
        </div>
      </div>

      <div className="ab-sec">
        <div><h2>Where it wins, where it loses</h2><p className="lead">Measured on one laptop, one 8-second run per network cell: gaps under ~15% are noise. Full method in docs/BENCHMARKS.md.</p></div>
        <div>
          <div className="wl">
            <div className="win"><h3>wins</h3><ul>
              <li>Read-heavy workload with durable writes, 32 connections: 41,208 vs 9,410 ops/s against Redis with <span className="mono">appendfsync always</span> (4.4×). Redis runs commands on one thread, so its fsync stalls reads.</li>
              <li>Group commit: 3,948 vs 405 ops/s on durable writes with and without it (~10×).</li>
            </ul></div>
            <div className="lose"><h3>loses</h3><ul>
              <li>Large 16 KiB values at 32 connections: 5.6k vs 48.0k ops/s (8.6× slower), limited by flush and compaction throughput.</li>
              <li>Non-durable writes at 32 connections: 47k vs 81k ops/s.</li>
              <li>Durable writes at 32 connections: about 20% behind Redis. The cause is not measured, so none is claimed.</li>
            </ul></div>
          </div>
          <div className="links"><a href={doc('docs/BENCHMARKS.md')}>benchmarks</a><a href={doc('docs/FAILURES.md')}>failure analysis</a><a href={doc('NOTES.md')}>design notes</a><a href={REPO}>source</a></div>
        </div>
      </div>

      <div className="ab-sec" style={{ borderBottom: 0 }}>
        <div><h2>Try it in a minute</h2><p className="lead">It speaks Redis, so your tools already work. Point this page at another server with <span className="mono">?admin=http://host:port</span>.</p></div>
        <div className="code"><span className="c"># build and start the server (RESP on :6380, admin + inspector API on :7380)</span>{'\n'}
<span className="p">$</span> go build -o bin/nebuladb ./cmd/nebuladb{'\n'}
<span className="p">$</span> bin/nebuladb -dir ./data -supervise{'\n'}
<span className="c"># talk to it with the real redis-cli</span>{'\n'}
<span className="p">$</span> redis-cli -p 6380 SET user:1 naman{'\n'}OK{'\n'}
<span className="p">$</span> redis-cli -p 6380 GET user:1{'\n'}"naman"</div>
      </div>
    </main>
  )
}
