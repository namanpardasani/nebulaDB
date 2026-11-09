const REPO = 'https://github.com/namanpardasani/nebulaDB'

export function About() {
  return (
    <main className="page">
      <article className="prose">
        <h1>NebulaDB</h1>
        <p>
          A Redis-compatible key-value database written from scratch in Go, built on an LSM tree and tested by
          killing it: thousands of randomized <code>kill -9</code> crash tests, with zero acknowledged writes lost.
          This inspector shows the engine working live.
        </p>

        <h2>What it is</h2>
        <ul>
          <li>RESP2 server: <code>redis-cli</code> and Redis client libraries connect as they would to Redis.</li>
          <li>Write-ahead log with CRC32C, three fsync policies and group commit.</li>
          <li>Skip-list memtable, SSTables with prefix-compressed blocks, sparse index and Bloom filters, block cache.</li>
          <li>Leveled and size-tiered compaction, snapshots, backpressure, MANIFEST with atomic file-set changes.</li>
          <li>Embedded Go API as well as the server.</li>
        </ul>

        <h2>What it is not</h2>
        <ul>
          <li>Not a full Redis: only strings, TTLs, SCAN and a core command set; no pub/sub, transactions or clustering.</li>
          <li>A learning and portfolio project: not audited for production use.</li>
        </ul>

        <h2>How to read the inspector</h2>
        <table>
          <thead><tr><th>Panel</th><th>Shows</th></tr></thead>
          <tbody>
            <tr><td>Incoming writes</td><td>A sample of recent commits with their sequence numbers.</td></tr>
            <tr><td>Memtable and WAL</td><td>How full the active memtable is, sealed memtables waiting to flush, WAL segment size.</td></tr>
            <tr><td>LSM tree</td><td>Every SSTable by level, placed by key range and sized by bytes. Green outline: just created by a flush or compaction. Dashed amber: being merged right now.</td></tr>
            <tr><td>Backpressure</td><td>L0 file count against the slowdown and stop limits; stalls so far.</td></tr>
            <tr><td>Bloom filters / Block cache</td><td>How much disk I/O the filters and cache are saving.</td></tr>
            <tr><td>Try it / Controls</td><td>Run commands, generate load, flush, compact, or crash the server and watch recovery.</td></tr>
          </tbody>
        </table>

        <h2>Run it</h2>
        <pre>{`go build -o bin/nebuladb ./cmd/nebuladb
bin/nebuladb -dir ./data -supervise        # serves RESP on :6380, admin on :7380
cd inspector && npm install && npm run dev  # http://127.0.0.1:5173`}</pre>
        <p>Point the UI at another admin server with <code>?admin=http://host:port</code>.</p>

        <h2>Guarantees, in one table</h2>
        <table>
          <thead><tr><th>fsync policy</th><th>process crash</th><th>power loss</th></tr></thead>
          <tbody>
            <tr><td><code>always</code> (default)</td><td>no acknowledged write lost</td><td>no acknowledged write lost</td></tr>
            <tr><td><code>every</code></td><td>no acknowledged write lost</td><td>up to one interval may be lost</td></tr>
            <tr><td><code>never</code></td><td>no acknowledged write lost</td><td>unbounded loss (unsafe)</td></tr>
          </tbody>
        </table>
        <p>Source, design notes, failure analysis and benchmarks: <a href={REPO}>{REPO}</a>.</p>
      </article>
    </main>
  )
}
