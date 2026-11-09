import type { InspectorState } from '../lib/reducer'
import { bytes, compact, percent } from '../lib/format'
import { StatCard } from './primitives'

export function Overview({ state }: { state: InspectorState }) {
  const t = state.tick
  const e = state.stats?.engine
  const s = state.stats?.server
  return (
    <div className="stat-row">
      <StatCard label="Writes / s" value={t ? compact(t.writes_per_sec) : '–'} note={t ? `${bytes(t.write_bytes_per_sec)}/s of user data` : undefined} />
      <StatCard label="Commands / s" value={t ? compact(t.ops_per_sec) : '–'} note={s ? `${s.active_connections} connection(s)` : undefined} />
      <StatCard label="Last sequence" value={e ? compact(e.last_seq) : '–'} note={e ? `${compact(e.wal_batches)} WAL batches` : undefined} />
      <StatCard label="SSTables" value={e ? e.table_files : '–'} note={e ? `${bytes(e.levels.reduce((a, l) => a + l.bytes, 0))} on disk` : undefined} />
      <StatCard label="Write amplification" value={e ? `${e.write_amplification.toFixed(2)}×` : '–'} note={e ? `${bytes(e.user_bytes)} user bytes` : undefined} />
      <StatCard label="Bloom useful" value={e ? percent(e.bloom_useful_rate) : '–'} note={e ? `${compact(e.bloom_skipped)} of ${compact(e.bloom_checked)} lookups skipped` : undefined} />
    </div>
  )
}
