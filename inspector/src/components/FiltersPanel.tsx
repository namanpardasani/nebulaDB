import type { InspectorState } from '../lib/reducer'
import { bytes, compact, percent } from '../lib/format'
import { Card, Sparkline } from './primitives'

export function FiltersPanel({ state }: { state: InspectorState }) {
  const t = state.totals
  const e = state.stats?.engine
  const bloomRate = t.bloomChecked > 0 ? t.bloomSkipped / t.bloomChecked : NaN
  const cacheTotal = t.cacheHits + t.cacheMisses
  const cacheRate = cacheTotal > 0 ? t.cacheHits / cacheTotal : NaN
  return (
    <div className="grid-2">
      <Card title="Bloom filters" sub="Share of table lookups answered “definitely absent” without touching disk">
        <div className="stack">
          <div className="stat-value">{percent(e ? e.bloom_useful_rate : bloomRate)}</div>
          <Sparkline points={state.history} pick={(p) => p.bloomSkipRate} max={1} />
          <dl className="kv">
            <dt>Since page load</dt><dd>{compact(t.bloomSkipped)} skipped of {compact(t.bloomChecked)}</dd>
            <dt>False positives (total)</dt><dd>{e ? compact(e.bloom_false_positive) : '–'}</dd>
            <dt>Data blocks read from disk</dt><dd>{e ? compact(e.block_reads) : '–'}</dd>
          </dl>
        </div>
      </Card>
      <Card title="Block cache" sub="Recently used SSTable blocks held in memory">
        <div className="stack">
          <div className="stat-value">{percent(e ? e.cache_hit_rate : cacheRate)}</div>
          <Sparkline points={state.history} pick={(p) => p.cacheHitRate} max={1} />
          <dl className="kv">
            <dt>Since page load</dt><dd>{compact(t.cacheHits)} hits / {compact(t.cacheMisses)} misses</dd>
            <dt>Held</dt><dd>{e ? `${bytes(e.cache_bytes)} · ${compact(e.cache_entries)} blocks` : '–'}</dd>
            <dt>Evictions</dt><dd>{e ? compact(e.cache_evictions) : '–'}</dd>
          </dl>
        </div>
      </Card>
    </div>
  )
}
