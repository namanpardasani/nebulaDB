import type { InspectorState } from '../lib/reducer'
import { bytes, compact, percent } from '../lib/format'
import { Panel, Spark } from './primitives'

/** Bloom filters and block cache: cumulative rates from the engine, sparklines from the live ticks. */
export function Filters({ state }: { state: InspectorState }) {
  const e = state.stats?.engine
  const h = state.history
  const bloom = e?.bloom_checked ? e.bloom_useful_rate : state.tick?.bloom_useful
  return (
    <Panel id="h-bc" title="bloom filters & block cache" num="06">
      <div className="meter2">
        <div className="m2">
          <div className="k">bloom useful</div>
          <div className="v">{bloom === undefined || !Number.isFinite(bloom) ? '–' : percent(bloom, 1)}</div>
          <div className="c">lookups that skipped a file</div>
          <Spark values={h.map((p) => p.bloomSkipRate)} h={30} min={0} max={1} />
          <dl>
            <dt>checked</dt><dd>{e ? compact(e.bloom_checked) : '–'}</dd>
            <dt>false positives</dt><dd>{e ? compact(e.bloom_false_positive) : '–'}</dd>
          </dl>
        </div>
        <div className="m2">
          <div className="k">cache hit</div>
          <div className="v">{e && e.cache_enabled && e.cache_hits + e.cache_misses > 0 ? percent(e.cache_hit_rate, 1) : '–'}</div>
          <div className="c">{e && !e.cache_enabled ? 'cache disabled' : 'blocks served from memory'}</div>
          <Spark values={h.map((p) => p.cacheHitRate)} h={30} min={0} max={1} />
          <dl>
            <dt>held</dt><dd>{e ? `${bytes(e.cache_bytes)}` : '–'}</dd>
            <dt>block reads</dt><dd>{e ? compact(e.block_reads) : '–'}</dd>
          </dl>
        </div>
      </div>
    </Panel>
  )
}
