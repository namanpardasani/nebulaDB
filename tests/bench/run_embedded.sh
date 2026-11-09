#!/usr/bin/env bash
# In-process comparison: NebulaDB vs Pebble vs BadgerDB (one engine per process),
# then the amplification runs. Results: results/embedded.jsonl, results/amp.jsonl
set -euo pipefail
cd "$(dirname "$0")/../.."
ENGINES=${ENGINES:-"nebuladb pebble badger"}; APPEND=${APPEND:-0}; THREADS=${THREADS:-"1 16"}; MAIN=${MAIN:-1}; AMP=${AMP:-1}
DURATION=${DURATION:-8}; WARMUP=${WARMUP:-2}; KEYS=${KEYS:-50000}; OPS=${OPS:-400000}
RES=tests/bench/results
EMB=${EMB:-$RES/embedded.jsonl}   # output file (repetitions write embedded-r2.jsonl, embedded-r3.jsonl)
(cd tests/bench && go build -o ../../bin/embedded ./embedded)
if [ "$APPEND" != 1 ]; then : > "$EMB"; [ "$AMP" = 1 ] && : > $RES/amp.jsonl; fi
run() { ./bin/embedded -dir "bin/bench-data/emb-$RANDOM" -duration ${DURATION}s -warmup ${WARMUP}s -keys $KEYS -out "$EMB" "$@"; }
main_runs() {
for threads in $THREADS; do
  for eng in $ENGINES; do
    for w in write read mixed scan; do
      run -engine $eng -workload $w -threads $threads -sync=false
    done
    for w in write mixed; do
      run -engine $eng -workload $w -threads $threads -sync=true
    done
  done
done
}
[ "$MAIN" = 1 ] && main_runs
[ "$AMP" = 1 ] || { echo done; exit 0; }
AMP_KEYS=${AMP_KEYS:-300000}; AMP_OPS=${AMP_OPS:-2000000}
echo "--- amplification (no fsync per write; $AMP_KEYS keys, $AMP_OPS random overwrites)"
for cfg in "nebuladb leveled" "nebuladb size-tiered" "pebble leveled" "badger leveled"; do
  set -- $cfg
  ./bin/embedded -dir "bin/bench-data/amp-$RANDOM" -engine $1 -style $2 -workload amp -keys $AMP_KEYS -ops $AMP_OPS -sync=false -out $RES/amp.jsonl
done
echo "done"
