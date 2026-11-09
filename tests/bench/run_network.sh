#!/usr/bin/env bash
# End-to-end network benchmark: NebulaDB (4 durability configs) vs Redis 8
# (3 persistence configs, same binary run natively; plus one Docker data point).
#
#   ./run_network.sh            full run (~15 min)
#   DURATION=3 WARMUP=1 ./run_network.sh   quick smoke run
#
# Results: results/network.jsonl (custom load generator), results/redis-benchmark/*.csv
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
DURATION=${DURATION:-8}
WARMUP=${WARMUP:-2}
KEYS=${KEYS:-50000}
RES=tests/bench/results
mkdir -p "$RES/redis-benchmark" bin/bench-data
: > "$RES/network.jsonl"
go build -o bin/nebuladb ./cmd/nebuladb
(cd tests/bench && go build -o ../../bin/loadgen ./loadgen)
REDIS=$ROOT/bin/redis/redis-server
[ -x "$REDIS" ] || { echo "missing $REDIS: docker create --name rx redis:8; docker cp rx:/usr/local/bin/redis-server bin/redis/; docker rm rx"; exit 1; }

PID=""
start_nebula() { # name port extra-flags...
  local name=$1 port=$2; shift 2
  local dir="$ROOT/bin/bench-data/$name"; rm -rf "$dir"; mkdir -p "$dir"
  "$ROOT/bin/nebuladb" -dir "$dir" -addr 127.0.0.1:$port -admin-addr "" -log-level warn "$@" 2>"$RES/server-$name.log" &
  PID=$!
}
start_redis() { # name port extra-flags...
  local name=$1 port=$2; shift 2
  local dir="$ROOT/bin/bench-data/$name"; rm -rf "$dir"; mkdir -p "$dir"
  (cd "$dir" && exec "$REDIS" --port $port --dir "$dir" --save "" --daemonize no "$@" >"$ROOT/$RES/server-$name.log" 2>&1) &
  PID=$!
}
wait_ready() { for _ in $(seq 1 100); do redis-cli -p $1 PING >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "server on $1 did not start"; exit 1; }
stop_server() { kill -TERM $PID 2>/dev/null || true; wait $PID 2>/dev/null || true; PID=""; }

run_target() { # name port rbn  (rbn = redis-benchmark request count)
  local name=$1 port=$2 rbn=$3
  wait_ready $port
  echo "=== $name (port $port)"
  ./bin/loadgen -addr 127.0.0.1:$port -workload preload -keys $KEYS
  for w in write read mixed large scan; do
    for c in 1 32; do
      # the single-connection fsync-per-write configs are slow by design; keep their cells short but still >= 3 s
      ./bin/loadgen -addr 127.0.0.1:$port -target "$name" -workload $w -conns $c -duration ${DURATION}s -warmup ${WARMUP}s -keys $KEYS -out "$RES/network.jsonl"
    done
  done
  # stock redis-benchmark: 100-byte values, 32 clients, no pipeline and 16-deep pipeline
  for P in 1 16; do
    redis-benchmark -p $port -t set,get -n $rbn -c 32 -P $P -d 100 -r $KEYS --csv 2>/dev/null \
      | sed "s/^/$name,P$P,/" >> "$RES/redis-benchmark/all.csv"
  done
  if redis-cli -p $port INFO 2>/dev/null | grep -q nebuladb_version; then redis-cli -p $port INFO > "$RES/info-$name.txt"; fi
}

: > "$RES/redis-benchmark/all.csv"
# ---- NebulaDB
start_nebula nebuladb-always 6501 -fsync always;                      run_target nebuladb-always 6501 50000; stop_server
start_nebula nebuladb-always-nogc 6502 -fsync always -group-commit=false; run_target nebuladb-always-nogc 6502 4000; stop_server
start_nebula nebuladb-every 6503 -fsync every -fsync-interval 1s;      run_target nebuladb-every 6503 50000; stop_server
start_nebula nebuladb-never 6504 -fsync never;                         run_target nebuladb-never 6504 50000; stop_server
# ---- Redis (same server binary the official image ships, run natively)
start_redis redis-aof-always 6511 --appendonly yes --appendfsync always;     run_target redis-aof-always 6511 50000; stop_server
start_redis redis-aof-everysec 6512 --appendonly yes --appendfsync everysec; run_target redis-aof-everysec 6512 50000; stop_server
start_redis redis-nopersist 6513 --appendonly no;                            run_target redis-nopersist 6513 50000; stop_server
# ---- Redis inside Docker (Docker Desktop publishes ports through a proxy: shows that overhead).
# Skipped automatically if the Docker daemon is not running.
if docker version >/dev/null 2>&1; then
  docker rm -f nebula-bench-redis >/dev/null 2>&1 || true
  docker run -d --rm --name nebula-bench-redis -p 127.0.0.1:6514:6379 redis:8 redis-server --appendonly no --save "" >/dev/null
  wait_ready 6514
  echo "=== redis-docker-nopersist (port 6514)"
  ./bin/loadgen -addr 127.0.0.1:6514 -workload preload -keys $KEYS
  for w in write read; do for c in 1 32; do
    ./bin/loadgen -addr 127.0.0.1:6514 -target redis-docker-nopersist -workload $w -conns $c -duration ${DURATION}s -warmup ${WARMUP}s -keys $KEYS -out "$RES/network.jsonl"
  done; done
  docker rm -f nebula-bench-redis >/dev/null
else
  echo "(docker daemon not available: skipping the Docker data point)"
fi
echo "done: $RES/network.jsonl"
