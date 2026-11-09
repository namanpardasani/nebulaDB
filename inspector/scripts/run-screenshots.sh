#!/usr/bin/env bash
# Builds the UI, starts a supervised NebulaDB (small memtable so flushes and
# compactions are frequent) and `vite preview`, takes all screenshots, cleans up.
#   LD_LIBRARY_PATH must include the browser libs if Chromium cannot start:
#   see ../scripts/setup-browser-libs.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(cd .. && pwd)
OUT=${OUT:-screenshots/baseline}
SCRIPT=${SCRIPT:-scripts/screenshots.mjs}
(cd "$ROOT" && go build -o bin/nebuladb ./cmd/nebuladb)
npm run build >/dev/null
DATA=$(mktemp -d "$ROOT/bin/shots-data-XXXX")
"$ROOT/bin/nebuladb" -dir "$DATA" -addr 127.0.0.1:6391 -admin-addr 127.0.0.1:7391 -supervise \
  -memtable-size 65536 -l0-compact 3 -l1-bytes 262144 -target-file-size 32768 -level-ratio 4 -max-immutables 1 -l0-slowdown 4 -l0-stop 6 -compaction-rate 400000 -log-level warn >/dev/null 2>&1 &
SERVER=$!
npx vite preview --host 127.0.0.1 --port 4173 >/dev/null 2>&1 &
PREVIEW=$!
cleanup() { kill $PREVIEW 2>/dev/null || true; kill -TERM $SERVER 2>/dev/null || true; sleep 1; pkill -f "$DATA" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
for _ in $(seq 1 50); do curl -sf http://127.0.0.1:7391/api/health >/dev/null && curl -sf http://127.0.0.1:4173 >/dev/null && break; sleep 0.2; done
OUT=$OUT node "$SCRIPT"
