#!/usr/bin/env bash
# Runs every Go fuzz target in the module for $FUZZTIME (default 10s) each.
# Usage: scripts/fuzz.sh [fuzztime]      (also called by `make fuzz`)
set -euo pipefail
FUZZTIME="${1:-${FUZZTIME:-10s}}"
cd "$(dirname "$0")/.."
status=0
for pkg in $(go list ./...); do
  for t in $(go test -list 'Fuzz.*' "$pkg" 2>/dev/null | grep '^Fuzz' || true); do
    echo "== fuzz $pkg $t ($FUZZTIME)"
    if ! go test "$pkg" -run '^$' -fuzz "^${t}\$" -fuzztime "$FUZZTIME" 2>&1 | tail -3; then
      echo "FUZZ FAILURE in $pkg $t"; status=1
    fi
  done
done
exit $status
