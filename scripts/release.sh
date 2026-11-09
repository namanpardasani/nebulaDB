#!/usr/bin/env bash
# Builds release archives for Linux, macOS and Windows (amd64 + arm64) into dist/.
# Nothing is uploaded or published: you get files and a SHA256SUMS list.
#
#   scripts/release.sh [VERSION]        # default: git describe, else "dev"
#
# Pure Go (CGO_ENABLED=0), so cross-compiling needs nothing but the Go toolchain.
# Windows builds compile, but the directory lock and -supervise are Unix-only
# (see NOTES.md); Linux and macOS are the supported platforms for the server.
# The script only creates files; clear dist/ yourself between versions.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
VERSION=${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}
OUT="$ROOT/dist"
mkdir -p "$OUT"
WORK=$(mktemp -d)   # staging area in the system temp dir
LDFLAGS="-s -w -X github.com/naman/nebuladb/internal/server.Version=${VERSION}"
for t in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do
  os=${t%/*}; arch=${t#*/}
  name="nebuladb-${VERSION}-${os}-${arch}"
  stage="$WORK/$name"
  mkdir -p "$stage"
  ext=""; [ "$os" = windows ] && ext=".exe"
  for cmd in nebuladb nebula; do
    CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "$LDFLAGS" -o "$stage/$cmd$ext" "./cmd/$cmd"
  done
  cp docs/INSTALL.md "$stage/INSTALL.md"
  if [ "$os" = windows ]; then
    (cd "$WORK" && python3 -m zipfile -c "$OUT/$name.zip" "$name")
  else
    tar -C "$WORK" -czf "$OUT/$name.tar.gz" "$name"
  fi
  echo "built $name"
done
(cd "$OUT" && sha256sum "nebuladb-${VERSION}"-* > SHA256SUMS)
echo
ls -l "$OUT"
