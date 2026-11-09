#!/usr/bin/env bash
# Playwright's Chromium needs a few system libraries (libnss3, libnspr4, libasound2).
# On a machine without sudo, download the official Ubuntu packages (apt verifies
# their signatures) and unpack them locally; no install, no root:
#
#   scripts/setup-browser-libs.sh
#   export LD_LIBRARY_PATH="$PWD/bin/chromelibs/root/usr/lib/x86_64-linux-gnu"
#
# With sudo you can instead run:  npx playwright install-deps chromium
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bin/chromelibs/debs && cd bin/chromelibs/debs
apt-get download libnspr4 libnss3 libasound2t64
for d in *.deb; do dpkg -x "$d" ../root; done
echo "export LD_LIBRARY_PATH=$(cd ../root/usr/lib/x86_64-linux-gnu && pwd)"
