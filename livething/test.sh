#!/bin/sh
# livething's tests: the unit tests, then the stock @livething/client (from
# npm, in testdata/e2e) against testdata/chat.dx.
#     ./test.sh              all of it (needs node 22+ and npm)
#     DEX=/path/to/dex ./test.sh
set -e
cd "$(dirname "$0")"
DEX=${DEX:-dex}
PORT=${PORT:-18900}

$DEX test livething_test.dx

(cd testdata && $DEX build chat.dx)
(cd testdata/e2e && [ -d node_modules ] || npm install --silent)

PORT=$PORT testdata/build/chat > testdata/build/chat.log 2>&1 &
server=$!
trap 'kill $server 2> /dev/null || true' EXIT
sleep 1

PORT=$PORT node testdata/e2e/e2e.mjs
