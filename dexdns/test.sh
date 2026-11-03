#!/bin/sh
# dexdns's tests: the unit tests, then every provider -- Cloudflare and Hetzner
# against testdata/fake-dns.py, PowerDNS for real in Docker.
#     ./test.sh            all of it (needs python3 and docker)
#     NO_DOCKER=1 ./test.sh  without PowerDNS
set -e
cd "$(dirname "$0")"
DEX=${DEX:-dex}
FAKE_PORT=${FAKE_PORT:-18898}
PDNS_PORT=${PDNS_PORT:-18081}

$DEX test dexdns_test.dx

python3 testdata/fake-dns.py "$FAKE_PORT" > /dev/null 2>&1 &
fake=$!
cleanup() {
    kill $fake 2> /dev/null || true
    [ -z "$NO_DOCKER" ] && docker rm -f dexdns-pdns > /dev/null 2>&1 || true
}
trap cleanup EXIT
export DEXDNS_FAKE="http://127.0.0.1:$FAKE_PORT"

if [ -z "$NO_DOCKER" ]; then
    docker rm -f dexdns-pdns > /dev/null 2>&1 || true
    docker run -d --name dexdns-pdns -p "127.0.0.1:$PDNS_PORT:8081" \
        -e PDNS_AUTH_API_KEY=testkey powerdns/pdns-auth-49:4.9.8 > /dev/null
    export DEXDNS_PDNS="http://127.0.0.1:$PDNS_PORT/api/v1/servers/localhost"
    export DEXDNS_PDNS_KEY=testkey
    i=0
    until curl -fs -H "X-API-Key: testkey" "$DEXDNS_PDNS" > /dev/null 2>&1; do
        i=$((i + 1))
        [ $i -gt 30 ] && { echo "PowerDNS did not start"; exit 1; }
        sleep 1
    done
fi
sleep 1

$DEX test providers_test.dx
