#!/usr/bin/env bash
# What happens when things die. Starts a RabbitMQ of its own on port 5680
# (nothing else is touched), streams numbered messages into a quorum queue and
# reads them back, while the broker is killed and restarted twice and the
# consumer is killed with SIGKILL twice. Afterwards checks:
#
#   - the producer reports every message accepted, and none failed
#   - every number reached a consumer: nothing accepted was lost
#
# A message read again after a crash, or sent again after a reconnect, is
# at-least-once working as it should, so it is counted and not failed.
#
#     ./chaos.sh [messages]
set -u

COUNT=${1:-80000}
HERE=$(cd "$(dirname "$0")" && pwd)
NAME=dexrmq-chaos
URL=amqp://dex:dexpass@localhost:5680
QUEUE=chaos
OUT=$(mktemp -d)
PIDS=()

cleanup() {
    for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    docker rm -f "$NAME" >/dev/null 2>&1
}
trap cleanup EXIT

say() { printf '[%s] %s\n' "$(date +%T)" "$*"; }

broker_up() {
    for i in $(seq 1 60); do
        docker exec "$NAME" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

cleanup
# --user: the image's own user from the start. Under OrbStack, starting as root
# leaves the Erlang cookie unreadable once the server drops privileges.
docker run -d --name "$NAME" --hostname "$NAME" --user 999:999 -p 5680:5672 \
    -e RABBITMQ_DEFAULT_USER=dex -e RABBITMQ_DEFAULT_PASS=dexpass \
    rabbitmq:4.1-management >/dev/null || exit 1
say "waiting for the broker"
broker_up || { say "the broker did not start"; exit 1; }

cd "$HERE/.." || exit 1
dex build --no-lint chaos/chaos_worker.dx >/dev/null || exit 1
WORKER=$PWD/build/chaos_worker

say "producing $COUNT messages"
"$WORKER" produce "$URL" "$QUEUE" "$COUNT" >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
PIDS+=("$PRODUCER")
"$WORKER" consume "$URL" "$QUEUE" 40000 >"$OUT/consumer1.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")

sleep 8
say "killing the broker"
docker kill "$NAME" >/dev/null
sleep 5
say "starting it again"
docker start "$NAME" >/dev/null
broker_up
sleep 6
say "killing the consumer with SIGKILL, and starting another"
kill -9 "$CONSUMER" 2>/dev/null
"$WORKER" consume "$URL" "$QUEUE" 40000 >"$OUT/consumer2.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")
sleep 6
say "killing the broker again"
docker kill "$NAME" >/dev/null
sleep 4
say "starting it again"
docker start "$NAME" >/dev/null
broker_up
sleep 3
say "killing the consumer with SIGKILL again, and starting another"
kill -9 "$CONSUMER" 2>/dev/null
"$WORKER" consume "$URL" "$QUEUE" 30000 >"$OUT/consumer3.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")

say "waiting for the producer"
wait "$PRODUCER"
say "$(grep -h RESULT "$OUT/producer.log" || tail -3 "$OUT/producer.log")"
say "waiting for the last consumer to go idle"
wait "$CONSUMER"

python3 - "$OUT" "$COUNT" <<'PY'
import glob, os, sys
out, count = sys.argv[1], int(sys.argv[2])
seen = {}
lines = 0
for path in sorted(glob.glob(os.path.join(out, "consumer*.log"))):
    for line in open(path, errors="replace"):
        f = line.split()
        if len(f) == 2 and f[0] == "MSG":
            lines += 1
            seen[int(f[1])] = seen.get(int(f[1]), 0) + 1
missing = [n for n in range(count) if n not in seen]
print(f"lines read={lines} distinct={len(seen)} of {count}")
print(f"missing={len(missing)} first missing={missing[:5]}")
print(f"read more than once (expected, at-least-once)={lines - len(seen)}")
ok = not missing
print("CHAOS PASSED" if ok else "CHAOS FAILED")
sys.exit(0 if ok else 1)
PY
RC=$?
grep -q "failed=0 left=0" "$OUT/producer.log" || { say "the producer reported failures"; RC=1; }
grep -l "FAILED" "$OUT"/*.log 2>/dev/null
say "logs kept in $OUT"
exit $RC
