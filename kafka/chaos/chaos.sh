#!/usr/bin/env bash
# What happens when things die. Starts a three-broker cluster of its own on ports
# 19092-19094 (nothing else is touched), writes a keyed stream to a topic with
# three replicas, and reads it with a group consumer, while brokers are killed
# and restarted and the consumer is killed with SIGKILL. Afterwards checks:
#
#   - the producer reports every message delivered
#   - the log, as read back, holds each key's whole sequence: nothing lost
#   - in offset order on one partition: nothing reordered
#   - exactly once each: idempotence kept retries from writing anything twice
#
# A consumer reading a message again after a crash is at-least-once working as
# it should, so that is counted and not failed.
#
#     ./chaos.sh [messages]
set -u

COUNT=${1:-120000}
HERE=$(cd "$(dirname "$0")" && pwd)
NET=dexkafka-chaos
IMAGE=confluentinc/cp-kafka:7.8.0
OUT=$(mktemp -d)
TOPIC=chaos
GROUP=chaos-readers
BROKERS=localhost:19092,localhost:19093,localhost:19094
VOTERS=1@dexkafka-chaos-1:29093,2@dexkafka-chaos-2:29093,3@dexkafka-chaos-3:29093
PIDS=()

cleanup() {
    for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    docker rm -f dexkafka-chaos-1 dexkafka-chaos-2 dexkafka-chaos-3 >/dev/null 2>&1
    docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

say() { printf '[%s] %s\n' "$(date +%T)" "$*"; }

cleanup
docker network create "$NET" >/dev/null || exit 1
for n in 1 2 3; do
    docker run -d --name dexkafka-chaos-$n --network "$NET" --hostname dexkafka-chaos-$n \
        -p 1909$((n + 1)):1909$((n + 1)) \
        -e KAFKA_NODE_ID=$n \
        -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=$VOTERS \
        -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:1909$((n + 1)) \
        -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://dexkafka-chaos-$n:29092,EXTERNAL://localhost:1909$((n + 1)) \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
        -e KAFKA_MIN_INSYNC_REPLICAS=2 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        -e KAFKA_HEAP_OPTS=-Xmx512m \
        -e CLUSTER_ID=ZGV4a2Fma2FjaGFvczEyMw \
        "$IMAGE" >/dev/null || exit 1
done

say "waiting for the cluster"
for i in $(seq 1 60); do
    if docker exec dexkafka-chaos-1 kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists \
        --topic "$TOPIC" --partitions 6 --replication-factor 3 --config min.insync.replicas=2 >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

cd "$HERE/.." || exit 1
dex build --no-lint chaos/chaos_worker.dx >/dev/null || exit 1
WORKER=$PWD/build/chaos_worker

say "producing $COUNT messages"
"$WORKER" produce "$BROKERS" "$TOPIC" "$COUNT" >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
PIDS+=("$PRODUCER")

"$WORKER" consume "$BROKERS" "$TOPIC" "$GROUP" 45000 >"$OUT/consumer1.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")

sleep 15
say "killing broker 1"
docker kill dexkafka-chaos-1 >/dev/null
sleep 15
say "killing the consumer with SIGKILL, and starting another"
kill -9 "$CONSUMER" 2>/dev/null
"$WORKER" consume "$BROKERS" "$TOPIC" "$GROUP" 45000 >"$OUT/consumer2.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")
sleep 10
say "restarting broker 1"
docker start dexkafka-chaos-1 >/dev/null
sleep 20
say "killing broker 2"
docker kill dexkafka-chaos-2 >/dev/null
sleep 15
say "restarting broker 2"
docker start dexkafka-chaos-2 >/dev/null
sleep 5
say "killing the consumer with SIGKILL again, and starting another"
kill -9 "$CONSUMER" 2>/dev/null
"$WORKER" consume "$BROKERS" "$TOPIC" "$GROUP" 45000 >"$OUT/consumer3.log" 2>&1 &
CONSUMER=$!
PIDS+=("$CONSUMER")

say "waiting for the producer"
wait "$PRODUCER"
say "$(grep RESULT "$OUT/producer.log" || tail -3 "$OUT/producer.log")"
say "waiting for the last consumer to go idle"
wait "$CONSUMER"

python3 - "$OUT" "$COUNT" <<'PY'
import glob, os, sys
out, count = sys.argv[1], int(sys.argv[2])
KEYS = 50
expected = {}
for i in range(count):
    expected.setdefault(f"key{i % KEYS}", []).append(i // KEYS)

# What the log holds, as read by every consumer run: (partition, offset) -> number.
log = {k: {} for k in expected}
lines = 0
for path in sorted(glob.glob(os.path.join(out, "consumer*.log"))):
    for line in open(path, errors="replace"):
        f = line.split()
        if len(f) != 5 or f[0] != "MSG":
            continue
        _, key, seq, part, off = f
        lines += 1
        log[key][(int(part), int(off))] = int(seq)

missing = written_twice = out_of_order = split = 0
for key, want in expected.items():
    held = sorted(log[key].items())
    numbers = [seq for _, seq in held]
    if len({part for (part, _), _ in held}) > 1:
        split += 1
    missing += len(set(want) - set(numbers))
    written_twice += len(numbers) - len(set(numbers))
    out_of_order += sum(1 for a, b in zip(numbers, numbers[1:]) if b <= a)

distinct = sum(len(v) for v in log.values())
print(f"lines read={lines} distinct log entries={distinct} of {count}")
print(f"missing={missing} written-twice={written_twice} out-of-order={out_of_order} keys-on-two-partitions={split}")
print(f"re-read after a crash or rebalance (expected, at-least-once)={lines - distinct}")
ok = missing == 0 and written_twice == 0 and out_of_order == 0 and split == 0 and distinct == count
print("CHAOS PASSED" if ok else "CHAOS FAILED")
sys.exit(0 if ok else 1)
PY
RC=$?
grep -q "failed=0 left=0" "$OUT/producer.log" || { say "the producer reported failures"; RC=1; }
grep -l "FAILED" "$OUT"/*.log 2>/dev/null
say "logs kept in $OUT"
exit $RC
