# kafka

A Kafka client for Dex, over [librdkafka](https://github.com/confluentinc/librdkafka),
the C library behind confluent-kafka-go and most other production Kafka clients.
So the protocol, batching, retries, consumer groups, transactions, compression,
TLS and SASL are all librdkafka's, and this package is how Dex talks to it.

```sh
brew install librdkafka          # or: apt install librdkafka-dev
dex get github.com/ensamuel7/dex-modules/kafka
```

```dex
import "github.com/ensamuel7/dex-modules/kafka"

let p: kafka.Producer = kafka.producer("localhost:9092")
p.produce("orders", "order-42", payload)        // batched, sent in the background
p.flush()

let c: kafka.Consumer = kafka.consumer("localhost:9092", "billing")
let topics: string[] = ["orders"]
c.subscribe(topics)
while (running) {
    let m: kafka.Message? = c.poll(1000)        // null when nothing arrived
    if (m != null) {
        handle(m.key, m.value)
        c.commitMessage(m)                      // only after it is handled
    }
}
c.close()
```

Needs **dex 0.9.33 or later**, which `dex.mod` records.

Every failed call throws `kafka.KafkaError`, carrying `message`, `code`, and whether
it is `fatal` (make a new client), `retriable` (the same call may work), or
`abortable` (abort the transaction, then begin another). `kafka.errorName(code)`
names a code, and the common ones are constants such as `kafka.ERR_QUEUE_FULL`.

## What it does by default

| | Default | Why | To change |
|---|---|---|---|
| Producer | `enable.idempotence=true` (implies `acks=all`) | A retry after a lost answer cannot write a message twice or reorder a key | set `acks` or `enable.idempotence` |
| Consumer | `enable.auto.commit=false` | Nothing is marked read until you say it was handled | set it |
| Consumer | `auto.offset.reset=earliest` | A new group sees everything rather than silently skipping what came before | set it |
| `produce` | blocks while the local queue is full | Backpressure, rather than an error every caller must retry | `tryProduce` |

Anything else goes straight to librdkafka, with its full
[configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md):

```dex
let settings: map[string, string] = {}
settings.set("bootstrap.servers", "broker-1:9093,broker-2:9093")
settings.set("security.protocol", "sasl_ssl")
settings.set("sasl.mechanism", "SCRAM-SHA-512")
settings.set("sasl.username", user)
settings.set("sasl.password", password)
settings.set("compression.type", "zstd")
settings.set("linger.ms", "20")
let p: kafka.Producer = kafka.producerWith(settings)
```

A setting librdkafka does not know throws when the client is made, naming it.

## Producing

| | |
|---|---|
| `p.produce(topic, key, value)` | Queue a message. A key of `""` sends none |
| `p.produceMessage(m)` | The same, for headers, a partition, a timestamp, a tombstone |
| `p.tryProduce(m): bool` | `false` rather than waiting when the queue is full |
| `p.produceSync(m): Delivery` | Wait for the broker's answer to this message |
| `p.produceTracked(m, tag)` | Queue it, and report it under `tag` through `nextDelivery` |
| `p.nextDelivery(timeoutMs): Delivery?` | The next tracked report, or `null` |
| `p.flush(timeoutMs): int` | Wait for everything; returns how many are undelivered |
| `p.queued(): int`, `p.delivered(): long`, `p.failed(): long` | How it is going |
| `p.errors(): ClientError[]` | What went wrong since last asked |
| `p.stats(): string` | The newest JSON, with `statistics.interval.ms` |
| `p.close(timeoutMs): int` | Flush, then fail the rest; returns how many did not make it |

```dex
let m: kafka.Message = kafka.message("orders", payload)
    .withKey("order-42")
    .withHeader("trace-id", trace)
    .withPartition(3)
    .withTimestamp(ms)

let gone: kafka.Message = kafka.tombstone("users", "user-7")    // deletes from a compacted topic
```

A message is delivered in the background, so `produce` returning says it was
queued, not that it arrived. Choose how much you need to know:

- **Counts.** `failed()` and `errors()` after a `flush()`. Costs nothing per message.
- **Per message, asynchronously.** `produceTracked` with a reader of `nextDelivery`.
  Read the reports: they wait in memory until you do.
- **Per message, in line.** `produceSync`. Threads' messages still batch together,
  so this costs latency rather than throughput.

A `Delivery` has `status`: `PERSISTED`, `NOT_PERSISTED`, or `POSSIBLY_PERSISTED`,
which is the case a failed message can be in when the broker's answer was lost, so a
retry might duplicate it.

Letting a producer's last reference go flushes for up to five seconds. Call `close`
with a timeout of your own where that matters.

## Consuming

| | |
|---|---|
| `c.subscribe(topics)`, `c.unsubscribe()`, `c.subscription()` | Join the group; a name starting `^` is a regex |
| `c.poll(timeoutMs): Message?` | The next message. Also what keeps the consumer in its group |
| `c.pollBatch(max, timeoutMs): Message[]` | Up to `max`, without waiting after the first |
| `c.commitMessage(m)` | Commit up to and including `m`, and wait |
| `c.commit()`, `c.commitAsync()` | Commit everything polled so far |
| `c.commitOffsets(partitions)` | Commit these offsets as given |
| `c.store(m)` | Mark `m` handled, for a later commit or auto-commit |
| `c.assign(partitions)`, `c.unassign()`, `c.assignment()` | Read chosen partitions, or see what is held |
| `c.seek(topic, partition, offset)` | Move a reading position |
| `c.pause(partitions)`, `c.resume(partitions)` | Stop fetching, staying in the group |
| `c.position(partitions)`, `c.committed(partitions)` | Where this consumer and its group are |
| `c.watermarks(topic, partition): Watermarks` | First and next-to-write offsets |
| `c.offsetsForTimes(partitions)` | The first offset at or after a time |
| `c.rebalances(): Rebalance[]` | Partitions gained and lost since last asked |
| `c.wake()` | Make a poll on another thread return now, with `null` |
| `c.close()` | Leave the group now rather than after `session.timeout.ms` |

A `Message` has `topic`, `partition`, `offset`, `timestamp`, `key`, `value`, `headers`,
`hasKey`, `hasValue` and `eof`, and `m.header(name)` finds a header. A key and a
value are bytes in a `string`, whatever they hold, NULs included. A missing one and
an empty one differ on the wire, so `hasKey` and `hasValue` say which: `!m.hasValue`
is a tombstone. A message is plain data, so keep it, pass it to other threads, or
outlive the consumer with it. (A header with a NULL value reads as `""`.)

**At-least-once.** Handle, then commit, and a crash reads the uncommitted messages
again, so handling must tolerate seeing one twice. A commit made during a rebalance can
fail because the partition has moved; its new owner reads those messages, so carry on:

```dex
try {
    c.commitMessage(m)
} catch (e: kafka.KafkaError) {
    if (!kafka.isRebalance(e.code)) {
        report(e)
    }
}
```

A round trip per message is slow. For throughput, commit a batch at a time, or turn
on `enable.auto.commit` with `enable.auto.offset.store=false` and call `c.store(m)` after
handling each: nothing unhandled is ever committed, and there is no round trip.

**Poll often enough.** A consumer that does not poll for `max.poll.interval.ms` is
removed from its group, and the next poll throws `ERR_MAX_POLL_EXCEEDED`. If handling
is slow, `pause` and keep polling while it happens. `poll` throwing is otherwise not
the end: unless the error is `fatal`, poll again.

**Brokers going away** is not an error to handle: librdkafka reconnects, retries, and
follows partition leaders. It is reported through `errors()` for you to log or alert
on, and no call throws for it.

## Exactly once

For consume-transform-produce, where a crash must neither lose nor double anything:

```dex
let settings: map[string, string] = {}
settings.set("bootstrap.servers", brokers)
settings.set("transactional.id", "billing-" + instance)   // stable across restarts
let p: kafka.Producer = kafka.producerWith(settings)
p.initTransactions()

// the consumer has isolation.level=read_committed, and auto-commit off
let batch: kafka.Message[] = c.pollBatch(500, 1000)
p.beginTransaction()
foreach (batch as m) {
    p.produce(output, m.key, transform(m.value))
}
p.sendOffsets(c)       // the input is marked read exactly when the output appears
p.commitTransaction()
```

On a failure, an `abortable` error means `abortTransaction`, a `c.seek` back, and
try again. A `fatal` one, such as another instance having taken over the
`transactional.id`, means this producer is finished.

## Admin

```dex
let a: kafka.Admin = kafka.admin(brokers)
a.createTopic("orders", 12, 3)
a.createTopicWith("users", 12, 3, settings)      // cleanup.policy and so on
a.createPartitions("orders", 24)
a.deleteTopic("orders")
let c: kafka.ClusterInfo = a.cluster()           // brokers, topics, partitions, leaders
let g: kafka.GroupInfo[] = a.groups()
a.deleteGroup("billing")
```

## Threads

A `Producer`, `Consumer` or `Admin` holds one client, and a copy of one shares it. A
**producer can be used by any number of threads at once**, and sharing one is better
than having several, since their messages batch together. A **consumer is polled by
one thread at a time**; to read in parallel, give each thread its own consumer in the
same group. `Message`s can go anywhere.

```dex
let p: kafka.Producer = kafka.producer(brokers)
for (let w: int = 0; w < 8; w++) {
    spawn {
        try {
            p.produce("events", "", event(w))     // one producer, shared
        } catch (e: kafka.KafkaError) {
            report(e)
        }
    }
}
```

## How it is built

Dex cannot be called from C, since `throw` is a `longjmp` and could cross C frames.
So every librdkafka callback lands in `kafka_shim.h`, which queues what happened for
Dex to pull: a thread of its own serves a producer's delivery reports, as the poller
goroutine does in confluent-kafka-go, and a consumer's are served by `poll`. A rebalance
is applied in the callback, including cooperative ones, and reported through
`rebalances()`, so you see it but cannot veto or delay it.

A message's bytes cross straight from librdkafka's buffer into a Dex `string`, by
length (`sized by` in the external declaration), so one copy is made and a NUL
anywhere is kept.

## How it has been tested

**`kafka_test.dx`**, against a real broker: 24 groups of assertions covering binary
round trips with NULs in keys, values and headers; NULL against empty; per-key order
across partitions; a producer shared by threads with exactly-once arrival; commit and
resume; hand-stored offsets; group rebalancing with both the eager and the cooperative
assignor; a consumer too slow to stay in its group; seek, pause, watermarks and times;
end of partition; transactions commit
and abort; exactly-once consume-transform-produce; zombie fencing; admin; misuse; no
broker at all, including a full queue and a thread stuck in `produceSync`; and clients
released from other threads and on unwinding.

It passes repeatedly (5 of 5 in a row, about a minute), under the memory checker
with leaks as errors, with 0 leaks under `leaks --atExit`, and under ASan, UBSan and
TSan. Those see the shim and the Dex side: librdkafka itself is not instrumented.

**`chaos/chaos.sh`** starts a three-broker cluster of its own and writes a stream of
120,000 keyed messages to a topic with three replicas, while killing and restarting two
brokers and sending SIGKILL to the consumer twice. It then reads the log back and
requires every key's whole sequence on one partition, in order, exactly once. Result:

```
RESULT produced=120000 delivered=120000 failed=0 left=0
missing=0 written-twice=0 out-of-order=0 keys-on-two-partitions=0
re-read after a crash or rebalance (expected, at-least-once)=62
```

**Not yet exercised:** TLS and SASL (settings pass straight through and the Homebrew
build has both, but no test has connected through them), network partitions and slow
brokers as opposed to dead ones, runs of days rather than minutes, and Linux. These
tests ran on macOS on Apple silicon against Confluent Platform 7.8 (Kafka 3.8).

```sh
cd kafka
dex run kafka_test.dx       # KAFKA_BROKERS, or localhost:9092
chaos/chaos.sh              # needs Docker; ports 19092-19094
dex run bench/bench.dx
```

Every topic and group `kafka_test` makes is named `dexkafka.test.*` and removed
afterwards, so it is safe against a broker in use. Using transactions makes a broker
create its `__transaction_state` topic, if it has none yet.

## Performance

One producer and one consumer, 100-byte messages, three partitions, on an M1 Pro
with a broker on localhost, so this measures the client more than the broker:

| msg/s | this package | confluent-kafka-go 2.16.1 |
|---|---|---|
| produce | 590,000 to 900,000 | 730,000 to 790,000 |
| consume | 400,000 to 2,300,000 | about 220,000 |

Both are librdkafka 2.16.0, so the work underneath is the same code; the difference
is the cost of crossing into it. Consuming spreads widely because the client is no
longer what limits it: the result depends on how much librdkafka had fetched ahead
when timing began. The Go numbers were measured on an earlier day, on the same
machine and broker. `bench/bench.dx` is the Dex side. This is a reference point on
one machine, not a claim about every workload.

A payload's bytes cost the same whatever they hold. At 1 KB, from three runs of each
build, alternating:

| msg/s | text | a NUL every 16 bytes | all NUL |
|---|---|---|---|
| now | 420,000 to 620,000 | 330,000 to 520,000 | 450,000 to 670,000 |
| before `sized by` (dex 0.9.31) | 450,000 to 620,000 | 335,000 to 360,000 | 45,000 to 70,000 |

## Compared with confluent-kafka-go

The same library underneath, so the same protocol support and behaviour. Here:

- **Safer defaults**: idempotence on, auto-commit off, and backpressure rather than
  `ErrQueueFull`, which Go leaves each caller to handle.
- **A shared producer needs no event loop.** In Go, forgetting to drain `Events()`
  stalls or leaks. Here nothing accumulates unless you ask for tracked reports.
- **No `Event` type switch.** `poll` returns a message or throws.
- **Not here yet**: describing and altering topic and broker configs, ACLs, admin
  operations on group offsets, OAUTHBEARER token refresh and custom partitioners
  (both need callbacks into Dex), a rebalance callback that can act before partitions
  move (`rebalances()` reports afterwards), and a log channel (librdkafka logs to stderr,
  and `log_level` sets how much).
