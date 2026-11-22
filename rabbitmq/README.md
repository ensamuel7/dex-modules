# rabbitmq

A RabbitMQ client for Dex that reconnects by itself. It speaks AMQP 1.0, RabbitMQ 4's
native protocol, over [Qpid Proton](https://qpid.apache.org/proton/). This is the same
protocol as RabbitMQ's own
[rabbitmq-amqp-go-client](https://github.com/rabbitmq/rabbitmq-amqp-go-client).

**It needs RabbitMQ 4.0 or later.** RabbitMQ 3.x speaks AMQP 1.0 only through a plugin,
with a different addressing scheme and no management over AMQP, so it is not supported.

```sh
brew install qpid-proton          # or: apt install libqpid-proton11-dev
dex get github.com/ensamuel7/dex-modules/rabbitmq
```

```dex
import "github.com/ensamuel7/dex-modules/rabbitmq" as "rmq"

let c: rmq.Connection = rmq.connect("amqp://user:password@localhost:5672")
c.declareQueue(rmq.quorumQueue("orders"))

let p: rmq.Publisher = c.publisher(rmq.toQueue("orders"))
p.send(rmq.message(payload))                     // async, windowed
let o: rmq.Outcome = p.publish(rmq.message(x))   // waits for the broker
p.flush()

let cons: rmq.Consumer = c.consumer("orders")
while (running) {
    let d: rmq.Delivery? = cons.receive(1000)    // null when nothing arrived
    if (d != null) {
        handle(d.message.body)
        d.accept()                               // only after it is handled
    }
}
```

Needs **dex 0.9.42 or later**, which `dex.mod` records. Every failed call throws
`rmq.RabbitError`, which carries a `message` and a `code`: one of the `ERR_` constants,
or the management API's HTTP-like status for a declaration it refused, such as
`ERR_PRECONDITION` (409) when a queue exists with other settings.

## Reconnection

A connection that drops reconnects without being asked, and then restores what it had,
in this order:

1. **The management link**, used for declaring.
2. **Every queue, exchange and binding declared through it**, in the order they were
   declared. Exclusive and auto-delete queues the old connection took with it come back.
   Deleted and unbound ones stay gone.
3. **Every publisher and consumer**, re-attached.
4. **Messages that were in flight**, sent again.

Options, from `rmq.options(uri)`:

| Field | Default | |
|---|---|---|
| `uris` | the one given | more than one is a cluster: each reconnect tries the next |
| `backoffMs` / `backoffMaxMs` | 500 / 30000 | doubling, with jitter of up to a quarter either way |
| `maxReconnectAttempts` | 0, forever | after this many failures in a row it closes for good |
| `publishTimeoutMs` | 30000 | how long a message or request may wait for an answer, outages included |
| `connectTimeoutMs` | 30000 | how long `connect` keeps trying, before the first open |
| `idleTimeoutMs` | 30000 | a broker silent this long is taken for dead, even with no error |
| `resendInFlight` | true | false fails in-flight messages instead of sending them again |
| `containerId`, `caFile`, `verifyPeer` | generated, none, true | |

What a reconnect means for each part:

- **A publish waits it out.** `publish` and a full window's `send` wait until the
  connection is back or `publishTimeoutMs` passes. A message sent but not yet answered
  when the connection dropped is sent again, so **delivery is at least once**:
  a consumer may see a message twice, and should tolerate that.
- **A consumer carries on.** `receive` returns null until messages flow again. Messages
  received but not settled when the connection dropped are the broker's again. Settling
  one returns false, and the broker delivers it again. A delivery let go of without being
  settled is requeued.
- **Bad credentials are not retried.** `connect` throws `ERR_AUTH` at once.
- **A declaration is retried once.** RabbitMQ ends the session over a request it cannot
  handle, so a request in flight when two connections in a row die fails with `ERR_LINK`
  and the broker's reason, rather than taking every later connection down with it.

`c.events()` reports what happened (connected, lost, re-declaring failures, links the
broker closed, undelivered messages), `c.reconnects()` counts reconnects, and
`c.waitOpen(ms)` waits for the connection to be open.

## Publishing

| | |
|---|---|
| `c.publisher(target, window = 2000)` | `rmq.toQueue(q)`, `rmq.toExchange(e, key)`, or `rmq.anywhere()` |
| `p.publish(m): Outcome` | Send, and wait for the broker's answer |
| `p.send(m)` / `p.sendBody(body)` | Send without waiting; blocks while the window is full |
| `p.sendTracked(m, tag)` / `p.nextOutcome(ms): Outcome?` | Send, and read each outcome later |
| `p.flush(ms): int` | Wait for every outcome; returns how many are still out |
| `p.accepted()`, `released()`, `rejected()`, `failed()`, `inFlight()` | How it is going |
| `p.failure(): string` | Why the broker detached it, or "" |
| `p.close(ms): int` | Flush, then close |

The **window** is how many messages may be in flight, unanswered, at once. A full window
is backpressure: `send` waits. RabbitMQ also stops granting credit under a memory or disk
alarm, which this turns into waiting rather than errors.

An **Outcome** is one of `ACCEPTED` (it has it), `RELEASED` (no queue took it, for example
an exchange with no matching binding), `REJECTED` (refused, with a reason), `MODIFIED`, or
`FAILED` (no answer came in time, so it may or may not have arrived).

```dex
let m: rmq.Message = rmq.message(body)
    .withSubject("order.created")
    .withContentType("application/json")
    .withMessageId(id)
    .withCorrelationId(request)
    .withProperty("tenant", "acme")      // application properties
    .withPriority(5)
    .withTtl(60000)
    .withDurable(false)                  // durable is the default
```

`rmq.anywhere()` makes a publisher whose messages each name their own target, with
`.withTo(rmq.toQueue(q))` or `.withTo(rmq.toExchange(e, key))`.

## Consuming

| | |
|---|---|
| `c.consumer(queue, credit = 1000)` | A consumer of a queue |
| `c.streamConsumer(queue, offset, credit = 1000)` | From `rmq.fromFirst()`, `fromLast()`, `fromNext()`, `fromOffset(n)`, `fromTime(ms)` |
| `cons.receive(ms): Delivery?` | The next message, or null after `ms` |
| `d.accept()` | Done with it |
| `d.requeue()` | Back to the queue, for this or another consumer |
| `d.discard()` | Dead-lettered if the queue says so, else dropped |
| `d.fail()` | Requeued and counted toward a quorum queue's delivery limit |
| `cons.pause()`, `cons.resume()` | Stop and start new deliveries, staying attached |
| `cons.close()`, `cons.failure()` | |

**Credit** is how many messages a consumer holds ready ahead of `receive`. More is faster,
up to a point, but more are redelivered if the process dies, and they are shared less
evenly between competing consumers. For a slow handler, use a small credit.

A `Delivery` has the `message`: its `body`, the fields above, `properties`, the
`exchange` and `routingKey` it was published with, `deliveryCount`, and `redelivered`.
`receive` throws once the consumer can deliver nothing more: closed, its queue deleted,
or the connection closed for good.

A quorum queue dead-letters a message once its delivery count **exceeds**
`deliveryLimit`, so with a limit of 2, a message is dead-lettered on its third `fail()`.
A stream read `fromTime` starts at the chunk holding that time, so it may begin a few
messages early.

## Topology

```dex
c.declareQueue(rmq.quorumQueue("orders").deliveryLimit(5).deadLetter("dlx", ""))
c.declareQueue(rmq.classicQueue("cache").maxLength(10000).messageTtl(60000))
c.declareQueue(rmq.streamQueue("events").maxAge("7D"))
c.declareQueue(rmq.classicQueue("replies").exclusiveToConnection())

c.declareExchange("events", "topic")
c.bind("events", "orders", "order.*")
c.bindExchange("all", "events", "")

let info: rmq.QueueInfo = c.queue("orders")     // kind, messages, consumers, leader
c.purgeQueue("orders")
c.unbind("events", "orders", "order.*")
c.deleteQueue("orders")                         // a missing queue is not an error
```

Any queue argument goes in with `withArg`, `withLongArg` or `withBoolArg`. Declaring
something already there with the same settings is fine; with other settings it throws
`ERR_PRECONDITION`.

## Threads

A `Connection`, `Publisher` or `Consumer` holds one client, and a copy of one shares
it. **A publisher can be used by any number of threads at once**, and their messages
share its window. **A consumer is read by one thread at a time**; for parallel
handling, give each thread its own consumer on the same queue.

Each connection has one I/O thread of its own, which alone talks to the broker. Dex
threads hand it work and wait on it. Messages are encoded and decoded on the calling
thread, not the I/O thread.

## How it is built

Each link has an AMQP session of its own. RabbitMQ ends the whole session over one
link's error, such as a publisher to a queue that does not exist. A shared session
would take every other link on the connection down with it. Here, a broken link fails
on its own, at once, with the broker's reason, and everything else carries on. A
session freed by a closed link is reused for the next one.

Declarations go through RabbitMQ 4's management API over AMQP (HTTP-over-AMQP
requests on `/management`), on the same connection, so no HTTP port is needed.

## How it has been tested

**`rabbitmq_test.dx`**, against RabbitMQ 4.1.8. 11 groups of assertions cover:

- connecting: wrong credentials fail fast, an unreachable host times out, a bad uri, a
  cluster list whose first entry is dead
- topology: every queue kind, conflicts, purge, names needing encoding
- routing: topic patterns, released outcomes, exchange-to-exchange bindings,
  per-message targets
- messages: every field and property, every byte value, 1 MB and empty bodies
- outcomes: tracked tags, a publisher to a missing queue, use after close
- settling: requeue, accept, discard to a dead-letter queue, fail past a delivery
  limit, unsettled deliveries requeued, pause and resume, a queue deleted under a
  consumer
- streams: from every kind of offset
- threads: six threads on one publisher, and three competing consumers seeing every
  message exactly once between them
- recovery: the broker cuts the connection twice; the exclusive queue, its binding,
  the publisher and the consumer all come back; a message held across the cut is
  redelivered; a publish made during the outage completes
- giving up after the allowed reconnect attempts
- clients dropped without closing

It passes under the memory checker with leaks as errors, with 0 leaks under
`leaks --atExit`, and under ASan, UBSan and TSan. Those cover the shim and the Dex
side; Proton itself is not instrumented.

**`chaos/chaos.sh`** starts a RabbitMQ of its own and streams 80,000 numbered messages
into a quorum queue. Meanwhile it kills and restarts the broker twice, and sends
SIGKILL to the consumer twice. It then requires the producer to report every message
accepted and none failed, and every number to have reached a consumer:

```
RESULT sent=80000 accepted=80000 failed=0 left=0 reconnects=2
lines read=80010 distinct=80000 of 80000
missing=0
read more than once (expected, at-least-once)=10
```

The producer was publishing through both outages (2 reconnects), and the consumer
through all four interruptions.

**Not yet exercised:** TLS (amqps:// and `caFile` are implemented, but no test has
connected through them), a multi-node cluster and failover between its nodes,
network partitions and slow brokers rather than dead ones, runs of days, and Linux.
These ran on macOS on Apple silicon.

```sh
cd rabbitmq
./test.sh                  # starts a broker on 5679 in Docker, runs the suite
RABBITMQ_URL=amqp://... dex run rabbitmq_test.dx   # against your own, without recovery tests
chaos/chaos.sh             # needs Docker; port 5680
dex run bench/bench.dx
```

Everything the tests make is named `dexrmq.test.*` and removed afterwards.

## Performance

200,000 durable 100-byte messages to one queue and back, on an M1 Pro with RabbitMQ 4.1
in Docker. Each client had a window of 2000 unanswered messages and a credit or
prefetch of 1000, with every message accepted or acked one at a time. Ranges are from
three runs each:

| msg/s | this package | amqp091-go 1.10 (AMQP 0-9-1) | rabbitmq-amqp-go-client 1.6 (AMQP 1.0) |
|---|---|---|---|
| classic publish | 70,000 to 75,000 | 68,000 to 81,000 | 47,000 |
| classic consume | 84,000 to 94,000 | 68,000 to 73,000 | 47,000 to 55,000 |
| quorum publish | 22,000 to 32,000 | 28,000 to 35,000 | 20,000 to 32,000 |
| quorum consume | 39,000 to 57,000 | 48,000 to 66,000 | 37,000 to 45,000 |

On classic queues this package matches amqp091-go publishing, and consumes faster. On
quorum queues it is behind amqp091-go, by about 10 to 25%, and level with or ahead of
RabbitMQ's own AMQP 1.0 client. Profiling shows the client idle there, waiting on the
broker, so the gap is RabbitMQ's AMQP 1.0 path for quorum queues rather than this code.
Run-to-run spread is as large as the differences, so read this as "the same league",
not a ranking.

## Not here yet

OAuth 2 token refresh, RPC helpers (request/reply over direct reply-to), stream
filtering (`rabbitmq:stream-filter` and SQL filters), single-active-consumer state
events, consumer priorities, typed application properties (they are strings here, and
other types read back as text), and a callback for a consumer to act on a rebalance
before it happens.
