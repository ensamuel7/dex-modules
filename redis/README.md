# redis

A Redis client for Dex, over the hiredis C library.

```dex
import "github.com/ensamuel7/dex-modules/redis"

let r: redis.Client = redis.connect("127.0.0.1", 6379, "")   // "" = no password
r.set("greeting", "hello")
let g: string? = r.get("greeting")

let reply: redis.Reply = r.command(["ZADD", "board", "42", "ada"])
```

Arguments are sent as a list, never through a format string, so a value
holding spaces or `%` is sent as written. Every failure throws
`redis.RedisError`: a refused connection, a dropped one, or an error reply.

## API

| | |
|---|---|
| `redis.connect(host, port, password): Client` | a pool of 8 connections; `""` for no password |
| `redis.connectPool(host, port, password, size): Client` | the same with `size` connections, up to 64 |
| `r.command(args: string[]): Reply` | any command |
| `r.ping(): string` | |
| `r.set(key, value)` / `r.setex(key, value, seconds)` | |
| `r.get(key): string?` | `null` when absent |
| `r.del(key): long` / `r.exists(key): bool` | |
| `r.incr(key): long` / `r.incrBy(key, by): long` | |
| `r.expire(key, seconds): bool` / `r.ttl(key): long` | |
| `r.keys(pattern): string[]` | |
| `r.hset(key, field, value): long` / `r.hget(key, field): string?` | |
| `r.hgetall(key): map[string, string]` | |
| `r.lpush / rpush(key, value): long` / `r.lrange(key, start, stop): string[]` | |
| `r.hdel(key, field): long` / `r.hkeys(key): string[]` | |
| `r.sadd(key, member): long` / `r.srem(key, member): long` | |
| `r.sismember(key, member): bool` / `r.smembers(key): string[]` | |
| `r.publish(channel, message): long` | |
| `r.subscribe(channel): Subscriber` | a connection of its own, subscribed |
| `sub.subscribe(channel)` / `sub.psubscribe(pattern)` | add a channel or a pattern |
| `sub.next(): Message?` | block for the next message; `null` once the connection drops |

## Threads

A `Client` can be shared by any number of threads. It holds a pool of
connections, and each command takes one for the length of the command, waiting
if all are in use, so two threads never read each other's replies. A connection
that drops is redialled the next time it is taken.

## Pub/sub

Subscribing takes a connection out of request/reply for good, so
`r.subscribe` opens one of its own rather than taking it from the pool. Messages
are pulled, usually from a thread that does nothing else:

```dex
let sub: redis.Subscriber = r.subscribe("jobs:node-a")
spawn {
    while (true) {
        let m: redis.Message? = sub.next()
        if (m == null) {
            break
        }
        handle(m.channel, m.payload)
    }
}
```

A `Message` has the `channel` it arrived on, the `pattern` that matched it (`""`
for a plain subscription) and the `payload`.

## Passwords

The password is sent with `AUTH` on every connection the client opens,
including a subscriber's, and `connect` fails at once if it is wrong or missing.

A `Reply` has a `kind` (`Text`, `Number`, `List`, `Nil`, `Status`) and the
matching `text`, `number` or `items`. Integers inside a list reply arrive as
their decimal text.
