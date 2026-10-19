# redis

A Redis client for Dex, over the hiredis C library.

It replaces the standard library's `redis` module. Until that module is removed
from the compiler, a program can import one or the other but not both under the
same name; `import "github.com/ensamuel7/dex-modules/redis" as "hredis"` is the
way to use both meanwhile.

```dex
import "github.com/ensamuel7/dex-modules/redis"

let r: redis.Client = redis.connect("127.0.0.1", 6379)
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
| `redis.connect(host, port): Client` | connect, or throw |
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
| `r.sadd(key, member): long` / `r.smembers(key): string[]` | |
| `r.publish(channel, message): long` | |

A `Reply` has a `kind` (`Text`, `Number`, `List`, `Nil`, `Status`) and the
matching `text`, `number` or `items`. Integers inside a list reply arrive as
their decimal text.
