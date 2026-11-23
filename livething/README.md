# livething

A [LiveThing](https://livething.dev) server in Dex. Browsers and Node programs
connect with the stock `@livething/client` package and speak LiveThing's
[wire protocol](https://livething.dev/core/learn/wire-protocol) to it over a
WebSocket. Pure Dex: `ws` and `json` only.

```dex
import "json"
import "github.com/ensamuel7/dex-modules/livething"

struct Chat {
    fn authenticate(c: livething.Conn): livething.Auth {
        let user: string = userForSession(c.header("Cookie"), c.token)
        if (user.isEmpty()) {
            return livething.refuse("signed out")
        }
        return livething.accept(user)
    }

    fn event(c: livething.Conn, channel: string, name: string, payload: json.Value): void {
        if (name == "send-message") {
            let text: string = payload["text"].asString()
            livething.broadcast(channel, "message", { text: text, from: c.user })
        }
    }

    fn request(c: livething.Conn, channel: string, name: string, payload: json.Value): livething.Reply {
        if (name == "get-profile") {
            return livething.reply({ user: c.user })
        }
        return livething.fail("not-found", livething.nothing())
    }

    fn subscribe(c: livething.Conn, channel: string, family: string, payload: json.Value): string {
        return ""   // or an error code, such as "channel-not-found"
    }

    fn closed(c: livething.Conn): void {}
}

fn main(): void {
    livething.serve(Chat {}, 9090)
}
```

```ts
import { channels, client, clientRequests, connect, keepalive } from '@livething/client'

const socket = client<any>()(connect('wss://example.com/live', { token: () => session }),
  channels(), clientRequests(), keepalive())
socket.connect()
const room = socket.channel('rooms', 'room:general')
room.on('message', (m) => show(m))
await room.subscribe()
room.send('send-message', { text: 'hi' })
const me = await socket.request('get-profile')
```

The client has no `InferClient` types here, since there is no TypeScript
server to infer them from: write the contract interface by hand, or use `any`.

## The App

A program implements `App` and calls `serve(app, port)`, which blocks. `serveOn(app, host, port)` is the same bound to one IPv4 address, such as
`"127.0.0.1"` behind a proxy on the same machine (dex 0.9.44 and later).

| Method | Called |
|---|---|
| `authenticate(c): Auth` | When a connection opens, before `__connected__`. `accept(user)` (`""` for a guest) or `refuse(reason)` |
| `event(c, channel, name, payload)` | For each event the client sends; `channel` is `""` outside one |
| `request(c, channel, name, payload): Reply` | For each request; answer `reply(payload)` or `fail(code, detail)`, which the client's promise rejects with (`err.code`, `err.detail`) |
| `subscribe(c, channel, family, payload): string` | When the client joins; `""` lets it in, anything else is the error code it gets |
| `closed(c)` | When the connection is gone, after it has left its channels |

`Conn` carries `id` (the client's `connectionId`), `user`, `path` (query
included) and `token`: an `Authorization: Bearer` header, or else the
client's `token` option, which a browser sends as the subprotocol
`livething-v1.token.<base64url>`. `c.header(name)` reads the opening request
(a `Cookie`, an `Origin`); `c.emit(event, payload)` and `c.close()` act on it.

From anywhere, any thread: `emit(connId, ...)`, `toUser(user, ...)`,
`broadcast(channel, ...)`, `broadcastExcept(channel, connId, ...)`,
`membersOf(channel)`, `kick(connId, channel, reason)`, `disconnect(connId)`,
`connections()`. `keepalive(pingMs, idleMs)` before `serve` sets the timings
(30 s and 60 s by default; 0 turns pings off): the server pings, answers the
client's pings, and closes a connection silent for `idleMs`.

## What it speaks

`__connected__`, events and requests at the connection and in channels,
typed `__error__` replies, `__subscribe__`/`__unsubscribe__` and their
confirmations (ephemeral membership: the client joins again itself after a
reconnect), `__kicked__`, and `ping`/`pong`. A channel event from a
connection that has not joined gets `channel-not-subscribed`.

Not yet: replay, streams, uploads, binary frames, presence and awareness,
inboxes and read receipts, server-to-client requests, and the `http()`
transport. `__connected__` lists no features, so a client using one of those
primitives logs a warning, and a framework frame for one gets
`unsupported-protocol`.

## Things to know

- **Handlers run on the WebSocket server's worker threads**, several at once,
  and two frames from one connection can be handled out of order (LiveThing's
  TypeScript server queues them per connection). `authenticate` runs on the
  event loop: keep it short.
- **A refused connection closes without a close code** (the client sees
  `1005`), because `ws.close` takes none. A LiveThing server closes with
  `4002`, which tells the client's `reconnection()` to stop; this one retries.
  It needs `ws.close` with a code and reason in DexLang.
- **Needs dex 0.9.43**: earlier `ws` servers answer 400 to Node's WebSocket,
  which sends its header names in lower case. Browsers capitalise them and
  work either way.
- Inside a `json.Value` literal, a method call or arithmetic encodes as
  `null` (`{ sum: a.asInt() + b }`); assign it to a local first.

## Tests

```sh
./test.sh
```

The unit tests (`livething_test.dx`), then `testdata/chat.dx` served on a
port and driven by `testdata/e2e/e2e.mjs` with `@livething/client`
1.0.0-alpha.0 from npm: sign-in by token, refusal, requests and typed errors,
events, joining, broadcasting to every member, refused joins, leaving, and
keepalive both ways. It needs Node 22 or later.
