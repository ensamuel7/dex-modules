# DexModules

Dex libraries that wrap C libraries through `external module`. One repository,
one module per folder: each has its own `dex.mod` and its own version.

These are first-party packages: client libraries such as Redis and Kafka live
here rather than in the language's standard library, so each can change and be
released without a new compiler.

| Folder | Module | Wraps | Install the C library |
|---|---|---|---|
| `sqlite/` | `github.com/ensamuel7/dex-modules/sqlite` | libsqlite3 | `brew install sqlite` / `apt install libsqlite3-dev` |
| `redis/` | `github.com/ensamuel7/dex-modules/redis` | hiredis | `brew install hiredis` / `apt install libhiredis-dev` |
| `example/` | `github.com/ensamuel7/dex-modules/example` | uses both | |

Both libraries find their C library through `pkg-config`, so `pkg-config` has
to be installed too.

## Layout of a library

```
sqlite/
  dex.mod             module github.com/ensamuel7/dex-modules/sqlite
  sqlite.dx           the library: an external module block, plus the Dex API
  sqlite_shim.h       small C shims for what Dex cannot express directly
  sqlite_test.dx      the library's tests
```

Importing the module path whole reads the file named after its last segment,
so `import "github.com/ensamuel7/dex-modules/sqlite"` reads `sqlite.dx`.

The `external module` block is private to `sqlite.dx`. A program that imports
the library sees `sqlite.Database`, `sqlite.Statement` and `sqlite.SqliteError`,
and never the C names, header or link flags.

Name the shim's C functions after the header (`sqlite_shim_exec`), never
`sqlite_<name>`: that is the C name the compiler gives the module's own
function `<name>`, and the two would collide.

The header holds what a declaration cannot say: a C function pointer argument
(`sqlite3_exec`'s callback, `SQLITE_TRANSIENT`), a struct field read
(`redisReply->type`), or an array of strings (`redisCommandArgv`).

## Using them from a program

```
module github.com/ensamuel7/dex-modules/example

require (
    github.com/ensamuel7/dex-modules/sqlite v0.1.0
    github.com/ensamuel7/dex-modules/redis v0.1.0
)

replace (
    github.com/ensamuel7/dex-modules/sqlite => ../sqlite
    github.com/ensamuel7/dex-modules/redis => ../redis
)
```

`replace` points each module at the folder beside it. Once they are published
and `dex get` exists, delete the `replace` block and nothing else changes.

## Running

```sh
cd sqlite  && dex run sqlite_test.dx
cd redis   && dex run redis_test.dx     # needs redis-server on 127.0.0.1:6379
cd example   && dex run main.dx            # needs both
```

## Releasing

Each module is versioned on its own, with a tag that starts with its folder:
`sqlite/v0.1.0`, `redis/v0.1.0`. A fix to one does not release the other.
