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
| `dexqu/` | `github.com/ensamuel7/dex-modules/dexqu` | nothing: a SQL query builder in pure Dex | |
| `example/` | `github.com/ensamuel7/dex-modules/example` | uses both | |

The two C wrappers find their library through `pkg-config`, so `pkg-config` has
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

```sh
dex mod init github.com/acme/app
dex get github.com/ensamuel7/dex-modules/redis
dex get github.com/ensamuel7/dex-modules/sqlite@v0.1.0
```

```dex
import "github.com/ensamuel7/dex-modules/redis"

let r: redis.Client = redis.connect("127.0.0.1", 6379, "")
```

`dex get` downloads the release into the module cache and writes `dex.mod` and
`dex.sum`; commit both. A Dockerfile or CI runs `dex mod download` before
building. The image needs `git`, and the C library each package wraps
(`libhiredis-dev`, `libsqlite3-dev`).

### Working on a package and a program together

`example/` reads both packages from the folders beside it instead of the cache:

```
replace (
    github.com/ensamuel7/dex-modules/sqlite => ../sqlite
    github.com/ensamuel7/dex-modules/redis => ../redis
)
```

A program changing one of these packages does the same with a `replace` to its
checkout, and deletes the line once the change is released.

## Running

```sh
cd sqlite  && dex run sqlite_test.dx
cd redis   && dex run redis_test.dx
cd dexqu   && dex run dexqu_test.dx && dex run sqldb_test.dx
cd example && dex run main.dx
```

The redis test needs `redis-server` on `127.0.0.1:6379`, and the example needs
both.

## Branches and releases

Each module is versioned on its own, with a tag that starts with its folder:
`sqlite/v0.1.0`, `redis/v0.1.0`. A fix to one does not release the other. A
module's first release is `v0.1.0`, and it stays below `v1.0.0` while its API
may still change.

Work on a module happens on a branch named after the module and the change,
never after a version:

```
dexqu/query-builder      redis/cluster-support      sqlite/fix-busy-timeout
```

A branch must not share a name with a tag. `dex get` fetches a release with
`git clone --branch <tag>`, and when a branch and a tag share a name, git
clones the branch. A branch called `dexqu/v0.2.0` would be served in place of
the release, and every push to it would change what `dex get` downloads and
fail the checksum in every `dex.sum`.

To release, merge the branch into `main` and tag the merge commit on `main`:

```sh
git checkout main && git pull
git tag dexqu/v0.1.0
git push origin dexqu/v0.1.0
```

Tag `main` rather than the branch, so that every release is in `main`'s
history. Bump the patch for a fix, the minor for anything new or, below
`v1.0.0`, anything breaking. Each module's README says which version of dex it
needs.

`dex get github.com/ensamuel7/dex-modules/dexqu` then picks it up as the latest
release. Never move or delete a tag once pushed: every project that fetched it
has its checksum in `dex.sum`, and a moved tag fails their builds.
