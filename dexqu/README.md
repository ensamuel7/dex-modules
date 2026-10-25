# dexqu

A SQL query builder for Dex. Pure Dex, no C library: it produces SQL text and
the values for its placeholders, for PostgreSQL, MySQL or SQLite.

```dex
import "github.com/ensamuel7/dex-modules/dexqu" as "q"

let active: q.Select = q.from("users u")
    .columns(["u.id", "u.email"])
    .join("orgs o", q.col("o.id").eq(q.col("u.org_id")))
    .where(q.col("u.active").eq(q.boolean(true)))
    .where(q.col("o.plan").in([q.text("pro"), q.text("team")]))
    .orderBy("u.created_at DESC")
    .limit(20)

let built: q.Query = active.build(q.Dialect.Postgres)
// built.sql:  SELECT "u"."id", "u"."email" FROM "users" AS "u"
//             JOIN "orgs" AS "o" ON "o"."id" = "u"."org_id"
//             WHERE "u"."active" = TRUE AND "o"."plan" IN ($1, $2)
//             ORDER BY "u"."created_at" DESC LIMIT $3
// built.args: [Text(pro), Text(team), Int(20)]
```

`build` throws `q.QueryError` for a query that cannot be written: a feature the
dialect lacks, an UPDATE with no condition, a malformed name.

Needs **dex 0.9.14 or later**. An older compiler fails inside `dexqu.dx` with
errors such as `undefined function 'noNodes'`. Run `make install` in the
compiler repo (or `dex update`) first. Until `dex.mod` can say this itself, the
note here is the only place it is written (see `dex-samples/problems/002`).

## Three rules

**Builders are values.** Every method returns a new builder and leaves its
receiver alone, so a base query can be shared and branched:

```dex
let base: q.Select = q.from("orders").where(q.col("tenant_id").eq(q.param("tenant")))
let open: q.Select = base.where(q.col("state").eq(q.text("open")))
let recent: q.Select = base.orderBy("created_at DESC").limit(50)
```

A call whose result is dropped does nothing: write `s = s.where(..)`, not
`s.where(..)`. Branching costs one small allocation, because clause lists are
persistent (each builder shares everything before its own addition) rather
than copied. Nodes never change after they are made, and Dex refcounts are
atomic, so a base built at startup can be branched from any number of handler
threads at once.

**Values are never SQL.** `q.text`, `q.integer`, `q.bigint` and `q.real` become
placeholders, with their values in `Query.args`. There is no interpolated mode.
`q.boolean` and `q.sqlNull` are written in as `TRUE`/`FALSE` (`1`/`0` on
SQLite) and `NULL`, since they carry no user text. `eq(q.sqlNull())` becomes
`IS NULL`.

**Names are always quoted.** `"u.name"` becomes `"u"."name"` (or `` `u`.`name` ``)
with any embedded quote doubled, so a reserved word works as a column and a
name taken from a request cannot become SQL. `orderBy` accepts only a name
and an optional `ASC`/`DESC`, so a sort key from a query string is safe to pass
straight in.

## Build once, run many

`q.param(name)` is a placeholder whose value comes later. Build the query once,
at startup, and bind it per request. Binding copies the argument array, which
costs about a tenth of a microsecond, while building costs about two:

```dex
let byEmail: q.Query = q.from("users")
    .where(q.col("email").eq(q.param("email")))
    .where(q.col("deleted_at").isNull())
    .build(q.Dialect.Postgres)

let args: q.Value[] = byEmail.bind().setText("email", email).args()
```

`args()` throws if a parameter has no value or a name matches none. On
Postgres a parameter used twice is one `$n`. LIMIT and OFFSET are bound too,
so every page of a listing is the same statement: `limitExpr(q.param("n"))`.

## Running a query

`Query.sql` and `Query.args` go to whatever driver you use. Each argument is a
`q.Value`: `Int(long)`, `Real(double)`, `Text(string)`, `Bool(bool)` or `Null`.
For the standard library's `db` module, `dexqu/sqldb` does the binding:

```dex
import "db"
import "github.com/ensamuel7/dex-modules/dexqu/sqldb"

let st: int = sqldb.query(conn, active.build(q.Dialect.SQLite))
while (db.next(st)) {
    let email: string = db.col(st, 1)
}
db.finalize(st)

let stale: q.Delete = q.deleteFrom("sessions").where(q.col("expires_at").lt(q.raw("CURRENT_TIMESTAMP")))
sqldb.exec(conn, stale.build(q.Dialect.SQLite))

let one: int = sqldb.prepare(conn, byEmail.sql, byEmail.bind().setText("email", email).args())
```

`sqldb` is a file of its own so that a program which only builds SQL does not
import `db`, and with it every database driver on the machine. `db` cannot
bind NULL, so `sqldb` refuses a parameter set with `setNull`. A literal
`q.sqlNull()` is written into the SQL and needs no binding.

### Columns from a struct

`fields` selects one column per struct field, in declaration order, which is
the order `db.fill` reads them back. A field is its name in snake_case, or its
`db` tag written as SQL:

```dex
struct Account {
    id: long
    fullName: string                    // full_name
    email: string `db:"lower(email)"`   // lower(email) AS "email"
}

let a: Account = Account {}
let q1: q.Select = q.from("accounts").fields(reflect.fields(a), reflect.fieldTags(a, "db"))
```

## API

**Values and names**: `col(name)`, `star()`, `integer(int)`, `bigint(long)`,
`real(double)`, `text(string)`, `boolean(bool)`, `sqlNull()`, `param(name)`,
`raw(sql)`, `rawWith(sql, args)`. In `raw`, each `?` is the next argument and
`??` is a literal `?`, whichever placeholder style the dialect uses.

**On an `Expr`**: `eq neq lt lte gt gte`, `isNull isNotNull`,
`like notLike ilike notIlike`, `in notIn` (an `Expr[]`), `inSelect notInSelect`,
`between notBetween`, `and or not`, `plus minus times div concat`, `asc desc`,
`alias(name)`, `cast(type)`, `over(partitionBy, orderBy)`.

**Functions**: `and(list)`, `or(list)`, `not(e)`, `tuple(list)`, `call(name, args)`,
`count countAll countDistinct sum avg min max coalesce lower upper`, `sub(select)`,
`exists notExists`, `excluded(column)`, `caseWhen(c, r).when(c, r).orElse(r)` or `.end()`.

**SELECT**: `from(table)` or `select(exprs)`, then `columns(names)`, `select(exprs)`,
`fields(names, tags)`, `distinct()`, `from(table)`, `fromExpr(e)`,
`join leftJoin rightJoin fullJoin (table, on)`, `crossJoin(table)`,
`joinExpr leftJoinExpr (e, on)`, `joinUsing(table, columns)`, `where(c)`,
`groupBy(names)`, `groupByExpr(exprs)`, `having(c)`, `orderBy(spec)`,
`orderByExpr(e)`, `limit offset (int)`, `limitExpr offsetExpr (e)`,
`forUpdate forShare skipLocked noWait`, `union unionAll intersect except (q)`,
`with withRecursive (name, q)`, `count()`, `build(d)`.

A table is `"users"`, `"public.users"`, `"users u"` or `"users AS u"`.
`count()` turns a page query into its total:
`SELECT COUNT(*) FROM (<query without ORDER BY/LIMIT/OFFSET>) AS "counted"`.

**INSERT**: `insertInto(table)`, then `columns(names)` and `values(row)` per
row, or `set(column, value)` per column for one row, or `fromSelect(q)`.
Upserts: `onConflict(columns)` with `doNothing()`, `doUpdate(columns)` or
`doUpdateSet(column, value)`. Also `returning(columns)` and `build(d)`.

**UPDATE**: `update(table).set(column, value).where(c)`, `returning`, `all()`.
**DELETE**: `deleteFrom(table).where(c)`, `returning`, `all()`.

An UPDATE or DELETE with no `where()` is refused when built, because a
forgotten condition rewrites or empties the table. `all()` says that is meant.

## Dialects

| | Postgres | MySQL | SQLite |
|---|---|---|---|
| identifiers | `"x"` | `` `x` `` | `"x"` |
| placeholders | `$1, $2` | `?` | `?` |
| `ilike` | `ILIKE` | `LIKE` | `LIKE` (both already ignore ASCII case) |
| `concat` | `a \|\| b` | `CONCAT(a, b)` | `a \|\| b` |
| `boolean` | `TRUE`/`FALSE` | `TRUE`/`FALSE` | `1`/`0` |
| `doNothing` | `ON CONFLICT … DO NOTHING` | `INSERT IGNORE` | `ON CONFLICT … DO NOTHING` |
| `doUpdate` | `ON CONFLICT (k) DO UPDATE SET c = EXCLUDED.c` | `ON DUPLICATE KEY UPDATE c = VALUES(c)` | as Postgres |
| OFFSET alone | `OFFSET $n` | `LIMIT 18446744073709551615 OFFSET ?` | `LIMIT -1 OFFSET ?` |
| `returning` | yes | refused | yes (3.35+) |
| row locks | yes | yes | refused |
| `fullJoin` | yes | refused | yes (3.39+) |

MySQL has no conflict target: `ON DUPLICATE KEY` applies to every unique key,
so `onConflict(columns)` is ignored there.

## Performance

A typical listing query (a join, three conditions, a five-item `IN`,
ORDER BY, LIMIT) built from scratch, on an Apple M1 Pro:

| | per query | |
|---|---|---|
| dexqu, built each time | ~2.2 µs | |
| dexqu, built once, bound per run | ~0.13 µs | `Query.bind().set…().args()` |
| goqu v9.19 (Go, prepared mode) | ~6.7 µs | 227 allocations |
| squirrel v1.5.4 (Go) | ~8.1 µs | 150 allocations |

The Go numbers are `go test -bench` on the same machine and the same query.
They are a reference point, not a contest between runtimes.

Where the time goes, and why:

- **One pass.** A query is a tree of nodes, rendered once into one
  `StringBuilder`. Placeholders are numbered as they are written, with no second
  pass rewriting `?` into `$n` the way squirrel's placeholder formats do.
- **No copying per method.** goqu clones its whole dataset on every call.
  Here a method allocates one list node and copies a struct of pointers.
- **No reflection and no boxing.** Values are a tagged union, so there is no
  `interface{}` to inspect at render time.
- **Lists are walked, not collected.** Clause lists render straight from the
  persistent list, with no intermediate arrays.
- **Templates skip all of it.** A query built once is a string and an array.

## How it compares

| | Strength | Weakness | What dexqu does |
|---|---|---|---|
| **goqu** (Go) | Broad dialect support; expression API; immutable datasets | Clones the dataset on every call; reflection and `interface{}` everywhere; interpolates values into the SQL unless you ask for prepared mode | Immutable at the cost of one node per call; parameters are the only mode |
| **squirrel** (Go) | Small and familiar | Raw strings for joins and ordering, so quoting is the caller's job; `?` rewritten to `$n` afterwards; immutability through a reflection-based map | Every name quoted; `orderBy` validated; placeholders numbered as written |
| **Knex** (JS) | Very wide feature set; migrations | Mutable builder, so reusing one leaks state into the next; identifiers in raw strings | Values, not mutation |
| **Kysely / Drizzle** (TS) | Column and result types checked against the schema | Needs a schema description kept in step with the database; heavy compile-time types | Not attempted yet. `fields()` derives the projection from the struct the rows are read into, the part of the type link that costs nothing |
| **jOOQ** (Java) | Generated from the live schema; nearly all of SQL | Code generation step; large runtime | Smaller scope; `raw`/`rawWith` for what is missing |
| **sqlc** (Go) | Zero runtime cost: SQL is checked and compiled ahead | Cannot build a query whose shape depends on input | `param()` templates give that zero-cost path for fixed queries, and the builder covers the dynamic ones |
| **sea-query / Diesel** (Rust) | AST rendered per backend; Diesel type-checks against the schema | Diesel's types are hard to compose dynamically | The same AST-then-render shape |

What dexqu adds that none of these default to:

- **A forgotten WHERE is an error.** UPDATE and DELETE without a condition
  need an explicit `all()`.
- **An empty IN is handled.** `in([])` renders `1 = 0` rather than the invalid
  `IN ()`, and `notIn([])` renders `1 = 1`.
- **One statement per query shape.** LIMIT and OFFSET are bound, so paging
  does not mint a new prepared statement per page.
- **Shared bases are safe across threads**, by construction rather than by
  convention.

Not done yet, in rough order of value: Postgres `= ANY($1)` for large IN lists
(needs array binding in `db`), `INSERT … SELECT` and DML inside CTEs, `UPDATE …
FROM` and `DELETE … USING`, `NULLS FIRST/LAST`, SQL Server, and a schema-checked
layer generated from the database.

## Shaped around the compiler

Each of these has a comment at its spot in `dexqu.dx`. All are worth removing
once the compiler is fixed:

- `from()` and `select()` set fields rather than call the methods of the same
  name. Inside a module, a method call named like one of the module's own
  functions resolves to the function.
- A `throw` of a borrowed string passes `"" + s`. A throw releases the string
  it is given as if it owned it, and the borrowed one is freed early.
- Rendering records an error and returns, and `build` throws it. A throw out
  of a method call on a struct local leaks that local's fields.
- `Binder` keeps its errors in an array it pushes to, not a string field it
  reassigns: a field written by a method on a temporary leaks when a later
  call in the same chain throws.
- `case Value.Int(v)`, not `case q.Value.Int(v)`. A qualified payload-variant
  pattern does not resolve through a module import.
- An enum that names a struct which names it back (`Node.Sub(Select)`) checks
  only when the file is read as a module, so `dex lint dexqu.dx` on its own
  reports unknown types. Lint the tests instead.
- `dex format` joins a chain written one `.method()` per line, then breaks it
  inside the first call's parentheses. Where a long chain comes out badly,
  build it in steps: `s = s.where(..)`. Written up as
  `dex-samples/problems/001-formatter-method-chains.md`.

## Running the tests

```sh
cd dexqu
dex run dexqu_test.dx     # rendering, binding, branching, threads
dex run sqldb_test.dx     # end to end on SQLite through db
```

Both pass under `DEX_MEM_CHECK=1 DEX_MEM_LEAKS=error` and report 0 leaks under
`leaks --atExit`. `dexqu_test` also passes under ASan, UBSan and TSan. On a
machine with DBngin's MySQL on `PATH`, run `sqldb_test` with that entry removed
(see `dex-samples/dx.sh`).
