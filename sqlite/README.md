# sqlite

SQLite for Dex, over libsqlite3.

```dex
import "github.com/ensamuel7/dex-modules/sqlite"

let db: sqlite.Database = sqlite.open("app.db")
db.exec("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT)")

let ins: sqlite.Statement = db.prepare("INSERT INTO users(name) VALUES (?)")
ins.bindText(1, "ada").run()

let q: sqlite.Statement = db.prepare("SELECT id, name FROM users WHERE id > ?")
q.bindInt(1, 0)
while (q.next()) {
    fmt.println(q.getText(1))
}
```

Every failure throws `sqlite.SqliteError`, carrying SQLite's message and
result code.

## API

| | |
|---|---|
| `sqlite.open(path): Database` | open or create a file |
| `sqlite.memory(): Database` | an in-memory database |
| `sqlite.version(): string` | the linked libsqlite3 version |
| `db.exec(sql)` | run SQL with no results; several statements allowed |
| `db.prepare(sql): Statement` | compile one statement |
| `db.queryInt(sql): int` | first column of the first row |
| `db.queryString(sql): string?` | same, `null` for no row or NULL |
| `db.changes(): int` | rows changed by the last statement |
| `db.lastInsertId(): long` | rowid of the last insert |
| `st.bindText / bindInt / bindLong / bindDouble (i, v)` | bind parameter `i`, counted from 1; chainable |
| `st.bindNull(i)` | bind NULL |
| `st.next(): bool` | step; `true` while there is a row |
| `st.run(): int` | step to the end, returning rows changed |
| `st.reset()` | rewind and clear bindings, to run it again |
| `st.columns(): string[]` | column names |
| `st.getText / getInt / getLong / getDouble (i)` | read column `i`, counted from 0 |
| `st.isNull(i): bool` | is column `i` NULL |

## Lifetime

`Database` and `Statement` hold C handles, closed when the last reference to
them goes. A statement keeps a reference to its connection, and the connection
is closed with `sqlite3_close_v2`, so it stays open until its last statement is
finalized whichever order they are released in.
