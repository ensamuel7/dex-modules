#ifndef SQLITE_SHIM_H
#define SQLITE_SHIM_H

#include <sqlite3.h>

static inline int sqlite_shim_exec(sqlite3 *db, const char *sql) {
    return sqlite3_exec(db, sql, 0, 0, 0);
}

static inline int sqlite_shim_bind_text(sqlite3_stmt *s, int i, const char *text) {
    return sqlite3_bind_text(s, i, text, -1, SQLITE_TRANSIENT);
}

static inline const char *sqlite_shim_column_text(sqlite3_stmt *s, int i) {
    const unsigned char *text = sqlite3_column_text(s, i);
    return text ? (const char *)text : "";
}

#endif
