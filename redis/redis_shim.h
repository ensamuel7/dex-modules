#ifndef REDIS_SHIM_H
#define REDIS_SHIM_H

#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int argc;
    int cap;
    char **argv;
} redis_shim_args;

static inline int redis_shim_failed(redisContext *c) {
    return c->err != 0;
}

static inline const char *redis_shim_error(redisContext *c) {
    return c->errstr;
}

static inline redis_shim_args *redis_shim_args_new(void) {
    return calloc(1, sizeof(redis_shim_args));
}

static inline void redis_shim_args_push(redis_shim_args *a, const char *s) {
    if (a->argc == a->cap) {
        a->cap = a->cap ? a->cap * 2 : 8;
        a->argv = realloc(a->argv, (size_t)a->cap * sizeof(char *));
    }
    a->argv[a->argc++] = strdup(s);
}

static inline void redis_shim_args_free(redis_shim_args *a) {
    for (int i = 0; i < a->argc; i++) {
        free(a->argv[i]);
    }
    free(a->argv);
    free(a);
}

static inline redisReply *redis_shim_args_call(redisContext *c, redis_shim_args *a) {
    return redisCommandArgv(c, a->argc, (const char **)a->argv, NULL);
}

static inline int redis_shim_type(redisReply *r) {
    return r->type;
}

static inline long long redis_shim_integer(redisReply *r) {
    return r->integer;
}

static inline const char *redis_shim_str(redisReply *r) {
    return r->str ? r->str : "";
}

static inline int redis_shim_count(redisReply *r) {
    return (int)r->elements;
}

static inline int redis_shim_item_type(redisReply *r, int i) {
    return r->element[i]->type;
}

static inline long long redis_shim_item_integer(redisReply *r, int i) {
    return r->element[i]->integer;
}

static inline const char *redis_shim_item_str(redisReply *r, int i) {
    const char *s = r->element[i]->str;
    return s ? s : "";
}

#endif
