#ifndef REDIS_SHIM_H
#define REDIS_SHIM_H

#include <hiredis/hiredis.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define REDIS_SHIM_MAX_POOL 64

typedef struct {
    int argc;
    int cap;
    char **argv;
} redis_shim_args;

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t freed;
    char *host;
    int port;
    char *password;
    int size;
    redisContext *conns[REDIS_SHIM_MAX_POOL];
    int busy[REDIS_SHIM_MAX_POOL];
} redis_shim_pool;

static _Thread_local char redis_shim_err[512];

static inline void redis_shim_set_error(const char *what, const char *detail) {
    snprintf(redis_shim_err, sizeof redis_shim_err, "%s%s%s", what, detail ? ": " : "", detail ? detail : "");
}

static inline const char *redis_shim_last_error(void) {
    return redis_shim_err;
}

static inline redisContext *redis_shim_dial(redis_shim_pool *p) {
    struct timeval timeout = {5, 0};
    redisContext *c = redisConnectWithTimeout(p->host, p->port, timeout);
    if (c == NULL) {
        redis_shim_set_error("out of memory connecting to", p->host);
        return NULL;
    }
    if (c->err) {
        redis_shim_set_error(c->errstr, NULL);
        redisFree(c);
        return NULL;
    }
    if (p->password[0] != '\0') {
        redisReply *r = redisCommand(c, "AUTH %s", p->password);
        if (r == NULL || r->type == REDIS_REPLY_ERROR) {
            redis_shim_set_error("AUTH failed", r ? r->str : c->errstr);
            if (r) {
                freeReplyObject(r);
            }
            redisFree(c);
            return NULL;
        }
        freeReplyObject(r);
    }
    redisReply *pong = redisCommand(c, "PING");
    if (pong == NULL || pong->type == REDIS_REPLY_ERROR) {
        redis_shim_set_error(pong ? pong->str : c->errstr, NULL);
        if (pong) {
            freeReplyObject(pong);
        }
        redisFree(c);
        return NULL;
    }
    freeReplyObject(pong);
    return c;
}

static inline void redis_shim_pool_free(redis_shim_pool *p) {
    for (int i = 0; i < p->size; i++) {
        if (p->conns[i]) {
            redisFree(p->conns[i]);
        }
    }
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->freed);
    free(p->host);
    free(p->password);
    free(p);
}

static inline redis_shim_pool *redis_shim_pool_new(const char *host, int port, const char *password, int size) {
    if (size < 1) {
        size = 1;
    }
    if (size > REDIS_SHIM_MAX_POOL) {
        size = REDIS_SHIM_MAX_POOL;
    }
    redis_shim_pool *p = calloc(1, sizeof(redis_shim_pool));
    if (p == NULL) {
        redis_shim_set_error("out of memory", NULL);
        return NULL;
    }
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->freed, NULL);
    p->host = strdup(host);
    p->port = port;
    p->password = strdup(password);
    p->size = size;
    p->conns[0] = redis_shim_dial(p);
    if (p->conns[0] == NULL) {
        redis_shim_pool_free(p);
        return NULL;
    }
    return p;
}

static inline int redis_shim_acquire(redis_shim_pool *p) {
    pthread_mutex_lock(&p->mu);
    for (;;) {
        for (int i = 0; i < p->size; i++) {
            if (!p->busy[i]) {
                p->busy[i] = 1;
                pthread_mutex_unlock(&p->mu);
                return i;
            }
        }
        pthread_cond_wait(&p->freed, &p->mu);
    }
}

static inline void redis_shim_release(redis_shim_pool *p, int i) {
    pthread_mutex_lock(&p->mu);
    p->busy[i] = 0;
    pthread_cond_signal(&p->freed);
    pthread_mutex_unlock(&p->mu);
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

static inline redisReply *redis_shim_send(redisContext *c, redis_shim_args *a) {
    redisReply *r = redisCommandArgv(c, a->argc, (const char **)a->argv, NULL);
    if (r == NULL) {
        redis_shim_set_error(c->errstr[0] ? c->errstr : "connection lost", NULL);
    }
    return r;
}

static inline redisReply *redis_shim_pool_call(redis_shim_pool *p, redis_shim_args *a) {
    int i = redis_shim_acquire(p);
    if (p->conns[i] && p->conns[i]->err) {
        redisFree(p->conns[i]);
        p->conns[i] = NULL;
    }
    if (p->conns[i] == NULL) {
        p->conns[i] = redis_shim_dial(p);
    }
    redisReply *r = NULL;
    if (p->conns[i]) {
        r = redis_shim_send(p->conns[i], a);
    }
    redis_shim_release(p, i);
    return r;
}

static inline redisContext *redis_shim_pool_dial(redis_shim_pool *p) {
    return redis_shim_dial(p);
}

static inline redisReply *redis_shim_next(redisContext *c) {
    void *r = NULL;
    if (redisGetReply(c, &r) != REDIS_OK) {
        redis_shim_set_error(c->errstr[0] ? c->errstr : "connection lost", NULL);
        return NULL;
    }
    return r;
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
