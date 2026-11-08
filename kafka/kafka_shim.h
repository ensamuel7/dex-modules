#ifndef KAFKA_SHIM_H
#define KAFKA_SHIM_H

// What Dex cannot say to librdkafka directly: C callbacks, struct fields and
// varargs.
//
// Dex cannot be called back from C (a throw is a longjmp, and it would cross
// the C frames), so every librdkafka callback lands here and leaves its result
// where Dex can pull it. A producer has a poller thread of its own serving
// them; a consumer's are served by rd_kafka_consumer_poll on the caller's thread.

#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#define KAFKA_SHIM_PRODUCER 0
#define KAFKA_SHIM_CONSUMER 1
#define KAFKA_SHIM_ADMIN 2

// The newest errors and rebalances are kept, up to this many of each, for a
// program that reads them late or never.
#define KAFKA_SHIM_RING 64

#define KAFKA_SHIM_REBALANCE_ASSIGNED 0
#define KAFKA_SHIM_REBALANCE_REVOKED 1
#define KAFKA_SHIM_REBALANCE_LOST 2

// ---- The last error on this thread ----

static _Thread_local struct {
    char message[512];
    int fatal;
    int retriable;
    int abortable;
} kafka_shim_last;

static inline int kafka_shim_fail_text(int code, const char *message) {
    snprintf(kafka_shim_last.message, sizeof kafka_shim_last.message, "%s", message ? message : "");
    kafka_shim_last.fatal = 0;
    kafka_shim_last.abortable = 0;
    switch (code) {
    case RD_KAFKA_RESP_ERR__QUEUE_FULL:
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
    case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
    case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS:
    case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
        kafka_shim_last.retriable = 1;
        break;
    default:
        kafka_shim_last.retriable = 0;
    }
    return code;
}

static inline int kafka_shim_fail_code(rd_kafka_t *rk, int code) {
    if (code == RD_KAFKA_RESP_ERR__FATAL && rk != NULL) {
        char reason[256];
        int original = rd_kafka_fatal_error(rk, reason, sizeof reason);
        kafka_shim_fail_text(original, reason);
        kafka_shim_last.fatal = 1;
        return original;
    }
    return kafka_shim_fail_text(code, rd_kafka_err2str((rd_kafka_resp_err_t)code));
}

// Takes the error object over, and returns 0 for none.
static inline int kafka_shim_fail_error(rd_kafka_error_t *e) {
    if (e == NULL) {
        return 0;
    }
    int code = rd_kafka_error_code(e);
    kafka_shim_fail_text(code, rd_kafka_error_string(e));
    kafka_shim_last.fatal = rd_kafka_error_is_fatal(e);
    kafka_shim_last.retriable = rd_kafka_error_is_retriable(e);
    kafka_shim_last.abortable = rd_kafka_error_txn_requires_abort(e);
    rd_kafka_error_destroy(e);
    return code;
}

static inline const char *kafka_shim_last_message(void) {
    return kafka_shim_last.message;
}

static inline bool kafka_shim_last_fatal(void) {
    return kafka_shim_last.fatal != 0;
}

static inline bool kafka_shim_last_retriable(void) {
    return kafka_shim_last.retriable != 0;
}

static inline bool kafka_shim_last_abortable(void) {
    return kafka_shim_last.abortable != 0;
}

static inline const char *kafka_shim_err_name(int code) {
    return rd_kafka_err2name((rd_kafka_resp_err_t)code);
}

static inline const char *kafka_shim_err_text(int code) {
    return rd_kafka_err2str((rd_kafka_resp_err_t)code);
}

// ---- Delivery ----

#define KAFKA_SHIM_OP_TICKET 0
#define KAFKA_SHIM_OP_TRACKED 1

// What a message carries to its delivery report: a thread waiting on it, or a
// tag to report under. A message nobody asked about carries nothing and costs
// no allocation.
typedef struct {
    int kind;
    long tag;
    pthread_mutex_t mu;
    pthread_cond_t cond;
    int done;
    int err;
    char reason[256];
    int partition;
    long offset;
    long timestamp;
    int status;
} kafka_shim_op;

typedef struct kafka_shim_report {
    struct kafka_shim_report *next;
    long tag;
    int err;
    char *reason;
    char *topic;
    int partition;
    long offset;
    long timestamp;
    int status;
} kafka_shim_report;

// ---- Conf ----

typedef struct {
    rd_kafka_conf_t *conf;
} kafka_shim_conf;

static inline kafka_shim_conf *kafka_shim_conf_new(void) {
    kafka_shim_conf *c = malloc(sizeof *c);
    if (c != NULL) {
        c->conf = rd_kafka_conf_new();
    }
    return c;
}

static inline void kafka_shim_conf_free(kafka_shim_conf *c) {
    if (c->conf != NULL) {
        rd_kafka_conf_destroy(c->conf);
    }
    free(c);
}

static inline int kafka_shim_conf_set(kafka_shim_conf *c, const char *key, const char *value) {
    char reason[256];
    if (rd_kafka_conf_set(c->conf, key, value, reason, sizeof reason) != RD_KAFKA_CONF_OK) {
        return kafka_shim_fail_text(RD_KAFKA_RESP_ERR__INVALID_ARG, reason);
    }
    return 0;
}

// ---- Client ----

typedef struct {
    int code;
    char *reason;
} kafka_shim_client_error;

typedef struct {
    int kind;
    rd_kafka_topic_partition_list_t *partitions;
} kafka_shim_rebalance;

typedef struct {
    rd_kafka_t *rk;
    int type;
    atomic_int closed;
    atomic_llong delivered;
    atomic_llong failed;

    pthread_t poller;
    atomic_int stop;
    int has_poller;

    rd_kafka_queue_t *consumer_queue;

    pthread_mutex_t mu;
    pthread_cond_t report_ready;
    char *stats;
    kafka_shim_report *reports_head;
    kafka_shim_report *reports_tail;
    kafka_shim_client_error errors[KAFKA_SHIM_RING];
    int errors_len;
    kafka_shim_rebalance rebalances[KAFKA_SHIM_RING];
    int rebalances_len;
} kafka_shim_client;

static void kafka_shim_record_error(kafka_shim_client *c, int code, const char *reason) {
    pthread_mutex_lock(&c->mu);
    if (c->errors_len == KAFKA_SHIM_RING) {
        free(c->errors[0].reason);
        memmove(&c->errors[0], &c->errors[1], sizeof(c->errors[0]) * (KAFKA_SHIM_RING - 1));
        c->errors_len--;
    }
    c->errors[c->errors_len].code = code;
    c->errors[c->errors_len].reason = strdup(reason ? reason : "");
    c->errors_len++;
    pthread_mutex_unlock(&c->mu);
}

static void kafka_shim_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    (void)rk;
    kafka_shim_record_error(opaque, err, reason);
}

static int kafka_shim_stats_cb(rd_kafka_t *rk, char *json, size_t len, void *opaque) {
    (void)rk;
    (void)len;
    kafka_shim_client *c = opaque;
    char *copy = strdup(json);
    pthread_mutex_lock(&c->mu);
    free(c->stats);
    c->stats = copy;
    pthread_mutex_unlock(&c->mu);
    return 0;
}

static void kafka_shim_dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *m, void *opaque) {
    (void)rk;
    kafka_shim_client *c = opaque;
    if (m->err) {
        atomic_fetch_add(&c->failed, 1);
    } else {
        atomic_fetch_add(&c->delivered, 1);
    }

    kafka_shim_op *op = m->_private;
    if (op == NULL) {
        if (m->err) {
            char reason[512];
            snprintf(reason, sizeof reason, "delivery to %s failed: %s", rd_kafka_topic_name(m->rkt), rd_kafka_err2str(m->err));
            kafka_shim_record_error(c, m->err, reason);
        }
        return;
    }

    rd_kafka_timestamp_type_t tstype;
    long timestamp = (long)rd_kafka_message_timestamp(m, &tstype);
    int status = (int)rd_kafka_message_status(m);

    if (op->kind == KAFKA_SHIM_OP_TICKET) {
        pthread_mutex_lock(&op->mu);
        op->err = m->err;
        snprintf(op->reason, sizeof op->reason, "%s", m->err ? rd_kafka_err2str(m->err) : "");
        op->partition = m->partition;
        op->offset = (long)m->offset;
        op->timestamp = timestamp;
        op->status = status;
        op->done = 1;
        pthread_cond_signal(&op->cond);
        pthread_mutex_unlock(&op->mu);
        return;
    }

    kafka_shim_report *r = malloc(sizeof *r);
    if (r != NULL) {
        r->next = NULL;
        r->tag = op->tag;
        r->err = m->err;
        r->reason = strdup(m->err ? rd_kafka_err2str(m->err) : "");
        r->topic = strdup(rd_kafka_topic_name(m->rkt));
        r->partition = m->partition;
        r->offset = (long)m->offset;
        r->timestamp = timestamp;
        r->status = status;
        pthread_mutex_lock(&c->mu);
        if (c->reports_tail != NULL) {
            c->reports_tail->next = r;
        } else {
            c->reports_head = r;
        }
        c->reports_tail = r;
        pthread_cond_signal(&c->report_ready);
        pthread_mutex_unlock(&c->mu);
    }
    free(op);
}

static void kafka_shim_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    kafka_shim_client *c = opaque;
    int cooperative = strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE") == 0;
    int kind = KAFKA_SHIM_REBALANCE_ASSIGNED;
    int code = 0;

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        if (cooperative) {
            code = kafka_shim_fail_error(rd_kafka_incremental_assign(rk, partitions));
        } else {
            code = rd_kafka_assign(rk, partitions);
        }
    } else {
        kind = rd_kafka_assignment_lost(rk) ? KAFKA_SHIM_REBALANCE_LOST : KAFKA_SHIM_REBALANCE_REVOKED;
        if (err != RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
            kafka_shim_record_error(c, err, rd_kafka_err2str(err));
        }
        if (cooperative) {
            code = kafka_shim_fail_error(rd_kafka_incremental_unassign(rk, partitions));
        } else {
            code = rd_kafka_assign(rk, NULL);
        }
    }
    if (code != 0) {
        kafka_shim_record_error(c, code, rd_kafka_err2str((rd_kafka_resp_err_t)code));
    }

    rd_kafka_topic_partition_list_t *copy = rd_kafka_topic_partition_list_copy(partitions);
    pthread_mutex_lock(&c->mu);
    if (c->rebalances_len == KAFKA_SHIM_RING) {
        rd_kafka_topic_partition_list_destroy(c->rebalances[0].partitions);
        memmove(&c->rebalances[0], &c->rebalances[1], sizeof(c->rebalances[0]) * (KAFKA_SHIM_RING - 1));
        c->rebalances_len--;
    }
    c->rebalances[c->rebalances_len].kind = kind;
    c->rebalances[c->rebalances_len].partitions = copy;
    c->rebalances_len++;
    pthread_mutex_unlock(&c->mu);
}

static void kafka_shim_commit_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *offsets, void *opaque) {
    (void)rk;
    (void)offsets;
    if (err != 0 && err != RD_KAFKA_RESP_ERR__NO_OFFSET) {
        char reason[512];
        snprintf(reason, sizeof reason, "commit failed: %s", rd_kafka_err2str(err));
        kafka_shim_record_error(opaque, err, reason);
    }
}

static void *kafka_shim_poller(void *arg) {
    kafka_shim_client *c = arg;
    while (!atomic_load(&c->stop)) {
        rd_kafka_poll(c->rk, 100);
    }
    return NULL;
}

// Takes conf over on success. On failure conf stays with the caller and the
// reason is the last error.
static inline kafka_shim_client *kafka_shim_client_new(kafka_shim_conf *conf, int type) {
    kafka_shim_client *c = calloc(1, sizeof *c);
    if (c == NULL) {
        kafka_shim_fail_text(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "out of memory");
        return NULL;
    }
    c->type = type;
    pthread_mutex_init(&c->mu, NULL);
    pthread_cond_init(&c->report_ready, NULL);

    rd_kafka_conf_set_opaque(conf->conf, c);
    rd_kafka_conf_set_error_cb(conf->conf, kafka_shim_error_cb);
    rd_kafka_conf_set_stats_cb(conf->conf, kafka_shim_stats_cb);
    if (type == KAFKA_SHIM_PRODUCER) {
        rd_kafka_conf_set_dr_msg_cb(conf->conf, kafka_shim_dr_cb);
    } else if (type == KAFKA_SHIM_CONSUMER) {
        rd_kafka_conf_set_rebalance_cb(conf->conf, kafka_shim_rebalance_cb);
        rd_kafka_conf_set_offset_commit_cb(conf->conf, kafka_shim_commit_cb);
    }

    char reason[512];
    rd_kafka_type_t rktype = type == KAFKA_SHIM_CONSUMER ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER;
    c->rk = rd_kafka_new(rktype, conf->conf, reason, sizeof reason);
    if (c->rk == NULL) {
        kafka_shim_fail_text(RD_KAFKA_RESP_ERR__INVALID_ARG, reason);
        pthread_cond_destroy(&c->report_ready);
        pthread_mutex_destroy(&c->mu);
        free(c);
        return NULL;
    }
    conf->conf = NULL;

    if (type == KAFKA_SHIM_CONSUMER) {
        rd_kafka_poll_set_consumer(c->rk);
        c->consumer_queue = rd_kafka_queue_get_consumer(c->rk);
    } else {
        if (pthread_create(&c->poller, NULL, kafka_shim_poller, c) != 0) {
            kafka_shim_fail_text(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "could not start the poller thread");
            rd_kafka_destroy(c->rk);
            pthread_cond_destroy(&c->report_ready);
            pthread_mutex_destroy(&c->mu);
            free(c);
            return NULL;
        }
        c->has_poller = 1;
    }
    return c;
}

static inline bool kafka_shim_is_closed(kafka_shim_client *c) {
    return atomic_load(&c->closed) != 0;
}

static inline int kafka_shim_closed_error(kafka_shim_client *c) {
    return kafka_shim_fail_text(RD_KAFKA_RESP_ERR__STATE, c->type == KAFKA_SHIM_CONSUMER ? "the consumer is closed" : "the producer is closed");
}

// Flushes for up to timeoutMs, then fails what is left, so that every thread
// waiting on a delivery is released. Returns how many messages did not make it.
static inline int kafka_shim_producer_close(kafka_shim_client *c, int timeoutMs) {
    if (atomic_exchange(&c->closed, 1)) {
        return 0;
    }
    rd_kafka_flush(c->rk, timeoutMs);
    int left = rd_kafka_outq_len(c->rk);
    if (left > 0) {
        rd_kafka_purge(c->rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
        rd_kafka_flush(c->rk, 1000);
    }
    return left;
}

static inline int kafka_shim_consumer_close(kafka_shim_client *c) {
    if (atomic_exchange(&c->closed, 1)) {
        return 0;
    }
    int err = rd_kafka_consumer_close(c->rk);
    return err == 0 ? 0 : kafka_shim_fail_code(c->rk, err);
}

// The closing function of a client handle, run when its last reference goes.
static inline void kafka_shim_client_free(kafka_shim_client *c) {
    if (c->type == KAFKA_SHIM_CONSUMER) {
        kafka_shim_consumer_close(c);
        // NULL when the client has no group.
        if (c->consumer_queue != NULL) {
            rd_kafka_queue_destroy(c->consumer_queue);
        }
    } else {
        kafka_shim_producer_close(c, c->type == KAFKA_SHIM_PRODUCER ? 5000 : 0);
    }
    if (c->has_poller) {
        atomic_store(&c->stop, 1);
        rd_kafka_yield(c->rk);
        pthread_join(c->poller, NULL);
    }
    rd_kafka_destroy(c->rk);

    while (c->reports_head != NULL) {
        kafka_shim_report *r = c->reports_head;
        c->reports_head = r->next;
        free(r->reason);
        free(r->topic);
        free(r);
    }
    for (int i = 0; i < c->errors_len; i++) {
        free(c->errors[i].reason);
    }
    for (int i = 0; i < c->rebalances_len; i++) {
        rd_kafka_topic_partition_list_destroy(c->rebalances[i].partitions);
    }
    free(c->stats);
    pthread_cond_destroy(&c->report_ready);
    pthread_mutex_destroy(&c->mu);
    free(c);
}

static inline long kafka_shim_delivered(kafka_shim_client *c) {
    return (long)atomic_load(&c->delivered);
}

static inline long kafka_shim_failed(kafka_shim_client *c) {
    return (long)atomic_load(&c->failed);
}

static inline int kafka_shim_queued(kafka_shim_client *c) {
    return rd_kafka_outq_len(c->rk);
}

static inline int kafka_shim_flush(kafka_shim_client *c, int timeoutMs) {
    rd_kafka_flush(c->rk, timeoutMs);
    return rd_kafka_outq_len(c->rk);
}

static inline const char *kafka_shim_lib_version(void) {
    return rd_kafka_version_str();
}

// The newest statistics JSON, "" before the first (statistics.interval.ms).
// Returned owned, because the poller may replace it at any moment.
static inline char *kafka_shim_stats(kafka_shim_client *c) {
    pthread_mutex_lock(&c->mu);
    char *copy = strdup(c->stats ? c->stats : "");
    pthread_mutex_unlock(&c->mu);
    return copy;
}

// Errors and rebalances are read oldest first: *_peek says whether there is
// one, and *_take removes it.
static inline bool kafka_shim_error_peek(kafka_shim_client *c) {
    pthread_mutex_lock(&c->mu);
    bool any = c->errors_len > 0;
    pthread_mutex_unlock(&c->mu);
    return any;
}

static inline int kafka_shim_error_take(kafka_shim_client *c) {
    pthread_mutex_lock(&c->mu);
    int code = 0;
    if (c->errors_len > 0) {
        code = c->errors[0].code;
        kafka_shim_fail_text(code, c->errors[0].reason);
        free(c->errors[0].reason);
        memmove(&c->errors[0], &c->errors[1], sizeof(c->errors[0]) * (size_t)(c->errors_len - 1));
        c->errors_len--;
    }
    pthread_mutex_unlock(&c->mu);
    return code;
}

// ---- Producing ----

static inline rd_kafka_headers_t *kafka_shim_headers_new(int n) {
    return rd_kafka_headers_new((size_t)n);
}

static inline void kafka_shim_headers_add(rd_kafka_headers_t *h, const char *name, const char *value, int len) {
    rd_kafka_header_add(h, name, -1, value, (ssize_t)len);
}

// keyLen or valueLen of -1 sends NULL, which differs from empty: a NULL value
// is the tombstone that deletes a key from a compacted topic. A timestamp of 0
// means now. The headers are copied, since librdkafka takes what it is given
// and the handle stays with Dex.
static inline int kafka_shim_send(kafka_shim_client *c, const char *topic, int partition, const char *key, int keyLen, const char *value, int valueLen, long timestamp, rd_kafka_headers_t *headers, int block, void *op) {
    if (atomic_load(&c->closed)) {
        return kafka_shim_closed_error(c);
    }
    rd_kafka_headers_t *mine = headers ? rd_kafka_headers_copy(headers) : NULL;
    int flags = RD_KAFKA_MSG_F_COPY | (block ? RD_KAFKA_MSG_F_BLOCK : 0);
    // Computed first: these macros cast their argument unparenthesised.
    void *keyBytes = keyLen < 0 ? NULL : (void *)key;
    size_t keySize = keyLen < 0 ? 0 : (size_t)keyLen;
    void *valueBytes = valueLen < 0 ? NULL : (void *)value;
    size_t valueSize = valueLen < 0 ? 0 : (size_t)valueLen;
    rd_kafka_resp_err_t err = rd_kafka_producev(c->rk,
        RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_PARTITION(partition),
        RD_KAFKA_V_MSGFLAGS(flags),
        RD_KAFKA_V_KEY(keyBytes, keySize),
        RD_KAFKA_V_VALUE(valueBytes, valueSize),
        RD_KAFKA_V_TIMESTAMP((int64_t)timestamp),
        RD_KAFKA_V_HEADERS(mine),
        RD_KAFKA_V_OPAQUE(op),
        RD_KAFKA_V_END);
    if (err) {
        if (mine != NULL) {
            rd_kafka_headers_destroy(mine);
        }
        return kafka_shim_fail_code(c->rk, err);
    }
    return 0;
}

static inline int kafka_shim_produce(kafka_shim_client *c, const char *topic, int partition, const char *key, int keyLen, const char *value, int valueLen, long timestamp, rd_kafka_headers_t *headers, bool block) {
    return kafka_shim_send(c, topic, partition, key, keyLen, value, valueLen, timestamp, headers, block, NULL);
}

static inline int kafka_shim_produce_tracked(kafka_shim_client *c, const char *topic, int partition, const char *key, int keyLen, const char *value, int valueLen, long timestamp, rd_kafka_headers_t *headers, long tag) {
    kafka_shim_op *op = malloc(sizeof *op);
    if (op == NULL) {
        return kafka_shim_fail_text(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "out of memory");
    }
    op->kind = KAFKA_SHIM_OP_TRACKED;
    op->tag = tag;
    int err = kafka_shim_send(c, topic, partition, key, keyLen, value, valueLen, timestamp, headers, 1, op);
    if (err) {
        free(op);
    }
    return err;
}

// Waits for this message's own report. message.timeout.ms bounds the wait, and
// closing the producer fails anything still queued, so it always ends.
static inline int kafka_shim_produce_sync(kafka_shim_client *c, const char *topic, int partition, const char *key, int keyLen, const char *value, int valueLen, long timestamp, rd_kafka_headers_t *headers, int *outPartition, long *outOffset, long *outTimestamp, int *outStatus) {
    kafka_shim_op op;
    op.kind = KAFKA_SHIM_OP_TICKET;
    op.done = 0;
    pthread_mutex_init(&op.mu, NULL);
    pthread_cond_init(&op.cond, NULL);
    int err = kafka_shim_send(c, topic, partition, key, keyLen, value, valueLen, timestamp, headers, 1, &op);
    if (err == 0) {
        pthread_mutex_lock(&op.mu);
        while (!op.done) {
            pthread_cond_wait(&op.cond, &op.mu);
        }
        pthread_mutex_unlock(&op.mu);
        *outPartition = op.partition;
        *outOffset = op.offset;
        *outTimestamp = op.timestamp;
        *outStatus = op.status;
        err = op.err;
        if (err != 0) {
            kafka_shim_fail_text(err, op.reason);
        }
    }
    pthread_cond_destroy(&op.cond);
    pthread_mutex_destroy(&op.mu);
    return err;
}

// The next report of a tracked message, waiting up to timeoutMs. NULL if none came.
static inline kafka_shim_report *kafka_shim_report_next(kafka_shim_client *c, int timeoutMs) {
    struct timeval now;
    gettimeofday(&now, NULL);
    struct timespec until;
    long nanos = now.tv_usec * 1000L + (timeoutMs % 1000) * 1000000L;
    until.tv_sec = now.tv_sec + timeoutMs / 1000 + nanos / 1000000000L;
    until.tv_nsec = nanos % 1000000000L;

    pthread_mutex_lock(&c->mu);
    while (c->reports_head == NULL) {
        if (pthread_cond_timedwait(&c->report_ready, &c->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    kafka_shim_report *r = c->reports_head;
    if (r != NULL) {
        c->reports_head = r->next;
        if (c->reports_head == NULL) {
            c->reports_tail = NULL;
        }
    }
    pthread_mutex_unlock(&c->mu);
    return r;
}

static inline void kafka_shim_report_free(kafka_shim_report *r) {
    free(r->reason);
    free(r->topic);
    free(r);
}

static inline long kafka_shim_report_tag(kafka_shim_report *r) { return r->tag; }
static inline int kafka_shim_report_err(kafka_shim_report *r) { return r->err; }
static inline const char *kafka_shim_report_reason(kafka_shim_report *r) { return r->reason; }
static inline const char *kafka_shim_report_topic(kafka_shim_report *r) { return r->topic; }
static inline int kafka_shim_report_partition(kafka_shim_report *r) { return r->partition; }
static inline long kafka_shim_report_offset(kafka_shim_report *r) { return r->offset; }
static inline long kafka_shim_report_timestamp(kafka_shim_report *r) { return r->timestamp; }
static inline int kafka_shim_report_status(kafka_shim_report *r) { return r->status; }

// ---- Transactions ----

static inline int kafka_shim_txn_init(kafka_shim_client *c, int timeoutMs) {
    return kafka_shim_fail_error(rd_kafka_init_transactions(c->rk, timeoutMs));
}

static inline int kafka_shim_txn_begin(kafka_shim_client *c) {
    return kafka_shim_fail_error(rd_kafka_begin_transaction(c->rk));
}

static inline int kafka_shim_txn_commit(kafka_shim_client *c, int timeoutMs) {
    return kafka_shim_fail_error(rd_kafka_commit_transaction(c->rk, timeoutMs));
}

static inline int kafka_shim_txn_abort(kafka_shim_client *c, int timeoutMs) {
    return kafka_shim_fail_error(rd_kafka_abort_transaction(c->rk, timeoutMs));
}

// Adds a consumer's current positions to the producer's open transaction, which
// is the step that makes consume-transform-produce exactly once.
static inline int kafka_shim_txn_send_offsets(kafka_shim_client *p, kafka_shim_client *consumer, int timeoutMs) {
    rd_kafka_topic_partition_list_t *assigned = NULL;
    rd_kafka_resp_err_t err = rd_kafka_assignment(consumer->rk, &assigned);
    if (err) {
        return kafka_shim_fail_code(consumer->rk, err);
    }
    if (assigned->cnt == 0) {
        rd_kafka_topic_partition_list_destroy(assigned);
        return 0;
    }
    rd_kafka_position(consumer->rk, assigned);
    rd_kafka_consumer_group_metadata_t *group = rd_kafka_consumer_group_metadata(consumer->rk);
    int code = kafka_shim_fail_error(rd_kafka_send_offsets_to_transaction(p->rk, assigned, group, timeoutMs));
    rd_kafka_consumer_group_metadata_destroy(group);
    rd_kafka_topic_partition_list_destroy(assigned);
    return code;
}

// ---- Partition lists ----

static inline rd_kafka_topic_partition_list_t *kafka_shim_list_new(int n) {
    return rd_kafka_topic_partition_list_new(n);
}

static inline void kafka_shim_list_add(rd_kafka_topic_partition_list_t *l, const char *topic, int partition, long offset) {
    rd_kafka_topic_partition_t *p = rd_kafka_topic_partition_list_add(l, topic, partition);
    p->offset = offset;
}

static inline int kafka_shim_list_count(rd_kafka_topic_partition_list_t *l) { return l->cnt; }
static inline const char *kafka_shim_list_topic(rd_kafka_topic_partition_list_t *l, int i) { return l->elems[i].topic; }
static inline int kafka_shim_list_partition(rd_kafka_topic_partition_list_t *l, int i) { return l->elems[i].partition; }
static inline long kafka_shim_list_offset(rd_kafka_topic_partition_list_t *l, int i) { return (long)l->elems[i].offset; }
static inline int kafka_shim_list_err(rd_kafka_topic_partition_list_t *l, int i) { return l->elems[i].err; }

// A call on a list can succeed as a whole and fail for one partition of it.
static inline int kafka_shim_list_check(rd_kafka_topic_partition_list_t *l) {
    for (int i = 0; i < l->cnt; i++) {
        if (l->elems[i].err != 0) {
            char reason[512];
            snprintf(reason, sizeof reason, "%s [%d]: %s", l->elems[i].topic, l->elems[i].partition, rd_kafka_err2str(l->elems[i].err));
            return kafka_shim_fail_text(l->elems[i].err, reason);
        }
    }
    return 0;
}

// ---- Consuming ----

static inline int kafka_shim_subscribe(kafka_shim_client *c, rd_kafka_topic_partition_list_t *topics) {
    rd_kafka_resp_err_t err = rd_kafka_subscribe(c->rk, topics);
    return err ? kafka_shim_fail_code(c->rk, err) : 0;
}

static inline int kafka_shim_unsubscribe(kafka_shim_client *c) {
    rd_kafka_resp_err_t err = rd_kafka_unsubscribe(c->rk);
    return err ? kafka_shim_fail_code(c->rk, err) : 0;
}

static inline rd_kafka_topic_partition_list_t *kafka_shim_subscription(kafka_shim_client *c) {
    rd_kafka_topic_partition_list_t *l = NULL;
    rd_kafka_resp_err_t err = rd_kafka_subscription(c->rk, &l);
    if (err) {
        kafka_shim_fail_code(c->rk, err);
        return NULL;
    }
    return l;
}

static inline rd_kafka_topic_partition_list_t *kafka_shim_assignment(kafka_shim_client *c) {
    rd_kafka_topic_partition_list_t *l = NULL;
    rd_kafka_resp_err_t err = rd_kafka_assignment(c->rk, &l);
    if (err) {
        kafka_shim_fail_code(c->rk, err);
        return NULL;
    }
    return l;
}

static inline int kafka_shim_assign(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l) {
    rd_kafka_resp_err_t err = rd_kafka_assign(c->rk, l);
    return err ? kafka_shim_fail_code(c->rk, err) : 0;
}

static inline int kafka_shim_commit_list(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l, bool async) {
    rd_kafka_resp_err_t err = rd_kafka_commit(c->rk, l, async ? 1 : 0);
    if (err == RD_KAFKA_RESP_ERR__NO_OFFSET) {
        return 0;
    }
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return async ? 0 : kafka_shim_list_check(l);
}

// Everything stored so far, which by default is everything polled.
static inline int kafka_shim_commit_stored(kafka_shim_client *c, bool async) {
    rd_kafka_resp_err_t err = rd_kafka_commit(c->rk, NULL, async ? 1 : 0);
    if (err == RD_KAFKA_RESP_ERR__NO_OFFSET) {
        return 0;
    }
    return err ? kafka_shim_fail_code(c->rk, err) : 0;
}

static inline int kafka_shim_store(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l) {
    rd_kafka_resp_err_t err = rd_kafka_offsets_store(c->rk, l);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_seek(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l, int timeoutMs) {
    rd_kafka_error_t *e = rd_kafka_seek_partitions(c->rk, l, timeoutMs);
    if (e != NULL) {
        return kafka_shim_fail_error(e);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_pause(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l) {
    rd_kafka_resp_err_t err = rd_kafka_pause_partitions(c->rk, l);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_resume(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l) {
    rd_kafka_resp_err_t err = rd_kafka_resume_partitions(c->rk, l);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_position(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l) {
    rd_kafka_resp_err_t err = rd_kafka_position(c->rk, l);
    return err ? kafka_shim_fail_code(c->rk, err) : 0;
}

static inline int kafka_shim_committed(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l, int timeoutMs) {
    rd_kafka_resp_err_t err = rd_kafka_committed(c->rk, l, timeoutMs);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_offsets_for_times(kafka_shim_client *c, rd_kafka_topic_partition_list_t *l, int timeoutMs) {
    rd_kafka_resp_err_t err = rd_kafka_offsets_for_times(c->rk, l, timeoutMs);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    return kafka_shim_list_check(l);
}

static inline int kafka_shim_watermarks(kafka_shim_client *c, const char *topic, int partition, long *low, long *high, int timeoutMs) {
    int64_t lo = 0;
    int64_t hi = 0;
    rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(c->rk, topic, partition, &lo, &hi, timeoutMs);
    if (err) {
        return kafka_shim_fail_code(c->rk, err);
    }
    *low = (long)lo;
    *high = (long)hi;
    return 0;
}

// Makes a poll in progress on another thread return at once.
static inline void kafka_shim_wake(kafka_shim_client *c) {
    if (c->consumer_queue != NULL) {
        rd_kafka_queue_yield(c->consumer_queue);
    }
}

// Returned owned: rd_kafka_memberid's buffer has to go back through
// rd_kafka_mem_free, which takes two arguments and so cannot be a `freed by`.
static inline char *kafka_shim_member_id(kafka_shim_client *c) {
    char *id = rd_kafka_memberid(c->rk);
    char *copy = strdup(id ? id : "");
    if (id != NULL) {
        rd_kafka_mem_free(c->rk, id);
    }
    return copy;
}

static inline bool kafka_shim_rebalance_peek(kafka_shim_client *c) {
    pthread_mutex_lock(&c->mu);
    bool any = c->rebalances_len > 0;
    pthread_mutex_unlock(&c->mu);
    return any;
}

// The oldest rebalance's partitions, with its kind written to *kind.
static inline rd_kafka_topic_partition_list_t *kafka_shim_rebalance_take(kafka_shim_client *c, int *kind) {
    rd_kafka_topic_partition_list_t *l = NULL;
    pthread_mutex_lock(&c->mu);
    if (c->rebalances_len > 0) {
        l = c->rebalances[0].partitions;
        *kind = c->rebalances[0].kind;
        memmove(&c->rebalances[0], &c->rebalances[1], sizeof(c->rebalances[0]) * (size_t)(c->rebalances_len - 1));
        c->rebalances_len--;
    }
    pthread_mutex_unlock(&c->mu);
    return l;
}

// ---- A polled message ----

// Parts of a message, as Dex reads them.
#define KAFKA_SHIM_PART_KEY 0
#define KAFKA_SHIM_PART_VALUE 1
#define KAFKA_SHIM_PART_HEADER 2 // value of header i is part 2 + i

typedef struct {
    rd_kafka_message_t *m;
    rd_kafka_headers_t *headers; // owned by m
} kafka_shim_msg;

static inline kafka_shim_msg *kafka_shim_poll(kafka_shim_client *c, int timeoutMs) {
    rd_kafka_message_t *m = rd_kafka_consumer_poll(c->rk, timeoutMs);
    if (m == NULL) {
        return NULL;
    }
    kafka_shim_msg *out = malloc(sizeof *out);
    if (out == NULL) {
        rd_kafka_message_destroy(m);
        return NULL;
    }
    out->m = m;
    out->headers = NULL;
    if (m->err == 0) {
        rd_kafka_message_headers(m, &out->headers);
    }
    return out;
}

static inline void kafka_shim_msg_free(kafka_shim_msg *m) {
    rd_kafka_message_destroy(m->m);
    free(m);
}

static inline int kafka_shim_msg_err(kafka_shim_msg *m) { return m->m->err; }

static inline int kafka_shim_msg_fail(kafka_shim_client *c, kafka_shim_msg *m) {
    if (m->m->err == RD_KAFKA_RESP_ERR__FATAL) {
        return kafka_shim_fail_code(c->rk, m->m->err);
    }
    return kafka_shim_fail_text(m->m->err, rd_kafka_message_errstr(m->m));
}

static inline const char *kafka_shim_msg_topic(kafka_shim_msg *m) {
    return m->m->rkt ? rd_kafka_topic_name(m->m->rkt) : "";
}

static inline int kafka_shim_msg_partition(kafka_shim_msg *m) { return m->m->partition; }
static inline long kafka_shim_msg_offset(kafka_shim_msg *m) { return (long)m->m->offset; }

static inline long kafka_shim_msg_timestamp(kafka_shim_msg *m) {
    rd_kafka_timestamp_type_t type;
    return (long)rd_kafka_message_timestamp(m->m, &type);
}

static inline int kafka_shim_msg_header_count(kafka_shim_msg *m) {
    return m->headers ? (int)rd_kafka_header_cnt(m->headers) : 0;
}

static inline const char *kafka_shim_msg_header_name(kafka_shim_msg *m, int i) {
    const char *name = "";
    const void *value = NULL;
    size_t size = 0;
    rd_kafka_header_get_all(m->headers, (size_t)i, &name, &value, &size);
    return name;
}

// A part's bytes, in librdkafka's own buffer, with their count written to *n:
// -1 for NULL, which Dex reads as "" and which is how a missing key and a
// tombstone's value differ from empty ones. Dex copies exactly *n bytes, so a
// NUL anywhere in them is kept.
static inline const char *kafka_shim_part_bytes(kafka_shim_msg *m, int part, long *n) {
    const char *ptr;
    size_t len;
    if (part == KAFKA_SHIM_PART_KEY) {
        ptr = m->m->key;
        len = m->m->key_len;
    } else if (part == KAFKA_SHIM_PART_VALUE) {
        ptr = m->m->payload;
        len = m->m->len;
    } else {
        const char *name;
        const void *value = NULL;
        size_t size = 0;
        rd_kafka_header_get_all(m->headers, (size_t)(part - KAFKA_SHIM_PART_HEADER), &name, &value, &size);
        ptr = value;
        len = size;
    }
    *n = ptr == NULL ? -1 : (long)len;
    return ptr;
}

// ---- Administration ----

static inline int kafka_shim_admin_wait(kafka_shim_client *c, rd_kafka_queue_t *q, int timeoutMs, rd_kafka_event_t **out) {
    (void)c;
    // The broker's own timeout is timeoutMs, so its answer gets a little longer.
    rd_kafka_event_t *ev = rd_kafka_queue_poll(q, timeoutMs + 2000);
    if (ev == NULL) {
        return kafka_shim_fail_text(RD_KAFKA_RESP_ERR__TIMED_OUT, "the admin request timed out");
    }
    int err = rd_kafka_event_error(ev);
    if (err) {
        kafka_shim_fail_text(err, rd_kafka_event_error_string(ev));
        rd_kafka_event_destroy(ev);
        return err;
    }
    *out = ev;
    return 0;
}

static inline rd_kafka_AdminOptions_t *kafka_shim_admin_options(kafka_shim_client *c, rd_kafka_admin_op_t op, int timeoutMs) {
    char reason[256];
    rd_kafka_AdminOptions_t *o = rd_kafka_AdminOptions_new(c->rk, op);
    rd_kafka_AdminOptions_set_request_timeout(o, timeoutMs, reason, sizeof reason);
    rd_kafka_AdminOptions_set_operation_timeout(o, timeoutMs, reason, sizeof reason);
    return o;
}

static inline int kafka_shim_topic_results(const rd_kafka_topic_result_t **results, size_t n) {
    for (size_t i = 0; i < n; i++) {
        int err = rd_kafka_topic_result_error(results[i]);
        if (err) {
            const char *reason = rd_kafka_topic_result_error_string(results[i]);
            return kafka_shim_fail_text(err, reason ? reason : rd_kafka_err2str(err));
        }
    }
    return 0;
}

static inline rd_kafka_NewTopic_t *kafka_shim_new_topic(const char *name, int partitions, int replicas) {
    char reason[256];
    rd_kafka_NewTopic_t *t = rd_kafka_NewTopic_new(name, partitions, replicas, reason, sizeof reason);
    if (t == NULL) {
        kafka_shim_fail_text(RD_KAFKA_RESP_ERR__INVALID_ARG, reason);
    }
    return t;
}

static inline int kafka_shim_new_topic_set(rd_kafka_NewTopic_t *t, const char *key, const char *value) {
    rd_kafka_resp_err_t err = rd_kafka_NewTopic_set_config(t, key, value);
    return err ? kafka_shim_fail_code(NULL, err) : 0;
}

static inline int kafka_shim_create_topic(kafka_shim_client *c, rd_kafka_NewTopic_t *t, int timeoutMs) {
    rd_kafka_queue_t *q = rd_kafka_queue_new(c->rk);
    rd_kafka_AdminOptions_t *o = kafka_shim_admin_options(c, RD_KAFKA_ADMIN_OP_CREATETOPICS, timeoutMs);
    rd_kafka_CreateTopics(c->rk, &t, 1, o, q);
    rd_kafka_event_t *ev = NULL;
    int err = kafka_shim_admin_wait(c, q, timeoutMs, &ev);
    if (err == 0) {
        size_t n = 0;
        const rd_kafka_topic_result_t **r = rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(ev), &n);
        err = kafka_shim_topic_results(r, n);
        rd_kafka_event_destroy(ev);
    }
    rd_kafka_AdminOptions_destroy(o);
    rd_kafka_queue_destroy(q);
    return err;
}

static inline int kafka_shim_delete_topic(kafka_shim_client *c, const char *name, int timeoutMs) {
    rd_kafka_DeleteTopic_t *t = rd_kafka_DeleteTopic_new(name);
    rd_kafka_queue_t *q = rd_kafka_queue_new(c->rk);
    rd_kafka_AdminOptions_t *o = kafka_shim_admin_options(c, RD_KAFKA_ADMIN_OP_DELETETOPICS, timeoutMs);
    rd_kafka_DeleteTopics(c->rk, &t, 1, o, q);
    rd_kafka_event_t *ev = NULL;
    int err = kafka_shim_admin_wait(c, q, timeoutMs, &ev);
    if (err == 0) {
        size_t n = 0;
        const rd_kafka_topic_result_t **r = rd_kafka_DeleteTopics_result_topics(rd_kafka_event_DeleteTopics_result(ev), &n);
        err = kafka_shim_topic_results(r, n);
        rd_kafka_event_destroy(ev);
    }
    rd_kafka_AdminOptions_destroy(o);
    rd_kafka_queue_destroy(q);
    rd_kafka_DeleteTopic_destroy(t);
    return err;
}

static inline int kafka_shim_create_partitions(kafka_shim_client *c, const char *name, int total, int timeoutMs) {
    char reason[256];
    rd_kafka_NewPartitions_t *t = rd_kafka_NewPartitions_new(name, (size_t)total, reason, sizeof reason);
    if (t == NULL) {
        return kafka_shim_fail_text(RD_KAFKA_RESP_ERR__INVALID_ARG, reason);
    }
    rd_kafka_queue_t *q = rd_kafka_queue_new(c->rk);
    rd_kafka_AdminOptions_t *o = kafka_shim_admin_options(c, RD_KAFKA_ADMIN_OP_CREATEPARTITIONS, timeoutMs);
    rd_kafka_CreatePartitions(c->rk, &t, 1, o, q);
    rd_kafka_event_t *ev = NULL;
    int err = kafka_shim_admin_wait(c, q, timeoutMs, &ev);
    if (err == 0) {
        size_t n = 0;
        const rd_kafka_topic_result_t **r = rd_kafka_CreatePartitions_result_topics(rd_kafka_event_CreatePartitions_result(ev), &n);
        err = kafka_shim_topic_results(r, n);
        rd_kafka_event_destroy(ev);
    }
    rd_kafka_AdminOptions_destroy(o);
    rd_kafka_queue_destroy(q);
    rd_kafka_NewPartitions_destroy(t);
    return err;
}

static inline int kafka_shim_delete_group(kafka_shim_client *c, const char *group, int timeoutMs) {
    rd_kafka_DeleteGroup_t *g = rd_kafka_DeleteGroup_new(group);
    rd_kafka_queue_t *q = rd_kafka_queue_new(c->rk);
    rd_kafka_AdminOptions_t *o = kafka_shim_admin_options(c, RD_KAFKA_ADMIN_OP_DELETEGROUPS, timeoutMs);
    rd_kafka_DeleteGroups(c->rk, &g, 1, o, q);
    rd_kafka_event_t *ev = NULL;
    int err = kafka_shim_admin_wait(c, q, timeoutMs, &ev);
    if (err == 0) {
        size_t n = 0;
        const rd_kafka_group_result_t **r = rd_kafka_DeleteGroups_result_groups(rd_kafka_event_DeleteGroups_result(ev), &n);
        for (size_t i = 0; i < n && err == 0; i++) {
            const rd_kafka_error_t *e = rd_kafka_group_result_error(r[i]);
            if (e != NULL) {
                err = kafka_shim_fail_text(rd_kafka_error_code(e), rd_kafka_error_string(e));
            }
        }
        rd_kafka_event_destroy(ev);
    }
    rd_kafka_AdminOptions_destroy(o);
    rd_kafka_queue_destroy(q);
    rd_kafka_DeleteGroup_destroy(g);
    return err;
}

// Cluster metadata, read through accessors.
typedef struct {
    const rd_kafka_metadata_t *md;
} kafka_shim_meta;

static inline kafka_shim_meta *kafka_shim_meta_get(kafka_shim_client *c, const char *topic, int timeoutMs) {
    const rd_kafka_metadata_t *md = NULL;
    rd_kafka_topic_t *rkt = topic[0] != '\0' ? rd_kafka_topic_new(c->rk, topic, NULL) : NULL;
    rd_kafka_resp_err_t err = rd_kafka_metadata(c->rk, rkt == NULL, rkt, &md, timeoutMs);
    if (rkt != NULL) {
        rd_kafka_topic_destroy(rkt);
    }
    if (err) {
        kafka_shim_fail_code(c->rk, err);
        return NULL;
    }
    kafka_shim_meta *m = malloc(sizeof *m);
    if (m == NULL) {
        rd_kafka_metadata_destroy(md);
        kafka_shim_fail_text(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "out of memory");
        return NULL;
    }
    m->md = md;
    return m;
}

static inline void kafka_shim_meta_free(kafka_shim_meta *m) {
    rd_kafka_metadata_destroy(m->md);
    free(m);
}

static inline int kafka_shim_meta_brokers(kafka_shim_meta *m) { return m->md->broker_cnt; }
static inline int kafka_shim_meta_broker_id(kafka_shim_meta *m, int i) { return m->md->brokers[i].id; }
static inline const char *kafka_shim_meta_broker_host(kafka_shim_meta *m, int i) { return m->md->brokers[i].host; }
static inline int kafka_shim_meta_broker_port(kafka_shim_meta *m, int i) { return m->md->brokers[i].port; }
static inline int kafka_shim_meta_topics(kafka_shim_meta *m) { return m->md->topic_cnt; }
static inline const char *kafka_shim_meta_topic(kafka_shim_meta *m, int i) { return m->md->topics[i].topic; }
static inline int kafka_shim_meta_topic_err(kafka_shim_meta *m, int i) { return m->md->topics[i].err; }
static inline int kafka_shim_meta_partitions(kafka_shim_meta *m, int i) { return m->md->topics[i].partition_cnt; }
static inline int kafka_shim_meta_partition_id(kafka_shim_meta *m, int i, int p) { return m->md->topics[i].partitions[p].id; }
static inline int kafka_shim_meta_partition_leader(kafka_shim_meta *m, int i, int p) { return m->md->topics[i].partitions[p].leader; }
static inline int kafka_shim_meta_partition_replicas(kafka_shim_meta *m, int i, int p) { return m->md->topics[i].partitions[p].replica_cnt; }
static inline int kafka_shim_meta_partition_isrs(kafka_shim_meta *m, int i, int p) { return m->md->topics[i].partitions[p].isr_cnt; }

// Consumer groups, read through accessors.
typedef struct {
    const struct rd_kafka_group_list *list;
} kafka_shim_groups;

static inline kafka_shim_groups *kafka_shim_groups_get(kafka_shim_client *c, const char *group, int timeoutMs) {
    const struct rd_kafka_group_list *list = NULL;
    rd_kafka_resp_err_t err = rd_kafka_list_groups(c->rk, group[0] != '\0' ? group : NULL, &list, timeoutMs);
    if (err) {
        kafka_shim_fail_code(c->rk, err);
        return NULL;
    }
    kafka_shim_groups *g = malloc(sizeof *g);
    if (g == NULL) {
        rd_kafka_group_list_destroy(list);
        kafka_shim_fail_text(RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "out of memory");
        return NULL;
    }
    g->list = list;
    return g;
}

static inline void kafka_shim_groups_free(kafka_shim_groups *g) {
    rd_kafka_group_list_destroy(g->list);
    free(g);
}

static inline int kafka_shim_groups_count(kafka_shim_groups *g) { return g->list->group_cnt; }
static inline const char *kafka_shim_groups_name(kafka_shim_groups *g, int i) { return g->list->groups[i].group; }
static inline const char *kafka_shim_groups_state(kafka_shim_groups *g, int i) { return g->list->groups[i].state; }
static inline const char *kafka_shim_groups_protocol(kafka_shim_groups *g, int i) { return g->list->groups[i].protocol; }
static inline int kafka_shim_groups_members(kafka_shim_groups *g, int i) { return g->list->groups[i].member_cnt; }

#endif
