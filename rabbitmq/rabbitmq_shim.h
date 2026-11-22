#ifndef RABBITMQ_SHIM_H
#define RABBITMQ_SHIM_H

// AMQP 1.0 to RabbitMQ 4, over Qpid Proton's proactor.
//
// Each connection has an I/O thread of its own, and only that thread touches
// Proton. Dex threads hand it work through a queue, and wake it with
// pn_connection_wake, because Proton only writes what was changed while handling
// that connection's own events. (pn_proactor_interrupt is used only while there
// is no connection.) They wait on condition variables for what needs an answer. Messages are encoded
// and decoded on the Dex thread, so the I/O thread only moves bytes.
//
// A connection that drops is reopened by the I/O thread, with backoff, and then
// put back as it was: the management link first, then every queue, exchange and
// binding declared through it, then publishers and consumers. Messages that were
// in flight are sent again, so delivery is at least once.
//
// Objects shared by Dex and the I/O thread are reference-counted, so either side
// can let go first. The connection is held only by Dex's handles (itself, its
// links, the messages received on them), so it is only ever torn down on a Dex
// thread; a link's memory is held by Dex, the I/O thread's registry, and each
// message from it.

#include <proton/codec.h>
#include <proton/condition.h>
#include <proton/connection.h>
#include <proton/delivery.h>
#include <proton/disposition.h>
#include <proton/event.h>
#include <proton/link.h>
#include <proton/message.h>
#include <proton/proactor.h>
#include <proton/sasl.h>
#include <proton/session.h>
#include <proton/ssl.h>
#include <proton/terminus.h>
#include <proton/transport.h>

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

// Error codes Dex sees. The HTTP-like codes are the management API's own.
#define RMQ_OK 0
#define RMQ_ERR_CLOSED -1
#define RMQ_ERR_TIMEOUT -2
#define RMQ_ERR_CONNECT -3
#define RMQ_ERR_AUTH -4
#define RMQ_ERR_LINK -5
#define RMQ_ERR_STALE -6
#define RMQ_ERR_INVALID -7
#define RMQ_ERR_UNKNOWN_OUTCOME -8
#define RMQ_ERR_MEMORY -9

#define RMQ_STATE_CONNECTING 0
#define RMQ_STATE_OPEN 1
#define RMQ_STATE_RECONNECTING 2
#define RMQ_STATE_CLOSED 3

#define RMQ_OUTCOME_ACCEPTED 0
#define RMQ_OUTCOME_RELEASED 1
#define RMQ_OUTCOME_REJECTED 2
#define RMQ_OUTCOME_MODIFIED 3
#define RMQ_OUTCOME_FAILED 4 // never reached the broker, or its outcome is unknown

#define RMQ_SETTLE_ACCEPT 0
#define RMQ_SETTLE_REQUEUE 1    // released
#define RMQ_SETTLE_DISCARD 2    // rejected: dead-lettered if the queue says so
#define RMQ_SETTLE_FAILED 3     // modified: delivery-failed, counts toward a delivery limit

#define RMQ_LINK_PUBLISHER 0
#define RMQ_LINK_CONSUMER 1

#define RMQ_EVENTS 64
#define RMQ_ENDPOINTS 16

// ---- The last error on this thread ----

static _Thread_local struct {
    char message[512];
    int status;
} rmq_last;

static inline int rmq_fail(int code, const char *fmt, const char *a, const char *b) {
    snprintf(rmq_last.message, sizeof rmq_last.message, fmt, a ? a : "", b ? b : "");
    return code;
}

static inline const char *rmq_shim_last_message(void) {
    return rmq_last.message;
}

static inline long rmq_now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long)tv.tv_sec * 1000L + tv.tv_usec / 1000;
}

static inline void rmq_deadline(struct timespec *ts, long ms) {
    struct timeval now;
    gettimeofday(&now, NULL);
    long nanos = now.tv_usec * 1000L + (ms % 1000) * 1000000L;
    ts->tv_sec = now.tv_sec + ms / 1000 + nanos / 1000000000L;
    ts->tv_nsec = nanos % 1000000000L;
}

// ---- Types ----

typedef struct rmq_conn rmq_conn;
typedef struct rmq_link rmq_link;

// A message on its way out, from the Dex call that sent it to its outcome.
typedef struct rmq_out {
    struct rmq_out *next;   // pending queue, or the unsettled list
    struct rmq_out *prev;   // unsettled list
    char *bytes;
    size_t size;
    long deadline;
    long tag;
    int tracked;
    struct rmq_ticket *ticket;
    pn_delivery_t *delivery; // while sent and unsettled, on the current link
} rmq_out;

// One thread waiting for one answer.
typedef struct rmq_ticket {
    pthread_mutex_t mu;
    pthread_cond_t cond;
    int done;
    int code;
    int outcome;
    char reason[256];
    char *reply;      // a management response, encoded
    size_t reply_size;
} rmq_ticket;

// A settlement waiting for the I/O thread. Kept in an array on the link rather
// than posted one op at a time, so a consumer settling at full speed costs an
// append, and a wake only when the array was empty.
typedef struct {
    pn_delivery_t *delivery;
    unsigned long generation;
    int how;
} rmq_settlement;

typedef struct rmq_report {
    struct rmq_report *next;
    long tag;
    int outcome;
    char *reason;
} rmq_report;

// A received message, as Dex holds it.
typedef struct rmq_in {
    rmq_link *link;
    pn_delivery_t *delivery;
    unsigned long generation;
    char *bytes;
    size_t size;
    struct rmq_in *next;
    pn_message_t *msg;  // decoded on the Dex thread
    atomic_int settled;
    char *scratch;      // rendered property values
} rmq_in;

struct rmq_link {
    atomic_int refs;    // Dex's handle, the I/O thread's registry, each rmq_in, each op
    rmq_conn *c;
    int kind;
    long id;
    char name[64];
    char *address;      // target of a publisher, source of a consumer
    char *filter_offset; // a stream consumer's offset: "first", "last", "next", or digits
    long filter_timestamp;

    // The I/O thread's. Each link has a session of its own: RabbitMQ ends the
    // whole session over one link's error, a queue that does not exist, say,
    // and a shared one would take every other link down with it.
    pn_session_t *session;
    pn_link_t *pn;
    atomic_ulong generation;  // bumped each time pn is replaced; read by Dex threads
    rmq_out *unsettled_head;
    rmq_out *unsettled_tail;
    struct rmq_link *reg_next;
    int draining;             // a pause asked the broker to use up or drop its credit

    // Shared, under mu.
    pthread_mutex_t mu;
    pthread_cond_t space;     // a publisher's window has room, or a consumer has a message
    pthread_cond_t drained;   // nothing in flight
    rmq_out *pending_head;
    rmq_out *pending_tail;
    int in_flight;            // pending + unsettled
    int window;
    int closed;
    char failed[256];         // why the broker detached it, "" if it has not
    rmq_report *reports_head;
    rmq_report *reports_tail;
    pthread_cond_t report_ready;
    rmq_in *buf_head;
    rmq_in *buf_tail;
    int buffered;
    int paused;
    rmq_settlement *settles;
    int settles_len;
    int settles_cap;
    atomic_long accepted;
    atomic_long released;
    atomic_long rejected;
    atomic_long failed_count;
};

typedef struct rmq_op {
    struct rmq_op *next;
    int kind;
    rmq_link *link;
    pn_delivery_t *delivery;
    unsigned long generation;
    int settle;
    char *bytes;
    size_t size;
    uint64_t id;
    long deadline;
    rmq_ticket *ticket;
    char *path;     // a request's, for recording it
    int record;     // 1 record on success, 2 forget the path
} rmq_op;

#define RMQ_OP_ATTACH 0
#define RMQ_OP_DETACH 1
#define RMQ_OP_SETTLE 2
#define RMQ_OP_REQUEST 3
#define RMQ_OP_CLOSE 4
#define RMQ_OP_PUMP 5
#define RMQ_OP_SHUTDOWN 6

// Marks a link we closed, so that the broker's detach returns its session.
static char rmq_closing_link;

typedef struct rmq_record {
    struct rmq_record *next;
    char *path;
    char *bytes;    // the encoded request, re-sent as it was
    size_t size;
} rmq_record;

typedef struct rmq_request {
    struct rmq_request *next;
    uint64_t id;
    char *bytes;
    size_t size;
    long deadline;
    rmq_ticket *ticket; // NULL for a replayed record
    int sent;
    int lost;           // connections that died with it sent and unanswered
    char why[256];
} rmq_request;

typedef struct {
    char host[256];
    char port[16];
    char user[256];
    char password[256];
    char vhost[256];
    int tls;
} rmq_endpoint;

struct rmq_conn {
    atomic_int refs;    // Dex's handles: the connection, each link, each message handed out
    pthread_t thread;
    int has_thread;
    int joined;
    pn_proactor_t *proactor;

    // Settings, fixed once the thread starts.
    rmq_endpoint endpoints[RMQ_ENDPOINTS];
    int endpoint_count;
    char container[128];
    int idle_timeout_ms;
    int connect_timeout_ms;
    int publish_timeout_ms;
    int backoff_initial_ms;
    int backoff_max_ms;
    int max_attempts;
    int resend;
    char ca_file[512];
    int verify;

    // The I/O thread's.
    pn_connection_t *pn;
    pn_session_t *session;  // the management link pair's
    // Sessions whose link was closed by us and detached by the broker, kept for
    // the next link. Closing and reopening sessions instead lets Proton reuse
    // a channel the broker has not finished ending, and RabbitMQ then ignores
    // the new one: a consumer closed with messages unsettled made the next
    // consumer on the connection receive nothing.
    pn_session_t *spare[64];
    int spares;
    pn_link_t *mgmt_out;
    pn_link_t *mgmt_in;
    int mgmt_out_open;
    int mgmt_in_open;
    int phase;              // 0 down, 1 opening, 2 replaying, 3 ready
    int replay_left;
    int attempt;
    int next_endpoint;
    long reconnect_at;
    int backoff_ms;
    uint64_t next_tag;
    rmq_link *links;
    rmq_request *requests;  // waiting to be sent, or for their answer
    rmq_record *records;
    int closing;            // closed for good once the transport is
    int shutdown;           // the last handle is gone: the thread may stop
    int ever_opened;

    // Shared, under mu.
    pthread_mutex_t mu;
    pthread_cond_t state_changed;
    pn_connection_t *wake_conn; // the connection to wake, until its transport closes
    rmq_op *ops_head;
    rmq_op *ops_tail;
    int state;
    int fatal;              // a reason not to reconnect: bad credentials
    char reason[512];
    char events[RMQ_EVENTS][256];
    int event_kinds[RMQ_EVENTS];
    int events_len;
    atomic_int woken;
    atomic_ullong next_request_id;
    atomic_long reconnects;
};

#define RMQ_EVENT_OPEN 0
#define RMQ_EVENT_DOWN 1
#define RMQ_EVENT_RECOVERY 2
#define RMQ_EVENT_LINK 3
#define RMQ_EVENT_FAILED 4

// ---- Small helpers ----

static void rmq_event(rmq_conn *c, int kind, const char *what, const char *detail) {
    pthread_mutex_lock(&c->mu);
    if (c->events_len == RMQ_EVENTS) {
        memmove(c->events[0], c->events[1], sizeof(c->events[0]) * (RMQ_EVENTS - 1));
        memmove(&c->event_kinds[0], &c->event_kinds[1], sizeof(c->event_kinds[0]) * (RMQ_EVENTS - 1));
        c->events_len--;
    }
    snprintf(c->events[c->events_len], sizeof c->events[0], "%s%s%s", what, detail && detail[0] ? ": " : "", detail ? detail : "");
    c->event_kinds[c->events_len] = kind;
    c->events_len++;
    pthread_mutex_unlock(&c->mu);
}

// Waking happens under mu, which the I/O thread also holds when it retires the
// connection, so a connection is never woken after Proton has freed it.
static void rmq_wake(rmq_conn *c) {
    if (!atomic_exchange(&c->woken, 1)) {
        pthread_mutex_lock(&c->mu);
        if (c->wake_conn != NULL) {
            pn_connection_wake(c->wake_conn);
        } else {
            pn_proactor_interrupt(c->proactor);
        }
        pthread_mutex_unlock(&c->mu);
    }
}

static void rmq_post(rmq_conn *c, rmq_op *op) {
    op->next = NULL;
    pthread_mutex_lock(&c->mu);
    if (c->ops_tail != NULL) {
        c->ops_tail->next = op;
    } else {
        c->ops_head = op;
    }
    c->ops_tail = op;
    pthread_mutex_unlock(&c->mu);
    rmq_wake(c);
}

static rmq_op *rmq_op_new(int kind) {
    rmq_op *op = calloc(1, sizeof *op);
    if (op != NULL) {
        op->kind = kind;
    }
    return op;
}

static void rmq_ticket_init(rmq_ticket *t) {
    memset(t, 0, sizeof *t);
    pthread_mutex_init(&t->mu, NULL);
    pthread_cond_init(&t->cond, NULL);
}

static void rmq_ticket_destroy(rmq_ticket *t) {
    free(t->reply);
    pthread_cond_destroy(&t->cond);
    pthread_mutex_destroy(&t->mu);
}

static void rmq_ticket_finish(rmq_ticket *t, int code, int outcome, const char *reason) {
    pthread_mutex_lock(&t->mu);
    t->code = code;
    t->outcome = outcome;
    snprintf(t->reason, sizeof t->reason, "%s", reason ? reason : "");
    t->done = 1;
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->mu);
}

// Waits until done. The I/O thread always finishes a ticket: on an outcome, a
// timeout it enforces, or the connection closing.
static void rmq_ticket_wait(rmq_ticket *t) {
    pthread_mutex_lock(&t->mu);
    while (!t->done) {
        pthread_cond_wait(&t->cond, &t->mu);
    }
    pthread_mutex_unlock(&t->mu);
}

static void rmq_conn_unref(rmq_conn *c);
static inline bool rmq_shim_conn_closed(rmq_conn *c);

static void rmq_link_destroy(rmq_link *l) {
    while (l->reports_head != NULL) {
        rmq_report *r = l->reports_head;
        l->reports_head = r->next;
        free(r->reason);
        free(r);
    }
    while (l->buf_head != NULL) {
        rmq_in *in = l->buf_head;
        l->buf_head = in->next;
        free(in->bytes);
        free(in);
    }
    free(l->settles);
    free(l->address);
    free(l->filter_offset);
    pthread_cond_destroy(&l->space);
    pthread_cond_destroy(&l->drained);
    pthread_cond_destroy(&l->report_ready);
    pthread_mutex_destroy(&l->mu);
    free(l);
}

// Frees a link's memory with its last reference. Never the connection: see above.
static void rmq_link_unref(rmq_link *l) {
    if (atomic_fetch_sub(&l->refs, 1) == 1) {
        rmq_link_destroy(l);
    }
}

// ---- Outcomes of published messages ----

static void rmq_out_free(rmq_out *o) {
    free(o->bytes);
    free(o);
}

// The end of a message's journey, whichever it was. Called with l->mu not held.
static void rmq_out_done(rmq_link *l, rmq_out *o, int outcome, const char *reason) {
    switch (outcome) {
    case RMQ_OUTCOME_ACCEPTED:
        atomic_fetch_add(&l->accepted, 1);
        break;
    case RMQ_OUTCOME_RELEASED:
    case RMQ_OUTCOME_MODIFIED:
        atomic_fetch_add(&l->released, 1);
        break;
    case RMQ_OUTCOME_REJECTED:
        atomic_fetch_add(&l->rejected, 1);
        break;
    default:
        atomic_fetch_add(&l->failed_count, 1);
    }

    if (o->ticket != NULL) {
        rmq_ticket_finish(o->ticket, outcome == RMQ_OUTCOME_FAILED ? RMQ_ERR_UNKNOWN_OUTCOME : RMQ_OK, outcome, reason);
    } else if (o->tracked) {
        rmq_report *r = malloc(sizeof *r);
        if (r != NULL) {
            r->next = NULL;
            r->tag = o->tag;
            r->outcome = outcome;
            r->reason = strdup(reason ? reason : "");
            pthread_mutex_lock(&l->mu);
            if (l->reports_tail != NULL) {
                l->reports_tail->next = r;
            } else {
                l->reports_head = r;
            }
            l->reports_tail = r;
            pthread_cond_signal(&l->report_ready);
            pthread_mutex_unlock(&l->mu);
        }
    } else if (outcome == RMQ_OUTCOME_FAILED || outcome == RMQ_OUTCOME_REJECTED) {
        rmq_event(l->c, RMQ_EVENT_LINK, outcome == RMQ_OUTCOME_FAILED ? "a message was not delivered" : "a message was rejected", reason);
    }

    rmq_out_free(o);
    pthread_mutex_lock(&l->mu);
    l->in_flight--;
    pthread_cond_signal(&l->space);
    if (l->in_flight == 0) {
        pthread_cond_broadcast(&l->drained);
    }
    pthread_mutex_unlock(&l->mu);
}

static void rmq_unsettled_remove(rmq_link *l, rmq_out *o) {
    if (o->prev) {
        o->prev->next = o->next;
    } else {
        l->unsettled_head = o->next;
    }
    if (o->next) {
        o->next->prev = o->prev;
    } else {
        l->unsettled_tail = o->prev;
    }
    o->next = o->prev = NULL;
}

// ---- The I/O thread ----

static void rmq_closed_for_good(rmq_conn *c, const char *why);

static void rmq_set_state(rmq_conn *c, int state, const char *reason) {
    pthread_mutex_lock(&c->mu);
    c->state = state;
    if (reason != NULL) {
        snprintf(c->reason, sizeof c->reason, "%s", reason);
    }
    pthread_cond_broadcast(&c->state_changed);
    pthread_mutex_unlock(&c->mu);
}

static void rmq_put_paired(pn_link_t *l) {
    pn_data_t *p = pn_link_properties(l);
    pn_data_put_map(p);
    pn_data_enter(p);
    pn_data_put_symbol(p, pn_bytes(6, "paired"));
    pn_data_put_bool(p, true);
    pn_data_exit(p);
}

static void rmq_attach(rmq_conn *c, rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    int closed = l->closed;
    l->failed[0] = '\0';
    pthread_mutex_unlock(&l->mu);
    if (closed || c->pn == NULL) {
        return;
    }
    l->generation++;
    l->draining = 0;
    if (c->spares > 0) {
        l->session = c->spare[--c->spares];
    } else {
        l->session = pn_session(c->pn);
        pn_session_open(l->session);
    }
    pn_session_set_context(l->session, l);
    if (l->kind == RMQ_LINK_PUBLISHER) {
        l->pn = pn_sender(l->session, l->name);
        pn_terminus_set_address(pn_link_target(l->pn), l->address);
        pn_link_set_snd_settle_mode(l->pn, PN_SND_UNSETTLED);
        pn_link_set_rcv_settle_mode(l->pn, PN_RCV_FIRST);
    } else {
        l->pn = pn_receiver(l->session, l->name);
        pn_terminus_t *src = pn_link_source(l->pn);
        pn_terminus_set_address(src, l->address);
        pn_link_set_snd_settle_mode(l->pn, PN_SND_UNSETTLED);
        pn_link_set_rcv_settle_mode(l->pn, PN_RCV_FIRST);
        if (l->filter_offset != NULL || l->filter_timestamp != 0) {
            pn_data_t *f = pn_terminus_filter(src);
            pn_data_put_map(f);
            pn_data_enter(f);
            pn_data_put_symbol(f, pn_bytes(27, "rabbitmq:stream-offset-spec"));
            pn_data_put_described(f);
            pn_data_enter(f);
            pn_data_put_symbol(f, pn_bytes(27, "rabbitmq:stream-offset-spec"));
            if (l->filter_timestamp != 0) {
                pn_data_put_timestamp(f, l->filter_timestamp);
            } else if (l->filter_offset[0] >= '0' && l->filter_offset[0] <= '9') {
                pn_data_put_ulong(f, strtoull(l->filter_offset, NULL, 10));
            } else {
                // A string, not a symbol: RabbitMQ ignores the filter otherwise,
                // and quietly starts at "next".
                pn_data_put_string(f, pn_bytes(strlen(l->filter_offset), l->filter_offset));
            }
            pn_data_exit(f);
            pn_data_exit(f);
        }
    }
    pn_link_set_context(l->pn, l);
    pn_link_open(l->pn);
    if (l->kind == RMQ_LINK_CONSUMER) {
        pthread_mutex_lock(&l->mu);
        int want = l->paused ? 0 : l->window - l->buffered;
        pthread_mutex_unlock(&l->mu);
        if (want > 0) {
            pn_link_flow(l->pn, want);
        }
    }
}

// Sends what a publisher has waiting, as far as its credit goes.
static void rmq_pump_publisher(rmq_conn *c, rmq_link *l) {
    if (l->pn == NULL || c->phase != 3) {
        return;
    }
    while (pn_link_credit(l->pn) > 0) {
        pthread_mutex_lock(&l->mu);
        rmq_out *o = l->pending_head;
        if (o != NULL) {
            l->pending_head = o->next;
            if (l->pending_head == NULL) {
                l->pending_tail = NULL;
            }
        }
        pthread_mutex_unlock(&l->mu);
        if (o == NULL) {
            return;
        }
        uint64_t tag = c->next_tag++;
        pn_delivery_t *d = pn_delivery(l->pn, pn_dtag((const char *)&tag, sizeof tag));
        pn_link_send(l->pn, o->bytes, o->size);
        pn_link_advance(l->pn);
        pn_delivery_set_context(d, o);
        o->delivery = d;
        o->next = NULL;
        o->prev = l->unsettled_tail;
        if (l->unsettled_tail) {
            l->unsettled_tail->next = o;
        } else {
            l->unsettled_head = o;
        }
        l->unsettled_tail = o;
    }
}

// Gives a consumer credit up to its window, less what is waiting to be read.
// Paused, it drains instead: the broker sends what the credit already granted
// covers, or drops it, and then sends nothing more.
static void rmq_top_up(rmq_link *l) {
    if (l->pn == NULL) {
        return;
    }
    pthread_mutex_lock(&l->mu);
    int paused = l->paused;
    int want = paused ? 0 : l->window - l->buffered;
    pthread_mutex_unlock(&l->mu);
    int credit = pn_link_credit(l->pn);
    if (paused) {
        if (credit > 0 && !l->draining) {
            l->draining = 1;
            pn_link_drain(l->pn, 0);
        }
        return;
    }
    if (l->draining) {
        // Still waiting for the broker's answer to the drain; top up after.
        return;
    }
    int quarter = l->window / 4 > 0 ? l->window / 4 : 1;
    if (want - credit >= quarter) {
        pn_link_flow(l->pn, want - credit);
    }
}

static void rmq_send_requests(rmq_conn *c) {
    if (c->mgmt_out == NULL || !c->mgmt_out_open || !c->mgmt_in_open) {
        return;
    }
    for (rmq_request *r = c->requests; r != NULL; r = r->next) {
        if (r->sent) {
            continue;
        }
        // A replayed record goes first; an application's request waits for recovery.
        if (r->ticket != NULL && c->phase != 3) {
            continue;
        }
        if (pn_link_credit(c->mgmt_out) <= 0) {
            return;
        }
        uint64_t tag = c->next_tag++;
        pn_delivery_t *d = pn_delivery(c->mgmt_out, pn_dtag((const char *)&tag, sizeof tag));
        pn_link_send(c->mgmt_out, r->bytes, r->size);
        pn_link_advance(c->mgmt_out);
        pn_delivery_settle(d);
        r->sent = 1;
    }
}

static void rmq_ready(rmq_conn *c) {
    c->phase = 3;
    c->attempt = 0;
    c->backoff_ms = c->backoff_initial_ms;
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        rmq_attach(c, l);
    }
    const rmq_endpoint *e = &c->endpoints[(c->next_endpoint + c->endpoint_count - 1) % c->endpoint_count];
    char where[300];
    snprintf(where, sizeof where, "%s:%s", e->host, e->port);
    rmq_event(c, RMQ_EVENT_OPEN, c->ever_opened ? "reconnected to" : "connected to", where);
    if (c->ever_opened) {
        atomic_fetch_add(&c->reconnects, 1);
    }
    c->ever_opened = 1;
    rmq_set_state(c, RMQ_STATE_OPEN, "");
    rmq_send_requests(c);
}

static void rmq_add_request(rmq_conn *c, uint64_t id, char *bytes, size_t size, long deadline, rmq_ticket *t) {
    rmq_request *r = calloc(1, sizeof *r);
    if (r == NULL) {
        free(bytes);
        if (t) {
            rmq_ticket_finish(t, RMQ_ERR_MEMORY, 0, "out of memory");
        }
        return;
    }
    r->id = id;
    r->bytes = bytes;
    r->size = size;
    r->deadline = deadline;
    r->ticket = t;
    rmq_request **at = &c->requests;
    while (*at) {
        at = &(*at)->next;
    }
    *at = r;
}

// Records are replayed with ids of their own, marked by the top bit.
#define RMQ_REPLAY_ID (1ULL << 63)

static void rmq_replay(rmq_conn *c) {
    c->replay_left = 0;
    uint64_t n = 0;
    for (rmq_record *rec = c->records; rec != NULL; rec = rec->next) {
        pn_message_t *m = pn_message();
        if (pn_message_decode(m, rec->bytes, rec->size) == 0) {
            pn_data_t *id = pn_message_id(m);
            pn_data_clear(id);
            pn_data_put_ulong(id, RMQ_REPLAY_ID | n++);
            // The length is what encode2 returns; out.size is the buffer's capacity.
            pn_rwbytes_t out = {0, NULL};
            ssize_t len = pn_message_encode2(m, &out);
            if (len > 0) {
                rmq_add_request(c, RMQ_REPLAY_ID | (n - 1), out.start, (size_t)len, 0, NULL);
                c->replay_left++;
            } else {
                free(out.start);
            }
        }
        pn_message_free(m);
    }
    if (c->replay_left == 0) {
        rmq_ready(c);
    } else {
        c->phase = 2;
        rmq_send_requests(c);
    }
}

static uint64_t rmq_msg_ulong(pn_data_t *d) {
    pn_data_rewind(d);
    if (!pn_data_next(d)) {
        return 0;
    }
    return pn_data_type(d) == PN_ULONG ? pn_data_get_ulong(d) : 0;
}

static void rmq_on_reply(rmq_conn *c, const char *bytes, size_t size) {
    pn_message_t *m = pn_message();
    if (pn_message_decode(m, bytes, size) != 0) {
        pn_message_free(m);
        return;
    }
    uint64_t id = rmq_msg_ulong(pn_message_correlation_id(m));
    const char *status = pn_message_get_subject(m);
    for (rmq_request **at = &c->requests; *at != NULL; at = &(*at)->next) {
        rmq_request *r = *at;
        if (r->id != id) {
            continue;
        }
        *at = r->next;
        if (r->ticket != NULL) {
            r->ticket->reply = malloc(size);
            if (r->ticket->reply != NULL) {
                memcpy(r->ticket->reply, bytes, size);
                r->ticket->reply_size = size;
            }
            rmq_ticket_finish(r->ticket, RMQ_OK, 0, status);
        } else {
            if (status == NULL || status[0] != '2') {
                rmq_event(c, RMQ_EVENT_RECOVERY, "re-declaring after reconnecting failed with status", status ? status : "none");
            }
            if (--c->replay_left == 0 && c->phase == 2) {
                rmq_ready(c);
            }
        }
        free(r->bytes);
        free(r);
        break;
    }
    pn_message_free(m);
}

static void rmq_remember(rmq_conn *c, const char *path, const char *bytes, size_t size, int mode) {
    rmq_record **at = &c->records;
    while (*at != NULL) {
        if (strcmp((*at)->path, path) == 0) {
            rmq_record *gone = *at;
            *at = gone->next;
            free(gone->path);
            free(gone->bytes);
            free(gone);
            break;
        }
        at = &(*at)->next;
    }
    if (mode == 1) {
        rmq_record *r = calloc(1, sizeof *r);
        if (r == NULL) {
            return;
        }
        r->path = strdup(path);
        r->bytes = malloc(size);
        if (r->path == NULL || r->bytes == NULL) {
            free(r->path);
            free(r->bytes);
            free(r);
            return;
        }
        memcpy(r->bytes, bytes, size);
        r->size = size;
        // Kept in declaration order: an exchange and a queue before their binding.
        rmq_record **end = &c->records;
        while (*end) {
            end = &(*end)->next;
        }
        *end = r;
    }
}

static void rmq_link_remove(rmq_conn *c, rmq_link *l) {
    for (rmq_link **at = &c->links; *at != NULL; at = &(*at)->reg_next) {
        if (*at == l) {
            *at = l->reg_next;
            rmq_link_unref(l);
            return;
        }
    }
}

// Everything the old connection held is gone: its links, deliveries and
// requests in flight. What can be tried again waits for the next one.
static void rmq_lost(rmq_conn *c, const char *why) {
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        l->pn = NULL;
        l->session = NULL;
        l->generation++;
        if (l->kind == RMQ_LINK_PUBLISHER) {
            rmq_out *o = l->unsettled_head;
            l->unsettled_head = l->unsettled_tail = NULL;
            if (c->resend && !c->closing) {
                // Back to the front of the queue, in the order they were sent.
                rmq_out *last = o;
                while (last && last->next) {
                    last = last->next;
                }
                pthread_mutex_lock(&l->mu);
                if (last != NULL) {
                    last->next = l->pending_head;
                    l->pending_head = o;
                    if (l->pending_tail == NULL) {
                        l->pending_tail = last;
                    }
                }
                pthread_mutex_unlock(&l->mu);
                for (rmq_out *p = o; p != NULL; p = p->next) {
                    p->prev = NULL;
                    p->delivery = NULL;
                }
            } else {
                while (o != NULL) {
                    rmq_out *next = o->next;
                    rmq_out_done(l, o, RMQ_OUTCOME_FAILED, why);
                    o = next;
                }
            }
        } else {
            // Unread messages are the broker's again; it redelivers them.
            pthread_mutex_lock(&l->mu);
            rmq_in *in = l->buf_head;
            l->buf_head = l->buf_tail = NULL;
            l->buffered = 0;
            pthread_mutex_unlock(&l->mu);
            while (in != NULL) {
                rmq_in *next = in->next;
                free(in->bytes);
                free(in);
                rmq_link_unref(l);
                in = next;
            }
        }
    }
    // Requests not answered go again on the next connection; replays are rebuilt.
    // RabbitMQ ends the whole session over a request it cannot handle, so one
    // that was out when two connections in a row died is the likely cause:
    // fail it, rather than take every connection down with it.
    for (rmq_request **at = &c->requests; *at != NULL;) {
        rmq_request *r = *at;
        if (r->ticket == NULL) {
            *at = r->next;
            free(r->bytes);
            free(r);
        } else if (r->sent && ++r->lost >= 2) {
            *at = r->next;
            char reason[600];
            snprintf(reason, sizeof reason, "the broker closed the session over this request: %s", why);
            rmq_ticket_finish(r->ticket, RMQ_ERR_LINK, 0, reason);
            free(r->bytes);
            free(r);
        } else {
            r->sent = 0;
            at = &r->next;
        }
    }
    c->pn = NULL;
    c->session = NULL;
    c->spares = 0;
    c->mgmt_out = c->mgmt_in = NULL;
    c->mgmt_out_open = c->mgmt_in_open = 0;
    c->phase = 0;
}

static void rmq_connect_next(rmq_conn *c) {
    const rmq_endpoint *e = &c->endpoints[c->next_endpoint];
    c->next_endpoint = (c->next_endpoint + 1) % c->endpoint_count;

    pn_connection_t *pc = pn_connection();
    pn_connection_set_container(pc, c->container);
    char host[300];
    snprintf(host, sizeof host, "vhost:%s", e->vhost);
    pn_connection_set_hostname(pc, host);
    if (e->user[0] != '\0') {
        pn_connection_set_user(pc, e->user);
        pn_connection_set_password(pc, e->password);
    }
    pn_connection_set_context(pc, c);

    pn_transport_t *t = pn_transport();
    pn_transport_set_idle_timeout(t, (pn_millis_t)c->idle_timeout_ms);
    pn_sasl_t *sasl = pn_sasl(t);
    pn_sasl_allowed_mechs(sasl, e->user[0] != '\0' ? "PLAIN" : "ANONYMOUS");
    pn_sasl_set_allow_insecure_mechs(sasl, true);
    if (e->tls) {
        pn_ssl_domain_t *dom = pn_ssl_domain(PN_SSL_MODE_CLIENT);
        if (c->ca_file[0] != '\0') {
            pn_ssl_domain_set_trusted_ca_db(dom, c->ca_file);
        }
        pn_ssl_domain_set_peer_authentication(dom, c->verify ? PN_SSL_VERIFY_PEER_NAME : PN_SSL_ANONYMOUS_PEER, NULL);
        pn_ssl_t *ssl = pn_ssl(t);
        pn_ssl_init(ssl, dom, NULL);
        pn_ssl_set_peer_hostname(ssl, e->host);
        pn_ssl_domain_free(dom);
    }

    c->pn = pc;
    c->phase = 1;
    pthread_mutex_lock(&c->mu);
    c->wake_conn = pc;
    pthread_mutex_unlock(&c->mu);
    char addr[300];
    pn_proactor_addr(addr, sizeof addr, e->host, e->port);
    pn_proactor_connect2(c->proactor, pc, t, addr);
}

static void rmq_schedule_reconnect(rmq_conn *c) {
    c->attempt++;
    if (c->max_attempts > 0 && c->attempt > c->max_attempts) {
        rmq_event(c, RMQ_EVENT_FAILED, "gave up reconnecting", c->reason);
        c->closing = 1;
        char why[600];
        snprintf(why, sizeof why, "gave up reconnecting: %s", c->reason);
        rmq_closed_for_good(c, why);
        return;
    }
    int delay = c->backoff_ms;
    // Jitter of up to a quarter either way, so a cluster's clients spread out.
    int spread = delay / 4;
    if (spread > 0) {
        delay += (int)(arc4random_uniform((uint32_t)(2 * spread + 1))) - spread;
    }
    c->backoff_ms = c->backoff_ms * 2 > c->backoff_max_ms ? c->backoff_max_ms : c->backoff_ms * 2;
    c->reconnect_at = rmq_now_ms() + delay;
}

static void rmq_finish_all(rmq_conn *c, const char *why);

static void rmq_wake_waiters(rmq_conn *c) {
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        pthread_mutex_lock(&l->mu);
        pthread_cond_broadcast(&l->space);
        pthread_cond_broadcast(&l->drained);
        pthread_cond_broadcast(&l->report_ready);
        pthread_mutex_unlock(&l->mu);
    }
}

// Closed for good: by the program, by bad credentials, or by giving up. The
// state goes first, so a publisher checking it under its lock either gets in
// before the messages are failed below, or sees it and refuses.
static void rmq_closed_for_good(rmq_conn *c, const char *why) {
    rmq_set_state(c, RMQ_STATE_CLOSED, why);
    rmq_finish_all(c, why);
    rmq_wake_waiters(c);
}

static void rmq_expire(rmq_conn *c) {
    long now = rmq_now_ms();
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        if (l->kind != RMQ_LINK_PUBLISHER) {
            continue;
        }
        // Waiting to be sent: fail it.
        for (;;) {
            pthread_mutex_lock(&l->mu);
            rmq_out *o = l->pending_head;
            if (o == NULL || o->deadline > now) {
                pthread_mutex_unlock(&l->mu);
                break;
            }
            l->pending_head = o->next;
            if (l->pending_head == NULL) {
                l->pending_tail = NULL;
            }
            pthread_mutex_unlock(&l->mu);
            rmq_out_done(l, o, RMQ_OUTCOME_FAILED, "timed out waiting to be sent");
        }
        // Sent and never answered: stop waiting. The broker may have it.
        while (l->unsettled_head != NULL && l->unsettled_head->deadline <= now) {
            rmq_out *o = l->unsettled_head;
            rmq_unsettled_remove(l, o);
            if (o->delivery != NULL) {
                pn_delivery_set_context(o->delivery, NULL);
                pn_delivery_settle(o->delivery);
            }
            rmq_out_done(l, o, RMQ_OUTCOME_FAILED, "timed out waiting for the broker's answer; it may have the message");
        }
    }
    for (rmq_request **at = &c->requests; *at != NULL;) {
        rmq_request *r = *at;
        if (r->ticket != NULL && r->deadline <= now) {
            *at = r->next;
            rmq_ticket_finish(r->ticket, RMQ_ERR_TIMEOUT, 0, "the management request timed out");
            free(r->bytes);
            free(r);
        } else {
            at = &r->next;
        }
    }
}

static void rmq_handle_op(rmq_conn *c, rmq_op *op) {
    switch (op->kind) {
    case RMQ_OP_ATTACH:
        op->link->reg_next = c->links;
        c->links = op->link; // the registry's reference was taken when posted
        if (c->phase == 3) {
            rmq_attach(c, op->link);
        }
        break;
    case RMQ_OP_DETACH: {
        rmq_link *l = op->link;
        if (l->pn != NULL) {
            // Its session goes back to the spares once the broker detaches it.
            pn_link_set_context(l->pn, &rmq_closing_link);
            pn_link_close(l->pn);
            l->pn = NULL;
        }
        if (l->session != NULL) {
            pn_session_set_context(l->session, NULL);
            l->session = NULL;
        }
        l->generation++;
        if (l->kind == RMQ_LINK_CONSUMER) {
            // Unread messages go back to the broker with the link.
            pthread_mutex_lock(&l->mu);
            rmq_in *in = l->buf_head;
            l->buf_head = l->buf_tail = NULL;
            l->buffered = 0;
            pthread_cond_broadcast(&l->space);
            pthread_mutex_unlock(&l->mu);
            while (in != NULL) {
                rmq_in *next = in->next;
                free(in->bytes);
                free(in);
                rmq_link_unref(l);
                in = next;
            }
        }
        // Whatever it still had is finished: messages fail, a buffer is dropped.
        rmq_out *o = l->unsettled_head;
        l->unsettled_head = l->unsettled_tail = NULL;
        while (o != NULL) {
            rmq_out *next = o->next;
            if (o->delivery) {
                pn_delivery_set_context(o->delivery, NULL);
            }
            rmq_out_done(l, o, RMQ_OUTCOME_FAILED, "the publisher was closed");
            o = next;
        }
        for (;;) {
            pthread_mutex_lock(&l->mu);
            rmq_out *p = l->pending_head;
            if (p) {
                l->pending_head = p->next;
                if (!l->pending_head) {
                    l->pending_tail = NULL;
                }
            }
            pthread_mutex_unlock(&l->mu);
            if (!p) {
                break;
            }
            rmq_out_done(l, p, RMQ_OUTCOME_FAILED, "the publisher was closed");
        }
        rmq_link_remove(c, l);
        break;
    }
    case RMQ_OP_SETTLE: {
        rmq_link *l = op->link;
        if (l->pn != NULL && op->generation == l->generation && op->delivery != NULL) {
            if (op->settle == RMQ_SETTLE_FAILED) {
                pn_disposition_set_failed(pn_delivery_local(op->delivery), true);
                pn_delivery_update(op->delivery, PN_MODIFIED);
            } else {
                pn_delivery_update(op->delivery, op->settle == RMQ_SETTLE_ACCEPT ? PN_ACCEPTED : op->settle == RMQ_SETTLE_REQUEUE ? PN_RELEASED : PN_REJECTED);
            }
            pn_delivery_settle(op->delivery);
        }
        rmq_top_up(l);
        rmq_link_unref(l);
        break;
    }
    case RMQ_OP_REQUEST:
        if (op->ticket != NULL && c->closing && c->pn == NULL) {
            rmq_ticket_finish(op->ticket, RMQ_ERR_CLOSED, 0, "the connection is closed");
            break;
        }
        if (op->record == 1) {
            rmq_remember(c, op->path, op->bytes, op->size, 1);
        } else if (op->record == 2) {
            rmq_remember(c, op->path, NULL, 0, 2);
        }
        if (op->ticket != NULL) {
            rmq_add_request(c, op->id, op->bytes, op->size, op->deadline, op->ticket);
            op->bytes = NULL;
            rmq_send_requests(c);
        }
        break;
    case RMQ_OP_PUMP:
        if (op->link != NULL) {
            if (op->link->kind == RMQ_LINK_PUBLISHER) {
                rmq_pump_publisher(c, op->link);
            } else {
                rmq_top_up(op->link);
            }
            rmq_link_unref(op->link);
        }
        break;
    case RMQ_OP_CLOSE:
        if (!c->closing) {
            c->closing = 1;
            if (c->pn != NULL) {
                pn_connection_close(c->pn);
            } else {
                rmq_closed_for_good(c, "the connection is closed");
            }
        }
        break;
    case RMQ_OP_SHUTDOWN:
        c->shutdown = 1;
        if (!c->closing) {
            c->closing = 1;
            if (c->pn != NULL) {
                pn_connection_close(c->pn);
            } else {
                rmq_closed_for_good(c, "the connection is closed");
            }
        }
        break;
    }
    free(op->bytes);
    free(op->path);
    free(op);
}

static void rmq_apply_settles(rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    rmq_settlement *batch = l->settles;
    int n = l->settles_len;
    l->settles = NULL;
    l->settles_len = l->settles_cap = 0;
    pthread_mutex_unlock(&l->mu);
    unsigned long generation = atomic_load(&l->generation);
    for (int i = 0; i < n; i++) {
        rmq_settlement *s = &batch[i];
        // A settlement from before a reconnect names a delivery that is gone.
        if (l->pn == NULL || s->generation != generation) {
            continue;
        }
        if (s->how == RMQ_SETTLE_FAILED) {
            pn_disposition_set_failed(pn_delivery_local(s->delivery), true);
            pn_delivery_update(s->delivery, PN_MODIFIED);
        } else {
            pn_delivery_update(s->delivery, s->how == RMQ_SETTLE_ACCEPT ? PN_ACCEPTED : s->how == RMQ_SETTLE_REQUEUE ? PN_RELEASED : PN_REJECTED);
        }
        pn_delivery_settle(s->delivery);
    }
    free(batch);
}

static void rmq_drain_ops(rmq_conn *c) {
    atomic_store(&c->woken, 0);
    pthread_mutex_lock(&c->mu);
    rmq_op *op = c->ops_head;
    c->ops_head = c->ops_tail = NULL;
    pthread_mutex_unlock(&c->mu);
    while (op != NULL) {
        rmq_op *next = op->next;
        rmq_handle_op(c, op);
        op = next;
    }
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        if (l->kind == RMQ_LINK_PUBLISHER) {
            rmq_pump_publisher(c, l);
        } else {
            rmq_apply_settles(l);
            rmq_top_up(l);
        }
    }
}

static const char *rmq_condition_text(pn_condition_t *cond, char *buf, size_t n) {
    if (cond == NULL || !pn_condition_is_set(cond)) {
        snprintf(buf, n, "the connection closed");
    } else {
        const char *desc = pn_condition_get_description(cond);
        snprintf(buf, n, "%s%s%s", pn_condition_get_name(cond), desc ? ": " : "", desc ? desc : "");
    }
    return buf;
}

static void rmq_on_delivery(rmq_conn *c, pn_delivery_t *d) {
    pn_link_t *pl = pn_delivery_link(d);
    if (pl == c->mgmt_in) {
        if (!pn_delivery_readable(d) || pn_delivery_partial(d)) {
            return;
        }
        size_t n = pn_delivery_pending(d);
        char *buf = malloc(n);
        if (buf != NULL) {
            pn_link_recv(pl, buf, n);
            pn_link_advance(pl);
            rmq_on_reply(c, buf, n);
            free(buf);
        }
        pn_delivery_settle(d);
        if (pn_link_credit(pl) < 50) {
            pn_link_flow(pl, 100 - pn_link_credit(pl));
        }
        return;
    }
    if (pl == c->mgmt_out) {
        return;
    }
    rmq_link *l = pn_link_get_context(pl);
    if (l == NULL) {
        if (pn_link_is_receiver(pl) && pn_delivery_readable(d) && !pn_delivery_partial(d)) {
            pn_link_advance(pl);
            pn_delivery_update(d, PN_RELEASED);
            pn_delivery_settle(d);
        }
        return;
    }

    if (l->kind == RMQ_LINK_PUBLISHER) {
        if (!pn_delivery_updated(d)) {
            return;
        }
        rmq_out *o = pn_delivery_get_context(d);
        uint64_t state = pn_delivery_remote_state(d);
        if (o == NULL || state == 0 || state == PN_RECEIVED) {
            return;
        }
        int outcome = RMQ_OUTCOME_FAILED;
        char reason[256] = "";
        switch (state) {
        case PN_ACCEPTED:
            outcome = RMQ_OUTCOME_ACCEPTED;
            break;
        case PN_RELEASED:
            outcome = RMQ_OUTCOME_RELEASED;
            snprintf(reason, sizeof reason, "released: no queue took it");
            break;
        case PN_MODIFIED:
            outcome = RMQ_OUTCOME_MODIFIED;
            snprintf(reason, sizeof reason, "modified by the broker");
            break;
        case PN_REJECTED:
            outcome = RMQ_OUTCOME_REJECTED;
            rmq_condition_text(pn_disposition_condition(pn_delivery_remote(d)), reason, sizeof reason);
            break;
        }
        pn_delivery_set_context(d, NULL);
        pn_delivery_settle(d);
        rmq_unsettled_remove(l, o);
        rmq_out_done(l, o, outcome, reason);
        rmq_pump_publisher(c, l);
        return;
    }

    if (!pn_delivery_readable(d) || pn_delivery_partial(d)) {
        return;
    }
    size_t n = pn_delivery_pending(d);
    rmq_in *in = calloc(1, sizeof *in);
    char *buf = malloc(n > 0 ? n : 1);
    if (in == NULL || buf == NULL) {
        free(in);
        free(buf);
        pn_link_advance(pl);
        pn_delivery_update(d, PN_RELEASED);
        pn_delivery_settle(d);
        return;
    }
    pn_link_recv(pl, buf, n);
    pn_link_advance(pl);
    in->bytes = buf;
    in->size = n;
    in->delivery = d;
    in->generation = l->generation;
    in->link = l;
    atomic_fetch_add(&l->refs, 1);
    pthread_mutex_lock(&l->mu);
    if (l->buf_tail) {
        l->buf_tail->next = in;
    } else {
        l->buf_head = in;
    }
    l->buf_tail = in;
    l->buffered++;
    pthread_cond_signal(&l->space);
    pthread_mutex_unlock(&l->mu);
}

// The broker took a link away, or its session: it will deliver nothing more,
// and nothing more can be sent on it, until a reconnect attaches it again.
static void rmq_link_failed(rmq_conn *c, rmq_link *l, const char *why) {
    pthread_mutex_lock(&l->mu);
    snprintf(l->failed, sizeof l->failed, "%s", why);
    pthread_cond_broadcast(&l->space);
    pthread_cond_broadcast(&l->drained);
    pthread_mutex_unlock(&l->mu);
    rmq_event(c, RMQ_EVENT_LINK, l->kind == RMQ_LINK_PUBLISHER ? "a publisher was detached by the broker" : "a consumer was detached by the broker", why);
    l->pn = NULL;
    l->generation++;
    if (l->session != NULL) {
        pn_session_set_context(l->session, NULL);
        pn_session_close(l->session);
        l->session = NULL;
    }
    // Its messages cannot go anywhere now. Fail them, rather than wait.
    if (l->kind != RMQ_LINK_PUBLISHER) {
        return;
    }
    rmq_out *o = l->unsettled_head;
    l->unsettled_head = l->unsettled_tail = NULL;
    while (o) {
        rmq_out *next = o->next;
        rmq_out_done(l, o, RMQ_OUTCOME_FAILED, why);
        o = next;
    }
    for (;;) {
        pthread_mutex_lock(&l->mu);
        rmq_out *p = l->pending_head;
        if (p) {
            l->pending_head = p->next;
            if (!l->pending_head) {
                l->pending_tail = NULL;
            }
        }
        pthread_mutex_unlock(&l->mu);
        if (!p) {
            break;
        }
        rmq_out_done(l, p, RMQ_OUTCOME_FAILED, why);
    }
}

static void rmq_handle(rmq_conn *c, pn_event_t *e) {
    char why[512];
    switch (pn_event_type(e)) {
    case PN_CONNECTION_INIT: {
        pn_connection_t *pc = pn_event_connection(e);
        pn_connection_open(pc);
        c->session = pn_session(pc);
        pn_session_open(c->session);
        c->mgmt_out = pn_sender(c->session, "management-link-pair");
        pn_terminus_set_address(pn_link_target(c->mgmt_out), "/management");
        pn_terminus_set_address(pn_link_source(c->mgmt_out), "/management");
        pn_link_set_snd_settle_mode(c->mgmt_out, PN_SND_SETTLED);
        pn_link_set_rcv_settle_mode(c->mgmt_out, PN_RCV_FIRST);
        rmq_put_paired(c->mgmt_out);
        pn_link_open(c->mgmt_out);
        c->mgmt_in = pn_receiver(c->session, "management-link-pair");
        pn_terminus_set_address(pn_link_source(c->mgmt_in), "/management");
        pn_terminus_set_address(pn_link_target(c->mgmt_in), "/management");
        pn_link_set_snd_settle_mode(c->mgmt_in, PN_SND_SETTLED);
        pn_link_set_rcv_settle_mode(c->mgmt_in, PN_RCV_FIRST);
        rmq_put_paired(c->mgmt_in);
        pn_link_open(c->mgmt_in);
        pn_link_flow(c->mgmt_in, 100);
        break;
    }
    case PN_LINK_REMOTE_OPEN: {
        pn_link_t *pl = pn_event_link(e);
        if (pl == c->mgmt_out) {
            c->mgmt_out_open = 1;
        } else if (pl == c->mgmt_in) {
            c->mgmt_in_open = 1;
        }
        if ((pl == c->mgmt_out || pl == c->mgmt_in) && c->mgmt_out_open && c->mgmt_in_open && c->phase == 1) {
            rmq_replay(c);
        }
        break;
    }
    case PN_LINK_FLOW: {
        pn_link_t *pl = pn_event_link(e);
        if (pl == c->mgmt_out) {
            rmq_send_requests(c);
        } else {
            rmq_link *l = pn_link_get_context(pl);
            if (l != NULL && l->kind == RMQ_LINK_PUBLISHER) {
                rmq_pump_publisher(c, l);
            } else if (l != NULL && l->draining && pn_link_credit(pl) == 0) {
                // The broker has drained it: done pausing, or ready to resume.
                pn_link_set_drain(pl, false);
                l->draining = 0;
                rmq_top_up(l);
            }
        }
        break;
    }
    case PN_DELIVERY:
        rmq_on_delivery(c, pn_event_delivery(e));
        break;
    case PN_LINK_REMOTE_CLOSE:
    case PN_LINK_REMOTE_DETACH: {
        pn_link_t *pl = pn_event_link(e);
        if (pn_link_get_context(pl) == &rmq_closing_link) {
            pn_session_t *ps = pn_link_session(pl);
            pn_link_set_context(pl, NULL);
            pn_link_free(pl);
            if (c->spares < 64 && pn_session_get_context(ps) == NULL && !(pn_session_state(ps) & PN_REMOTE_CLOSED)) {
                c->spare[c->spares++] = ps;
            } else {
                pn_session_close(ps);
            }
            break;
        }
        rmq_link *l = pn_link_get_context(pl);
        rmq_condition_text(pn_link_remote_condition(pl), why, sizeof why);
        if (pl == c->mgmt_out || pl == c->mgmt_in) {
            // Without its management link a connection is no use; start again.
            if (c->pn != NULL) {
                pn_connection_close(c->pn);
            }
            break;
        }
        if (l != NULL && l->pn == pl) {
            rmq_link_failed(c, l, why);
        }
        pn_link_set_context(pl, NULL);
        pn_link_close(pl);
        break;
    }
    case PN_SESSION_REMOTE_CLOSE: {
        pn_session_t *ps = pn_event_session(e);
        if (ps == c->session) {
            if (c->pn != NULL) {
                pn_connection_close(c->pn);
            }
            break;
        }
        for (int i = 0; i < c->spares; i++) {
            if (c->spare[i] == ps) {
                c->spare[i] = c->spare[--c->spares];
                break;
            }
        }
        rmq_link *l = pn_session_get_context(ps);
        if (l != NULL && l->session == ps) {
            rmq_condition_text(pn_session_remote_condition(ps), why, sizeof why);
            if (l->pn != NULL) {
                pn_link_set_context(l->pn, NULL);
            }
            rmq_link_failed(c, l, why);
        }
        pn_session_set_context(ps, NULL);
        pn_session_close(ps);
        pn_session_free(ps);
        break;
    }
    case PN_CONNECTION_REMOTE_CLOSE: {
        pn_connection_t *pc = pn_event_connection(e);
        rmq_condition_text(pn_connection_remote_condition(pc), why, sizeof why);
        pthread_mutex_lock(&c->mu);
        snprintf(c->reason, sizeof c->reason, "%s", why);
        pthread_mutex_unlock(&c->mu);
        pn_connection_close(pc);
        break;
    }
    case PN_TRANSPORT_CLOSED: {
        pthread_mutex_lock(&c->mu);
        c->wake_conn = NULL;
        pthread_mutex_unlock(&c->mu);
        pn_transport_t *t = pn_event_transport(e);
        pn_condition_t *tc = pn_transport_condition(t);
        if (tc != NULL && pn_condition_is_set(tc)) {
            rmq_condition_text(tc, why, sizeof why);
        } else {
            pthread_mutex_lock(&c->mu);
            snprintf(why, sizeof why, "%s", c->reason[0] ? c->reason : "the connection closed");
            pthread_mutex_unlock(&c->mu);
        }
        int auth = strstr(why, "unauthorized") != NULL || strstr(why, "SASL") != NULL || strstr(why, "authentication") != NULL;
        rmq_lost(c, why);
        if (c->closing) {
            rmq_closed_for_good(c, "the connection is closed");
            break;
        }
        pthread_mutex_lock(&c->mu);
        snprintf(c->reason, sizeof c->reason, "%s", why);
        pthread_mutex_unlock(&c->mu);
        if (auth && !c->ever_opened) {
            // Wrong credentials do not get better with retrying.
            pthread_mutex_lock(&c->mu);
            c->fatal = RMQ_ERR_AUTH;
            pthread_mutex_unlock(&c->mu);
            c->closing = 1;
            rmq_closed_for_good(c, why);
            break;
        }
        rmq_event(c, RMQ_EVENT_DOWN, "disconnected", why);
        rmq_set_state(c, c->ever_opened ? RMQ_STATE_RECONNECTING : RMQ_STATE_CONNECTING, why);
        rmq_schedule_reconnect(c);
        break;
    }
    default:
        break;
    }
}

static void rmq_finish_all(rmq_conn *c, const char *why) {
    for (rmq_link *l = c->links; l != NULL; l = l->reg_next) {
        if (l->kind != RMQ_LINK_PUBLISHER) {
            continue;
        }
        rmq_out *o = l->unsettled_head;
        l->unsettled_head = l->unsettled_tail = NULL;
        while (o) {
            rmq_out *next = o->next;
            rmq_out_done(l, o, RMQ_OUTCOME_FAILED, why);
            o = next;
        }
        for (;;) {
            pthread_mutex_lock(&l->mu);
            rmq_out *p = l->pending_head;
            if (p) {
                l->pending_head = p->next;
                if (!l->pending_head) {
                    l->pending_tail = NULL;
                }
            }
            pthread_mutex_unlock(&l->mu);
            if (!p) {
                break;
            }
            rmq_out_done(l, p, RMQ_OUTCOME_FAILED, why);
        }
    }
    while (c->requests) {
        rmq_request *r = c->requests;
        c->requests = r->next;
        if (r->ticket) {
            rmq_ticket_finish(r->ticket, RMQ_ERR_CLOSED, 0, why);
        }
        free(r->bytes);
        free(r);
    }
}

static void *rmq_thread(void *arg) {
    rmq_conn *c = arg;
    rmq_connect_next(c);
    for (;;) {
        // The timer serves reconnecting and expiring both: the sooner of the two.
        long now = rmq_now_ms();
        long wait = 1000;
        if (c->pn == NULL && !c->closing) {
            wait = c->reconnect_at - now;
        }
        if (wait < 1) {
            wait = 1;
        }
        if (wait > 1000) {
            wait = 1000;
        }
        pn_proactor_set_timeout(c->proactor, (pn_millis_t)wait);

        pn_event_batch_t *b = pn_proactor_wait(c->proactor);
        pn_event_t *e;
        while ((e = pn_event_batch_next(b)) != NULL) {
            switch (pn_event_type(e)) {
            case PN_PROACTOR_INTERRUPT:
            case PN_PROACTOR_TIMEOUT:
                // Not the connection's context: with one, go through it.
                if (c->pn != NULL) {
                    pn_connection_wake(c->pn);
                } else {
                    rmq_drain_ops(c);
                    rmq_expire(c);
                }
                break;
            case PN_CONNECTION_WAKE:
                rmq_drain_ops(c);
                rmq_expire(c);
                break;
            default:
                rmq_handle(c, e);
            }
        }
        pn_proactor_done(c->proactor, b);

        if (c->pn == NULL && !c->closing && c->reconnect_at != 0 && rmq_now_ms() >= c->reconnect_at) {
            rmq_set_state(c, c->ever_opened ? RMQ_STATE_RECONNECTING : RMQ_STATE_CONNECTING, NULL);
            c->reconnect_at = 0;
            rmq_connect_next(c);
        }
        if (c->shutdown && c->pn == NULL) {
            break;
        }
    }
    return NULL;
}

// ---- The connection, from Dex ----

static void rmq_shim_conn_wait_closed(rmq_conn *c) {
    pthread_mutex_lock(&c->mu);
    while (c->state != RMQ_STATE_CLOSED) {
        pthread_cond_wait(&c->state_changed, &c->mu);
    }
    pthread_mutex_unlock(&c->mu);
}

static inline rmq_conn *rmq_shim_conn_new(void) {
    rmq_conn *c = calloc(1, sizeof *c);
    if (c == NULL) {
        return NULL;
    }
    atomic_store(&c->refs, 1);
    pthread_mutex_init(&c->mu, NULL);
    pthread_cond_init(&c->state_changed, NULL);
    c->idle_timeout_ms = 30000;
    c->connect_timeout_ms = 30000;
    c->publish_timeout_ms = 30000;
    c->backoff_initial_ms = 500;
    c->backoff_max_ms = 30000;
    c->resend = 1;
    c->verify = 1;
    c->state = RMQ_STATE_CONNECTING;
    snprintf(c->container, sizeof c->container, "dex-%08x%08x", arc4random(), arc4random());
    return c;
}

static inline int rmq_shim_conn_endpoint(rmq_conn *c, const char *host, const char *port, const char *user, const char *password, const char *vhost, bool tls) {
    if (c->endpoint_count == RMQ_ENDPOINTS) {
        return rmq_fail(RMQ_ERR_INVALID, "at most 16 endpoints%s%s", NULL, NULL);
    }
    rmq_endpoint *e = &c->endpoints[c->endpoint_count++];
    snprintf(e->host, sizeof e->host, "%s", host);
    snprintf(e->port, sizeof e->port, "%s", port);
    snprintf(e->user, sizeof e->user, "%s", user);
    snprintf(e->password, sizeof e->password, "%s", password);
    snprintf(e->vhost, sizeof e->vhost, "%s", vhost);
    e->tls = tls;
    return 0;
}

static inline void rmq_shim_conn_settings(rmq_conn *c, const char *container, int idleMs, int connectMs, int publishMs, int backoffMs, int backoffMaxMs, int maxAttempts, bool resend) {
    if (container[0] != '\0') {
        snprintf(c->container, sizeof c->container, "%s", container);
    }
    c->idle_timeout_ms = idleMs;
    c->connect_timeout_ms = connectMs;
    c->publish_timeout_ms = publishMs;
    c->backoff_initial_ms = backoffMs;
    c->backoff_max_ms = backoffMaxMs;
    c->max_attempts = maxAttempts;
    c->resend = resend;
}

static inline void rmq_shim_conn_tls(rmq_conn *c, const char *caFile, bool verify) {
    snprintf(c->ca_file, sizeof c->ca_file, "%s", caFile);
    c->verify = verify;
}

// Starts the I/O thread and waits up to the connect timeout for the first open.
// A network failure is retried in that time; bad credentials end it at once.
static inline int rmq_shim_conn_start(rmq_conn *c) {
    c->backoff_ms = c->backoff_initial_ms;
    c->proactor = pn_proactor();
    if (c->proactor == NULL) {
        return rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
    }
    if (pthread_create(&c->thread, NULL, rmq_thread, c) != 0) {
        return rmq_fail(RMQ_ERR_MEMORY, "could not start the connection's thread%s%s", NULL, NULL);
    }
    c->has_thread = 1;
    struct timespec until;
    rmq_deadline(&until, c->connect_timeout_ms);
    pthread_mutex_lock(&c->mu);
    while (c->state != RMQ_STATE_OPEN && c->state != RMQ_STATE_CLOSED) {
        if (pthread_cond_timedwait(&c->state_changed, &c->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    int state = c->state;
    int fatal = c->fatal;
    char reason[512];
    snprintf(reason, sizeof reason, "%s", c->reason);
    pthread_mutex_unlock(&c->mu);
    if (state == RMQ_STATE_OPEN) {
        return 0;
    }
    return rmq_fail(fatal ? fatal : (state == RMQ_STATE_CLOSED ? RMQ_ERR_CONNECT : RMQ_ERR_TIMEOUT),
        fatal ? "authentication failed: %s%s" : "could not connect: %s%s", reason[0] ? reason : "no answer", NULL);
}

// Closing is final. It waits until the connection is closed, and the thread
// stays to answer anything sent afterwards with "closed".
static inline void rmq_shim_conn_close(rmq_conn *c) {
    if (!c->has_thread) {
        return;
    }
    rmq_op *op = rmq_op_new(RMQ_OP_CLOSE);
    if (op != NULL) {
        rmq_post(c, op);
    }
    rmq_shim_conn_wait_closed(c);
}

static void rmq_conn_unref(rmq_conn *c) {
    if (atomic_fetch_sub(&c->refs, 1) != 1) {
        return;
    }
    // No Dex handle is left to post anything, so the thread can stop.
    if (c->has_thread) {
        rmq_op *op = rmq_op_new(RMQ_OP_SHUTDOWN);
        if (op != NULL) {
            rmq_post(c, op);
        }
        pthread_join(c->thread, NULL);
    }
    while (c->links != NULL) {
        rmq_link *l = c->links;
        c->links = l->reg_next;
        rmq_link_destroy(l);
    }
    while (c->records) {
        rmq_record *r = c->records;
        c->records = r->next;
        free(r->path);
        free(r->bytes);
        free(r);
    }
    while (c->requests) {
        rmq_request *r = c->requests;
        c->requests = r->next;
        free(r->bytes);
        free(r);
    }
    rmq_op *op = c->ops_head;
    while (op) {
        rmq_op *next = op->next;
        free(op->bytes);
        free(op->path);
        free(op);
        op = next;
    }
    if (c->proactor) {
        pn_proactor_free(c->proactor);
    }
    pthread_cond_destroy(&c->state_changed);
    pthread_mutex_destroy(&c->mu);
    free(c);
}

static inline void rmq_shim_conn_free(rmq_conn *c) {
    rmq_conn_unref(c);
}

static inline int rmq_shim_conn_state(rmq_conn *c) {
    pthread_mutex_lock(&c->mu);
    int s = c->state;
    pthread_mutex_unlock(&c->mu);
    return s;
}

static inline long rmq_shim_conn_reconnects(rmq_conn *c) {
    return atomic_load(&c->reconnects);
}

static inline const char *rmq_shim_conn_container(rmq_conn *c) {
    return c->container;
}

// Waits up to timeoutMs for the connection to be open, as after an outage.
static inline bool rmq_shim_conn_wait_open(rmq_conn *c, int timeoutMs) {
    struct timespec until;
    rmq_deadline(&until, timeoutMs);
    pthread_mutex_lock(&c->mu);
    while (c->state != RMQ_STATE_OPEN && c->state != RMQ_STATE_CLOSED) {
        if (pthread_cond_timedwait(&c->state_changed, &c->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    bool open = c->state == RMQ_STATE_OPEN;
    pthread_mutex_unlock(&c->mu);
    return open;
}

static inline bool rmq_shim_event_peek(rmq_conn *c) {
    pthread_mutex_lock(&c->mu);
    bool any = c->events_len > 0;
    pthread_mutex_unlock(&c->mu);
    return any;
}

// The oldest event's kind; its text becomes the last message.
static inline int rmq_shim_event_take(rmq_conn *c) {
    pthread_mutex_lock(&c->mu);
    int kind = -1;
    if (c->events_len > 0) {
        kind = c->event_kinds[0];
        snprintf(rmq_last.message, sizeof rmq_last.message, "%s", c->events[0]);
        memmove(c->events[0], c->events[1], sizeof(c->events[0]) * (size_t)(c->events_len - 1));
        memmove(&c->event_kinds[0], &c->event_kinds[1], sizeof(c->event_kinds[0]) * (size_t)(c->events_len - 1));
        c->events_len--;
    }
    pthread_mutex_unlock(&c->mu);
    return kind;
}

static inline bool rmq_shim_conn_closed(rmq_conn *c) {
    return rmq_shim_conn_state(c) == RMQ_STATE_CLOSED;
}

// ---- Links, from Dex ----

static rmq_link *rmq_link_new(rmq_conn *c, int kind, const char *address, int window) {
    rmq_link *l = calloc(1, sizeof *l);
    if (l == NULL) {
        return NULL;
    }
    l->address = strdup(address);
    if (l->address == NULL) {
        free(l);
        return NULL;
    }
    // One for Dex's handle, one for the I/O thread's registry.
    atomic_store(&l->refs, 2);
    l->c = c;
    atomic_fetch_add(&c->refs, 1);
    l->kind = kind;
    l->window = window > 0 ? window : 256;
    static atomic_long ids;
    l->id = atomic_fetch_add(&ids, 1) + 1;
    snprintf(l->name, sizeof l->name, "%s-%s-%ld", c->container, kind == RMQ_LINK_PUBLISHER ? "out" : "in", l->id);
    pthread_mutex_init(&l->mu, NULL);
    pthread_cond_init(&l->space, NULL);
    pthread_cond_init(&l->drained, NULL);
    pthread_cond_init(&l->report_ready, NULL);
    return l;
}

static int rmq_link_start(rmq_link *l) {
    rmq_op *op = rmq_op_new(RMQ_OP_ATTACH);
    if (op == NULL) {
        return RMQ_ERR_MEMORY;
    }
    op->link = l;
    rmq_post(l->c, op);
    return 0;
}

// The address a publisher sends to: "/queues/q", "/exchanges/e/key", or "" for
// a publisher whose messages each name their own.
static inline rmq_link *rmq_shim_publisher_new(rmq_conn *c, const char *address, int window) {
    if (rmq_shim_conn_closed(c)) {
        rmq_fail(RMQ_ERR_CLOSED, "the connection is closed%s%s", NULL, NULL);
        return NULL;
    }
    rmq_link *l = rmq_link_new(c, RMQ_LINK_PUBLISHER, address, window);
    if (l == NULL) {
        rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
        return NULL;
    }
    if (address[0] == '\0') {
        free(l->address);
        l->address = NULL;
    }
    rmq_link_start(l);
    return l;
}

static inline rmq_link *rmq_shim_consumer_new(rmq_conn *c, const char *address, int window, const char *offset, long timestamp) {
    if (rmq_shim_conn_closed(c)) {
        rmq_fail(RMQ_ERR_CLOSED, "the connection is closed%s%s", NULL, NULL);
        return NULL;
    }
    rmq_link *l = rmq_link_new(c, RMQ_LINK_CONSUMER, address, window);
    if (l == NULL) {
        rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
        return NULL;
    }
    if (offset[0] != '\0') {
        l->filter_offset = strdup(offset);
    }
    l->filter_timestamp = timestamp;
    rmq_link_start(l);
    return l;
}

static void rmq_detach_once(rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    int was = l->closed;
    l->closed = 1;
    pthread_cond_broadcast(&l->space);
    pthread_cond_broadcast(&l->drained);
    pthread_mutex_unlock(&l->mu);
    if (!was) {
        // The registry's reference keeps l alive until DETACH has run.
        rmq_op *op = rmq_op_new(RMQ_OP_DETACH);
        if (op != NULL) {
            op->link = l;
            rmq_post(l->c, op);
        }
    }
}

// A link's closing function, run when Dex lets go of it.
static inline void rmq_shim_link_free(rmq_link *l) {
    rmq_conn *c = l->c;
    rmq_detach_once(l);
    rmq_link_unref(l);
    rmq_conn_unref(c);
}

static inline void rmq_shim_link_close(rmq_link *l) {
    rmq_detach_once(l);
}

static inline const char *rmq_shim_link_failure(rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    snprintf(rmq_last.message, sizeof rmq_last.message, "%s", l->failed);
    pthread_mutex_unlock(&l->mu);
    return rmq_last.message;
}

// ---- Building a message ----

typedef struct {
    pn_message_t *m;
    // Application properties, written as one map when the message is encoded.
    char **keys;
    char **values;
    size_t *sizes;
    int props;
    int cap;
} rmq_draft;

static inline rmq_draft *rmq_shim_draft_new(const char *body, int size) {
    rmq_draft *d = calloc(1, sizeof *d);
    if (d == NULL) {
        return NULL;
    }
    d->m = pn_message();
    pn_message_set_durable(d->m, true);
    pn_data_t *b = pn_message_body(d->m);
    pn_data_put_binary(b, pn_bytes((size_t)size, body));
    return d;
}

static inline void rmq_shim_draft_free(rmq_draft *d) {
    for (int i = 0; i < d->props; i++) {
        free(d->keys[i]);
        free(d->values[i]);
    }
    free(d->keys);
    free(d->values);
    free(d->sizes);
    pn_message_free(d->m);
    free(d);
}

static inline void rmq_shim_draft_text(rmq_draft *d, int field, const char *value) {
    switch (field) {
    case 0:
        pn_message_set_subject(d->m, value);
        break;
    case 1:
        pn_message_set_content_type(d->m, value);
        break;
    case 2:
        pn_message_set_reply_to(d->m, value);
        break;
    case 3:
        pn_message_set_address(d->m, value);
        break;
    case 4: {
        pn_data_t *id = pn_message_id(d->m);
        pn_data_clear(id);
        pn_data_put_string(id, pn_bytes(strlen(value), value));
        break;
    }
    case 5: {
        pn_data_t *id = pn_message_correlation_id(d->m);
        pn_data_clear(id);
        pn_data_put_string(id, pn_bytes(strlen(value), value));
        break;
    }
    case 6:
        pn_message_set_content_encoding(d->m, value);
        break;
    case 7:
        pn_message_set_group_id(d->m, value);
        break;
    }
}

static inline void rmq_shim_draft_numbers(rmq_draft *d, bool durable, int priority, long ttl, long created) {
    pn_message_set_durable(d->m, durable);
    if (priority >= 0) {
        pn_message_set_priority(d->m, (uint8_t)priority);
    }
    if (ttl > 0) {
        pn_message_set_ttl(d->m, (pn_millis_t)ttl);
    }
    if (created > 0) {
        pn_message_set_creation_time(d->m, created);
    }
}

static inline void rmq_shim_draft_prop(rmq_draft *d, const char *key, const char *value, int size) {
    if (d->props == d->cap) {
        int cap = d->cap ? d->cap * 2 : 4;
        char **k = realloc(d->keys, sizeof(char *) * (size_t)cap);
        if (k) {
            d->keys = k;
        }
        char **v = realloc(d->values, sizeof(char *) * (size_t)cap);
        if (v) {
            d->values = v;
        }
        size_t *z = realloc(d->sizes, sizeof(size_t) * (size_t)cap);
        if (z) {
            d->sizes = z;
        }
        if (!k || !v || !z) {
            return;
        }
        d->cap = cap;
    }
    d->keys[d->props] = strdup(key);
    d->values[d->props] = malloc((size_t)size + 1);
    if (d->keys[d->props] == NULL || d->values[d->props] == NULL) {
        free(d->keys[d->props]);
        free(d->values[d->props]);
        return;
    }
    memcpy(d->values[d->props], value, (size_t)size);
    d->sizes[d->props] = (size_t)size;
    d->props++;
}

static void rmq_write_props(rmq_draft *d) {
    if (d->props == 0) {
        return;
    }
    pn_data_t *p = pn_message_properties(d->m);
    pn_data_clear(p);
    pn_data_put_map(p);
    pn_data_enter(p);
    for (int i = 0; i < d->props; i++) {
        pn_data_put_string(p, pn_bytes(strlen(d->keys[i]), d->keys[i]));
        pn_data_put_string(p, pn_bytes(d->sizes[i], d->values[i]));
    }
    pn_data_exit(p);
}

static int rmq_encode(rmq_draft *d, char **bytes, size_t *size) {
    rmq_write_props(d);
    pn_rwbytes_t out = {0, NULL};
    ssize_t n = pn_message_encode2(d->m, &out);
    if (n < 0) {
        free(out.start);
        return rmq_fail(RMQ_ERR_INVALID, "the message could not be encoded%s%s", NULL, NULL);
    }
    *bytes = out.start;
    *size = (size_t)n;
    return 0;
}

// ---- Publishing ----

// Queues an encoded message, waiting while the window is full. Takes bytes over.
static int rmq_enqueue(rmq_link *l, char *bytes, size_t size, long tag, int tracked, rmq_ticket *t) {
    rmq_conn *c = l->c;
    struct timespec until;
    rmq_deadline(&until, c->publish_timeout_ms);
    pthread_mutex_lock(&l->mu);
    while (l->in_flight >= l->window && !l->closed && l->failed[0] == '\0' && !rmq_shim_conn_closed(c)) {
        if (pthread_cond_timedwait(&l->space, &l->mu, &until) == ETIMEDOUT) {
            pthread_mutex_unlock(&l->mu);
            free(bytes);
            return rmq_fail(RMQ_ERR_TIMEOUT, "timed out waiting for room to publish%s%s", NULL, NULL);
        }
    }
    if (l->closed || rmq_shim_conn_closed(c)) {
        pthread_mutex_unlock(&l->mu);
        free(bytes);
        return rmq_fail(RMQ_ERR_CLOSED, l->closed ? "the publisher is closed%s%s" : "the connection is closed%s%s", NULL, NULL);
    }
    if (l->failed[0] != '\0') {
        char why[300];
        snprintf(why, sizeof why, "%s", l->failed);
        pthread_mutex_unlock(&l->mu);
        free(bytes);
        return rmq_fail(RMQ_ERR_LINK, "the broker detached the publisher: %s%s", why, NULL);
    }
    rmq_out *o = calloc(1, sizeof *o);
    if (o == NULL) {
        pthread_mutex_unlock(&l->mu);
        free(bytes);
        return rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
    }
    o->bytes = bytes;
    o->size = size;
    o->tag = tag;
    o->tracked = tracked;
    o->ticket = t;
    o->deadline = rmq_now_ms() + c->publish_timeout_ms;
    if (l->pending_tail) {
        l->pending_tail->next = o;
    } else {
        l->pending_head = o;
    }
    l->pending_tail = o;
    l->in_flight++;
    pthread_mutex_unlock(&l->mu);
    rmq_wake(c);
    return 0;
}

// mode 0: send and return; 1: tracked under tag; 2: wait for the outcome.
static inline int rmq_shim_publish(rmq_link *l, rmq_draft *d, int mode, long tag, int *outcome) {
    char *bytes;
    size_t size;
    int rc = rmq_encode(d, &bytes, &size);
    if (rc) {
        return rc;
    }
    if (mode != 2) {
        return rmq_enqueue(l, bytes, size, tag, mode == 1, NULL);
    }
    rmq_ticket t;
    rmq_ticket_init(&t);
    rc = rmq_enqueue(l, bytes, size, 0, 0, &t);
    if (rc == 0) {
        rmq_ticket_wait(&t);
        *outcome = t.outcome;
        rc = t.code;
        snprintf(rmq_last.message, sizeof rmq_last.message, "%s", t.reason);
    }
    rmq_ticket_destroy(&t);
    return rc;
}

// The common case: a body and nothing else. Durable, as every message is by default.
static inline int rmq_shim_send_body(rmq_link *l, const char *body, int size) {
    rmq_draft *d = rmq_shim_draft_new(body, size);
    if (d == NULL) {
        return rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
    }
    int outcome = 0;
    int rc = rmq_shim_publish(l, d, 0, 0, &outcome);
    rmq_shim_draft_free(d);
    return rc;
}

static inline rmq_report *rmq_shim_report_next(rmq_link *l, int timeoutMs) {
    struct timespec until;
    rmq_deadline(&until, timeoutMs);
    pthread_mutex_lock(&l->mu);
    while (l->reports_head == NULL) {
        if (pthread_cond_timedwait(&l->report_ready, &l->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    rmq_report *r = l->reports_head;
    if (r) {
        l->reports_head = r->next;
        if (!l->reports_head) {
            l->reports_tail = NULL;
        }
    }
    pthread_mutex_unlock(&l->mu);
    return r;
}

static inline void rmq_shim_report_free(rmq_report *r) {
    free(r->reason);
    free(r);
}

static inline long rmq_shim_report_tag(rmq_report *r) { return r->tag; }
static inline int rmq_shim_report_outcome(rmq_report *r) { return r->outcome; }
static inline const char *rmq_shim_report_reason(rmq_report *r) { return r->reason; }

// Waits for everything in flight. Returns how many are still in flight.
static inline int rmq_shim_flush(rmq_link *l, int timeoutMs) {
    struct timespec until;
    rmq_deadline(&until, timeoutMs);
    pthread_mutex_lock(&l->mu);
    while (l->in_flight > 0 && !rmq_shim_conn_closed(l->c)) {
        if (pthread_cond_timedwait(&l->drained, &l->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    int left = l->in_flight;
    pthread_mutex_unlock(&l->mu);
    return left;
}

static inline int rmq_shim_in_flight(rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    int n = l->in_flight;
    pthread_mutex_unlock(&l->mu);
    return n;
}

static inline long rmq_shim_count(rmq_link *l, int which) {
    switch (which) {
    case 0:
        return atomic_load(&l->accepted);
    case 1:
        return atomic_load(&l->released);
    case 2:
        return atomic_load(&l->rejected);
    default:
        return atomic_load(&l->failed_count);
    }
}

// ---- Consuming ----

// The next message, waiting up to timeoutMs. NULL on a timeout, or when the
// consumer is closed or failed with nothing left to read; rmq_shim_link_failure
// says which.
static inline rmq_in *rmq_shim_receive(rmq_link *l, int timeoutMs) {
    struct timespec until;
    rmq_deadline(&until, timeoutMs);
    pthread_mutex_lock(&l->mu);
    while (l->buf_head == NULL && !l->closed && l->failed[0] == '\0' && !rmq_shim_conn_closed(l->c)) {
        if (pthread_cond_timedwait(&l->space, &l->mu, &until) == ETIMEDOUT) {
            break;
        }
    }
    rmq_in *in = l->buf_head;
    int low = 0;
    if (in) {
        l->buf_head = in->next;
        if (!l->buf_head) {
            l->buf_tail = NULL;
        }
        l->buffered--;
        low = l->buffered < l->window / 2;
        in->next = NULL;
    }
    pthread_mutex_unlock(&l->mu);
    if (in) {
        atomic_fetch_add(&l->c->refs, 1);
    }
    if (in && low) {
        // The I/O thread tops up every consumer whenever it runs.
        rmq_wake(l->c);
    }
    return in;
}

static inline bool rmq_shim_consumer_done(rmq_link *l) {
    pthread_mutex_lock(&l->mu);
    bool done = l->buf_head == NULL && (l->closed || l->failed[0] != '\0');
    pthread_mutex_unlock(&l->mu);
    return done || (l->buf_head == NULL && rmq_shim_conn_closed(l->c));
}

static inline void rmq_shim_pause(rmq_link *l, bool paused) {
    pthread_mutex_lock(&l->mu);
    l->paused = paused;
    pthread_mutex_unlock(&l->mu);
    rmq_op *op = rmq_op_new(RMQ_OP_PUMP);
    if (op) {
        op->link = l;
        atomic_fetch_add(&l->refs, 1);
        rmq_post(l->c, op);
    }
}

static int rmq_settle_post(rmq_in *in, int how) {
    if (atomic_exchange(&in->settled, 1)) {
        return rmq_fail(RMQ_ERR_INVALID, "this message was already settled%s%s", NULL, NULL);
    }
    rmq_link *l = in->link;
    // The I/O thread checks again; this is only to tell the caller early.
    if (in->generation != atomic_load(&l->generation)) {
        return rmq_fail(RMQ_ERR_STALE, "the connection this message came on is gone; the broker will deliver it again%s%s", NULL, NULL);
    }
    pthread_mutex_lock(&l->mu);
    if (l->settles_len == l->settles_cap) {
        int cap = l->settles_cap ? l->settles_cap * 2 : 64;
        rmq_settlement *grown = realloc(l->settles, sizeof(rmq_settlement) * (size_t)cap);
        if (grown == NULL) {
            pthread_mutex_unlock(&l->mu);
            return rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
        }
        l->settles = grown;
        l->settles_cap = cap;
    }
    l->settles[l->settles_len].delivery = in->delivery;
    l->settles[l->settles_len].generation = in->generation;
    l->settles[l->settles_len].how = how;
    int first = l->settles_len == 0;
    l->settles_len++;
    pthread_mutex_unlock(&l->mu);
    if (first) {
        rmq_wake(l->c);
    }
    return 0;
}

static inline int rmq_shim_settle(rmq_in *in, int how) {
    return rmq_settle_post(in, how);
}

// A message let go of without being settled goes back to the queue.
static inline void rmq_shim_in_free(rmq_in *in) {
    rmq_link *l = in->link;
    rmq_conn *c = l->c;
    if (!atomic_load(&in->settled) && !rmq_shim_conn_closed(c)) {
        rmq_settle_post(in, RMQ_SETTLE_REQUEUE);
    }
    if (in->msg) {
        pn_message_free(in->msg);
    }
    free(in->bytes);
    free(in->scratch);
    free(in);
    rmq_link_unref(l);
    rmq_conn_unref(c);
}

static inline int rmq_shim_in_decode(rmq_in *in) {
    in->msg = pn_message();
    if (pn_message_decode(in->msg, in->bytes, in->size) != 0) {
        return rmq_fail(RMQ_ERR_INVALID, "a message could not be decoded%s%s", NULL, NULL);
    }
    return 0;
}

// The body: a data section's bytes, or an AMQP value that is a string or bytes.
static inline const char *rmq_shim_in_body(rmq_in *in, long *n) {
    pn_data_t *b = pn_message_body(in->msg);
    pn_data_rewind(b);
    *n = 0;
    if (!pn_data_next(b)) {
        return "";
    }
    pn_bytes_t v = {0, NULL};
    switch (pn_data_type(b)) {
    case PN_BINARY:
        v = pn_data_get_binary(b);
        break;
    case PN_STRING:
        v = pn_data_get_string(b);
        break;
    case PN_SYMBOL:
        v = pn_data_get_symbol(b);
        break;
    default:
        return "";
    }
    *n = (long)v.size;
    return v.start;
}

// The value at the data's cursor as text: strings as they are, everything else
// written out. pn_data_format would print the whole tree, not this node.
static const char *rmq_scalar_text(pn_data_t *d, char *buf, size_t size, long *n) {
    switch (pn_data_type(d)) {
    case PN_STRING: {
        pn_bytes_t v = pn_data_get_string(d);
        *n = (long)v.size;
        return v.start ? v.start : "";
    }
    case PN_SYMBOL: {
        pn_bytes_t v = pn_data_get_symbol(d);
        *n = (long)v.size;
        return v.start ? v.start : "";
    }
    case PN_BINARY: {
        pn_bytes_t v = pn_data_get_binary(d);
        *n = (long)v.size;
        return v.start ? v.start : "";
    }
    case PN_BOOL:
        snprintf(buf, size, "%s", pn_data_get_bool(d) ? "true" : "false");
        break;
    case PN_UBYTE:
        snprintf(buf, size, "%u", (unsigned)pn_data_get_ubyte(d));
        break;
    case PN_BYTE:
        snprintf(buf, size, "%d", (int)pn_data_get_byte(d));
        break;
    case PN_USHORT:
        snprintf(buf, size, "%u", (unsigned)pn_data_get_ushort(d));
        break;
    case PN_SHORT:
        snprintf(buf, size, "%d", (int)pn_data_get_short(d));
        break;
    case PN_UINT:
        snprintf(buf, size, "%u", pn_data_get_uint(d));
        break;
    case PN_INT:
        snprintf(buf, size, "%d", pn_data_get_int(d));
        break;
    case PN_ULONG:
        snprintf(buf, size, "%llu", (unsigned long long)pn_data_get_ulong(d));
        break;
    case PN_LONG:
        snprintf(buf, size, "%lld", (long long)pn_data_get_long(d));
        break;
    case PN_TIMESTAMP:
        snprintf(buf, size, "%lld", (long long)pn_data_get_timestamp(d));
        break;
    case PN_FLOAT:
        snprintf(buf, size, "%g", (double)pn_data_get_float(d));
        break;
    case PN_DOUBLE:
        snprintf(buf, size, "%g", pn_data_get_double(d));
        break;
    case PN_UUID: {
        pn_uuid_t u = pn_data_get_uuid(d);
        const unsigned char *b = (const unsigned char *)u.bytes;
        snprintf(buf, size, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
        break;
    }
    default:
        buf[0] = '\0';
    }
    *n = (long)strlen(buf);
    return buf;
}

static const char *rmq_atom_text(rmq_in *in, pn_data_t *d) {
    pn_data_rewind(d);
    if (!pn_data_next(d)) {
        return "";
    }
    char buf[128];
    long n = 0;
    const char *v = rmq_scalar_text(d, buf, sizeof buf, &n);
    free(in->scratch);
    in->scratch = strndup(v, (size_t)n);
    return in->scratch ? in->scratch : "";
}

static inline const char *rmq_shim_in_text(rmq_in *in, int field) {
    const char *s = NULL;
    switch (field) {
    case 0:
        s = pn_message_get_subject(in->msg);
        break;
    case 1:
        s = pn_message_get_content_type(in->msg);
        break;
    case 2:
        s = pn_message_get_reply_to(in->msg);
        break;
    case 3:
        s = pn_message_get_address(in->msg);
        break;
    case 4:
        return rmq_atom_text(in, pn_message_id(in->msg));
    case 5:
        return rmq_atom_text(in, pn_message_correlation_id(in->msg));
    case 6:
        s = pn_message_get_content_encoding(in->msg);
        break;
    case 7:
        s = pn_message_get_group_id(in->msg);
        break;
    }
    return s ? s : "";
}

static inline long rmq_shim_in_number(rmq_in *in, int field) {
    switch (field) {
    case 0:
        return pn_message_is_durable(in->msg) ? 1 : 0;
    case 1:
        return pn_message_get_priority(in->msg);
    case 2:
        return (long)pn_message_get_ttl(in->msg);
    case 3:
        return (long)pn_message_get_creation_time(in->msg);
    case 4:
        return (long)pn_message_get_delivery_count(in->msg);
    case 5:
        return pn_message_is_first_acquirer(in->msg) ? 1 : 0;
    }
    return 0;
}

// Application properties, as text whatever their type.
static inline int rmq_shim_in_prop_count(rmq_in *in) {
    pn_data_t *p = pn_message_properties(in->msg);
    pn_data_rewind(p);
    if (!pn_data_next(p) || pn_data_type(p) != PN_MAP) {
        return 0;
    }
    return (int)pn_data_get_map(p) / 2;
}

static pn_data_t *rmq_prop_at(rmq_in *in, int i, int value) {
    pn_data_t *p = pn_message_properties(in->msg);
    pn_data_rewind(p);
    pn_data_next(p);
    pn_data_enter(p);
    for (int k = 0; k <= 2 * i + value; k++) {
        pn_data_next(p);
    }
    return p;
}

// Copied into scratch when converted, since a caller may ask for a key and then
// its value.
static const char *rmq_node_text(rmq_in *in, pn_data_t *p, long *n) {
    char buf[128];
    const char *v = rmq_scalar_text(p, buf, sizeof buf, n);
    if (v != buf) {
        return v;
    }
    free(in->scratch);
    in->scratch = strndup(buf, (size_t)*n);
    return in->scratch ? in->scratch : "";
}

static inline const char *rmq_shim_in_prop_key(rmq_in *in, int i, long *n) {
    return rmq_node_text(in, rmq_prop_at(in, i, 0), n);
}

static inline const char *rmq_shim_in_prop_value(rmq_in *in, int i, long *n) {
    return rmq_node_text(in, rmq_prop_at(in, i, 1), n);
}

// A message annotation RabbitMQ sets, such as x-exchange or x-routing-key.
static inline const char *rmq_shim_in_annotation(rmq_in *in, const char *key, long *n) {
    pn_data_t *a = pn_message_annotations(in->msg);
    *n = 0;
    pn_data_rewind(a);
    if (!pn_data_next(a) || pn_data_type(a) != PN_MAP) {
        return "";
    }
    size_t count = pn_data_get_map(a);
    pn_data_enter(a);
    for (size_t i = 0; i + 1 < count; i += 2) {
        pn_data_next(a);
        pn_bytes_t k = pn_data_type(a) == PN_SYMBOL ? pn_data_get_symbol(a) : pn_data_type(a) == PN_STRING ? pn_data_get_string(a) : (pn_bytes_t){0, NULL};
        pn_data_next(a);
        if (k.start && k.size == strlen(key) && memcmp(k.start, key, k.size) == 0) {
            return rmq_node_text(in, a, n);
        }
    }
    return "";
}

// ---- Management ----

typedef struct {
    pn_message_t *m;
    pn_data_t *body;
    char *path;
    int record;
    int depth;
} rmq_req;

static inline rmq_req *rmq_shim_req_new(const char *method, const char *path) {
    rmq_req *r = calloc(1, sizeof *r);
    if (r == NULL) {
        return NULL;
    }
    r->m = pn_message();
    r->path = strdup(path);
    pn_message_set_subject(r->m, method);
    pn_message_set_address(r->m, path);
    pn_message_set_reply_to(r->m, "$me");
    r->body = pn_message_body(r->m);
    return r;
}

static inline void rmq_shim_req_free(rmq_req *r) {
    pn_message_free(r->m);
    free(r->path);
    free(r);
}

// The body is a map, built a key at a time. Keys are strings; a nested map
// ("arguments") is entered with begin and left with end.
static inline void rmq_shim_req_map(rmq_req *r) {
    pn_data_put_map(r->body);
    pn_data_enter(r->body);
}

static inline void rmq_shim_req_end(rmq_req *r) {
    pn_data_exit(r->body);
}

static inline void rmq_shim_req_key(rmq_req *r, const char *key) {
    pn_data_put_string(r->body, pn_bytes(strlen(key), key));
}

static inline void rmq_shim_req_string(rmq_req *r, const char *key, const char *value) {
    rmq_shim_req_key(r, key);
    pn_data_put_string(r->body, pn_bytes(strlen(value), value));
}

static inline void rmq_shim_req_bool(rmq_req *r, const char *key, bool value) {
    rmq_shim_req_key(r, key);
    pn_data_put_bool(r->body, value);
}

static inline void rmq_shim_req_long(rmq_req *r, const char *key, long value) {
    rmq_shim_req_key(r, key);
    pn_data_put_long(r->body, value);
}

// Remember this request, to be sent again after a reconnect (1), or forget the
// one at this path (2).
static inline void rmq_shim_req_record(rmq_req *r, int mode) {
    r->record = mode;
}

static inline void rmq_shim_req_forget(rmq_req *r, const char *path) {
    free(r->path);
    r->path = strdup(path);
    r->record = 2;
}

typedef struct {
    pn_message_t *m;
    char *scratch;
    int status;
} rmq_reply;

static inline void rmq_shim_reply_free(rmq_reply *r) {
    pn_message_free(r->m);
    free(r->scratch);
    free(r);
}

// Sends a request and waits for the answer, through reconnects, for up to
// the publish timeout. NULL on failure, with the reason as the last message.
static inline rmq_reply *rmq_shim_req_send(rmq_conn *c, rmq_req *r) {
    if (rmq_shim_conn_closed(c)) {
        rmq_fail(RMQ_ERR_CLOSED, "the connection is closed%s%s", NULL, NULL);
        return NULL;
    }
    uint64_t id = atomic_fetch_add(&c->next_request_id, 1) + 1;
    pn_data_t *mid = pn_message_id(r->m);
    pn_data_clear(mid);
    pn_data_put_ulong(mid, id);
    rmq_draft d = {r->m, NULL, NULL, NULL, 0, 0};
    char *bytes;
    size_t size;
    if (rmq_encode(&d, &bytes, &size)) {
        return NULL;
    }
    // What a recovery re-sends is a copy of exactly the request that succeeded.
    char *copy = NULL;
    if (r->record == 1) {
        copy = malloc(size);
        if (copy != NULL) {
            memcpy(copy, bytes, size);
        }
    }
    rmq_ticket t;
    rmq_ticket_init(&t);
    rmq_op *op = rmq_op_new(RMQ_OP_REQUEST);
    if (op == NULL) {
        free(bytes);
        free(copy);
        rmq_ticket_destroy(&t);
        rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
        return NULL;
    }
    op->bytes = bytes;
    op->size = size;
    op->id = id;
    op->ticket = &t;
    op->deadline = rmq_now_ms() + c->publish_timeout_ms;
    rmq_post(c, op);
    rmq_ticket_wait(&t);
    if (t.code != 0) {
        rmq_fail(t.code, "%s%s", t.reason, NULL);
        rmq_ticket_destroy(&t);
        free(copy);
        return NULL;
    }
    rmq_reply *reply = calloc(1, sizeof *reply);
    if (reply == NULL) {
        free(copy);
        rmq_ticket_destroy(&t);
        rmq_fail(RMQ_ERR_MEMORY, "out of memory%s%s", NULL, NULL);
        return NULL;
    }
    reply->m = pn_message();
    pn_message_decode(reply->m, t.reply, t.reply_size);
    reply->status = atoi(t.reason);

    // A request that worked is recorded now, in the order requests succeeded.
    if (r->record != 0 && reply->status >= 200 && reply->status < 300) {
        rmq_op *rec = rmq_op_new(RMQ_OP_REQUEST);
        if (rec != NULL) {
            rec->path = strdup(r->path);
            rec->record = r->record;
            if (r->record == 1) {
                rec->bytes = copy;
                rec->size = size;
                copy = NULL;
            }
            rmq_post(c, rec);
        }
    }
    free(copy);
    rmq_ticket_destroy(&t);
    return reply;
}

static inline int rmq_shim_reply_status(rmq_reply *r) {
    return r->status;
}

// A top-level key of the reply's map body, as text. "" when absent.
static inline const char *rmq_shim_reply_text(rmq_reply *r, const char *key, long *n) {
    pn_data_t *b = pn_message_body(r->m);
    *n = 0;
    pn_data_rewind(b);
    if (!pn_data_next(b) || pn_data_type(b) != PN_MAP) {
        return "";
    }
    size_t count = pn_data_get_map(b);
    pn_data_enter(b);
    for (size_t i = 0; i + 1 < count; i += 2) {
        pn_data_next(b);
        pn_bytes_t k = pn_data_type(b) == PN_STRING ? pn_data_get_string(b) : pn_data_type(b) == PN_SYMBOL ? pn_data_get_symbol(b) : (pn_bytes_t){0, NULL};
        pn_data_next(b);
        if (k.start && k.size == strlen(key) && memcmp(k.start, key, k.size) == 0) {
            char buf[128];
            const char *v = rmq_scalar_text(b, buf, sizeof buf, n);
            if (v != buf) {
                return v;
            }
            free(r->scratch);
            r->scratch = strndup(buf, (size_t)*n);
            return r->scratch ? r->scratch : "";
        }
    }
    return "";
}

static inline long rmq_shim_reply_long(rmq_reply *r, const char *key) {
    long n = 0;
    const char *s = rmq_shim_reply_text(r, key, &n);
    return n > 0 ? strtol(s, NULL, 10) : 0;
}

// A path segment, percent-encoded as RabbitMQ's management API expects.
static inline char *rmq_shim_encode_segment(const char *s) {
    size_t n = strlen(s);
    char *out = malloc(n * 3 + 1);
    if (out == NULL) {
        return NULL;
    }
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' || ch == '~') {
            out[j++] = (char)ch;
        } else {
            snprintf(out + j, 4, "%%%02X", ch);
            j += 3;
        }
    }
    out[j] = '\0';
    return out;
}

static int rmq_hex(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

// Percent-decoding, for the parts of a uri.
static inline char *rmq_shim_unescape(const char *s) {
    size_t n = strlen(s);
    char *out = malloc(n + 1);
    if (out == NULL) {
        return NULL;
    }
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '%' && i + 2 < n + 1 && rmq_hex(s[i + 1]) >= 0 && rmq_hex(s[i + 2]) >= 0) {
            out[j++] = (char)(rmq_hex(s[i + 1]) * 16 + rmq_hex(s[i + 2]));
            i += 2;
        } else {
            out[j++] = s[i];
        }
    }
    out[j] = '\0';
    return out;
}

#endif
