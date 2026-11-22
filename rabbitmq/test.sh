#!/usr/bin/env bash
# Runs rabbitmq_test.dx against a RabbitMQ 4 of its own, on port 5679, started
# here and removed afterwards (or reused, if one by that name is running).
# RABBITMQ_URL runs against another broker instead, without the recovery tests.
#
#     ./test.sh            the suite
#     ./test.sh settling   one group of it
set -u
cd "$(dirname "$0")" || exit 1

if [ -n "${RABBITMQ_URL:-}" ]; then
    RMQ_ONLY=${1:-} exec dex run rabbitmq_test.dx
fi

NAME=dexrmq-test
STARTED=0
if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
    docker rm -f "$NAME" >/dev/null 2>&1
    # --user: the image's own user from the start. Under OrbStack, starting as
    # root leaves the Erlang cookie unreadable once the server drops privileges.
    docker run -d --name "$NAME" --hostname "$NAME" --user 999:999 -p 5679:5672 \
        -e RABBITMQ_DEFAULT_USER=dex -e RABBITMQ_DEFAULT_PASS=dexpass \
        rabbitmq:4.1-management >/dev/null || exit 1
    STARTED=1
    for i in $(seq 1 60); do
        docker exec "$NAME" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1 && break
        sleep 1
    done
fi

RABBITMQ_CONTAINER=$NAME RMQ_ONLY=${1:-} dex run rabbitmq_test.dx
RC=$?
[ "$STARTED" = 1 ] && docker rm -f "$NAME" >/dev/null 2>&1
exit $RC
