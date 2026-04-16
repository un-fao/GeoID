#!/bin/bash
# Dev/test-only postgres entrypoint wrapper.
#
# Purpose: replace the old one-shot `db_init` sidecar container with an in-process
# step that runs the boot-tier reset from inside the db container itself.
#
# Sequence:
#   1. boot postgres via its stock entrypoint in the background
#   2. wait for pg_isready
#   3. run boot-tier reset (db_reset.sh reset ...), controlled by $RESET_MODE
#   4. touch /tmp/reset_done — this is the sentinel the docker healthcheck waits on
#      so app services (`service_healthy`) do not race with the reset
#   5. wait on postgres; forward TERM/INT so graceful shutdown still works
#
# $RESET_MODE is injected by docker-compose.{dev,test}.yml:
#   default  — drop user schemas + user cron jobs, keep keycloak realm state (dev)
#   keycloak — same + TRUNCATE keycloak schema tables (test; forces realm re-import)
#   skip     — no reset (escape hatch)
#
# NEVER mounted or used by docker-compose.yml (prod) or docker-compose.local.yml
# (on-prem); both keep postgres's stock entrypoint.

set -euo pipefail

trap 'kill -TERM "${PG_PID:-0}" 2>/dev/null || true; wait "${PG_PID:-0}" 2>/dev/null || true' TERM INT

/usr/local/bin/docker-entrypoint.sh "$@" &
PG_PID=$!

until pg_isready -h localhost -U "${POSTGRES_USER:-testuser}" -d "${POSTGRES_DB:-gis_dev}" -q; do
    sleep 1
done

case "${RESET_MODE:-default}" in
    default)  bash /scripts/db_reset.sh reset --yes ;;
    keycloak) bash /scripts/db_reset.sh reset --yes --keycloak ;;
    skip)     : ;;
    *) echo "db_entrypoint_dev: unknown RESET_MODE='${RESET_MODE}'" >&2; exit 2 ;;
esac

touch /tmp/reset_done
wait "$PG_PID"
