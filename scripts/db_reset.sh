#!/bin/bash
#
# db_reset.sh — inspect or wipe all app-owned schemas in the PostgreSQL DB.
#
# DynaStore modules create schemas on startup via CREATE SCHEMA IF NOT EXISTS.
# A clean DB means next startup rebuilds every schema from current DDL — useful
# for verifying migrations, events rewrites, or catching stale-schema drift.
#
# System schemas (pg_*, information_schema, cron) are always preserved.
#
# Usage:
#   ./scripts/db_reset.sh check         # list every non-system schema + row counts
#   ./scripts/db_reset.sh drop SCHEMA   # drop a specific schema (e.g. events, catalog)
#   ./scripts/db_reset.sh reset         # drop ALL non-system schemas (with confirm)
#   ./scripts/db_reset.sh reset --yes   # skip confirmation
#
# Connection resolution (first match wins):
#   1. $DATABASE_URL
#   2. Running container named in $DB_CONTAINER (default: geoid_db)
#   3. Default localhost:54320 (dev docker-compose mapping)

set -euo pipefail

cmd="${1:-check}"
arg="${2:-}"

DB_CONTAINER="${DB_CONTAINER:-geoid_db}"

run_psql() {
    local sql="$1"
    if [[ -n "${DATABASE_URL:-}" ]]; then
        psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "$sql"
    elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${DB_CONTAINER}$"; then
        docker exec "$DB_CONTAINER" psql -U testuser -d gis_dev -v ON_ERROR_STOP=1 -c "$sql"
    else
        psql "postgresql://testuser:testpassword@localhost:54320/gis_dev" -v ON_ERROR_STOP=1 -c "$sql"
    fi
}

list_schemas_sql="SELECT nspname FROM pg_namespace
 WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast','cron','public')
   AND nspname NOT LIKE 'pg_temp_%'
   AND nspname NOT LIKE 'pg_toast_temp_%'
 ORDER BY nspname;"

case "$cmd" in
    check)
        echo "=== non-system schemas ==="
        run_psql "$list_schemas_sql"
        echo
        echo "=== schema sizes ==="
        run_psql "SELECT n.nspname AS schema,
                         pg_size_pretty(sum(pg_total_relation_size(c.oid))::bigint) AS size
                  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast','cron')
                    AND n.nspname NOT LIKE 'pg_temp_%'
                    AND n.nspname NOT LIKE 'pg_toast_temp_%'
                  GROUP BY n.nspname ORDER BY n.nspname;"
        ;;
    drop)
        [[ -n "$arg" ]] || { echo "Error: drop requires a schema name"; exit 2; }
        case "$arg" in
            pg_*|information_schema|cron|public)
                echo "Refusing to drop system schema: $arg"; exit 2 ;;
        esac
        run_psql "DROP SCHEMA IF EXISTS \"$arg\" CASCADE;"
        echo "Dropped schema: $arg"
        ;;
    reset)
        if [[ "$arg" != "--yes" ]]; then
            read -r -p "DROP every non-system schema CASCADE — wipes ALL app data. Continue? [y/N] " ans
            [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "Aborted."; exit 1; }
        fi
        schemas=$(run_psql "$list_schemas_sql" | awk 'NR>2 && NF==1 && $1!="" {print $1}' | grep -Ev '^\(' || true)
        if [[ -z "$schemas" ]]; then
            echo "Nothing to drop."; exit 0
        fi
        echo "Dropping:"; echo "$schemas" | sed 's/^/  - /'
        drop_sql=""
        while IFS= read -r s; do
            [[ -z "$s" ]] && continue
            drop_sql+="DROP SCHEMA IF EXISTS \"$s\" CASCADE;"$'\n'
        done <<< "$schemas"
        run_psql "$drop_sql"
        echo "All non-system schemas dropped. Restart dynastore to rebuild."
        ;;
    *)
        echo "Unknown command: $cmd"
        echo "Usage: $0 {check|drop SCHEMA|reset [--yes]}"
        exit 2
        ;;
esac
