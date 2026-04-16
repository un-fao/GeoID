#!/bin/bash
#
# rebuild.sh — wipe volumes + rebuild + start a dev/test docker composition.
#
# SAFETY: this script is hard-wired to `docker-compose.dev.yml` and
# `docker-compose.test.yml` only. It will NEVER touch:
#   - `docker-compose.yml`         (production base)
#   - `docker-compose.local.yml`   (local / on-premise override)
# because it never invokes compose without an explicit `-f <dev|test>` file,
# and the arg parser explicitly rejects `local` and `prod`.
#
# Running `docker compose up` or `docker compose -f docker-compose.local.yml up`
# directly bypasses this script — volumes are preserved, no wipe happens.
#
# Usage:
#   ./docker/scripts/rebuild.sh dev            # wipe + rebuild + up dev stack
#   ./docker/scripts/rebuild.sh test           # wipe + rebuild + up test stack
#   ./docker/scripts/rebuild.sh dev --no-wipe  # rebuild images only, keep volumes
#
#   (or via the unified CLI:  ./docker/scripts/db.sh rebuild dev)
#
# What it does (for dev/test only):
#   1. docker compose -f docker/docker-compose.<env>.yml down -v --remove-orphans
#      (volumes wiped → totally fresh DB on next start)
#   2. docker compose -f docker/docker-compose.<env>.yml build
#   3. docker compose -f docker/docker-compose.<env>.yml up -d
#
# Guard rails:
#   - Refuses any env other than "dev" or "test".
#   - Refuses if COMPOSE_FILE env var is pre-set (prevents user override to prod).
#   - Uses --project-name to keep dev/test volumes isolated from each other.

set -euo pipefail

env="${1:-}"
flag="${2:-}"

case "$env" in
    dev|test) ;;
    local|prod|production|on-prem|onprem)
        echo "REFUSED: '$env' is a persistent-data composition."
        echo "This script only rebuilds dev/test. For $env, use 'docker compose' directly."
        exit 2
        ;;
    *)
        echo "Usage: $0 {dev|test} [--no-wipe]"
        echo "Refuses to run against local/prod/on-prem compositions (data preserved)."
        exit 2
        ;;
esac

if [[ -n "${COMPOSE_FILE:-}" ]]; then
    echo "Refusing to run: \$COMPOSE_FILE is set ($COMPOSE_FILE)."
    echo "Unset it — this script picks the compose file explicitly."
    exit 2
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." >/dev/null 2>&1 && pwd )"
compose_file="${DIR}/docker/docker-compose.${env}.yml"
project="geoid_${env}"

if [[ ! -f "$compose_file" ]]; then
    echo "Compose file not found: $compose_file"; exit 1
fi

cd "$DIR/docker"

echo "Environment: $env"
echo "Compose file: $compose_file"
echo "Project name: $project"
echo

if [[ "$flag" == "--no-wipe" ]]; then
    echo "=== [1/3] stop (keeping volumes) ==="
    docker compose -p "$project" -f "docker-compose.${env}.yml" down --remove-orphans
else
    echo "=== [1/3] stop + wipe volumes ==="
    docker compose -p "$project" -f "docker-compose.${env}.yml" down -v --remove-orphans
fi

echo "=== [2/3] build ==="
docker compose -p "$project" -f "docker-compose.${env}.yml" build

echo "=== [3/3] up -d ==="
docker compose -p "$project" -f "docker-compose.${env}.yml" up -d

echo
echo "Done. Tail logs with:"
echo "  docker compose -p $project -f docker-compose.${env}.yml logs -f"
