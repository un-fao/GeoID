#!/bin/bash
# Unified dev-ops CLI for the geoid docker composition.
# Delegates to db_reset.sh (schema/cron ops) and rebuild.sh (compose lifecycle).
#
#   db.sh check                            list non-system schemas + sizes
#   db.sh drop SCHEMA                      drop a single schema
#   db.sh reset [--yes] [--keycloak] [--keep-cron]
#                                          drop user schemas; default preserves keycloak tables
#                                          --keycloak  also TRUNCATE tables in keycloak schema
#                                                      (forces realm re-import on next keycloak start)
#                                          --keep-cron do NOT wipe cron.job rows
#   db.sh rebuild dev|test [--no-wipe]     stop + (wipe) + build + up
#
# See also src/dynastore/scripts/reset_policy.env for preserved-schemas / system-cron-jobs policy.
#
# db_reset.sh / rebuild.sh live under src/dynastore/scripts/ (SSOT — shipped as
# package data so downstream wrappers can reuse them). This thin delegator
# resolves them by walking up to the repo root.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../.." && pwd)"
scripts="$repo_root/src/dynastore/scripts"
cmd="${1:-}"
shift || true

case "$cmd" in
    check|drop|reset)
        exec "$scripts/db_reset.sh" "$cmd" "$@"
        ;;
    rebuild)
        exec "$scripts/rebuild.sh" "$@"
        ;;
    ""|-h|--help)
        sed -n '2,15p' "$0"
        ;;
    *)
        echo "Unknown command: $cmd" >&2
        echo "Run '$0 --help' for usage." >&2
        exit 2
        ;;
esac
