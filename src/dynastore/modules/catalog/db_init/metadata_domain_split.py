#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Metadata-domain DDL split (M2.0 of the role-based driver refactor).

Additive, idempotent. Creates the per-domain metadata tables alongside the
existing ``catalog.catalogs`` legacy columns and the per-tenant
``{schema}.metadata`` table. **Does not migrate, read from, or drop the
legacy locations** — those flips happen in M2.3 / M2.4 / M2.5.

Global (under ``catalog.`` schema):

- ``catalog.catalog_metadata_core`` — CORE fields (title, description,
  keywords, license, extra_metadata) keyed on ``catalog_id``.
- ``catalog.catalog_metadata_stac`` — STAC fields (stac_version,
  stac_extensions, conforms_to, links, assets) keyed on ``catalog_id``.
  Plan §Domain separation says this table should only be created when
  the STAC extension is loaded; for M2.0 it is created unconditionally
  (zero rows, zero cost) so M2 doesn't need to touch the STAC extension.
  Gating moves to the STAC extension's lifecycle in a follow-up sub-PR.
- ``catalog.catalogs`` gains ``created_at`` + ``updated_at`` columns
  (ALTER TABLE ADD COLUMN IF NOT EXISTS).  These are the canonical
  freshness tokens used by INDEX / BACKUP propagation (plan §Freshness
  contract — tentative).

Per-tenant (under each ``{schema}``):

- ``{schema}.metadata_core`` — CORE collection metadata (title,
  description, keywords, license, extra_metadata).
- ``{schema}.metadata_stac`` — STAC collection metadata (extent,
  providers, summaries, assets, item_assets, links, stac_version,
  stac_extensions).

``{schema}.collections`` already has ``created_at`` / ``updated_at`` (see
``TENANT_COLLECTIONS_DDL`` in ``catalog_service.py``), so no ALTER needed.

Idempotency: every DDL uses ``IF NOT EXISTS`` so re-runs are no-ops.
Applied globally at ``CatalogModule`` init and per-tenant from
``initialize_core_tenant_tables``.
"""

from dynastore.modules.db_config.query_executor import DDLQuery, DbResource


# ---------------------------------------------------------------------------
# Global (catalog.* schema)
# ---------------------------------------------------------------------------

CATALOG_METADATA_CORE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalog_metadata_core (
    catalog_id     VARCHAR PRIMARY KEY REFERENCES catalog.catalogs(id) ON DELETE CASCADE,
    title          JSONB,
    description    JSONB,
    keywords       JSONB,
    license        JSONB,
    extra_metadata JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CATALOG_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalog_metadata_stac (
    catalog_id      VARCHAR PRIMARY KEY REFERENCES catalog.catalogs(id) ON DELETE CASCADE,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    conforms_to     JSONB,
    links           JSONB,
    assets          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# catalog.catalogs freshness columns.  ADD COLUMN IF NOT EXISTS is PG 9.6+.
# The defaults backfill any existing rows to NOW() at the migration moment —
# acceptable because updated_at semantics for pre-existing catalogs are
# "last known write time unknown, treat as now".  The M2.3 dual-write phase
# starts producing real per-write timestamps; INDEX / BACKUP consumers
# (M3 / M4) read from this point onwards.
CATALOGS_FRESHNESS_COLUMNS_DDL = """
ALTER TABLE catalog.catalogs
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE catalog.catalogs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
"""


# ---------------------------------------------------------------------------
# Per-tenant ({schema}.* schema)
# ---------------------------------------------------------------------------

TENANT_METADATA_CORE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.metadata_core (
    collection_id  VARCHAR PRIMARY KEY,
    title          JSONB,
    description    JSONB,
    keywords       JSONB,
    license        JSONB,
    extra_metadata JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

TENANT_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.metadata_stac (
    collection_id   VARCHAR PRIMARY KEY,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    extent          JSONB,
    providers       JSONB,
    summaries       JSONB,
    links           JSONB,
    assets          JSONB,
    item_assets     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def ensure_global_metadata_domain_tables(conn: DbResource) -> None:
    """Apply the global DDL (``catalog.catalog_metadata_core`` + ``_stac``
    tables, plus the freshness columns on ``catalog.catalogs``).

    Idempotent.  Called once during ``CatalogModule`` init after the
    legacy ``catalog.catalogs`` table has been created.

    The three DDLs are grouped into one call because they share the same
    transaction scope and either all apply or none do — if the freshness-
    column ALTER fails (e.g. PG version mismatch), the new tables stay
    uncreated, which keeps the system consistent with the pre-M2 shape.
    """
    await DDLQuery(
        CATALOGS_FRESHNESS_COLUMNS_DDL
        + CATALOG_METADATA_CORE_DDL
        + CATALOG_METADATA_STAC_DDL
    ).execute(conn)


async def ensure_tenant_metadata_domain_tables(conn: DbResource, schema: str) -> None:
    """Apply the per-tenant DDL (``{schema}.metadata_core`` + ``_stac``).

    Idempotent.  Called from ``initialize_core_tenant_tables`` after the
    legacy ``{schema}.metadata`` table is created.

    Both tables share ``collection_id`` as PK.  FK to
    ``{schema}.collections(id)`` is intentionally omitted because the
    collection row is inserted after ``initialize_core_tenant_tables``
    completes; enforcing the FK would require reordering or deferring
    constraints, which isn't needed for M2.0's additive-only scope.
    The FK can be added in M2.4 once dual-write semantics are in place.
    """
    await DDLQuery(
        TENANT_METADATA_CORE_DDL + TENANT_METADATA_STAC_DDL
    ).execute(conn, schema=schema)
