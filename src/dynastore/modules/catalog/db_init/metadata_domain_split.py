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
``{schema}.collection_metadata`` table.  **Does not migrate, read from, or
drop legacy data** — dual-write / read-flip happen in M2.3 / M2.4 / M2.5.

Phase 2 (naming harmonisation) renamed the per-tenant metadata tables
from ``{schema}.metadata*`` to ``{schema}.collection_metadata*`` to
clarify the tier (counterpart to ``{schema}.assets`` / ``{schema}.collections``).
:func:`rename_legacy_metadata_tables` performs the idempotent rename for
existing deployments; fresh deployments get the new names directly from
the ``CREATE TABLE IF NOT EXISTS`` statements below.

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

- ``{schema}.collection_metadata_core`` — CORE collection metadata
  (title, description, keywords, license, extra_metadata).
- ``{schema}.collection_metadata_stac`` — STAC collection metadata
  (extent, providers, summaries, assets, item_assets, links,
  stac_version, stac_extensions).

``{schema}.collections`` already has ``created_at`` / ``updated_at`` (see
``TENANT_COLLECTIONS_DDL`` in ``catalog_service.py``), so no ALTER needed.

Idempotency: every DDL uses ``IF NOT EXISTS`` so re-runs are no-ops.
Applied globally at ``CatalogModule`` init and per-tenant from
``initialize_core_tenant_tables``.
"""

import logging

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
)
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


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
CREATE TABLE IF NOT EXISTS {schema}.collection_metadata_core (
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
CREATE TABLE IF NOT EXISTS {schema}.collection_metadata_stac (
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
# Legacy → canonical table rename (Phase 2 of the naming harmonisation)
# ---------------------------------------------------------------------------

# Renames three tables inside one tenant schema:
#   {schema}.metadata       → {schema}.collection_metadata
#   {schema}.metadata_core  → {schema}.collection_metadata_core
#   {schema}.metadata_stac  → {schema}.collection_metadata_stac
#
# Implementation note
# -------------------
# The previous revision used a single PL/pgSQL DO block with
# ``WHERE table_schema = '{schema}'`` literals.  That did not work against
# :class:`TemplateQueryBuilder`, which regex-finds every ``{schema}``
# placeholder — including inside string literals — and replaces each with
# the dialect-quoted identifier form (``"myschema"``).  The resulting
# predicate ``WHERE table_schema = '"myschema"'`` never matched the
# unquoted ``information_schema`` value, so the IF-EXISTS check was
# always false and the rename silently no-op'd.  Existing tenants kept
# their data in ``{schema}.metadata`` while the CREATE TABLE DDL below
# created a fresh empty ``{schema}.collection_metadata`` alongside.
#
# The replacement uses a parameterised DQL probe (binds schema + table
# name) and a targeted DDL rename per pair.  The schema name is also
# validated through ``validate_sql_identifier`` before use so a crafted
# schema value can't smuggle SQL.  Each ALTER TABLE is a standalone
# DDLQuery whose ``{schema}`` placeholder is the sole occurrence of the
# pattern, correctly handled as an identifier by TemplateQueryBuilder.
_TABLE_EXISTS_QUERY = DQLQuery(
    "SELECT EXISTS("
    "  SELECT 1 FROM information_schema.tables "
    "  WHERE table_schema = :schema AND table_name = :table_name"
    ");",
    result_handler=ResultHandler.SCALAR,
)


# ``legacy → canonical`` rename pairs applied in a fixed order.  The
# order is not semantically meaningful (each pair is independent and
# only runs when its own legacy table exists) — the tuple is stable for
# reproducibility in logs.
_LEGACY_METADATA_RENAMES = (
    ("metadata", "collection_metadata"),
    ("metadata_core", "collection_metadata_core"),
    ("metadata_stac", "collection_metadata_stac"),
)


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


async def rename_legacy_metadata_tables(conn: DbResource, schema: str) -> None:
    """Rename ``{schema}.metadata*`` to ``{schema}.collection_metadata*``.

    Runs BEFORE the ``CREATE TABLE IF NOT EXISTS`` statements (both the
    legacy ``METADATA_DDL`` in ``catalog_service.py`` and the M2.0 DDLs
    above) so that:

    - Existing tenants get their tables renamed once; subsequent
      ``CREATE TABLE IF NOT EXISTS collection_metadata*`` statements find
      the table already present and are a no-op.
    - Fresh tenants have no legacy tables; the probe returns False for
      every pair and no rename runs.  The ``CREATE TABLE`` statements
      then create the canonical names directly.

    Each (legacy, canonical) pair is handled independently:

      1. Validate the schema identifier (rejects reserved words / bad
         characters before they reach the SQL path).
      2. Probe ``information_schema.tables`` with bind parameters for
         both source and target existence.
      3. Run ``ALTER TABLE {schema}.<legacy> RENAME TO <canonical>``
         only when the legacy table exists AND the canonical one does
         not — guaranteeing idempotency.

    Idempotent and safe to call on every
    ``initialize_core_tenant_tables`` invocation.  Partial runs that
    fail mid-pair leave earlier pairs renamed (they become no-ops on
    re-run); the caller wraps this function in the same transaction as
    the subsequent DDL, so a rollback undoes everything atomically.
    """
    validate_sql_identifier(schema)
    for legacy, canonical in _LEGACY_METADATA_RENAMES:
        legacy_exists = await _TABLE_EXISTS_QUERY.execute(
            conn, schema=schema, table_name=legacy,
        )
        if not legacy_exists:
            continue
        canonical_exists = await _TABLE_EXISTS_QUERY.execute(
            conn, schema=schema, table_name=canonical,
        )
        if canonical_exists:
            # Both exist — a previous run raced with this one, or the
            # operator manually created the canonical table.  Skip
            # rather than risk data loss.
            logger.info(
                "Skipping rename of %s.%s -> %s: both tables present.",
                schema, legacy, canonical,
            )
            continue
        # SAFETY: ``schema`` has been validated above; the source /
        # target table names are module-level constants (literals), so
        # the TemplateQueryBuilder identifier quoting only needs to
        # handle ``{schema}`` — the one and only placeholder in the
        # template.  ALTER TABLE … RENAME TO does not support bind
        # parameters for identifiers, which is why this is a DDLQuery
        # rather than a DQLQuery.
        rename_sql = (
            f"ALTER TABLE {{schema}}.{legacy} RENAME TO {canonical};"
        )
        await DDLQuery(rename_sql).execute(conn, schema=schema)
        logger.info(
            "Renamed %s.%s -> %s.%s", schema, legacy, schema, canonical,
        )


async def ensure_tenant_metadata_domain_tables(conn: DbResource, schema: str) -> None:
    """Apply the per-tenant DDL (``{schema}.collection_metadata_core`` + ``_stac``).

    Idempotent.  Called from ``initialize_core_tenant_tables`` after the
    legacy ``{schema}.collection_metadata`` table is created.

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
