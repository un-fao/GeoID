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
    DbResource,
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
# Idempotent: each rename runs only when the source table exists AND the
# target does not — on re-run (or fresh deployment) every IF branch is false
# and the block is a no-op.
#
# Runs as a single anonymous PL/pgSQL block so all three renames share the
# same transaction scope — if any fail, all fail, leaving the schema in
# its pre-rename state.  A partial rename cannot leave the system in a
# mixed mode because the CREATE TABLE statements above reference only the
# canonical names.
TENANT_LEGACY_METADATA_RENAME_DDL = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = '{schema}' AND table_name = 'metadata')
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema = '{schema}' AND table_name = 'collection_metadata')
    THEN
        ALTER TABLE {schema}.metadata RENAME TO collection_metadata;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = '{schema}' AND table_name = 'metadata_core')
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema = '{schema}' AND table_name = 'collection_metadata_core')
    THEN
        ALTER TABLE {schema}.metadata_core RENAME TO collection_metadata_core;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = '{schema}' AND table_name = 'metadata_stac')
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                       WHERE table_schema = '{schema}' AND table_name = 'collection_metadata_stac')
    THEN
        ALTER TABLE {schema}.metadata_stac RENAME TO collection_metadata_stac;
    END IF;
END $$;
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


# ---------------------------------------------------------------------------
# One-shot backfill: legacy catalog.catalogs columns → split tables (M2.3a)
# ---------------------------------------------------------------------------

# Atomic probe-and-backfill inside a single PL/pgSQL DO block.
#
# Why the DO block: the previous two-step form (DQLQuery probe followed
# by DDLQuery INSERT-SELECT) had a probe-then-act race under multi-pod
# startup — during the M2.5b rollout, pod-A could observe the legacy
# columns as present, pod-B could drop them, then pod-A's INSERT-SELECT
# would fail with ``UndefinedColumn`` mid-run.  Collapsing probe + write
# into one DO block makes the operation a single transaction: pod-A
# holds the ACCESS SHARE lock on catalog.catalogs for the duration, so
# pod-B's ALTER TABLE DROP COLUMN (which needs ACCESS EXCLUSIVE) waits
# until pod-A commits.
#
# Why dynamic ``EXECUTE`` around each INSERT: PL/pgSQL parses every
# static SQL statement inside a DO block at block-entry time.  If the
# legacy columns (``title`` etc.) are already dropped, a static
# ``INSERT … SELECT title …`` would fail with ``UndefinedColumn``
# BEFORE the ``IF EXISTS`` check runs — defeating the point of the
# probe.  ``EXECUTE`` with a dollar-quoted literal defers parsing to
# the moment the IF branch actually runs; if the columns are absent we
# never execute the string, so we never parse it.
#
# ``ON CONFLICT (catalog_id) DO NOTHING`` guarantees idempotency:
# repeat runs are no-ops (rows already present); post-M2.2 rows written
# by the lifecycle hooks are not clobbered by stale legacy data.
_BACKFILL_CATALOG_METADATA_DDL = """
DO $backfill$
BEGIN
    -- ACCESS SHARE conflicts with ACCESS EXCLUSIVE (held by ALTER TABLE
    -- DROP COLUMN) but not with other readers.  If M2.5b's drop is in
    -- flight on another pod, one side waits for the other to commit.
    LOCK TABLE catalog.catalogs IN ACCESS SHARE MODE;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'catalog'
          AND table_name   = 'catalogs'
          AND column_name  = 'title'
    ) THEN
        EXECUTE $q$
            INSERT INTO catalog.catalog_metadata_core
                (catalog_id, title, description, keywords, license, extra_metadata)
            SELECT
                id, title, description, keywords, license, extra_metadata
            FROM catalog.catalogs
            WHERE
                title IS NOT NULL
                OR description IS NOT NULL
                OR keywords IS NOT NULL
                OR license IS NOT NULL
                OR extra_metadata IS NOT NULL
            ON CONFLICT (catalog_id) DO NOTHING
        $q$;

        EXECUTE $q$
            INSERT INTO catalog.catalog_metadata_stac
                (catalog_id, stac_version, stac_extensions, conforms_to, links, assets)
            SELECT
                id, stac_version, stac_extensions, conforms_to, links, assets
            FROM catalog.catalogs
            WHERE
                stac_version IS NOT NULL
                OR stac_extensions IS NOT NULL
                OR conforms_to IS NOT NULL
                OR links IS NOT NULL
                OR assets IS NOT NULL
            ON CONFLICT (catalog_id) DO NOTHING
        $q$;
    END IF;
END
$backfill$;
"""


# ---------------------------------------------------------------------------
# Legacy column drop (M2.5b — post-backfill, post-read-flip)
# ---------------------------------------------------------------------------

# Removes the legacy metadata columns from ``catalog.catalogs`` once the
# split tables are the canonical source.  Runs AFTER
# :func:`backfill_catalog_metadata_from_legacy` in the CatalogModule init
# sequence so existing tenants keep their data (in the split tables) when
# the columns disappear.
#
# ``DROP COLUMN IF EXISTS`` is idempotent per column — a second run on an
# already-migrated DB observes nothing to drop and is a no-op.  Bundled
# in one DDLQuery so the drops share a transaction scope: a partial drop
# would leave the DDL in an inconsistent shape relative to
# ``CATALOGS_TABLE_DDL``.
_DROP_LEGACY_CATALOG_METADATA_DDL = """
ALTER TABLE catalog.catalogs
    DROP COLUMN IF EXISTS title,
    DROP COLUMN IF EXISTS description,
    DROP COLUMN IF EXISTS keywords,
    DROP COLUMN IF EXISTS license,
    DROP COLUMN IF EXISTS conforms_to,
    DROP COLUMN IF EXISTS links,
    DROP COLUMN IF EXISTS assets,
    DROP COLUMN IF EXISTS stac_version,
    DROP COLUMN IF EXISTS stac_extensions,
    DROP COLUMN IF EXISTS extra_metadata;
"""


async def drop_legacy_catalog_metadata_columns(conn: DbResource) -> None:
    """Idempotently drop the legacy metadata columns from ``catalog.catalogs``.

    Pre-conditions:

    - :func:`backfill_catalog_metadata_from_legacy` has copied the
      legacy column values into ``catalog.catalog_metadata_core`` /
      ``_stac``.  Running this drop without the backfill would orphan
      every pre-M2.0 catalog's metadata.
    - No code path writes to the legacy columns (M2.5a).
    - No code path reads from the legacy columns (M2.4 read flip).

    Idempotent: re-runs after the first successful drop find nothing
    to drop and issue a no-op ALTER.  Safe to call on every boot.
    """
    await DDLQuery(_DROP_LEGACY_CATALOG_METADATA_DDL).execute(conn)


async def backfill_catalog_metadata_from_legacy(conn: DbResource) -> None:
    """Copy legacy ``catalog.catalogs`` metadata columns into the split tables.

    One-shot migration for deployments that predate M2.0.  After this
    runs once, subsequent catalog writes land in the split tables via
    the M2.2 ``init_catalog_metadata`` lifecycle hooks; readers in M2.4+
    pull from the split tables first.

    The call ships a single ``DO $$ … $$`` PL/pgSQL block that:

    1. Takes ``ACCESS SHARE`` on ``catalog.catalogs`` for the duration
       of the transaction.  M2.5b's ``DROP COLUMN`` needs ``ACCESS
       EXCLUSIVE`` and therefore waits for this block to commit — no
       race between probe and INSERT under multi-pod startup.
    2. Probes ``information_schema.columns`` for the ``title`` column
       (canary for the full legacy column set).  If absent (post-M2.5b
       deployment), the INSERTs are skipped — this function is safe to
       call on every boot.
    3. Runs two ``INSERT … SELECT … ON CONFLICT DO NOTHING`` statements
       inside the IF branch via ``EXECUTE`` so the parser only touches
       legacy column names when they are actually there.  Both INSERTs
       share the DO block's transaction scope — partial backfills are
       impossible.

    Idempotency: re-runs after a successful backfill find the target
    rows already present and the ``ON CONFLICT DO NOTHING`` turns each
    statement into a no-op.  Re-runs after M2.5b's column drop skip
    the INSERTs entirely via the ``IF EXISTS`` check.
    """
    await DDLQuery(_BACKFILL_CATALOG_METADATA_DDL).execute(conn)


async def rename_legacy_metadata_tables(conn: DbResource, schema: str) -> None:
    """Rename ``{schema}.metadata*`` to ``{schema}.collection_metadata*``.

    Runs BEFORE the ``CREATE TABLE IF NOT EXISTS`` statements (both the
    legacy ``METADATA_DDL`` in ``catalog_service.py`` and the M2.0 DDLs
    above) so that:

    - Existing tenants get their tables renamed once; subsequent
      ``CREATE TABLE IF NOT EXISTS collection_metadata*`` statements find
      the table already present and are a no-op.
    - Fresh tenants have no legacy tables; the DO block sees both source
      and target missing and is a no-op.  The ``CREATE TABLE`` statements
      then create the canonical names directly.

    Idempotent, atomic (single transaction block), and safe to call on
    every ``initialize_core_tenant_tables`` invocation.

    Implementation note
    -------------------
    The DO block uses ``{schema}`` in two syntactic positions:

    1. As a table qualifier (``ALTER TABLE {schema}.metadata …``) — an
       SQL *identifier*, which needs the dialect's identifier quoting
       (PostgreSQL: double-quoted).
    2. Inside an ``information_schema`` predicate
       (``WHERE table_schema = '{schema}'``) — an SQL *string literal*,
       which needs the raw, unquoted schema name.

    ``TemplateQueryBuilder`` in ``query_executor.py`` rewrites every
    ``{schema}`` placeholder as an identifier, so handing the template
    to ``DDLQuery(...).execute(conn, schema=schema)`` would substitute
    ``'"myschema"'`` into the string-literal positions — the
    ``information_schema.tables.table_schema`` column stores the
    unquoted value ``myschema``, so the comparison would never match
    and the rename would silently no-op, leaving legacy data orphaned.

    Work-around: validate the schema name (rejects reserved words /
    non-identifier characters), then format the DDL body in Python so
    the identifier-vs-literal distinction is preserved.  ``DDLQuery``
    is still used for execution — it just no longer has any template
    placeholders to substitute.

    Schema-name guard
    -----------------
    :func:`dynastore.tools.db.validate_sql_identifier` is general-
    purpose: it accepts ``[a-z0-9_.>-]`` because it also guards JSON
    path expressions like ``data.key`` and ``data->key``.  That
    permissiveness is unsafe for a PG schema name — a value like
    ``my.schema`` would pass validation but produce
    ``ALTER TABLE my.schema.metadata RENAME TO …`` which PG parses as
    three dotted identifiers, unambiguously the WRONG target.  Add a
    stricter guard here rejecting ``.`` and ``-`` for this specific
    call site.  (Existing tenant schemas only use
    ``[a-z0-9_]`` so this is a tightening, not a regression.)
    """
    safe_schema = validate_sql_identifier(schema)
    if "." in safe_schema or "-" in safe_schema:
        from dynastore.tools.db import InvalidIdentifierError

        raise InvalidIdentifierError(
            f"Schema name {schema!r} contains characters disallowed for a "
            "PostgreSQL schema identifier in rename DDL ('.' or '-')."
        )
    ddl = TENANT_LEGACY_METADATA_RENAME_DDL.format(schema=safe_schema)
    await DDLQuery(ddl).execute(conn)


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
