#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Core PostgreSQL metadata DDL — catalog + tenant ``*_metadata_core`` tables.

Backs the ``CatalogMetadataStore`` / ``CollectionMetadataStore`` core
drivers (``modules.storage.drivers.metadata_postgresql``).

Global (under ``catalog.`` schema):

- ``catalog.catalog_metadata_core`` — CORE fields (title, description,
  keywords, license, extra_metadata) keyed on ``catalog_id``.

Per-tenant (under each ``{schema}``):

- ``{schema}.collection_metadata_core`` — CORE collection metadata
  (title, description, keywords, license, extra_metadata).

The STAC sidecar tables (``*.catalog_metadata_stac`` /
``*.collection_metadata_stac``) live in
``modules.stac.db_init.metadata_stac_tables`` and run only when the STAC
module is installed.
"""

import logging

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DbResource,
)

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

# Freshness columns on ``catalog.catalogs``.  Canonical freshness tokens
# used by INDEX / BACKUP propagation (plan §Freshness contract).  Kept
# as an idempotent ALTER so deployments that have ``catalog.catalogs``
# from an older DDL pick up the columns without needing a manual hand-
# off; on fresh rebuilds the ALTER is a no-op.
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

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def ensure_global_metadata_core_tables(conn: DbResource) -> None:
    """Apply the global CORE DDL (``catalog.catalog_metadata_core``)
    plus the freshness columns on ``catalog.catalogs``.

    The STAC sidecar table (``catalog.catalog_metadata_stac``) is owned
    by the STAC module — when installed, ``StacModule.lifespan`` applies
    its DDL via ``ensure_global_stac_metadata_tables``.

    Idempotent.  Called once during ``CatalogModule`` init after the
    ``catalog.catalogs`` table has been created.
    """
    # Gate the ALTER TABLE batch on a column-existence check to skip the
    # ACCESS EXCLUSIVE lock acquisition when both freshness columns are
    # already present. Even ``ADD COLUMN IF NOT EXISTS`` requires the
    # exclusive lock during planning, and concurrent traffic on the
    # ``catalog.catalogs`` row keeps it perpetually pending — observed:
    # ``QueryCanceledError: canceling statement due to statement timeout``
    # blocks ``CatalogModule`` lifespan on every restart of a busy catalog.
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    column_count = await DQLQuery(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'catalog'
          AND table_name = 'catalogs'
          AND column_name IN ('created_at', 'updated_at')
        """,
        result_handler=ResultHandler.SCALAR,
    ).execute(conn)
    freshness_columns_present = (column_count or 0) >= 2

    if not freshness_columns_present:
        await DDLQuery(CATALOGS_FRESHNESS_COLUMNS_DDL).execute(conn)

    await DDLQuery(CATALOG_METADATA_CORE_DDL).execute(conn)


async def ensure_tenant_metadata_core_tables(conn: DbResource, schema: str) -> None:
    """Apply the per-tenant CORE DDL (``{schema}.collection_metadata_core``).

    The per-tenant STAC sidecar (``{schema}.collection_metadata_stac``) is
    owned by the STAC module — when installed, the lifecycle-registered
    ``_ensure_tenant_stac_metadata_tables`` initializer in
    ``modules.stac.db_init.metadata_stac_tables`` runs alongside this.

    Idempotent.  Called from ``create_catalog`` as the canonical
    collection-metadata storage provision step.  FK to
    ``{schema}.collections(id)`` is intentionally omitted because the
    collection row is inserted after ``create_catalog`` completes;
    enforcing the FK would require reordering or deferring constraints.
    A later cleanup pass may add deferred FKs once every tenant init has
    completed.
    """
    await DDLQuery(TENANT_METADATA_CORE_DDL).execute(conn, schema=schema)
