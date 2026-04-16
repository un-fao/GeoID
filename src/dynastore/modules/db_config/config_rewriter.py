#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Startup config-row rewriter.

Translates stale ``class_key`` values in all config tables to the new
PascalCase names introduced in v0.3.14+ (class-as-identity model).

Called once from :func:`PlatformConfigService.initialize_storage` after the
DDL is ensured.  Each ``UPDATE`` is idempotent: rows whose ``class_key`` is
already the new name are unaffected.
"""

import logging
from typing import List

from sqlalchemy import text
from .query_executor import managed_transaction, DbResource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rename map — old class_key  →  new class_key
# ---------------------------------------------------------------------------

# Ordered so that longer / more-specific names come first to prevent
# inadvertent substring matches if any future key shares a prefix.
_CLASS_KEY_RENAMES = [
    # Tiles
    ("tiles", "TilesConfig"),
    # Driver classes
    ("DriverRecordsElasticsearchObfuscated", "CollectionElasticsearchObfuscatedDriver"),
    ("DriverRecordsElasticsearch", "CollectionElasticsearchDriver"),
    ("DriverRecordsPostgresql", "CollectionPostgresqlDriver"),
    ("DriverRecordsIceberg", "CollectionIcebergDriver"),
    ("DriverRecordsDuckdb", "CollectionDuckdbDriver"),
    ("DriverAssetElasticsearch", "AssetElasticsearchDriver"),
    ("DriverAssetPostgresql", "AssetPostgresqlDriver"),
    ("DriverMetadataPostgresql", "MetadataPostgresqlDriver"),
    ("DriverMetadataElasticsearch", "MetadataElasticsearchDriver"),
    # Metadata driver configs
    ("DriverMetadataElasticsearchConfig", "MetadataElasticsearchDriverConfig"),
    # Config classes
    ("DriverRecordsElasticsearchConfig", "CollectionElasticsearchDriverConfig"),
    ("DriverRecordsPostgresqlConfig", "CollectionPostgresqlDriverConfig"),
    ("DriverRecordsIcebergConfig", "CollectionIcebergDriverConfig"),
    ("DuckDbCollectionDriverConfig", "CollectionDuckdbDriverConfig"),
    ("DriverAssetElasticsearchConfig", "AssetElasticsearchDriverConfig"),
    ("DriverAssetPostgresqlConfig", "AssetPostgresqlDriverConfig"),
    # Routing / feature-type configs
    ("AssetRoutingPluginConfig", "AssetRoutingConfig"),
    ("RoutingPluginConfig", "CollectionRoutingConfig"),
    ("FeatureTypePluginConfig", "CollectionSchema"),
    # M8: FeatureTypeConfig (M1 rename target) → CollectionSchema (M8 rename)
    ("FeatureTypeConfig", "CollectionSchema"),
    ("TilesPluginConfig", "TilesConfig"),
]


async def _rewrite_table(conn, qualified_table: str, total: int) -> int:
    """Update stale class_keys in *qualified_table*.  Returns count of updated rows."""
    updated = 0
    for old_key, new_key in _CLASS_KEY_RENAMES:
        result = await conn.execute(
            f"UPDATE {qualified_table} SET class_key = :new_key WHERE class_key = :old_key",
            {"new_key": new_key, "old_key": old_key},
        )
        rows_affected = result.rowcount if hasattr(result, "rowcount") else 0
        if rows_affected:
            logger.info(
                "config_rewriter: %s  %r → %r  (%d row(s))",
                qualified_table, old_key, new_key, rows_affected,
            )
            updated += rows_affected
    return updated


async def rewrite_config_class_keys(engine: DbResource) -> None:
    """Rewrite all stale class_keys across platform + all tenant config tables.

    Idempotent — safe to run on every startup.  A fresh database with only
    new-style keys produces zero updates and exits quickly.
    """
    from .locking_tools import check_table_exists

    total_updated = 0

    async with managed_transaction(engine) as conn:
        # 1. Platform-level table
        if await check_table_exists(conn, "platform_configs", "configs"):
            updated = await _rewrite_table(conn, "configs.platform_configs", 0)
            total_updated += updated

        # 2. Tenant schemas — catalog_configs + collection_configs
        schemas = await _get_tenant_schemas(conn)
        for schema in schemas:
            if await check_table_exists(conn, "catalog_configs", schema):
                updated = await _rewrite_table(
                    conn, f'"{schema}".catalog_configs', 0
                )
                total_updated += updated
            if await check_table_exists(conn, "collection_configs", schema):
                updated = await _rewrite_table(
                    conn, f'"{schema}".collection_configs', 0
                )
                total_updated += updated

    if total_updated:
        logger.info(
            "config_rewriter: completed — %d row(s) updated across all scopes.",
            total_updated,
        )
    else:
        logger.debug("config_rewriter: no stale class_keys found (no-op).")


# ---------------------------------------------------------------------------
# Field-strip rewriter — remove connection fields from per-collection rows
# ---------------------------------------------------------------------------

# Iceberg connection fields that migrated to IcebergConfig env vars in M4.
# These keys are stripped from config_data JSONB for CollectionIcebergDriverConfig rows.
_ICEBERG_REMOVED_FIELDS = (
    "catalog_name",
    "catalog_uri",
    "catalog_type",
    "catalog_properties",
    "warehouse_uri",
    "warehouse_scheme",
)


async def _strip_iceberg_fields_from_table(conn, qualified_table: str) -> int:
    """Remove stale Iceberg connection fields from CollectionIcebergDriverConfig rows.

    Uses PostgreSQL JSONB subtraction to atomically remove all keys in one UPDATE.
    Only touches rows with class_key = 'CollectionIcebergDriverConfig'.
    Returns count of updated rows.
    """
    # Build: config_data - 'key1' - 'key2' - ...
    subtraction = " - ".join(
        ["config_data"] + [f"'{k}'" for k in _ICEBERG_REMOVED_FIELDS]
    )
    condition = " OR ".join(f"config_data ? '{k}'" for k in _ICEBERG_REMOVED_FIELDS)
    result = await conn.execute(
        text(
            f"UPDATE {qualified_table} "
            f"SET config_data = {subtraction} "
            f"WHERE class_key = 'CollectionIcebergDriverConfig' "
            f"  AND ({condition})"
        ),
    )
    rows_affected = result.rowcount if hasattr(result, "rowcount") else 0
    if rows_affected:
        logger.info(
            "config_rewriter: stripped Iceberg connection fields from %s (%d row(s))",
            qualified_table, rows_affected,
        )
    return rows_affected


async def rewrite_metadata_routing_fields(engine) -> None:
    """Migrate CollectionRoutingConfig rows from the old metadata slot shape to the new.

    Old shape (pre-M5):
        ``config_data.metadata.override`` — list of CollectionMetadataStore entries
        ``config_data.metadata.storage``  — list of CollectionItemsStore entries

    New shape (M5):
        ``config_data.metadata.operations.READ``      — was ``override``
        ``config_data.metadata.operations.TRANSFORM`` — was ``storage``

    The ``override`` and ``storage`` keys are removed after migration.
    Idempotent — rows already in the new shape produce 0 updates.
    """
    from .locking_tools import check_table_exists

    total_updated = 0

    # SQL that restructures the metadata sub-object in one JSONB expression.
    # Conditions: only runs when either old key is present.
    _MIGRATE_SQL = """
        UPDATE {table}
        SET config_data = jsonb_set(
            config_data,
            '{{metadata}}',
            (config_data->'metadata' - 'override' - 'storage')
            || jsonb_build_object(
                'operations', jsonb_build_object(
                    'READ', COALESCE(config_data->'metadata'->'override', '[]'::jsonb),
                    'TRANSFORM', COALESCE(config_data->'metadata'->'storage', '[]'::jsonb)
                )
            )
        )
        WHERE class_key = 'CollectionRoutingConfig'
          AND (
            config_data->'metadata' ? 'override'
            OR config_data->'metadata' ? 'storage'
          )
    """

    async with managed_transaction(engine) as conn:
        schemas = await _get_tenant_schemas(conn)

        # Platform-level table
        if await check_table_exists(conn, "platform_configs", "configs"):
            result = await conn.execute(text(_MIGRATE_SQL.format(table="configs.platform_configs")))  # type: ignore[misc]
            rows = result.rowcount if hasattr(result, "rowcount") else 0  # type: ignore[union-attr]
            if rows:
                logger.info(
                    "config_rewriter: metadata routing migrated in configs.platform_configs (%d row(s))",
                    rows,
                )
            total_updated += rows

        # Tenant schemas — catalog_configs + collection_configs
        for schema in schemas:
            for tbl in ("catalog_configs", "collection_configs"):
                if await check_table_exists(conn, tbl, schema):
                    qualified = f'"{schema}".{tbl}'
                    result = await conn.execute(text(_MIGRATE_SQL.format(table=qualified)))  # type: ignore[misc]
                    rows = result.rowcount if hasattr(result, "rowcount") else 0  # type: ignore[union-attr]
                    if rows:
                        logger.info(
                            "config_rewriter: metadata routing migrated in %s (%d row(s))",
                            qualified, rows,
                        )
                    total_updated += rows

    if total_updated:
        logger.info(
            "config_rewriter: metadata routing migration complete — %d row(s) updated.",
            total_updated,
        )
    else:
        logger.debug("config_rewriter: metadata routing migration — no stale rows found (no-op).")


async def rewrite_iceberg_catalog_fields(engine) -> None:
    """Strip Iceberg catalog/warehouse connection fields from all collection config tables.

    These fields moved to ``IcebergConfig`` env vars in M4.  Rows that still
    carry them will fail the per-collection model validator; this rewriter removes
    them at startup before any config is loaded.

    Idempotent — rows already stripped produce 0 updates.
    """
    from .locking_tools import check_table_exists

    total_updated = 0

    async with managed_transaction(engine) as conn:
        schemas = await _get_tenant_schemas(conn)
        for schema in schemas:
            if await check_table_exists(conn, "collection_configs", schema):
                updated = await _strip_iceberg_fields_from_table(
                    conn, f'"{schema}".collection_configs'
                )
                total_updated += updated

    if total_updated:
        logger.info(
            "config_rewriter: Iceberg field-strip complete — %d row(s) updated.",
            total_updated,
        )
    else:
        logger.debug("config_rewriter: Iceberg field-strip — no stale rows found (no-op).")


async def _get_tenant_schemas(conn) -> List[str]:
    """Return physical_schema values from catalog.catalogs (if table exists)."""
    from .locking_tools import check_table_exists
    from .query_executor import DQLQuery, ResultHandler

    if not await check_table_exists(conn, "catalogs", "catalog"):
        return []

    rows = await DQLQuery(
        "SELECT physical_schema FROM catalog.catalogs WHERE physical_schema IS NOT NULL;",
        result_handler=ResultHandler.ALL_DICTS,
    ).execute(conn) or []
    return [r["physical_schema"] for r in rows]
