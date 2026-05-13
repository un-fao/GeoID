#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Global one-shot migration: purge stored ``AssetRoutingConfig`` rows.

#637 / #620 flipped the asset routing default from ES-first READ to
PG-first READ (PG is the canonical SOR; ES is the index). Existing
per-catalog and per-collection overrides stored in the typed-config
tables still ship the old ES-first shape and would silently keep the
inverted routing on already-provisioned tenants.

This migration deletes every row whose ``class_key`` is
``asset_routing_config`` from ``configs.platform_configs`` and from
every tenant's ``catalog_configs`` / ``collection_configs`` table. Once
the rows are gone, the runtime falls back to the new defaults declared
in ``AssetRoutingConfig.operations``.

No backward-compatibility shim is provided: operators who had bespoke
asset routing overrides must re-create them after upgrade with the new
shape.

Design
------
* ``pg_class`` / ``pg_namespace`` scan locates every schema that owns a
  ``catalog_configs`` or ``collection_configs`` table; the platform
  table is handled directly at ``configs.platform_configs``.
* Each DELETE runs in its own ``managed_transaction`` so a single
  schema failure does not block the rest.
* Self-idempotent: subsequent invocations re-run the same DELETEs but
  find zero matching rows.
"""

from __future__ import annotations

import logging
from typing import Any

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_ASSET_ROUTING_CLASS_KEY = "asset_routing_config"


_find_tenant_config_tables_query = DQLQuery(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND c.relname IN ('catalog_configs', 'collection_configs')
       AND n.nspname <> 'configs'
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


async def purge_asset_routing_config_rows(engine: Any) -> int:
    """Delete every stored ``AssetRoutingConfig`` row across the database.

    Returns the number of tables successfully purged (``0`` if engine is
    None). Safe to call on every startup.
    """
    if engine is None:
        return 0

    async with managed_transaction(engine) as conn:
        tenant_tables = await _find_tenant_config_tables_query.execute(conn)

    targets = [("configs", "platform_configs")] + [
        (row["schema_name"], row["table_name"]) for row in tenant_tables
    ]

    purged = 0
    for schema, table in targets:
        sql = (
            f'DELETE FROM "{schema}"."{table}" '
            f"WHERE class_key = '{_ASSET_ROUTING_CLASS_KEY}';"
        )
        try:
            async with managed_transaction(engine) as conn:
                await DDLQuery(sql).execute(conn)
            purged += 1
            logger.info(
                "AssetRoutingConfig purge: cleared %s.%s", schema, table,
            )
        except Exception as exc:  # noqa: BLE001 — never block startup
            logger.error(
                "AssetRoutingConfig purge: failed on %s.%s: %s",
                schema, table, exc,
            )
    return purged


__all__ = ["purge_asset_routing_config_rows"]
