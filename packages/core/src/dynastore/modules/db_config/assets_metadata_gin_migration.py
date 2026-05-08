#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Global one-shot migration: backfill the GIN index on ``assets.metadata``
across every tenant schema.

Pre-2026-05-08 the per-tenant ``assets`` DDL did not include a GIN index on
the ``metadata`` JSONB column. JSONB containment queries (``@>``, ``?``,
``?&``, ``?|``) issued by ``POST /assets/catalogs/{catalog_id}/assets-search``
with metadata filters degrade to a full sequential scan once a catalog
crosses ~100k assets. The DDL was corrected to emit
``idx_assets_metadata_gin_{schema_tag}`` at table creation time, but
``CREATE INDEX IF NOT EXISTS`` is only run during ``ensure_storage()`` for
each catalog — already-provisioned catalogs do not pick it up.

Design
------
* Scanned by ``pg_class`` / ``pg_index`` across **every schema** in one
  query. The pattern (table named ``assets``, has a ``metadata`` column,
  no GIN index on that column) is narrow enough that the scan is cheap
  and self-idempotent: once the index exists, the row drops out of the
  result set.
* No ``PropertiesProtocol`` bookkeeping. The query itself is the "done
  flag": if it returns zero rows the migration is over.
* ``CREATE INDEX CONCURRENTLY`` is NOT used — it cannot run inside a
  transaction and the catalog module's startup path is transactional.
  A regular ``CREATE INDEX`` briefly blocks writes on ``assets`` while
  the index builds; this is acceptable on startup before the API is
  serving traffic.
* Each backfill runs in its own ``managed_transaction`` so a single
  failed schema doesn't block the rest.
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


_find_missing_gin_query = DQLQuery(
    """
    SELECT n.nspname AS schema_name
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid
     WHERE c.relname = 'assets'
       AND c.relkind IN ('r', 'p')
       AND a.attname = 'metadata'
       AND a.attnum > 0
       AND NOT EXISTS (
           SELECT 1
             FROM pg_index x
             JOIN pg_class i ON i.oid = x.indexrelid
             JOIN pg_am am   ON am.oid = i.relam
            WHERE x.indrelid = c.oid
              AND am.amname = 'gin'
              AND a.attnum = ANY(x.indkey::int[])
       )
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


async def migrate_assets_metadata_gin_index(engine: Any) -> int:
    """Create the missing ``idx_assets_metadata_gin_*`` GIN index on
    every tenant ``assets`` table that lacks one.

    Returns the number of indexes created (``0`` when the tree is clean).
    Safe to call on every startup: the scan is narrow, the create is
    driven by query output, and duplicate invocations are no-ops because
    the second pass finds no candidates.
    """
    if engine is None:
        return 0

    async with managed_transaction(engine) as conn:
        rows = await _find_missing_gin_query.execute(conn)

    if not rows:
        return 0

    logger.warning(
        "Assets metadata GIN backfill: creating index on %d tenant schema(s).",
        len(rows),
    )

    created = 0
    for row in rows:
        schema = row["schema_name"]
        schema_tag = schema.replace(".", "_")
        sql = (
            f'CREATE INDEX IF NOT EXISTS idx_assets_metadata_gin_{schema_tag} '
            f'ON "{schema}".assets USING GIN (metadata jsonb_path_ops);'
        )
        try:
            async with managed_transaction(engine) as conn:
                await DDLQuery(sql).execute(conn)
            created += 1
            logger.info(
                "Assets metadata GIN backfill: created index on %s.assets",
                schema,
            )
        except Exception as exc:  # noqa: BLE001 — surface but keep going
            logger.error(
                "Assets metadata GIN backfill: failed on %s.assets: %s",
                schema, exc,
            )
    return created


__all__ = ["migrate_assets_metadata_gin_index"]
