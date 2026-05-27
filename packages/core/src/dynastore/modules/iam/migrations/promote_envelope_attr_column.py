#    Copyright 2026 FAO
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

"""Migration: promote an ``_attrs.<key>`` JSONB path to a first-class PG column.

For each per-collection opt-in promotion the operator calls::

    POST /admin/catalogs/{cat}/collections/{coll}/attrs/promote
    {"column": "dept", "pg_type": "TEXT"}

This triggers :func:`promote_envelope_attr_column` which:

1. Checks idempotency via ``iam.applied_presets``
   (sentinel ``promote_envelope_attr:{catalog_id}:{collection_id}:{column_name}``).
2. Adds the column ``_attr_{column_name} {pg_type}`` via DDL (IF NOT EXISTS).
3. Creates a btree (or GIN for ``TEXT[]``) index on the new column.
4. Backfills existing rows in batches of 5000.
5. Records the sentinel.

All DDL/DML uses the ``DDLQuery`` / ``DQLQuery`` wrappers — never raw
``conn.execute(text(...))``.  The whole sequence runs inside a single
:func:`~dynastore.modules.db_config.query_executor.managed_transaction`.

Identifier safety: ``column_name`` comes from operator input via the admin
endpoint. It is validated against :data:`~dynastore.modules.iam.stamping_config._KEY_PATTERN`
before reaching this function, and the resulting column identifier is always
interpolated as ``_attr_{column_name}`` where ``column_name`` is guaranteed to
match ``[A-Za-z_][A-Za-z0-9_]*``.  The prefix ``_attr_`` ensures the combined
name is always a valid unquoted PG identifier and prevents collisions with
system columns.  Schema and catalog identifiers come from the internal catalog
registry (not user input) — they are double-quoted at interpolation sites for
defence-in-depth.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.iam.stamping_config import _ALLOWED_PG_TYPES, _KEY_PATTERN

logger = logging.getLogger(__name__)

_BATCH_SIZE = 5000

# ---------------------------------------------------------------------------
# Sentinel helpers (mirrors add_grants_attribute_predicates pattern)
# ---------------------------------------------------------------------------

_SELECT_APPLIED = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

_INSERT_APPLIED = DQLQuery(
    """
    INSERT INTO iam.applied_presets
        (preset_name, scope_key, state, applied_at, applied_by,
         params_snapshot, revoke_descriptor, updated_at)
    VALUES
        (:preset_name, :scope_key, 'applied', NOW(), NULL,
         :params_snapshot, :revoke_descriptor, NOW())
    ON CONFLICT (preset_name, scope_key) DO NOTHING
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

# ---------------------------------------------------------------------------
# Backfill count query (run per batch)
# ---------------------------------------------------------------------------

_COUNT_BACKFILL_REMAINING = DQLQuery(
    """
    SELECT COUNT(*) AS cnt
    FROM {schema}.items
    WHERE {col_ident} IS NULL
      AND (envelope->'attrs'->>'{{attr_key}}') IS NOT NULL
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)


def _sentinel_key(catalog_id: str, collection_id: str, column_name: str) -> str:
    return f"promote_envelope_attr:{catalog_id}:{collection_id}:{column_name}"


async def promote_envelope_attr_column(
    engine: Any,
    catalog_id: str,
    collection_id: str,
    column_name: str,
    pg_type: str,
    schema: str,
) -> None:
    """Promote ``_attrs.{column_name}`` to a first-class PG column.

    Parameters
    ----------
    engine:
        Async SQLAlchemy engine.
    catalog_id:
        Catalog identifier (used for sentinel key only).
    collection_id:
        Collection identifier (used for sentinel key only).
    column_name:
        Attribute key — must match ``[A-Za-z_][A-Za-z0-9_]*``.  The column
        created will be ``_attr_{column_name}``.
    pg_type:
        PostgreSQL type string; must be in ``_ALLOWED_PG_TYPES``.
    schema:
        Physical PG schema name for this catalog (e.g. ``"s_abc123"``).
    """
    # --- Input validation (last-defence; callers validate too) --------------
    if not _KEY_PATTERN.fullmatch(column_name):
        raise ValueError(
            f"column_name {column_name!r} must match [A-Za-z_][A-Za-z0-9_]*"
        )
    if pg_type not in _ALLOWED_PG_TYPES:
        raise ValueError(
            f"pg_type {pg_type!r} not allowed; must be one of {sorted(_ALLOWED_PG_TYPES)}"
        )

    # Build safe identifiers.  column_name is validated to [A-Za-z_][A-Za-z0-9_]*
    # so `_attr_` prefix + validated name is always a safe unquoted identifier.
    col_name = f"_attr_{column_name}"
    # Double-quote schema name for defence-in-depth.
    schema_q = f'"{schema}"'
    col_ident = f'"{col_name}"'

    preset_name = _sentinel_key(catalog_id, collection_id, column_name)
    scope_key = f"{catalog_id}:{collection_id}"

    async with managed_transaction(engine) as conn:
        # --- Idempotency check -----------------------------------------------
        already = await _SELECT_APPLIED.execute(
            conn, preset_name=preset_name, scope_key=scope_key
        )
        if already is not None:
            logger.debug(
                "promote_envelope_attr_column: already applied for %s/%s/%s — skipping",
                catalog_id, collection_id, column_name,
            )
            return

        # --- 1. ADD COLUMN ---------------------------------------------------
        add_col_sql = (
            f"ALTER TABLE {schema_q}.items "
            f"ADD COLUMN IF NOT EXISTS {col_ident} {pg_type};"
        )
        await DDLQuery(add_col_sql).execute(conn)
        logger.info(
            "promote_envelope_attr_column: column %s added to %s.items",
            col_name, schema,
        )

        # --- 2. CREATE INDEX -------------------------------------------------
        idx_name = f'"idx_items_{col_name}"'
        if pg_type == "TEXT[]":
            idx_sql = (
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON {schema_q}.items USING GIN ({col_ident});"
            )
        else:
            idx_sql = (
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON {schema_q}.items ({col_ident});"
            )
        await DDLQuery(idx_sql).execute(conn)
        logger.info(
            "promote_envelope_attr_column: index %s created on %s.items",
            idx_name, schema,
        )

        # --- 3. BACKFILL (batched) -------------------------------------------
        # For TEXT[], the JSONB value may be a JSON array string; cast via
        # array constructor is driver-dependent.  For simplicity (and matching
        # the existing TEXT/NUMERIC/TIMESTAMPTZ pattern) we cast the raw
        # string value.  Operators requiring array promotion should verify
        # the cast is appropriate for their data.
        total_updated = 0
        while True:
            # column_name is validated against _KEY_PATTERN above;
            # quoting as a SQL string literal is safe.
            cn_sql_lit = f"'{column_name}'"
            backfill_sql = (
                f"UPDATE {schema_q}.items "
                f"SET {col_ident} = CAST((envelope->'attrs'->>{cn_sql_lit}) AS {pg_type}) "
                f"WHERE {col_ident} IS NULL "
                f"  AND (envelope->'attrs'->>{cn_sql_lit}) IS NOT NULL "
                f"LIMIT {_BATCH_SIZE};"
            )
            # DQLQuery ROWCOUNT gives us the number of rows updated.
            rows_updated = await DQLQuery(
                backfill_sql,
                result_handler=ResultHandler.ROWCOUNT,
            ).execute(conn)
            count = int(rows_updated or 0)
            total_updated += count
            logger.debug(
                "promote_envelope_attr_column: backfill batch %d rows for %s/%s/%s",
                count, catalog_id, collection_id, column_name,
            )
            if count < _BATCH_SIZE:
                break

        logger.info(
            "promote_envelope_attr_column: backfill complete — %d rows updated for %s/%s/%s",
            total_updated, catalog_id, collection_id, column_name,
        )

        # --- 4. Record sentinel ----------------------------------------------
        await _INSERT_APPLIED.execute(
            conn,
            preset_name=preset_name,
            scope_key=scope_key,
            params_snapshot=json.dumps(
                {
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "column_name": column_name,
                    "pg_type": pg_type,
                    "schema": schema,
                }
            ),
            revoke_descriptor=json.dumps(
                {
                    "note": (
                        f"Drop column {schema}.items.{col_name} and "
                        f"index idx_items_{col_name} to revert."
                    )
                }
            ),
        )
        logger.info(
            "promote_envelope_attr_column: sentinel recorded for %s/%s/%s",
            catalog_id, collection_id, column_name,
        )
