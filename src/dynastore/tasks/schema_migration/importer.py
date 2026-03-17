#    Copyright 2025 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
Collection data importer for schema migrations.

Reads Parquet files produced by ``exporter.py`` and bulk-INSERTs rows
into the newly created physical tables, mapping common columns and
re-encoding WKB hex strings back to PostGIS geometry.

Usage::

    counts = await import_collection(engine, schema, physical_table, sidecar_ids, export_dir)
"""

import logging
import os
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.tasks.schema_migration.exporter import _describe_table, _GEOMETRY_UDTS

logger = logging.getLogger(__name__)

# Batch size for INSERT loops
_BATCH_SIZE = 500


async def import_table(
    engine: DbResource,
    schema: str,
    table_name: str,
    parquet_path: str,
) -> int:
    """
    Import rows from a Parquet file into ``schema.table_name``.

    Only columns that exist in **both** the Parquet file and the target table
    are transferred. New columns in the target receive their DEFAULT value.
    Columns removed from the target are silently dropped from the insert.

    Geometry columns (detected by UDT) are re-encoded from WKB hex to PostGIS
    geometry via ``ST_GeomFromEWKB``.

    Returns:
        Number of rows inserted.
    """
    import pandas as pd  # lazy import

    if not os.path.exists(parquet_path):
        logger.warning(f"import_table: file not found '{parquet_path}', skipping.")
        return 0

    from dynastore.modules.concurrency import run_in_thread

    df = await run_in_thread(pd.read_parquet, parquet_path, engine="pyarrow")
    if df.empty:
        logger.info(f"import_table: '{table_name}' has 0 rows, nothing to import.")
        return 0

    # Introspect target table columns
    target_cols = await _describe_table(engine, schema, table_name)
    if not target_cols:
        raise ValueError(
            f"import_table: target table '{schema}'.'{table_name}' not found."
        )
    target_col_info = {c["name"]: c["udt_name"].lower() for c in target_cols}

    # Determine intersection of columns: source ∩ target
    common_cols = [c for c in df.columns if c in target_col_info]
    if not common_cols:
        raise ValueError(
            f"import_table: no common columns between source and '{schema}'.'{table_name}'."
        )

    geo_cols = {c for c in common_cols if target_col_info[c] in _GEOMETRY_UDTS}

    # Build INSERT SQL with geometry casting for geo columns
    col_list = ", ".join(f'"{c}"' for c in common_cols)
    value_placeholders = []
    for c in common_cols:
        if c in geo_cols:
            # WKB hex → PostGIS geometry
            value_placeholders.append(f"ST_GeomFromEWKB(decode(:{c}, 'hex'))")
        else:
            value_placeholders.append(f":{c}")
    values_clause = ", ".join(value_placeholders)

    insert_sql = (
        f'INSERT INTO "{schema}"."{table_name}" ({col_list}) '
        f"VALUES ({values_clause}) "
        f"ON CONFLICT DO NOTHING;"
    )

    total = 0
    df_subset = df[common_cols]
    rows = df_subset.where(df_subset.notna(), None).to_dict(orient="records")

    for i in range(0, len(rows), _BATCH_SIZE):
        batch = rows[i : i + _BATCH_SIZE]
        async with managed_transaction(engine) as conn:
            for row in batch:
                # None values stay as None (NULL) which asyncpg handles correctly
                await DQLQuery(
                    insert_sql, result_handler=ResultHandler.NONE
                ).execute(conn, **{k: v for k, v in row.items() if k in common_cols})
        total += len(batch)
        logger.debug(f"import_table: '{table_name}' inserted batch {i // _BATCH_SIZE + 1}")

    logger.info(f"import_table: Imported {total} rows into '{schema}'.'{table_name}'")
    return total


async def import_collection(
    engine: DbResource,
    schema: str,
    physical_table: str,
    sidecar_ids: List[str],
    export_dir: str,
) -> Dict[str, int]:
    """
    Import hub + sidecar tables from Parquet files produced by ``export_collection``.

    Args:
        engine:         DB engine.
        schema:         Tenant schema name.
        physical_table: Hub table name (destination table must already exist).
        sidecar_ids:    Sidecar IDs; each maps to ``{physical_table}_{sidecar_id}`` table.
        export_dir:     Directory containing ``.parquet`` files from ``export_collection``.

    Returns:
        Dict mapping table name → row count inserted.
    """
    tables = [physical_table] + [f"{physical_table}_{sid}" for sid in sidecar_ids]
    counts: Dict[str, int] = {}
    for table in tables:
        parquet_path = os.path.join(export_dir, f"{table}.parquet")
        try:
            count = await import_table(engine, schema, table, parquet_path)
            counts[table] = count
        except Exception as e:
            logger.error(
                f"import_collection: failed importing '{table}': {e}", exc_info=True
            )
            raise
    return counts
