#    Copyright 2025 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
Collection data exporter for schema migrations.

Exports all rows from a collection's physical tables (hub + sidecars) to
Parquet files in a temporary directory.  Geometry columns are serialised as
WKB hex strings so no GIS library is required at export time.

Usage::

    reports = await export_collection(engine, schema, physical_table, sidecar_ids, tmp_dir)
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.tasks.schema_migration.models import TableExportReport

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Geometry column detection
# ---------------------------------------------------------------------------

_GEOMETRY_UDTS = frozenset({"geometry", "geography"})


async def _describe_table(
    engine: DbResource, schema: str, table_name: str
) -> List[Dict[str, str]]:
    """Return [{name, udt_name}, ...] for every column in a table."""
    sql = """
        SELECT column_name AS name, udt_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, schema=schema, table=table_name
        )
    return rows or []


def _build_select(columns: List[Dict[str, str]], table_expr: str) -> Tuple[str, List[str]]:
    """
    Build a SELECT statement that casts geometry/geography to WKB hex.
    Returns (sql_fragment, ordered_column_names).
    """
    parts = []
    names = []
    for col in columns:
        name = col["name"]
        udt = col["udt_name"].lower()
        if udt in _GEOMETRY_UDTS:
            # Export as hex-encoded WKB for portability
            parts.append(f'encode(ST_AsEWKB("{name}"), \'hex\') AS "{name}"')
        else:
            parts.append(f'"{name}"')
        names.append(name)
    return f'SELECT {", ".join(parts)} FROM {table_expr}', names


# ---------------------------------------------------------------------------
# Core export
# ---------------------------------------------------------------------------


async def export_table(
    engine: DbResource,
    schema: str,
    table_name: str,
    output_path: str,
) -> TableExportReport:
    """
    Export a single table to a Parquet file.

    Args:
        engine:       DB engine / connection.
        schema:       Tenant schema name (e.g. ``s_abc123``).
        table_name:   Physical table name within that schema.
        output_path:  Full path for the output ``.parquet`` file.

    Returns:
        ``TableExportReport`` with row count, column list, and file path.
    """
    import pandas as pd  # lazy — not available in all build targets

    columns = await _describe_table(engine, schema, table_name)
    if not columns:
        raise ValueError(
            f"export_table: table '{schema}'.'{table_name}' not found or has no columns."
        )

    table_expr = f'"{schema}"."{table_name}"'
    select_sql, col_names = _build_select(columns, table_expr)

    logger.info(f"Exporting '{schema}'.'{table_name}' → {output_path} …")
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(
            select_sql, result_handler=ResultHandler.ALL_DICTS
        ).execute(conn)

    rows = rows or []

    # Convert to DataFrame and write Parquet in a thread (blocking I/O)
    from dynastore.modules.concurrency import run_in_thread

    def _write_parquet(rows_: list, path_: str, cols_: list) -> int:
        df_ = pd.DataFrame(rows_, columns=cols_)
        os.makedirs(os.path.dirname(path_), exist_ok=True)
        df_.to_parquet(path_, index=False, engine="pyarrow")
        return len(df_)

    row_count = await run_in_thread(_write_parquet, rows, output_path, col_names)

    logger.info(
        f"Exported {row_count} rows from '{schema}'.'{table_name}' → {output_path}"
    )
    return TableExportReport(
        table_name=table_name,
        backup_name="",  # filled in by caller after rename
        row_count=row_count,
        file_path=output_path,
        columns=col_names,
    )


async def export_collection(
    engine: DbResource,
    schema: str,
    physical_table: str,
    sidecar_ids: List[str],
    export_dir: str,
) -> List[TableExportReport]:
    """
    Export hub table + all sidecar tables to Parquet files.

    Args:
        engine:         DB engine.
        schema:         Tenant schema name.
        physical_table: Hub table name.
        sidecar_ids:    Sidecar IDs (each produces table ``{physical_table}_{sidecar_id}``).
        export_dir:     Directory in which to create the Parquet files.

    Returns:
        List of ``TableExportReport`` objects — one per table.
    """
    tables = [physical_table] + [f"{physical_table}_{sid}" for sid in sidecar_ids]
    reports = []
    for table in tables:
        out_path = os.path.join(export_dir, f"{table}.parquet")
        try:
            report = await export_table(engine, schema, table, out_path)
            reports.append(report)
        except Exception as e:
            logger.warning(
                f"export_collection: skipping table '{table}' — {e}"
            )
    return reports


# ---------------------------------------------------------------------------
# Table rename helpers (backup / restore)
# ---------------------------------------------------------------------------


async def rename_table(
    engine: DbResource, schema: str, old_name: str, new_name: str
) -> None:
    """Rename a table within a schema."""
    sql = f'ALTER TABLE "{schema}"."{old_name}" RENAME TO "{new_name}";'
    async with managed_transaction(engine) as conn:
        from dynastore.modules.db_config.query_executor import DDLQuery
        await DDLQuery(sql).execute(conn)
    logger.info(f"Renamed '{schema}'.'{old_name}' → '{new_name}'")


async def backup_table_names(
    engine: DbResource,
    schema: str,
    physical_table: str,
    sidecar_ids: List[str],
    timestamp: str,
) -> Dict[str, str]:
    """
    Rename hub + sidecar tables to ``{name}_bkp_{timestamp}`` pattern.

    Returns a mapping ``{original_name: backup_name}`` for all renamed tables.
    """
    tables = [physical_table] + [f"{physical_table}_{sid}" for sid in sidecar_ids]
    mapping: Dict[str, str] = {}
    for table in tables:
        backup_name = f"{table}_bkp_{timestamp}"
        try:
            await rename_table(engine, schema, table, backup_name)
            mapping[table] = backup_name
        except Exception as e:
            logger.warning(
                f"backup_table_names: could not rename '{table}' — {e}"
            )
    return mapping


async def restore_backup_table_names(
    engine: DbResource,
    schema: str,
    backup_mapping: Dict[str, str],
) -> None:
    """Reverse a backup rename mapping (used on migration failure)."""
    for original, backup in backup_mapping.items():
        try:
            await rename_table(engine, schema, backup, original)
        except Exception as e:
            logger.error(
                f"restore_backup_table_names: could not restore '{backup}' → '{original}': {e}"
            )
