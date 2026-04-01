#    Copyright 2025 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
Schema Migration Task.

Safe, data-preserving migration of a physical collection's schema.

Workflow:
    1. Validate target collection is physical and has a hub table.
    2. Set collection lock in ``extra_metadata`` (prevents concurrent migrations).
    3. Export hub + sidecar tables to Parquet in a temp directory.
    4. Verify export row counts.
    5. Rename old tables to ``{table}_bkp_{timestamp}``.
    6. Create new physical tables via ``create_physical_collection_impl``
       using the target config (new schema).
    7. Import data from Parquet → new tables, mapping common columns.
    8. Verify import row counts match export row counts.
    9. Update stored config hash; release lock.
   10. On any failure: rename backup tables back; set lock to "migration_failed".

Backup tables are never auto-dropped.  An operator must explicitly call
``DELETE /admin/schemas/{catalog_id}/{collection_id}/backups/{timestamp}``
to drop them.

Task type: ``schema_migration``
"""

import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.schema_migration.models import (
    SchemaMigrationInputs,
    SchemaMigrationReport,
    TableExportReport,
)

logger = logging.getLogger(__name__)


class SchemaMigrationTask(TaskProtocol):
    """
    Safe schema migration for a physical collection.

    Idempotent on the export side: backup tables left from a previous failed
    run are not overwritten (different timestamps).
    """

    priority: int = 10
    task_type = "schema_migration"

    async def run(
        self, payload: TaskPayload[SchemaMigrationInputs]
    ) -> Dict[str, Any]:
        from dynastore.tools.protocol_helpers import get_engine
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import DatabaseProtocol

        inputs = payload.inputs
        if isinstance(inputs, dict):
            inputs = SchemaMigrationInputs(**inputs)

        engine = get_engine()
        if engine is None:
            db_proto = get_protocol(DatabaseProtocol)
            engine = db_proto.engine if db_proto else None
        if engine is None:
            raise RuntimeError("SchemaMigrationTask: no database engine available.")

        report = await run_schema_migration(
            engine=engine,
            catalog_id=inputs.catalog_id,
            collection_id=inputs.collection_id,
            target_config_dict=inputs.target_config,
            dry_run=inputs.dry_run,
        )
        return report.model_dump()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


async def run_schema_migration(
    engine: Any,
    catalog_id: str,
    collection_id: str,
    target_config_dict: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> SchemaMigrationReport:
    """
    Orchestrate a safe schema migration for a single collection.

    This function is also usable standalone (e.g. from admin tooling)
    without going through the task system.
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        DDLQuery,
        ResultHandler,
        managed_transaction,
    )
    from dynastore.tasks.schema_migration.exporter import (
        export_collection,
        backup_table_names,
        restore_backup_table_names,
    )
    from dynastore.tasks.schema_migration.importer import import_collection

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    catalogs = get_protocol(CatalogsProtocol)

    # ── Resolve physical schema and hub table ────────────────────────────────
    schema = await catalogs.resolve_physical_schema(catalog_id)
    if not schema:
        raise ValueError(f"SchemaMigrationTask: catalog '{catalog_id}' not found.")

    hub_table_row = await _get_physical_table(engine, schema, collection_id)
    if not hub_table_row:
        raise ValueError(
            f"SchemaMigrationTask: collection '{collection_id}' has no physical table."
        )
    physical_table: str = hub_table_row

    # ── Resolve target config ────────────────────────────────────────────────
    col_config, sidecar_ids = await _resolve_config_and_sidecars(
        engine, schema, catalog_id, collection_id, target_config_dict
    )

    report = SchemaMigrationReport(
        catalog_id=catalog_id,
        collection_id=collection_id,
        physical_table=physical_table,
        schema=schema,
        timestamp=timestamp,
        status="dry_run" if dry_run else "no_op",
        dry_run=dry_run,
    )

    if dry_run:
        logger.info(
            f"SchemaMigrationTask [dry_run]: would migrate "
            f"'{schema}'.'{physical_table}' + sidecars {sidecar_ids}"
        )
        report.status = "dry_run"
        return report

    # ── Set migration lock ───────────────────────────────────────────────────
    await _set_collection_status(engine, schema, collection_id, "migrating")

    with tempfile.TemporaryDirectory(prefix=f"schema_migration_{collection_id}_") as tmp_dir:
        backup_mapping: Dict[str, str] = {}
        try:
            # ── 1. Export ────────────────────────────────────────────────────
            logger.info(
                f"SchemaMigrationTask: exporting '{schema}'.'{physical_table}' …"
            )
            export_reports = await export_collection(
                engine, schema, physical_table, sidecar_ids, tmp_dir
            )
            if not export_reports:
                raise RuntimeError("No tables exported — nothing to migrate.")

            export_totals = {r.table_name: r.row_count for r in export_reports}
            logger.info(f"SchemaMigrationTask: exported {export_totals}")

            # ── 2. Rename old tables to backup ───────────────────────────────
            logger.info(f"SchemaMigrationTask: renaming tables to backup names …")
            backup_mapping = await backup_table_names(
                engine, schema, physical_table, sidecar_ids, timestamp
            )
            # Attach backup names to reports
            for r in export_reports:
                r.backup_name = backup_mapping.get(r.table_name, "")
            report.tables = export_reports

            # ── 3. Create new tables ─────────────────────────────────────────
            logger.info(
                f"SchemaMigrationTask: creating new tables from target config …"
            )
            await _recreate_physical_collection(
                engine, schema, catalog_id, collection_id,
                physical_table, col_config
            )

            # ── 4. Import data ───────────────────────────────────────────────
            logger.info(f"SchemaMigrationTask: importing data …")
            imported = await import_collection(
                engine, schema, physical_table, sidecar_ids, tmp_dir
            )
            report.imported_rows = imported

            # ── 5. Verify row counts ─────────────────────────────────────────
            for table, expected in export_totals.items():
                actual = imported.get(table, 0)
                if actual != expected:
                    raise RuntimeError(
                        f"Row count mismatch for '{table}': "
                        f"exported {expected}, imported {actual}"
                    )

            # ── 6. Update config hash ────────────────────────────────────────
            await _update_schema_hash(
                engine, schema, catalog_id, collection_id, col_config
            )

            # ── 7. Release lock ──────────────────────────────────────────────
            await _set_collection_status(engine, schema, collection_id, "ready")

            report.status = "completed"
            logger.info(
                f"SchemaMigrationTask: migration completed for "
                f"'{catalog_id}/{collection_id}'. "
                f"Backup tables: {list(backup_mapping.values())}"
            )

        except Exception as exc:
            logger.error(
                f"SchemaMigrationTask: migration failed for "
                f"'{catalog_id}/{collection_id}': {exc}",
                exc_info=True,
            )
            report.status = "failed"
            report.error = str(exc)

            # Attempt rollback: rename backups back
            if backup_mapping:
                logger.warning(
                    "SchemaMigrationTask: attempting to restore backup tables …"
                )
                try:
                    await restore_backup_table_names(engine, schema, backup_mapping)
                except Exception as restore_exc:
                    logger.error(
                        f"SchemaMigrationTask: rollback failed: {restore_exc}"
                    )

            await _set_collection_status(
                engine, schema, collection_id, "migration_failed"
            )
            raise

    return report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_physical_table(engine: Any, schema: str, collection_id: str) -> Optional[str]:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction
    )
    sql = f"""
        SELECT physical_table FROM "{schema}".pg_storage_locations
        WHERE collection_id = :collection_id;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, collection_id=collection_id
        )
    return row["physical_table"] if row else None


async def _resolve_config_and_sidecars(
    engine: Any,
    schema: str,
    catalog_id: str,
    collection_id: str,
    target_config_dict: Optional[Dict[str, Any]],
):
    """Load PostgresCollectionDriverConfig and extract sidecar IDs."""
    from dynastore.modules.storage.driver_config import get_pg_collection_config, PG_DRIVER_PLUGIN_ID

    col_config = await get_pg_collection_config(catalog_id, collection_id)

    if target_config_dict:
        from dynastore.modules.storage.driver_config import PostgresCollectionDriverConfig
        col_config = PostgresCollectionDriverConfig.model_validate(target_config_dict)

    sidecar_ids = [sc.sidecar_id for sc in col_config.sidecars]
    return col_config, sidecar_ids


async def _recreate_physical_collection(
    engine: Any,
    schema: str,
    catalog_id: str,
    collection_id: str,
    physical_table: str,
    col_config: Any,
) -> None:
    """Recreate physical tables via the write driver's ensure_storage()."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.router import get_driver

    driver = await get_driver("WRITE", catalog_id, collection_id)
    async with managed_transaction(engine) as conn:
        await driver.ensure_storage(
            catalog_id,
            collection_id,
            physical_table=physical_table,
            layer_config=col_config,
            db_resource=conn,
        )


async def _update_schema_hash(
    engine: Any,
    schema: str,
    catalog_id: str,
    collection_id: str,
    col_config: Any,
) -> None:
    """Store the new config hash in collection_configs."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction
    )

    schema_hash = hashlib.sha256(
        json.dumps(col_config.model_dump(), sort_keys=True, default=str).encode()
    ).hexdigest()

    sql = f"""
        UPDATE "{schema}".collection_configs
        SET schema_hash = :schema_hash, updated_at = NOW()
        WHERE catalog_id = :catalog_id
          AND collection_id = :collection_id
          AND plugin_id = 'collection';
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            schema_hash=schema_hash,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )


async def _set_collection_status(
    engine: Any, schema: str, collection_id: str, status: str
) -> None:
    """
    Write a migration_status marker into ``collections.extra_metadata``.
    Uses JSON merge so other extra_metadata keys are preserved.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction
    )

    sql = f"""
        UPDATE "{schema}".collections
        SET extra_metadata = COALESCE(extra_metadata, '{{}}') ||
                             CAST(:patch AS jsonb)
        WHERE id = :collection_id AND deleted_at IS NULL;
    """
    patch = json.dumps({"migration_status": status})
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, patch=patch, collection_id=collection_id
        )
    logger.debug(
        f"_set_collection_status: '{schema}'.'{collection_id}' → {status}"
    )
