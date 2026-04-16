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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)

"""
Admin API endpoints for database migration management.

All endpoints require admin/sysadmin role.

Endpoints:
    GET  /admin/migrations/status       — Current migration status + manifest
    GET  /admin/migrations/pending      — Dry-run: what would be applied
    POST /admin/migrations/apply        — Apply pending migrations
    GET  /admin/migrations/history      — Full migration history
    POST /admin/migrations/rollback     — Rollback a specific migration
    GET  /admin/schemas/{catalog_id}/health — Schema health for collections
"""

import logging
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from dynastore.models.protocols.policies import Principal


@runtime_checkable
class PhysicalTableResolver(Protocol):
    """Capability: driver can resolve a collection's physical table name."""
    async def resolve_physical_table(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]: ...

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Migrations"], prefix="/migrations")
schema_router = APIRouter(tags=["Schema Evolution"], prefix="/schemas")
configs_router = APIRouter(tags=["Config Portability"], prefix="/configs")


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def _require_admin(request: Request) -> Principal:
    principal: Optional[Principal] = getattr(request.state, "principal", None)
    if not principal:
        raise HTTPException(status_code=401, detail="Authentication required.")
    admin_roles = {"sysadmin", "admin"}
    if not admin_roles.intersection(set(principal.roles or [])):
        raise HTTPException(status_code=403, detail="Admin role required.")
    return principal


def _get_engine(request: Request) -> Any:
    """Get the database engine from app state."""
    engine = getattr(request.app.state, "sync_engine", None)
    if not engine:
        engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Database not available.")
    return engine


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------


class ApplyRequest(BaseModel):
    scope: str = "all"  # "all", "global", "tenant"
    dry_run: bool = False


class RollbackRequest(BaseModel):
    module: str
    version: str
    schema_name: str = "platform"


class EvolutionRequest(BaseModel):
    apply_safe: bool = False


class SchemaMigrateRequest(BaseModel):
    target_config: Optional[Dict[str, Any]] = None
    dry_run: bool = False


# ---------------------------------------------------------------------------
# Migration endpoints
# ---------------------------------------------------------------------------


@router.get("/status", summary="Current migration status and manifest")
async def migration_status(
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules.db_config.migration_runner import (
        MigrationStatus,
        check_migration_status,
        get_expected_manifest,
        get_expected_tenant_manifest,
        get_stored_manifest,
    )

    status = await check_migration_status(engine)
    stored = await get_stored_manifest(engine)
    expected = await get_expected_manifest()
    expected_tenant = await get_expected_tenant_manifest()

    return {
        "status": status.value,
        "stored_manifest": stored,
        "expected_manifest": expected,
        "expected_tenant_manifest": expected_tenant or {},
    }


@router.get("/pending", summary="Preview pending migrations (dry-run)")
async def pending_migrations(
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules.db_config.migration_runner import get_pending_migrations

    return await get_pending_migrations(engine)


@router.post("/apply", summary="Apply pending migrations")
async def apply_migrations(
    body: ApplyRequest,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules.db_config.migration_runner import run_migrations

    if body.scope not in ("all", "global", "tenant"):
        raise HTTPException(
            status_code=400, detail=f"Invalid scope: '{body.scope}'"
        )

    result = await run_migrations(
        engine, dry_run=body.dry_run, scope=body.scope
    )

    action = "previewed" if body.dry_run else "applied"
    logger.info(
        f"Migrations {action} by {principal.subject_id} "
        f"(scope={body.scope}, dry_run={body.dry_run})"
    )
    return {"action": action, "scope": body.scope, **result}


@router.get("/history", summary="Full migration history")
async def migration_history(
    schema_name: str = Query("platform", alias="schema"),
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> List[Dict[str, Any]]:
    from dynastore.modules.db_config.migration_runner import get_migration_history

    return await get_migration_history(engine, schema=schema_name)


@router.post("/rollback", summary="Rollback a specific migration")
async def rollback_migration_endpoint(
    body: RollbackRequest,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules.db_config.migration_runner import (
        MigrationError,
        rollback_migration,
    )

    try:
        result = await rollback_migration(
            engine,
            module=body.module,
            version=body.version,
            schema=body.schema_name,
        )
        logger.info(
            f"Migration rollback [{body.module}] {body.version} "
            f"by {principal.subject_id} in schema '{body.schema_name}'"
        )
        return result
    except MigrationError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# Schema evolution endpoints
# ---------------------------------------------------------------------------


@schema_router.get(
    "/{catalog_id}/health",
    summary="Schema health for all collections in a catalog",
)
async def schema_health(
    catalog_id: str,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        raise HTTPException(status_code=503, detail="Catalog service not available.")

    schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True)
    if not schema:
        raise HTTPException(
            status_code=404, detail=f"Catalog '{catalog_id}' not found."
        )

    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )
    from dynastore.modules.db_config.locking_tools import check_table_exists

    async with managed_transaction(engine) as conn:
        if not await check_table_exists(conn, "collection_configs", schema):
            rows = []
        else:
            sql = f"""
                SELECT DISTINCT collection_id, updated_at
                FROM "{schema}".collection_configs
                ORDER BY collection_id;
            """
            rows = (
                await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
                or []
            )

    collections = []
    for r in rows:
        collections.append(
            {
                "collection_id": r["collection_id"],
                "schema_hash": None,
                "has_snapshot": False,
                "updated_at": str(r["updated_at"]) if r.get("updated_at") else None,
            }
        )

    return {"catalog_id": catalog_id, "schema": schema, "collections": collections}


@schema_router.post(
    "/{catalog_id}/{collection_id}/evolve",
    summary="Diff and optionally apply safe schema evolution",
)
async def evolve_collection(
    catalog_id: str,
    collection_id: str,
    body: EvolutionRequest = EvolutionRequest(),
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
    from dynastore.modules.catalog.schema_evolution import SchemaEvolutionEngine
    catalogs = get_protocol(CatalogsProtocol)
    configs_proto = get_protocol(ConfigsProtocol)
    if not catalogs or not configs_proto:
        raise HTTPException(
            status_code=503, detail="Required services not available."
        )

    schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True)
    if not schema:
        raise HTTPException(
            status_code=404, detail=f"Catalog '{catalog_id}' not found."
        )

    from dynastore.modules.catalog.collection_service import CollectionService
    collection_svc = CollectionService()
    physical_table = await collection_svc.resolve_physical_table(catalog_id, collection_id)
    if not physical_table:
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found or has no physical table.",
        )

    # Get driver config (sidecars, partitioning, etc.)
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    _driver = await get_driver(Operation.READ, catalog_id, collection_id)
    col_config = await _driver.get_driver_config(catalog_id, collection_id)

    # Resolve partition context
    partition_keys: List[str] = []
    partition_key_types: Dict[str, str] = {
        "transaction_time": "TIMESTAMPTZ",
        "validity": "TSTZRANGE",
        "geoid": "UUID",
        "asset_id": "VARCHAR(255)",
    }
    if col_config.partitioning.enabled:
        partition_keys = col_config.partitioning.partition_keys
        for sc_config in col_config.sidecars:
            partition_key_types.update(sc_config.partition_key_types)

    # Introspect + diff
    evo = SchemaEvolutionEngine()
    current = await evo.introspect_collection(engine, schema, physical_table)
    plan = evo.diff(
        current, col_config, physical_table, partition_keys, partition_key_types
    )

    result = plan.summary()

    if body.apply_safe and plan.safe_ops and not plan.unsafe_ops:
        executed = await evo.apply_safe_ops(engine, schema, plan)
        result["applied_sql"] = executed
        result["applied"] = True
        logger.info(
            f"Schema evolution applied for {catalog_id}/{collection_id} "
            f"by {principal.subject_id}: {len(executed)} statement(s)"
        )
    else:
        result["applied"] = False

    return result


@schema_router.post(
    "/{catalog_id}/{collection_id}/migrate",
    summary="Trigger safe schema migration (export → rename → recreate → import)",
)
async def migrate_collection(
    catalog_id: str,
    collection_id: str,
    body: SchemaMigrateRequest = SchemaMigrateRequest(),
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.tasks.schema_migration.task import run_schema_migration

    logger.info(
        f"Schema migration requested for {catalog_id}/{collection_id} "
        f"by {principal.subject_id} (dry_run={body.dry_run})"
    )
    report = await run_schema_migration(
        engine=engine,
        catalog_id=catalog_id,
        collection_id=collection_id,
        target_config_dict=body.target_config,
        dry_run=body.dry_run,
    )
    return report.model_dump()


@schema_router.get(
    "/{catalog_id}/{collection_id}/backups",
    summary="List backup tables left by previous migrations",
)
async def list_collection_backups(
    catalog_id: str,
    collection_id: str,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction
    )
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise HTTPException(status_code=503, detail="CatalogsProtocol unavailable.")
    schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

    # Get physical_table from driver config
    driver = await get_driver(Operation.READ, catalog_id, collection_id)
    physical_table = (
        await driver.resolve_physical_table(catalog_id, collection_id)
        if isinstance(driver, PhysicalTableResolver)
        else None
    )
    if not physical_table:
        raise HTTPException(status_code=404, detail="Collection has no physical table.")

    # Find all tables matching the _bkp_ pattern
    sql_tables = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_name LIKE :pattern
        ORDER BY table_name;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql_tables, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, schema=schema, pattern=f"{physical_table}_bkp_%"
        )

    backups = [r["table_name"] for r in (rows or [])]
    return {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "physical_table": physical_table,
        "backups": backups,
    }


@schema_router.delete(
    "/{catalog_id}/{collection_id}/backups/{timestamp}",
    summary="Drop backup tables for a specific migration timestamp",
)
async def drop_collection_backup(
    catalog_id: str,
    collection_id: str,
    timestamp: str,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules.db_config.query_executor import (
        DDLQuery, DQLQuery, ResultHandler, managed_transaction
    )
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise HTTPException(status_code=503, detail="CatalogsProtocol unavailable.")
    schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

    driver = await get_driver(Operation.READ, catalog_id, collection_id)
    physical_table = (
        await driver.resolve_physical_table(catalog_id, collection_id)
        if isinstance(driver, PhysicalTableResolver)
        else None
    )
    if not physical_table:
        raise HTTPException(status_code=404, detail="Collection has no physical table.")

    suffix = f"_bkp_{timestamp}"

    sql_list = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = :schema AND table_name LIKE :pattern;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql_list, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, schema=schema, pattern=f"{physical_table}%{suffix}"
        )

    tables_to_drop = [r["table_name"] for r in (rows or [])]
    if not tables_to_drop:
        raise HTTPException(
            status_code=404,
            detail=f"No backup tables found for timestamp '{timestamp}'.",
        )

    dropped = []
    async with managed_transaction(engine) as conn:
        for table in tables_to_drop:
            await DDLQuery(f'DROP TABLE IF EXISTS "{schema}"."{table}";').execute(conn)
            dropped.append(table)

    logger.info(
        f"Dropped backup tables {dropped} for {catalog_id}/{collection_id} "
        f"by {principal.subject_id}"
    )
    return {"dropped": dropped, "count": len(dropped)}


# ---------------------------------------------------------------------------
# Config portability endpoints
# ---------------------------------------------------------------------------


class ConfigImportEntry(BaseModel):
    plugin_id: str
    config_data: Dict[str, Any]
    collection_id: Optional[str] = None


class ConfigImportRequest(BaseModel):
    catalog_configs: List[ConfigImportEntry] = []
    collection_configs: List[ConfigImportEntry] = []
    overwrite: bool = True


@configs_router.get(
    "/{catalog_id}/export",
    summary="Export all catalog and collection-level config overrides as JSON",
)
async def export_catalog_configs(
    catalog_id: str,
    engine: Any = Depends(_get_engine),
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from datetime import datetime, timezone
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction
    )

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise HTTPException(status_code=503, detail="Configs service not available.")

    # Catalog-level overrides
    cat_page = await configs.list_configs(catalog_id=catalog_id, limit=1000)
    catalog_entries = [
        {"plugin_id": r["plugin_id"], "config_data": r.get("config_data", {})}
        for r in (cat_page.get("results") or [])
    ]

    # Collection-level overrides: enumerate collections then fetch per-collection configs
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise HTTPException(status_code=503, detail="CatalogsProtocol unavailable.")
    schema = await catalogs.resolve_physical_schema(catalog_id, allow_missing=True) if catalogs else None

    collection_entries: List[Dict[str, Any]] = []
    if schema:
        sql = f"""
            SELECT id FROM "{schema}".collections
            WHERE deleted_at IS NULL ORDER BY id;
        """
        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn) or []
        for row in rows:
            cid = row["id"]
            col_page = await configs.list_configs(
                catalog_id=catalog_id, collection_id=cid, limit=1000
            )
            for r in (col_page.get("results") or []):
                collection_entries.append(
                    {
                        "plugin_id": r["plugin_id"],
                        "collection_id": cid,
                        "config_data": r.get("config_data", {}),
                    }
                )

    return {
        "catalog_id": catalog_id,
        "export_timestamp": datetime.now(timezone.utc).isoformat(),
        "catalog_configs": catalog_entries,
        "collection_configs": collection_entries,
    }


@configs_router.post(
    "/{catalog_id}/import",
    summary="Import catalog and collection-level config overrides from JSON",
)
async def import_catalog_configs(
    catalog_id: str,
    body: ConfigImportRequest,
    principal: Principal = Depends(_require_admin),
) -> Dict[str, Any]:
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.db_config.platform_config_service import resolve_config_class

    configs = get_protocol(ConfigsProtocol)
    if not configs:
        raise HTTPException(status_code=503, detail="Configs service not available.")

    applied: List[str] = []
    errors: List[str] = []

    # Merge all entries: catalog-level (no collection_id) + collection-level
    all_entries: List[ConfigImportEntry] = list(body.catalog_configs) + list(body.collection_configs)

    for entry in all_entries:
        label = f"{entry.plugin_id}@{catalog_id}" + (
            f"/{entry.collection_id}" if entry.collection_id else ""
        )
        try:
            model_class = resolve_config_class(entry.plugin_id)
            if not model_class:
                errors.append(f"{label}: plugin not registered, skipped")
                continue
            config_model = model_class.model_validate(entry.config_data)
            await configs.set_config(
                model_class,
                config_model,
                catalog_id=catalog_id,
                collection_id=entry.collection_id,
                check_immutability=not body.overwrite,
            )
            applied.append(label)
        except Exception as e:
            errors.append(f"{label}: {e}")
            logger.warning(f"import_catalog_configs: failed {label}: {e}")

    logger.info(
        f"Config import for {catalog_id} by {principal.subject_id}: "
        f"{len(applied)} applied, {len(errors)} errors"
    )
    return {
        "catalog_id": catalog_id,
        "applied": applied,
        "errors": errors,
    }
