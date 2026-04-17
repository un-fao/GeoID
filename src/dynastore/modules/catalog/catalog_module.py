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
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
CatalogModule: Composition root for catalog, asset, and config management.

This module orchestrates the initialization and lifecycle of:
- CatalogService (Catalog and Collection CRUD)
- AssetManager (Asset lifecycle)
- ConfigManager (Hierarchical configurations)
- LogService (Buffered event logging)

It implements multiple protocols (CatalogsProtocol, AssetsProtocol, ConfigsProtocol, LogsProtocol, DatabaseProtocol)
by delegating to its internal services.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, List, Optional, Any, Dict, Union, Set

if TYPE_CHECKING:
    from dynastore.models.shared_models import CatalogUpdate, CollectionUpdate
    from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
    from geojson_pydantic import Feature
    from dynastore.models.query_builder import QueryResponse

from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DDLQuery,
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.catalog.models import (
    Catalog,
    Collection,
)
from dynastore.models.protocols import (
    CatalogsProtocol,
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol,
    ConfigsProtocol,
    DatabaseProtocol,
    LocalizationProtocol,
)
from dynastore.tools.discovery import register_plugin, get_protocol
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService
from dynastore.models.driver_context import DriverContext
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.config_service import ConfigService
from dynastore.modules.catalog.asset_service import AssetService, AssetEventType
from dynastore.modules.catalog.properties_service import PropertiesService
from dynastore.modules.catalog.localization_service import LocalizationService
from dynastore.modules.catalog.event_service import (
    EventService,
    CatalogEventType,
    register_event_listener,
    emit_event,
)
from dynastore.modules.catalog.log_manager import LogService, initialize_system_logs

logger = logging.getLogger(__name__)

# --- Asset event bridge: AssetEventType → CatalogEventType via EventsProtocol ---

_ASSET_EVENT_MAP = {
    AssetEventType.ASSET_CREATED: CatalogEventType.ASSET_CREATION,
    AssetEventType.ASSET_UPDATED: CatalogEventType.ASSET_UPDATE,
    AssetEventType.ASSET_DELETED: CatalogEventType.ASSET_DELETION,
    AssetEventType.ASSET_HARD_DELETED: CatalogEventType.ASSET_HARD_DELETION,
}


async def _asset_event_bridge(event_type: AssetEventType, data: dict) -> None:
    """Bridge AssetService events to CatalogEventType for secondary drivers."""
    catalog_event = _ASSET_EVENT_MAP.get(event_type)
    if catalog_event:
        await emit_event(
            catalog_event,
            catalog_id=data.get("catalog_id"),
            collection_id=data.get("collection_id"),
            asset_id=data.get("asset_id"),
            payload=data,
        )


# --- Legacy Constants and DDL (Shared by Module Initialization) ---

CATALOGS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalogs (
    id VARCHAR PRIMARY KEY,
    physical_schema VARCHAR NOT NULL UNIQUE,
    title JSONB,
    description JSONB,
    keywords JSONB,
    license JSONB,
    conforms_to JSONB,
    links JSONB,
    assets JSONB,
    stac_version VARCHAR(20) DEFAULT '1.0.0',
    stac_extensions JSONB DEFAULT '[]'::jsonb,
    extra_metadata JSONB,
    provisioning_status VARCHAR(50) NOT NULL DEFAULT 'ready',
    deleted_at TIMESTAMPTZ DEFAULT NULL
);
"""

SHARED_PROPERTIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS catalog.shared_properties (
    key_name VARCHAR PRIMARY KEY, 
    key_value VARCHAR NOT NULL, 
    owner_code VARCHAR, 
    created_at TIMESTAMPTZ DEFAULT NOW(), 
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


_module_instance: Optional[ModuleProtocol] = None
class CatalogModule(ModuleProtocol):
    priority: int = 20
    """
    Manages catalog lifecycle.
    Functions as a composition root, initializing services and registering them as protocol providers.
    Must start after DBService (priority=10) and TasksModule (priority=15),
    which creates tables that the event consumer depends on.
    Priority < 20 would cause hard-abort on startup failure; 20 is still foundational.
    """


    def __init__(self):
        self.app_state: Optional[Any] = None
        self.log_service: Optional[LogService] = None
        self.catalog_service: Optional[CatalogService] = None
        self.collection_service: Optional[CollectionService] = None
        self.items_service: Optional[ItemService] = None
        self.config_service: Optional[ConfigService] = None
        self.asset_service: Optional[AssetService] = None
        self.properties_service: Optional[PropertiesService] = None
        self.localization_service: Optional[LocalizationService] = None
        self.event_service: Optional[EventService] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Standard CatalogModule lifespan with physical storage initialization."""
        self.app_state = app_state

        engine = get_engine()

        if not engine:
            logger.critical("CatalogModule: No DB engine found during startup.")
            yield
            return

        global _module_instance
        _module_instance = self

        # --- Instantiate and register internal services ---
        self.log_service = LogService()
        self.catalog_service = CatalogService(engine=engine)  # type: ignore[abstract]
        self.collection_service = CollectionService(engine=engine)  # type: ignore[abstract]
        self.items_service = ItemService(engine=engine)
        self.config_service = ConfigService(engine=engine)
        self.asset_service = AssetService(engine=engine, event_emitter=_asset_event_bridge)  # type: ignore[abstract]
        self.properties_service = PropertiesService(engine=engine)
        self.localization_service = LocalizationService()
        self.event_service = EventService()

        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        self.pg_asset_driver = AssetPostgresqlDriver(engine=engine)

        from dynastore.modules.storage.drivers.postgresql import CollectionPostgresqlDriver
        self.pg_storage_driver = CollectionPostgresqlDriver()  # type: ignore[abstract]

        from dynastore.modules.storage.entity_transform_pipeline import EntityTransformPipeline
        self.driver_metadata_enricher = EntityTransformPipeline()

        from contextlib import AsyncExitStack
        async with AsyncExitStack() as stack:
            for svc in (
                self.log_service,
                self.catalog_service,
                self.collection_service,
                self.items_service,
                self.config_service,
                self.asset_service,
                self.pg_asset_driver,
                self.pg_storage_driver,
                self.driver_metadata_enricher,
                self.properties_service,
                self.localization_service,
                self.event_service,
            ):
                # Enter plugin lifespan if it exists
                if hasattr(svc, "lifespan"):
                    await stack.enter_async_context(svc.lifespan(app_state))  # type: ignore[attr-defined]
                
                # Register for discovery
                register_plugin(svc)

            logger.info("Initialized CatalogModule services.")

            # 4. Initialize Storage & Schemas
            # Hub/sidecar creation is handled by CollectionPostgresqlDriver.ensure_storage()
            # which is called from _create_collection_internal(). No lifecycle hook needed.

            async with managed_transaction(engine) as conn:
                await ensure_schema_exists(conn, "catalog")

                # Centralized system-level maintenance initialization
                await initialize_system_logs(conn)

                from dynastore.modules.db_config.maintenance_tools import ensure_global_cron_cleanup
                await ensure_global_cron_cleanup(conn)

                await DDLQuery(CATALOGS_TABLE_DDL + SHARED_PROPERTIES_SCHEMA).execute(conn)

                # Ensure stored procedures (replacing init.sql)
                from dynastore.modules.catalog.db_init.stored_procedures import (
                    ensure_stored_procedures,
                )

                await ensure_stored_procedures(conn)

            # 5. Register Internal Observers
            # Observers will use get_protocol() to access services
            register_event_listener(
                CatalogEventType.AFTER_CATALOG_HARD_DELETION, self._on_catalog_hard_deletion
            )
            register_event_listener(
                CatalogEventType.CATALOG_DELETION, self._on_catalog_deletion
            )
            register_event_listener(
                CatalogEventType.AFTER_COLLECTION_HARD_DELETION,
                self._on_collection_hard_deletion,
            )
            register_event_listener(
                CatalogEventType.COLLECTION_DELETION, self._on_collection_deletion
            )
            from dynastore.modules.catalog.event_service import async_event_listener

            @async_event_listener("task.failed")
            async def _on_task_failed_impl(**kwargs):
                await self._on_task_failed(**kwargs)


            # 6. Start Background Event Consumer (automatic)
            # If any module registered async event listeners, start the
            # durable event consumer automatically — no env var needed.
            _consumer_shutdown = asyncio.Event()
            if self.event_service.has_listeners():
                logger.info(
                    "CatalogModule: Async event listeners detected — "
                    "starting durable event consumer automatically."
                )
                await self.event_service.start_consumer(_consumer_shutdown)
            else:
                logger.info(
                    "CatalogModule: No async event listeners registered — "
                    "event consumer not started."
                )

            try:
                yield
            finally:
                _consumer_shutdown.set()
                await self.event_service.stop_consumer()
                # Services cleanup handled by AsyncExitStack (stack.close() via __aexit__)

    # === Private service accessors (assert-narrowed for pyright) ===

    @property
    def _cs(self) -> CatalogService:
        assert self.catalog_service is not None
        return self.catalog_service

    @property
    def _col_svc(self) -> CollectionService:
        assert self.collection_service is not None
        return self.collection_service

    @property
    def _item_svc(self) -> ItemService:
        assert self.items_service is not None
        return self.items_service

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        return self._cs.items

    @property
    def collections(self) -> CollectionsProtocol:
        return self._cs.collections

    @property
    def localization(self) -> LocalizationProtocol:
        assert self.localization_service is not None
        return self.localization_service

    # === Delegated CRUD Methods ===

    async def get_catalog(
        self, catalog_id: str, lang: str = "en", ctx: Optional[DriverContext] = None
    ) -> Catalog:
        return await self._cs.get_catalog(
            catalog_id, lang=lang, ctx=ctx
        )

    async def get_catalog_model(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> Optional[Catalog]:
        return await self._cs.get_catalog_model(
            catalog_id, ctx=ctx
        )

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], Catalog],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Catalog:
        return await self._cs.create_catalog(
            catalog_data, lang=lang, ctx=ctx
        )

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], "CatalogUpdate"],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Catalog]:
        return await self._cs.update_catalog(
            catalog_id, updates, lang=lang, ctx=ctx
        )

    async def delete_catalog(
        self, catalog_id: str, force: bool = False, ctx: Optional[DriverContext] = None
    ) -> bool:
        return await self._cs.delete_catalog(
            catalog_id, force=force, ctx=ctx
        )

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, ctx: Optional[DriverContext] = None
    ) -> bool:
        return await self._cs.delete_catalog_language(
            catalog_id, lang, ctx=ctx
        )

    async def list_catalogs(
        self,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        q: Optional[str] = None,
    ) -> List[Catalog]:
        return await self._cs.list_catalogs(
            limit=limit, offset=offset, lang=lang, ctx=ctx, q=q
        )

    async def search_catalogs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> List[Catalog]:
        return await self._cs.search_catalogs(
            filters=filters, limit=limit, offset=offset, db_resource=db_resource
        )

    # === Collection Operations ===

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.get_collection(
            catalog_id, collection_id, lang=lang, ctx=ctx
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        **kwargs,
    ) -> Collection:
        return await self._col_svc.create_collection(
            catalog_id, collection_data, lang=lang, ctx=ctx, **kwargs
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Union[Dict[str, Any], "CollectionUpdate"],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.update_collection(
            catalog_id, collection_id, updates, lang=lang, ctx=ctx  # type: ignore[arg-type]
        )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        return await self._col_svc.delete_collection(
            catalog_id, collection_id, force=force, ctx=ctx
        )

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        return await self._col_svc.delete_collection_language(
            catalog_id, collection_id, lang, ctx=ctx
        )

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        q: Optional[str] = None,
    ) -> List[Any]:
        return await self._col_svc.list_collections(
            catalog_id, limit=limit, offset=offset, lang=lang, ctx=ctx, q=q
        )

    # === Item Operations ===

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk)."""
        return await self._item_svc.upsert(
            catalog_id,
            collection_id,
            items,
            ctx=ctx,
            processing_context=processing_context,
        )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._item_svc.get_item(  # type: ignore[return-value]
            catalog_id, collection_id, item_id, ctx=ctx
        )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        return await self._item_svc.delete_item(
            catalog_id, collection_id, item_id, ctx=ctx
        )

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        ext_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        return await self._item_svc.delete_item_language(
            catalog_id, collection_id, ext_id, lang, ctx=ctx
        )

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
    ) -> "List[Feature]":
        return await self._item_svc.search_items(  # type: ignore[return-value]
            catalog_id, collection_id, request, config=config, ctx=ctx
        )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
    ) -> "QueryResponse":
        return await self._item_svc.stream_items(
            catalog_id, collection_id, request, config=config, ctx=ctx
        )

    # === Schema/Table Resolution ===

    async def resolve_physical_schema(
        self,
        catalog_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        return await self._cs.resolve_physical_schema(
            catalog_id, ctx=ctx, allow_missing=allow_missing  # type: ignore[arg-type]
        )

    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hint: Optional[str] = None,
    ):
        return await self._cs.resolve_datasource(
            catalog_id, collection_id, operation=operation, hint=hint
        )

    async def resolve_physical_table(
        self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None
    ) -> Optional[str]:
        return await self._cs.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self._cs.set_physical_table(
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    async def ensure_catalog_exists(
        self, catalog_id: str, lang: str = "en", ctx: Optional[DriverContext] = None
    ) -> None:
        return await self._cs.ensure_catalog_exists(
            catalog_id, lang=lang, ctx=ctx
        )

    async def ensure_collection_exists(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> None:
        db_resource = ctx.db_resource if ctx else None
        return await self._col_svc.ensure_collection_exists(
            db_resource, catalog_id, collection_id, lang=lang  # type: ignore[arg-type]
        )

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self._item_svc.ensure_physical_table_exists(
            catalog_id, collection_id, config, db_resource=db_resource  # type: ignore[arg-type]
        )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        partition_value: Any,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        db_resource = ctx.db_resource if ctx else None
        return await self._item_svc.ensure_partition_exists(
            catalog_id, collection_id, config, partition_value, ctx=ctx  # type: ignore[arg-type]
        )

    @property
    def assets(self) -> AssetsProtocol:
        assert self.asset_service is not None
        return self.asset_service

    @property
    def configs(self) -> ConfigsProtocol:
        assert self.config_service is not None
        return self.config_service

    def get_config_service(self) -> ConfigService:
        # DEPRECATED: use .configs or get_protocol(ConfigsProtocol)
        logger.warning(
            "get_config_service is deprecated, use the 'configs' protocol property instead.",
            stack_info=True,
        )
        assert self.config_service is not None
        return self.config_service

    def get_asset_service(self) -> AssetService:
        # DEPRECATED: use .assets or get_protocol(AssetsProtocol)
        logger.warning(
            "get_asset_service is deprecated, use the 'assets' protocol property instead.",
            stack_info=True,
        )
        assert self.asset_service is not None
        return self.asset_service

    @property
    def count_items_by_asset_id_query(self) -> Any:
        return self._item_svc.count_items_by_asset_id_query

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional[DriverContext] = None
    ) -> "CollectionPluginConfig":
        return await self._cs.get_collection_config(
            catalog_id, collection_id, ctx=ctx
        )

    async def get_collection_column_names(
        self, catalog_id: str, collection_id: str, ctx: Optional[DriverContext] = None
    ) -> Set[str]:
        return await self._cs.get_collection_column_names(
            catalog_id, collection_id, ctx=ctx
        )

    # --- Internal Observers for Module Maintenance ---

    # --- Internal Observers (Using Protocols) ---

    async def _on_catalog_hard_deletion(self, catalog_id: str, **kwargs):
        """Final physical destruction for catalog (Assets, Schema, Record)."""
        logger.info(f"Finalizing deletion for catalog '{catalog_id}'")

        # Resolve dependencies via protocols
        assets = get_protocol(AssetsProtocol)
        catalogs = get_protocol(CatalogsProtocol)
        configs = get_protocol(ConfigsProtocol)
        db = get_protocol(DatabaseProtocol)

        db_resource = kwargs.get("db_resource")
        if not db_resource and db:
            db_resource = db.engine

        if not db_resource:
            logger.error(
                "No database resource available for catalog hard deletion cleanup."
            )
            return

        # Check if physical_schema was provided in event payload (from CatalogService)
        physical_schema = kwargs.get("physical_schema")

        # 1. Purge Assets
        # If schema is dropped, assets table is gone.
        # But if AssetManager uses external storage (e.g. S3), it might need cleanup?
        # Current implementation: AssetManager.delete_assets operates on DB table.
        # If schema is gone, delete_assets will fail or return 0.
        # However, we can't easily check if schema is gone without querying DB or checking the passed arg.

        # If physical_schema is provided, it implies it was known before deletion.
        # But if it was already dropped by the emitter, we can't run delete_assets query on it.
        # So we skip Asset deletion if schema is dropped.

        # BUT: AssetManager logic might handle file deletions (S3)?
        # The current implementation of delete_assets only deletes rows.
        # So skipping is correct if table is gone.

        # 2. Drop logical record and configuration (Redundant if CatalogService handled it?)
        # CatalogService deletes schema and row.
        # We only need to cleanup if CatalogService failed to do so, or if this event came from elsewhere.

        # If physical_schema is passed, it likely means the emitter (CatalogService) already handled schema drop.
        if physical_schema:
            logger.info(
                f"Schema {physical_schema} was handled by emitter. Skipping redundant cleanup."
            )
            return

        # Fallback for manual events or failures: try to cleanup if still exists
        async with managed_transaction(db_resource) as conn:
            phys_schema = None
            if catalogs:
                phys_schema = await catalogs.resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )

            # If schema exists, drop it
            if phys_schema:
                # If we found schema, assets table exists, so we can try to purge assets first?
                # Actually dropping schema cascades to assets table.
                await DDLQuery(f'DROP SCHEMA IF EXISTS "{phys_schema}" CASCADE;').execute(
                    conn
                )

    async def _on_catalog_deletion(self, catalog_id: str, **kwargs):
        """Soft deletion of catalog assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id, hard=False, db_resource=kwargs.get("db_resource")  # type: ignore[misc]
            )

    async def _on_collection_hard_deletion(
        self, catalog_id: str, collection_id: str, **kwargs
    ):
        """Purge assets for a hard-deleted collection."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id,
                collection_id=collection_id,
                hard=True,
                db_resource=kwargs.get("db_resource"),  # type: ignore[misc]
            )

    async def _on_collection_deletion(
        self, catalog_id: str, collection_id: str, **kwargs
    ):
        """Soft deletion of collection assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id,
                collection_id=collection_id,
                hard=False,
                db_resource=kwargs.get("db_resource"),  # type: ignore[misc]
            )

    async def _on_task_failed(
        self,
        task_id: str,
        task_type: str,
        error_message: str,
        severity: str = "unrecoverable",
        inputs: Optional[Dict[str, Any]] = None,
        originating_event: Optional[str] = None,
        **kwargs,
    ):
        """Generic task failure handler. Routes rollback by originating_event, not task_type.

        Any module (GCP, Elasticsearch, etc.) can dispatch tasks without coupling
        to CatalogModule. The caller only needs to store the triggering catalog event
        in extra_context['originating_event'] when creating the TaskCreate.
        """
        logger.warning(
            f"Task '{task_type}' ({task_id}) FAILED [{severity}]: {error_message}"
        )
        inputs = inputs or {}

        # Route rollback by what catalog lifecycle event triggered this task
        if originating_event in {
            CatalogEventType.CATALOG_CREATION,
            str(CatalogEventType.CATALOG_CREATION),
        }:
            catalog_id = inputs.get("catalog_id")
            if catalog_id:
                logger.info(
                    f"Rolling back catalog '{catalog_id}' provisioning to 'failed' "
                    f"(triggered by '{task_type}' failure, severity={severity})."
                )
                catalogs = get_protocol(CatalogsProtocol)
                if catalogs:
                    try:
                        _dr = kwargs.get("db_resource")
                        await catalogs.update_catalog(
                            catalog_id,
                            {"extra_metadata": {"provisioning_status": "failed", "error": error_message}},
                            ctx=DriverContext(db_resource=_dr) if _dr else None,
                        )
                    except Exception as e:
                        logger.error(f"Rollback for catalog '{catalog_id}' failed: {e}")

# --- Module level proxies for common protocol operations ---

async def list_catalogs(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.list_catalogs"""
    cat = get_protocol(CatalogsProtocol)
    if cat:
        return await cat.list_catalogs(*args, **kwargs)
    return []


async def get_catalog(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.get_catalog"""
    cat = get_protocol(CatalogsProtocol)
    if cat:
        return await cat.get_catalog(*args, **kwargs)
    return None


async def get_collection(*args, **kwargs):
    """Module-level proxy for CollectionsProtocol.get_collection"""
    coll = get_protocol(CollectionsProtocol)
    if coll:
        return await coll.get_collection(*args, **kwargs)
    return None


async def list_collections(*args, **kwargs):
    """Module-level proxy for CollectionsProtocol.list_collections"""
    coll = get_protocol(CollectionsProtocol)
    if coll:
        return await coll.list_collections(*args, **kwargs)
    return []


async def get_collection_config(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.get_collection_config"""
    from dynastore.models.protocols import CatalogsProtocol as _CatalogsProtocol
    conf = get_protocol(_CatalogsProtocol)
    if conf:
        return await conf.get_collection_config(*args, **kwargs)
    return None
