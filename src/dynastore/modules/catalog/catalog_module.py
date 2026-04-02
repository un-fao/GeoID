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
import os
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Union, Set, AsyncIterator

import dynastore.modules as dm
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists,
    acquire_startup_lock,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.catalog.models import (
    Catalog,
    Collection,
    LocalizedText,
)
from dynastore.models.protocols import (
    CatalogsProtocol,
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol,
    ConfigsProtocol,
    LogsProtocol,
    DatabaseProtocol,
    PropertiesProtocol,
    LocalizationProtocol,
)
from dynastore.models.protocols.event_bus import EventBusProtocol

from dynastore.tools.discovery import register_plugin, get_protocol
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService
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

from dynastore.modules import ModuleProtocol

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
        self.catalog_service = CatalogService(engine=engine)
        self.collection_service = CollectionService(engine=engine)
        self.items_service = ItemService(engine=engine)
        self.config_service = ConfigService(engine=engine)
        self.asset_service = AssetService(engine=engine, event_emitter=_asset_event_bridge)
        self.properties_service = PropertiesService(engine=engine)
        self.localization_service = LocalizationService()
        self.event_service = EventService()

        from dynastore.modules.catalog.drivers.pg_asset_driver import PostgresAssetDriver
        self.pg_asset_driver = PostgresAssetDriver(engine=engine)

        from dynastore.modules.storage.drivers.postgresql import PostgresStorageDriver
        self.pg_storage_driver = PostgresStorageDriver()

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
                self.properties_service,
                self.localization_service,
                self.event_service,
            ):
                # Enter plugin lifespan if it exists
                if hasattr(svc, "lifespan"):
                    await stack.enter_async_context(svc.lifespan(app_state))
                
                # Register for discovery
                register_plugin(svc)

            logger.info("Initialized CatalogModule services.")

            # 4. Initialize Storage & Schemas
            # Hub/sidecar creation is handled by PostgresStorageDriver.ensure_storage()
            # which is called from _create_collection_internal(). No lifecycle hook needed.
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

            async with managed_transaction(engine) as conn:
                await ensure_schema_exists(conn, "catalog")
                
                # Initialize tenant event space if available
                event_bus = get_protocol(EventBusProtocol)
                if event_bus:
                    from dynastore.modules.db_config.locking_tools import check_table_exists
                    if not await check_table_exists(conn, "catalog_events", "catalog"):
                        await event_bus.init_tenant_events("catalog", db_resource=conn)

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

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        return self.catalog_service.items

    @property
    def collections(self) -> CollectionsProtocol:
        return self.catalog_service.collections

    @property
    def assets(self) -> AssetsProtocol:
        return self.asset_service

    @property
    def configs(self) -> ConfigsProtocol:
        return self.config_manager

    @property
    def localization(self) -> LocalizationProtocol:
        return self.localization_service

    # === Delegated CRUD Methods ===

    async def get_catalog(
        self, catalog_id: str, lang: str = "en", db_resource: Optional[Any] = None
    ) -> Catalog:
        return await self.catalog_service.get_catalog(
            catalog_id, lang=lang, db_resource=db_resource
        )

    async def get_catalog_model(
        self, catalog_id: str, db_resource: Optional[Any] = None
    ) -> Optional[Catalog]:
        return await self.catalog_service.get_catalog_model(
            catalog_id, db_resource=db_resource
        )

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], Catalog],
        lang: str = "en",
        db_resource: Optional[Any] = None,
    ) -> Catalog:
        return await self.catalog_service.create_catalog(
            catalog_data, lang=lang, db_resource=db_resource
        )

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], "CatalogUpdate"],
        lang: str = "en",
        db_resource: Optional[Any] = None,
    ) -> Optional[Catalog]:
        return await self.catalog_service.update_catalog(
            catalog_id, updates, lang=lang, db_resource=db_resource
        )

    async def delete_catalog(
        self, catalog_id: str, force: bool = False, db_resource: Optional[Any] = None
    ) -> bool:
        return await self.catalog_service.delete_catalog(
            catalog_id, force=force, db_resource=db_resource
        )

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, db_resource: Optional[Any] = None
    ) -> bool:
        return await self.catalog_service.delete_catalog_language(
            catalog_id, lang, db_resource=db_resource
        )

    async def list_catalogs(
        self,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        db_resource: Optional[Any] = None,
        q: Optional[str] = None,
    ) -> List[Catalog]:
        return await self.catalog_service.list_catalogs(
            limit=limit, offset=offset, lang=lang, db_resource=db_resource, q=q
        )

    async def search_catalogs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> List[Catalog]:
        return await self.catalog_service.search_catalogs(
            filters=filters, limit=limit, offset=offset, db_resource=db_resource
        )

    # === Collection Operations ===

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        db_resource: Optional[Any] = None,
    ) -> Optional[Collection]:
        return await self.collection_service.get_collection(
            catalog_id, collection_id, lang=lang, db_resource=db_resource
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], Collection],
        lang: str = "en",
        db_resource: Optional[Any] = None,
        **kwargs,
    ) -> Collection:
        return await self.collection_service.create_collection(
            catalog_id, collection_data, lang=lang, db_resource=db_resource, **kwargs
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Union[Dict[str, Any], "CollectionUpdate"],
        lang: str = "en",
        db_resource: Optional[Any] = None,
    ) -> Optional[Collection]:
        return await self.collection_service.update_collection(
            catalog_id, collection_id, updates, lang=lang, db_resource=db_resource
        )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        db_resource: Optional[Any] = None,
    ) -> bool:
        return await self.collection_service.delete_collection(
            catalog_id, collection_id, force=force, db_resource=db_resource
        )

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        db_resource: Optional[Any] = None,
    ) -> bool:
        return await self.collection_service.delete_collection_language(
            catalog_id, collection_id, lang, db_resource=db_resource
        )

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        db_resource: Optional[Any] = None,
        q: Optional[str] = None,
    ) -> List[Any]:
        return await self.collection_service.list_collections(
            catalog_id, limit=limit, offset=offset, lang=lang, db_resource=db_resource, q=q
        )

    # === Item Operations ===

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        db_resource: Optional[Any] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk)."""
        return await self.items_service.upsert(
            catalog_id,
            collection_id,
            items,
            db_resource=db_resource,
            processing_context=processing_context,
        )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self.item_service.get_item(
            catalog_id, collection_id, item_id, db_resource=db_resource
        )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[Any] = None,
    ) -> int:
        return await self.item_service.delete_item(
            catalog_id, collection_id, item_id, db_resource=db_resource
        )

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        ext_id: str,
        lang: str,
        db_resource: Optional[Any] = None,
    ) -> int:
        return await self.item_service.delete_item_language(
            catalog_id, collection_id, ext_id, lang, db_resource=db_resource
        )

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        return await self.item_service.search_items(
            catalog_id, collection_id, request, config=config, db_resource=db_resource
        )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        return await self.item_service.stream_items(
            catalog_id, collection_id, request, config=config, db_resource=db_resource
        )

    # === Schema/Table Resolution ===

    async def resolve_physical_schema(
        self,
        catalog_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        return await self.catalog_service.resolve_physical_schema(
            catalog_id, db_resource=db_resource, allow_missing=allow_missing
        )

    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hint: Optional[str] = None,
    ):
        return await self.catalog_service.resolve_datasource(
            catalog_id, collection_id, operation=operation, hint=hint
        )

    async def resolve_physical_table(
        self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None
    ) -> Optional[str]:
        return await self.catalog_service.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def ensure_catalog_exists(
        self, catalog_id: str, lang: str = "en", db_resource: Optional[Any] = None
    ) -> None:
        return await self.catalog_service.ensure_catalog_exists(
            catalog_id, lang=lang, db_resource=db_resource
        )

    async def ensure_collection_exists(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self.collection_service.ensure_collection_exists(
            db_resource, catalog_id, collection_id, lang=lang
        )

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self.item_service.ensure_physical_table_exists(
            catalog_id, collection_id, config, db_resource=db_resource
        )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        partition_value: Any,
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self.item_service.ensure_partition_exists(
            catalog_id, collection_id, config, partition_value, db_resource=db_resource
        )

    @property
    def assets(self) -> AssetsProtocol:
        return self.asset_service

    @property
    def configs(self) -> ConfigsProtocol:
        return self.config_service

    def get_config_service(self) -> ConfigService:
        # DEPRECATED: use .configs or get_protocol(ConfigsProtocol)
        logger.warning(
            "get_config_service is deprecated, use the 'configs' protocol property instead.",
            stack_info=True,
        )
        return self.config_service

    def get_asset_service(self) -> AssetService:
        # DEPRECATED: use .assets or get_protocol(AssetsProtocol)
        logger.warning(
            "get_asset_service is deprecated, use the 'assets' protocol property instead.",
            stack_info=True,
        )
        return self.asset_service

    @property
    def count_items_by_asset_id_query(self) -> Any:
        from dynastore.modules.catalog.item_service import (
            count_items_by_asset_id_query as _query,
        )

        return _query

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None
    ) -> "CollectionPluginConfig":
        return await self.catalog_service.get_collection_config(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection_column_names(
        self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None
    ) -> Set[str]:
        return await self.catalog_service.get_collection_column_names(
            catalog_id, collection_id, db_resource=db_resource
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
                    catalog_id, db_resource=conn, allow_missing=True
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
                catalog_id=catalog_id, hard=False, db_resource=kwargs.get("db_resource")
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
                db_resource=kwargs.get("db_resource"),
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
                db_resource=kwargs.get("db_resource"),
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
                        await catalogs.update_catalog(
                            catalog_id,
                            {"extra_metadata": {"provisioning_status": "failed", "error": error_message}},
                            db_resource=kwargs.get("db_resource"),
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
    """Module-level proxy for ConfigsProtocol.get_collection_config"""
    conf = get_protocol(ConfigsProtocol)
    if conf:
        return await conf.get_collection_config(*args, **kwargs)
    return None
