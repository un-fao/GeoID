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

import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Union, Set, AsyncIterator

import dynastore.modules as dm
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    DbResource, managed_transaction, DDLQuery, DQLQuery, ResultHandler
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists, acquire_startup_lock, execute_ddl_block
)
from dynastore.modules.db_config.tools import get_any_engine
from dynastore.modules.catalog.models import Catalog, Collection, ItemDataForDB, LocalizedText
from dynastore.models.protocols import (
    CatalogsProtocol, 
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol, 
    ConfigsProtocol, 
    LogsProtocol,
    DatabaseProtocol,
    PropertiesProtocol,
    LocalizationProtocol
)

from dynastore.tools.discovery import Provider, initialize_providers, get_protocol
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.config_manager import ConfigManager
from dynastore.modules.catalog.asset_manager import AssetManager
from dynastore.modules.catalog.properties_service import PropertiesService
from dynastore.modules.catalog.localization_service import LocalizationService
from dynastore.modules.catalog.event_manager import (
    event_manager, 
    CatalogEventType, 
    register_event_listener,
    emit_event,
    process_queued_event as _process_queued_event
)
from dynastore.modules.catalog.log_manager import LOG_SERVICE, LogService

logger = logging.getLogger(__name__)

# --- Legacy Constants and DDL (Shared by Module Initialization) ---

CATALOGS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalogs (
    id VARCHAR PRIMARY KEY,
    physical_schema VARCHAR NOT NULL UNIQUE,
    title JSONB,
    description JSONB,
    keywords JSONB,
    license JSONB,
    extra_metadata JSONB,
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

from dynastore.modules.protocols import ModuleProtocol

_module_instance: Optional[ModuleProtocol] = None

@dm.dynastore_module
class CatalogModule(ModuleProtocol):
    """
    Manages catalog lifecycle.
    Functions as a composition root, initializing services and registering them as protocol providers.
    """
    
    # Protocol attributes
    priority: int = 10  # High priority for protocol discovery
    
    # Services (Provider Annotations)
    # Providers are defined declaratively and initialized via initialize_providers

    # Define providers declaratively
    # Priority determines initialization order (highest first)
    log_service = Provider(LogService, priority=100)
    catalog_service = Provider(CatalogService, priority=95)
    collection_service = Provider(CollectionService, priority=90)
    items_service = Provider(ItemService, priority=90)
    config_manager = Provider(ConfigManager, priority=90)
    asset_manager = Provider(AssetManager, priority=90)
    properties_service = Provider(PropertiesService, priority=85)
    localization_service = Provider(LocalizationService, priority=85)
    
    def __init__(self):
        self.app_state: Optional[Any] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Standard CatalogModule lifespan with physical storage initialization."""
        self.app_state = app_state
        
        # 1. Resolve Database Engine
        db = dm.get_protocol(DatabaseProtocol)
        # Use existing protocol or fallback to tools if strictly necessary for bootstrapping
        engine = db.engine if db else get_any_engine(app_state)
        
        if not engine:
            logger.critical("CatalogModule: No DB engine found during startup.")
            yield; return
            
        global _module_instance
        _module_instance = self
        
        # 2. Initialize Providers (Declarative)
        # Managed automatically by dynastore.modules.lifespan
        
        logger.info("Initialized CatalogModule providers.")

        # 4. Initialize Storage & Schemas
        # 2. Register core collection lifecycle handlers
        from dynastore.modules.catalog.collection_service import create_physical_collection_impl
        from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
        lifecycle_registry.sync_collection_initializer(create_physical_collection_impl)
        
        async with managed_transaction(engine) as conn:
            async with acquire_startup_lock(conn, "catalog_module"):
                await ensure_schema_exists(conn, "catalog")
                if event_manager.storage:
                    await event_manager.storage.initialize(conn)

                await execute_ddl_block(conn, CATALOGS_TABLE_DDL)
                await execute_ddl_block(conn, SHARED_PROPERTIES_SCHEMA)
                
                # Ensure stored procedures (replacing init.sql)
                from dynastore.modules.catalog.db_init.stored_procedures import ensure_stored_procedures
                await ensure_stored_procedures(conn)

        # 5. Register Internal Observers
        # Observers will use get_protocol() to access services
        register_event_listener(CatalogEventType.AFTER_CATALOG_HARD_DELETION, self._on_catalog_hard_deletion)
        register_event_listener(CatalogEventType.CATALOG_DELETION, self._on_catalog_deletion)
        register_event_listener(CatalogEventType.AFTER_COLLECTION_HARD_DELETION, self._on_collection_hard_deletion)
        register_event_listener(CatalogEventType.COLLECTION_DELETION, self._on_collection_deletion)

        # 6. Start Background Tasks
        ENABLE_EVENT_CONSUMER = os.environ.get("DYNASTORE_ENABLE_EVENT_CONSUMER", "false").lower() == "true"
        event_consumer = None
        if event_manager.storage and ENABLE_EVENT_CONSUMER:
            event_consumer = event_manager.create_consumer(engine=engine, handler=_process_queued_event, batch_size=50, poll_interval=2.0)
            await event_consumer.start()

        try:
            yield
        finally:
            if event_consumer:
                await event_consumer.stop()

    # === Unified Protocol Properties (Delegation) ===

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        return self

    @property
    def collections(self) -> CollectionsProtocol:
        return self

    @property
    def assets(self) -> AssetsProtocol:
        return self.asset_manager

    @property
    def configs(self) -> ConfigsProtocol:
        return self.config_manager

    @property
    def localization(self) -> LocalizationProtocol:
        return self.localization_service

    # === Delegated CRUD Methods ===

    async def get_catalog(self, catalog_id: str, lang: str = "en", db_resource: Optional[Any] = None) -> Catalog:
        return await self.catalog_service.get_catalog(catalog_id, lang=lang, db_resource=db_resource)

    async def get_catalog_model(self, catalog_id: str, db_resource: Optional[Any] = None) -> Optional[Catalog]:
        return await self.catalog_service.get_catalog_model(catalog_id, db_resource=db_resource)

    async def create_catalog(self, catalog_data: Union[Dict[str, Any], Catalog], lang: str = "en", db_resource: Optional[Any] = None) -> Catalog:
        return await self.catalog_service.create_catalog(catalog_data, lang=lang, db_resource=db_resource)

    async def update_catalog(self, catalog_id: str, updates: Union[Dict[str, Any], "CatalogUpdate"], lang: str = "en", db_resource: Optional[Any] = None) -> Optional[Catalog]:
        return await self.catalog_service.update_catalog(catalog_id, updates, lang=lang, db_resource=db_resource)

    async def delete_catalog(self, catalog_id: str, force: bool = False, db_resource: Optional[Any] = None) -> bool:
        return await self.catalog_service.delete_catalog(catalog_id, force=force, db_resource=db_resource)

    async def delete_catalog_language(self, catalog_id: str, lang: str, db_resource: Optional[Any] = None) -> bool:
        return await self.catalog_service.delete_catalog_language(catalog_id, lang, db_resource=db_resource)

    async def list_catalogs(self, limit: int = 10, offset: int = 0, lang: str = "en", db_resource: Optional[Any] = None) -> List[Catalog]:
        return await self.catalog_service.list_catalogs(limit=limit, offset=offset, lang=lang, db_resource=db_resource)

    async def search_catalogs(self, filters: Optional[Dict[str, Any]] = None, limit: int = 10, offset: int = 0, db_resource: Optional[Any] = None) -> List[Catalog]:
        return await self.catalog_service.search_catalogs(filters=filters, limit=limit, offset=offset, db_resource=db_resource)

    # === Catalog Localized Convenience Methods ===

    async def create_catalog_localized(self, catalog_data: Union[Dict[str, Any], Catalog], lang: str, db_resource: Optional[Any] = None) -> Catalog:
        return await self.create_catalog(catalog_data, lang=lang, db_resource=db_resource)

    async def create_catalog_multilanguage(self, catalog_data: Union[Dict[str, Any], Catalog], db_resource: Optional[Any] = None) -> Catalog:
        return await self.create_catalog(catalog_data, lang="*", db_resource=db_resource)

    async def get_catalog_localized(self, catalog_id: str, lang: str, db_resource: Optional[Any] = None) -> Catalog:
        return await self.get_catalog(catalog_id, lang=lang, db_resource=db_resource)

    async def get_catalog_multilanguage(self, catalog_id: str, db_resource: Optional[Any] = None) -> Catalog:
        return await self.get_catalog(catalog_id, lang="*", db_resource=db_resource)

    # === Collection Operations ===

    async def get_collection(self, catalog_id: str, collection_id: str, lang: str = "en", db_resource: Optional[Any] = None) -> Optional[Collection]:
        return await self.collection_service.get_collection(catalog_id, collection_id, lang=lang, db_resource=db_resource)

    async def create_collection(self, catalog_id: str, collection_data: Union[Dict[str, Any], Collection], lang: str = "en", db_resource: Optional[Any] = None) -> Collection:
        return await self.collection_service.create_collection(catalog_id, collection_data, lang=lang, db_resource=db_resource)

    async def update_collection(self, catalog_id: str, collection_id: str, updates: Union[Dict[str, Any], "CollectionUpdate"], lang: str = "en", db_resource: Optional[Any] = None) -> Optional[Collection]:
        return await self.collection_service.update_collection(catalog_id, collection_id, updates, lang=lang, db_resource=db_resource)

    async def delete_collection(self, catalog_id: str, collection_id: str, force: bool = False, db_resource: Optional[Any] = None) -> bool:
        return await self.collection_service.delete_collection(catalog_id, collection_id, force=force, db_resource=db_resource)

    async def delete_collection_language(self, catalog_id: str, collection_id: str, lang: str, db_resource: Optional[Any] = None) -> bool:
        return await self.collection_service.delete_collection_language(catalog_id, collection_id, lang, db_resource=db_resource)

    async def list_collections(self, catalog_id: str, limit: int = 10, offset: int = 0, lang: str = "en", db_resource: Optional[Any] = None) -> List[Any]:
        return await self.collection_service.list_collections(catalog_id, limit=limit, offset=offset, lang=lang, db_resource=db_resource)

    # === Collection Localized Helpers ===

    async def create_collection_localized(self, catalog_id: str, collection_data: Union[Dict[str, Any], Collection], lang: str, db_resource: Optional[Any] = None) -> Collection:
        return await self.create_collection(catalog_id, collection_data, lang=lang, db_resource=db_resource)

    async def create_collection_multilanguage(self, catalog_id: str, collection_data: Union[Dict[str, Any], Collection], db_resource: Optional[Any] = None) -> Collection:
        return await self.create_collection(catalog_id, collection_data, lang="*", db_resource=db_resource)

    async def get_collection_localized(self, catalog_id: str, collection_id: str, lang: str, db_resource: Optional[Any] = None) -> Collection:
        return await self.get_collection(catalog_id, collection_id, lang=lang, db_resource=db_resource)

    async def get_collection_multilanguage(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> Collection:
        return await self.get_collection(catalog_id, collection_id, lang="*", db_resource=db_resource)

    # === Item Operations ===

    async def upsert(
        self, 
        catalog_id: str, 
        collection_id: str, 
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any], 
        db_resource: Optional[Any] = None,
        processing_context: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk)."""
        return await self.items_service.upsert(catalog_id, collection_id, items, db_resource=db_resource, processing_context=processing_context)

    async def get_item(self, catalog_id: str, collection_id: str, geoid: Any, db_resource: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        return await self.item_service.get_item(catalog_id, collection_id, geoid, db_resource=db_resource)

    async def get_item_by_external_id(self, catalog_id: str, collection_id: str, ext_id: str, db_resource: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """Logically retrieves an item by its external ID, resolving schema and table internally."""
        phys_schema = await self.resolve_physical_schema(catalog_id, db_resource=db_resource)
        phys_table = await self.resolve_physical_table(catalog_id, collection_id, db_resource=db_resource)
        if not phys_schema or not phys_table:
            return None
        return await self.item_service.get_active_row_by_external_id(phys_schema, phys_table, ext_id, db_resource=db_resource)

    async def delete_item(self, catalog_id: str, collection_id: str, ext_id: str, db_resource: Optional[Any] = None) -> int:
        return await self.item_service.delete_item(catalog_id, collection_id, ext_id, db_resource=db_resource)

    async def delete_item_language(self, catalog_id: str, collection_id: str, ext_id: str, lang: str, db_resource: Optional[Any] = None) -> int:
        return await self.item_service.delete_item_language(catalog_id, collection_id, ext_id, lang, db_resource=db_resource)

    async def search_items(self, catalog_id: str, collection_id: str, request: QueryRequest, config: Optional[ConfigsProtocol] = None, db_resource: Optional[Any] = None) -> List[Dict[str, Any]]:
        return await self.item_service.search_items(catalog_id, collection_id, request, config=config, db_resource=db_resource)

    async def stream_items(self, catalog_id: str, collection_id: str, request: QueryRequest, config: Optional[ConfigsProtocol] = None, db_resource: Optional[Any] = None) -> AsyncIterator[Dict[str, Any]]:
        return await self.item_service.stream_items(catalog_id, collection_id, request, config=config, db_resource=db_resource)

    # === Schema/Table Resolution ===

    async def resolve_physical_schema(self, catalog_id: Optional[str] = None, db_resource: Optional[Any] = None, allow_missing: bool = False) -> Optional[str]:
        return await self.catalog_service.resolve_physical_schema(catalog_id, db_resource=db_resource, allow_missing=allow_missing)

    async def resolve_physical_table(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> Optional[str]:
        return await self.catalog_service.resolve_physical_table(catalog_id, collection_id, db_resource=db_resource)

    async def ensure_catalog_exists(self, catalog_id: str, db_resource: Optional[Any] = None) -> None:
        return await self.catalog_service.ensure_catalog_exists(catalog_id, db_resource=db_resource)

    async def ensure_collection_exists(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> None:
        return await self.collection_service.ensure_collection_exists(db_resource, catalog_id, collection_id)

    async def ensure_physical_table_exists(self, catalog_id: str, collection_id: str, config: "CollectionPluginConfig", db_resource: Optional[Any] = None) -> None:
        return await self.item_service.ensure_physical_table_exists(catalog_id, collection_id, config, db_resource=db_resource)

    async def ensure_partition_exists(self, catalog_id: str, collection_id: str, config: "CollectionPluginConfig", partition_value: Any, db_resource: Optional[Any] = None) -> None:
        return await self.item_service.ensure_partition_exists(catalog_id, collection_id, config, partition_value, db_resource=db_resource)

    @property
    def assets(self) -> AssetsProtocol:
        return self.asset_manager

    @property
    def configs(self) -> ConfigsProtocol:
        return self.config_manager

    def get_config_manager(self) -> ConfigManager:
        """Deprecated: use self.configs instead."""
        import warnings
        warnings.warn("get_config_manager is deprecated, use the 'configs' protocol property instead.", DeprecationWarning, stacklevel=2)
        return self.config_manager

    def get_asset_manager(self) -> AssetManager:
        """Deprecated: use self.assets instead."""
        import warnings
        warnings.warn("get_asset_manager is deprecated, use the 'assets' protocol property instead.", DeprecationWarning, stacklevel=2)
        return self.asset_manager

    @property
    def count_items_by_asset_id_query(self) -> Any:
        from dynastore.modules.catalog.item_service import count_items_by_asset_id_query as _query
        return _query

    async def get_collection_config(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> "CollectionPluginConfig":
        return await self.catalog_service.get_collection_config(catalog_id, collection_id, db_resource=db_resource)

    async def get_collection_column_names(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> Set[str]:
        return await self.catalog_service.get_collection_column_names(catalog_id, collection_id, db_resource=db_resource)


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
        
        db_resource = kwargs.get('db_resource')
        if not db_resource and db:
            db_resource = db.engine

        if not db_resource:
            logger.error("No database resource available for catalog hard deletion cleanup.")
            return

        # 1. Purge Assets
        if assets:
            await assets.delete_assets(catalog_id=catalog_id, hard=True, db_resource=db_resource)
        
        # 2. Drop logical record and configuration
        async with managed_transaction(db_resource) as conn:
            phys_schema = None
            if catalogs:
                phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn, allow_missing=True)
            
            if phys_schema:
                await DDLQuery(f"DROP SCHEMA IF EXISTS {phys_schema} CASCADE;").execute(conn)
            

    async def _on_catalog_deletion(self, catalog_id: str, **kwargs):
        """Soft deletion of catalog assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(catalog_id=catalog_id, hard=False, db_resource=kwargs.get('db_resource'))

    async def _on_collection_hard_deletion(self, catalog_id: str, collection_id: str, **kwargs):
        """Purge assets for a hard-deleted collection."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(catalog_id=catalog_id, collection_id=collection_id, hard=True, db_resource=kwargs.get('db_resource'))

    async def _on_collection_deletion(self, catalog_id: str, collection_id: str, **kwargs):
        """Soft deletion of collection assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(catalog_id=catalog_id, collection_id=collection_id, hard=False, db_resource=kwargs.get('db_resource'))

