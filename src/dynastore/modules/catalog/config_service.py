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

import logging
import json
import inspect
import asyncio
from typing import Optional, Callable, Dict, Any, cast, List, TYPE_CHECKING
from async_lru import alru_cache
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder

# Updated import: pulling enforcement logic from the platform manager
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    ConfigRegistry,
    enforce_config_immutability,
)
from dynastore.modules.db_config.partition_tools import (
    ensure_partition_exists,
    ensure_list_hash_partitions,
)
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.locking_tools import (
    acquire_startup_lock,
    check_table_exists,
)
from dynastore.modules.catalog.event_service import event_service
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from .catalog_config import CollectionPluginConfig, COLLECTION_PLUGIN_CONFIG_ID

if TYPE_CHECKING:
    from dynastore.modules.catalog.catalog_module import CatalogModule

logger = logging.getLogger(__name__)

# ==============================================================================
#  LEGACY ALIASES
# ==============================================================================

CatalogConfig = PluginConfig
CollectionConfig = PluginConfig

# ==============================================================================
#  STORAGE & SCHEMAS
# ==============================================================================

COLLECTION_CONFIGS_TABLE = "collection_configs"
CATALOG_CONFIGS_TABLE = "catalog_configs"

# ==============================================================================
#  CACHES
# ==============================================================================


# Global caches for shared configuration lookups
@alru_cache(maxsize=1024)
async def _catalog_config_cache(
    engine: DbResource,
    catalog_manager: "CatalogModule",
    catalog_id: str,
    plugin_id: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, db_resource=conn, allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
            return None

        sql = f'SELECT config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND plugin_id = :plugin_id;'
        return await DQLQuery(
            sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(conn, catalog_id=catalog_id, plugin_id=plugin_id)


@alru_cache(maxsize=1024)
async def _collection_config_cache(
    engine: DbResource,
    catalog_manager: "CatalogModule",
    catalog_id: str,
    collection_id: str,
    plugin_id: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    validate_sql_identifier(collection_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, db_resource=conn, allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
            return None

        sql = f'SELECT config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND plugin_id = :plugin_id;'
        return await DQLQuery(
            sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(
            conn,
            catalog_id=catalog_id,
            collection_id=collection_id,
            plugin_id=plugin_id,
        )


# ==============================================================================
#  MANAGER
# ==============================================================================


class ConfigService(ConfigsProtocol):
    """The Hierarchical Configuration Manager with Framework-Level Immutability Enforcement."""

    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        catalog_manager: Optional[CatalogsProtocol] = None,
        platform_config_manager: Optional[PlatformConfigsProtocol] = None,
    ):
        self.engine = engine
        self._catalog_manager = catalog_manager
        self._catalogs_service = catalog_manager
        self._platform_config_service = platform_config_manager

    @property
    def catalog_manager(self) -> CatalogsProtocol:
        return self._get_catalog_manager()

    @catalog_manager.setter
    def catalog_manager(self, value: CatalogsProtocol):
        self._catalogs_service = value

    @property
    def platform_config_service(self) -> PlatformConfigsProtocol:
        return self._get_platform_config_service()

    @platform_config_service.setter
    def platform_config_service(self, value: PlatformConfigsProtocol):
        self._platform_config_service = value

    def is_available(self) -> bool:
        """Returns True if the manager is initialized and ready."""
        return self.engine is not None

    def _get_catalog_manager(self) -> CatalogsProtocol:
        if self._catalogs_service is None:
            from dynastore.tools.discovery import get_protocol
            self._catalogs_service = get_protocol(CatalogsProtocol)
        return self._catalogs_service

    def _get_platform_config_service(self) -> PlatformConfigsProtocol:
        if self._platform_config_service is None:
            from dynastore.tools.discovery import get_protocol
            self._platform_config_service = get_protocol(PlatformConfigsProtocol)
        if self._platform_config_service is None:
            # Fallback for late initialization
            from dynastore.modules.db_config.platform_config_service import PlatformConfigService
            self._platform_config_service = PlatformConfigService()
        return self._platform_config_service

    async def get_config(
        self,
        plugin_id: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> PluginConfig:
        """
        Retrieves configuration with a 4-tier waterfall:
        1. Snapshot (if provided, overrides everything)
        2. Collection (if provided)
        3. Catalog (if provided)
        4. Platform (global)
        5. Code-level Defaults (via ConfigRegistry)
        """
        # Tier 0: Snapshot
        if config_snapshot is not None:
            if plugin_id in config_snapshot:
                data = config_snapshot[plugin_id]
                if data:
                    return ConfigRegistry.validate_config(plugin_id, data)
            # If a snapshot was explicitly provided, it acts as the authoritative truth 
            # for the runtime state at that moment (e.g. within an async initialization task).
            # To prevent Visibility Gap errors, we skip direct DB lookups for Collection/Catalog levels
            # and fall straight through to the Platform level.
            catalog_id = None
            collection_id = None

        # Tier 1: Collection
        if catalog_id and collection_id:
            if not db_resource:
                data = await self.get_collection_config_internal_cached(
                    catalog_id, collection_id, plugin_id
                )
                if data:
                    return ConfigRegistry.validate_config(plugin_id, data)
            else:
                config = await self._get_collection_config_internal(
                    catalog_id, collection_id, plugin_id, db_resource=db_resource
                )
                if config:
                    return config

        # Tier 2: Catalog
        if catalog_id:
            if not db_resource:
                data = await self.get_catalog_config_internal_cached(
                    catalog_id, plugin_id
                )
                if data:
                    return ConfigRegistry.validate_config(plugin_id, data)
            else:
                config = await self._get_catalog_config_internal(
                    catalog_id, plugin_id, db_resource=db_resource
                )
                if config:
                    return config

        # Tier 3 & 4: Platform & Defaults
        return await self._get_platform_config_service().get_config(
            plugin_id, db_resource=db_resource
        )

    async def get_catalog_config_internal_cached(
        self, catalog_id: str, plugin_id: str
    ) -> Optional[dict]:
        """Global cache for catalog config."""
        return await _catalog_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, plugin_id
        )

    async def get_collection_config_internal_cached(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ) -> Optional[dict]:
        """Global cache for collection config."""
        return await _collection_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, plugin_id
        )

    async def _get_catalog_config_internal_db(
        self, catalog_id: str, plugin_id: str
    ) -> Optional[dict]:
        """Delegates to global cache."""
        return await self.get_catalog_config_internal_cached(catalog_id, plugin_id)

    async def _get_catalog_config_internal(
        self, catalog_id: str, plugin_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[PluginConfig]:
        """Public fetcher that respects the provided db_resource, falling back to cache."""
        if not db_resource:
            data = await self.get_catalog_config_internal_cached(catalog_id, plugin_id)
            return ConfigRegistry.validate_config(plugin_id, data) if data else None

        # Live query if db_resource provided
        async with managed_transaction(db_resource) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn, allow_missing=True
            )
            if not phys_schema:
                return None

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return None

            sql = f'SELECT config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND plugin_id = :plugin_id;'
            data = await DQLQuery(
                sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(conn, catalog_id=catalog_id, plugin_id=plugin_id)

        return ConfigRegistry.validate_config(plugin_id, data) if data else None

    async def set_config(
        self,
        plugin_id: str,
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """
        Unified method to set configuration at any level.
        """
        if collection_id is not None:
            # Collection level
            if catalog_id is None:
                raise ValueError(
                    "catalog_id is required when collection_id is provided"
                )
            await self._set_collection_config(
                catalog_id,
                collection_id,
                plugin_id,
                config,
                check_immutability=check_immutability,
                db_resource=db_resource,
            )
        elif catalog_id is not None:
            # Catalog level
            await self._set_catalog_config(
                catalog_id,
                plugin_id,
                config,
                check_immutability=check_immutability,
                db_resource=db_resource,
            )
        else:
            # Platform level
            await self._get_platform_config_service().set_config(
                plugin_id,
                config,
                check_immutability=check_immutability,
                db_resource=db_resource,
            )

    async def set_catalog_config(
        self,
        catalog_id: str,
        plugin_id: str,
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Sets configuration at the Catalog level."""
        await self._set_catalog_config(
            catalog_id,
            plugin_id,
            config,
            check_immutability=check_immutability,
            db_resource=db_resource,
        )

    async def _set_catalog_config(
        self,
        catalog_id: str,
        plugin_id: str,
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Internal: Sets configuration at the Catalog level."""
        validate_sql_identifier(catalog_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            # 0. Ensure catalog exists (and thus schema)
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, db_resource=conn
            )

            # Resolve physical schema
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn, allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, db_resource=conn
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, db_resource=conn
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            if check_immutability:
                # Dynamic locking query
                sql = f'SELECT config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND plugin_id = :plugin_id FOR UPDATE;'
                current_data = await DQLQuery(
                    sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
                ).execute(conn, catalog_id=catalog_id, plugin_id=plugin_id)
                if current_data:
                    current_config = ConfigRegistry.validate_config(
                        plugin_id, current_data
                    )
                    enforce_config_immutability(current_config, config)

            # Dynamic upsert
            upsert_sql = f"""
            INSERT INTO "{phys_schema}".{CATALOG_CONFIGS_TABLE} (catalog_id, plugin_id, config_data, updated_at)
            VALUES (:catalog_id, :plugin_id, :config_data, NOW()) 
            ON CONFLICT (catalog_id, plugin_id) DO UPDATE SET config_data = EXCLUDED.config_data, updated_at = NOW()
            """
            # Handle both dict and Pydantic model types
            config_data = (
                config.model_dump() if hasattr(config, "model_dump") else config
            )
            await DQLQuery(upsert_sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                catalog_id=catalog_id,
                plugin_id=plugin_id,
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Trigger active configuration application (Level 2 - Catalog)
            apply_handler = ConfigRegistry.get_apply_handler(plugin_id)
            if apply_handler:
                try:
                    res = apply_handler(config, catalog_id, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply catalog configuration for '{plugin_id}' on catalog '{catalog_id}': {e}",
                        exc_info=True,
                    )

        _catalog_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, plugin_id
        )

    async def _get_collection_config_internal_db(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ) -> Optional[dict]:
        """Delegates to global cache."""
        return await self.get_collection_config_internal_cached(
            catalog_id, collection_id, plugin_id
        )

    async def _get_collection_config_internal(
        self,
        catalog_id: str,
        collection_id: str,
        plugin_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[PluginConfig]:
        """Public fetcher that respects the provided db_resource, falling back to cache."""
        if not db_resource:
            data = await self.get_collection_config_internal_cached(
                catalog_id, collection_id, plugin_id
            )
            return ConfigRegistry.validate_config(plugin_id, data) if data else None

        # Live query if db_resource provided
        async with managed_transaction(db_resource) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return None

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return None

            sql = f'SELECT config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND plugin_id = :plugin_id;'
            data = await DQLQuery(
                sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=collection_id,
                plugin_id=plugin_id,
            )

        return ConfigRegistry.validate_config(plugin_id, data) if data else None

    async def _set_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        plugin_id: str,
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Internal: Sets configuration at the Collection level."""
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            # 0. Ensure catalog exists (and thus schema)
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, db_resource=conn
            )

            # Resolve physical schema
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn, allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, db_resource=conn
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, db_resource=conn
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            # First, ensure the collection exists to prevent foreign key violations.
            collection = await self._get_catalog_manager().get_collection(
                catalog_id, collection_id, db_resource=conn
            )
            if not collection:
                raise ValueError(
                    f"Cannot set configuration. Collection '{collection_id}' not found in catalog '{catalog_id}'."
                )

            if check_immutability:
                sql = f'SELECT config_data FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND plugin_id = :plugin_id FOR UPDATE;'
                current_data = await DQLQuery(
                    sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
                ).execute(
                    conn,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    plugin_id=plugin_id,
                )
                if current_data:
                    current_config = ConfigRegistry.validate_config(
                        plugin_id, current_data
                    )
                    enforce_config_immutability(current_config, config)

            # Dynamic upsert
            upsert_sql = f"""
            INSERT INTO "{phys_schema}".{COLLECTION_CONFIGS_TABLE} (catalog_id, collection_id, plugin_id, config_data, updated_at)
            VALUES (:catalog_id, :collection_id, :plugin_id, :config_data, NOW()) 
            ON CONFLICT (catalog_id, collection_id, plugin_id) DO UPDATE SET config_data = EXCLUDED.config_data, updated_at = NOW()
            """
            # Handle both dict and Pydantic model types
            config_data = (
                config.model_dump() if hasattr(config, "model_dump") else config
            )
            await DQLQuery(upsert_sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=collection_id,
                plugin_id=plugin_id,
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            # Trigger active configuration application (Level 1 - Collection)
            apply_handler = ConfigRegistry.get_apply_handler(plugin_id)
            if apply_handler:
                try:
                    res = apply_handler(config, catalog_id, collection_id, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply collection configuration for '{plugin_id}' on '{catalog_id}/{collection_id}': {e}",
                        exc_info=True,
                    )

        _collection_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, plugin_id
        )

    async def set_platform_config(self, plugin_id: str, config: PluginConfig) -> None:
        await self._get_platform_config_service().set_config(plugin_id, config)

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """
        Unified method to list configurations at any level with pagination and total count.
        """
        if collection_id is not None:
            # Collection level
            if catalog_id is None:
                raise ValueError(
                    "catalog_id is required when collection_id is provided"
                )

            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                sql = f"""
                SELECT COUNT(*) OVER() as total_count, plugin_id, config_data
                FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                WHERE catalog_id = :catalog_id AND collection_id = :collection_id
                ORDER BY plugin_id
                LIMIT :limit OFFSET :offset;
                """
                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    limit=limit,
                    offset=offset,
                )

            total = rows[0]["total_count"] if rows else 0
            results = [
                {
                    "plugin_id": r["plugin_id"],
                    "config": ConfigRegistry.validate_config(
                        r["plugin_id"], r["config_data"]
                    ).model_dump(),
                }
                for r in rows
            ]
            return {"total": total, "results": results}

        elif catalog_id is not None:
            # Catalog level
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                sql = f"""
                SELECT COUNT(*) OVER() as total_count, plugin_id, config_data
                FROM \"{phys_schema}\".{CATALOG_CONFIGS_TABLE}
                WHERE catalog_id = :catalog_id
                ORDER BY plugin_id
                LIMIT :limit OFFSET :offset;
                """
                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, catalog_id=catalog_id, limit=limit, offset=offset)

            total = rows[0]["total_count"] if rows else 0
            results = [
                {
                    "plugin_id": r["plugin_id"],
                    "config": ConfigRegistry.validate_config(
                        r["plugin_id"], r["config_data"]
                    ).model_dump(),
                }
                for r in rows
            ]
            return {"total": total, "results": results}
        else:
            # Platform level - implementation in platform_config_service doesn't support pagination yet,
            # so we fetch all and slice locally for now.
            configs = await self._get_platform_config_service().list_configs()
            # Simple manual pagination for platform level as it's usually small
            all_results = [
                {"plugin_id": pid, "config": cfg.model_dump()}
                for pid, cfg in configs.items()
            ]
            total = len(all_results)
            return {"total": total, "results": all_results[offset : offset + limit]}

    async def list_catalog_configs(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Dict[str, PluginConfig]:
        """Lists all configurations explicitly set at the Catalog level."""
        validate_sql_identifier(catalog_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return {}

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return {}

            sql = f'SELECT plugin_id, config_data FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id;'
            rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
                conn, catalog_id=catalog_id
            )

        configs = {}
        for row in rows:
            plugin_id = row["plugin_id"]
            config_data = row["config_data"]
            configs[plugin_id] = ConfigRegistry.validate_config(plugin_id, config_data)
        return configs

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Searches for configurations across the hierarchy.
        Matches against plugin_id (simple LIKE search).
        """
        if catalog_id:
            validate_sql_identifier(catalog_id)
        if collection_id:
            validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            if catalog_id:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                catalog_table_exists = await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema)
                collection_table_exists = await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema)
                if not catalog_table_exists and not collection_table_exists:
                    return {"total": 0, "results": []}

                # Search in both catalog and collection configs if only catalog_id provided
                # or just collection if collection_id provided.
                if collection_id:
                    if not collection_table_exists:
                        return {"total": 0, "results": []}
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, 'collection' as level, catalog_id, collection_id, plugin_id, config_data
                    FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                    WHERE catalog_id = :catalog_id AND collection_id = :collection_id
                    """
                else:
                    # Build UNION from whichever tables exist
                    parts = []
                    if catalog_table_exists:
                        parts.append(f"""
                        SELECT 'catalog' as level, catalog_id, NULL as collection_id, plugin_id, config_data
                        FROM \"{phys_schema}\".{CATALOG_CONFIGS_TABLE}
                        WHERE catalog_id = :catalog_id""")
                    if collection_table_exists:
                        parts.append(f"""
                        SELECT 'collection' as level, catalog_id, collection_id, plugin_id, config_data
                        FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                        WHERE catalog_id = :catalog_id""")
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, level, catalog_id, collection_id, plugin_id, config_data FROM (
                        {' UNION ALL '.join(parts)}
                    ) sub
                    """

                if query:
                    sql += " WHERE plugin_id ILIKE :query"

                sql += " ORDER BY level, catalog_id, collection_id, plugin_id LIMIT :limit OFFSET :offset;"

                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    query=f"%{query}%" if query else None,
                    limit=limit,
                    offset=offset,
                )

                total = rows[0]["total_count"] if rows else 0
                results = []
                for r in rows:
                    results.append(
                        {
                            "level": r["level"],
                            "catalog_id": r["catalog_id"],
                            "collection_id": r["collection_id"],
                            "plugin_id": r["plugin_id"],
                            "config": ConfigRegistry.validate_config(
                                r["plugin_id"], r["config_data"]
                            ).model_dump(),
                        }
                    )
                return {"total": total, "results": results}
            else:
                # Platform search fallback
                configs = await self._get_platform_config_service().list_configs()
                all_results = []
                for pid, cfg in configs.items():
                    if not query or query.lower() in pid.lower():
                        all_results.append(
                            {
                                "level": "platform",
                                "plugin_id": pid,
                                "config": cfg.model_dump(),
                            }
                        )

                total = len(all_results)
                return {"total": total, "results": all_results[offset : offset + limit]}

    async def delete_catalog_config(self, catalog_id: str, plugin_id: str) -> bool:
        """Deletes a configuration at the Catalog level."""
        validate_sql_identifier(catalog_id)
        async with managed_transaction(self.engine) as conn:
            phys_schema = await self.catalog_manager.resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return False

            sql = f'DELETE FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND plugin_id = :plugin_id;'
            rows_affected = await DQLQuery(
                sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, catalog_id=catalog_id, plugin_id=plugin_id)

            if rows_affected > 0:
                _catalog_config_cache.cache_invalidate(
                    self.engine, self.catalog_manager, catalog_id, plugin_id
                )
                return True
        return False

    async def delete_config(
        self,
        plugin_id: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """
        Unified method to delete configuration at any level.
        Delegates to specific methods based on provided parameters.
        Deleting platform config acts as a reset to defaults.
        """
        if collection_id is not None:
            # Collection level
            if catalog_id is None:
                raise ValueError(
                    "catalog_id is required when collection_id is provided"
                )
            await self._delete_collection_config(
                catalog_id, collection_id, plugin_id, db_resource=db_resource
            )
        elif catalog_id is not None:
            # Catalog level
            await self._delete_catalog_config(
                catalog_id, plugin_id, db_resource=db_resource
            )
        else:
            # Platform level - delete acts as reset
            # Note: The provided diff changes this from delete_config to set_config.
            # Assuming 'config' parameter should be passed, but it's not available here.
            # Reverting to original behavior of calling delete_config on the service.
            await self._get_platform_config_service().delete_config(
                plugin_id, db_resource=db_resource
            )

    async def _delete_catalog_config(
        self, catalog_id: str, plugin_id: str, db_resource: Optional[DbResource] = None
    ) -> bool:
        """Internal: Deletes a configuration at the Catalog level."""
        validate_sql_identifier(catalog_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return False

            sql = f'DELETE FROM "{phys_schema}".{CATALOG_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND plugin_id = :plugin_id;'
            rows_affected = await DQLQuery(
                sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, catalog_id=catalog_id, plugin_id=plugin_id)

            if rows_affected > 0:
                _catalog_config_cache.cache_invalidate(
                    self.engine, self.catalog_manager, catalog_id, plugin_id
                )
                return True
        return False

    async def _delete_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        plugin_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """Internal: Deletes a configuration at the Collection level."""
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return False

            sql = f'DELETE FROM "{phys_schema}".{COLLECTION_CONFIGS_TABLE} WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND plugin_id = :plugin_id;'
            rows_affected = await DQLQuery(
                sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn,
                catalog_id=catalog_id,
                collection_id=collection_id,
                plugin_id=plugin_id,
            )

            if rows_affected > 0:
                _collection_config_cache.cache_invalidate(
                    self.engine,
                    self.catalog_manager,
                    catalog_id,
                    collection_id,
                    plugin_id,
                )
                return True
        return False
