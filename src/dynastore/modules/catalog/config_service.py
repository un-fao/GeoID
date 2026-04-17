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
from typing import Optional, Dict, Any, Type, Union, TYPE_CHECKING
from dynastore.tools.cache import cached
from dynastore.modules.storage.router import invalidate_router_cache
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.models.driver_context import DriverContext
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder

# Class-as-identity config API.
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    enforce_config_immutability,
    require_config_class,
    resolve_config_class,
    list_registered_configs,
    _register_schema,
)
from dynastore.modules.db_config.maintenance_tools import ensure_schema_exists
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.typed_store.ddl import (
    CATALOG_CONFIGS_TABLE,
    COLLECTION_CONFIGS_TABLE,
)
from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from .catalog_config import CollectionPluginConfig

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

# Table name constants live in typed_store/ddl.py — imported above.


# Identity normaliser: accept either a class or a class_key string and return
# (class, class_key).  Endpoints that receive dynamic string path params go
# through this at the service boundary.
def _resolve(config_cls: "Union[str, Type[PluginConfig]]") -> "tuple[Type[PluginConfig], str]":
    cls = require_config_class(config_cls)
    return cls, cls.class_key()


# ==============================================================================
#  CACHES
# ==============================================================================


@cached(maxsize=8192, ttl=300, namespace="catalog_config", ignore=["engine", "catalog_manager"])
async def _catalog_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_catalog_config(phys_schema).execute(conn, class_key=class_key)


@cached(maxsize=16384, ttl=300, namespace="collection_config", ignore=["engine", "catalog_manager"])
async def _collection_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    collection_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    validate_sql_identifier(collection_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_collection_config(phys_schema).execute(
            conn,
            collection_id=collection_id,
            class_key=class_key,
        )


# ==============================================================================
#  MANAGER
# ==============================================================================


class ConfigService(ConfigsProtocol):
    """Hierarchical Configuration Manager with framework-level immutability enforcement."""

    priority: int = 10

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
        return self.engine is not None

    def _get_catalog_manager(self) -> CatalogsProtocol:
        if self._catalogs_service is None:
            from dynastore.tools.discovery import get_protocol
            self._catalogs_service = get_protocol(CatalogsProtocol)
        assert self._catalogs_service is not None, "CatalogsProtocol not registered"
        return self._catalogs_service

    def _get_platform_config_service(self) -> PlatformConfigsProtocol:
        if self._platform_config_service is None:
            from dynastore.tools.discovery import get_protocol
            self._platform_config_service = get_protocol(PlatformConfigsProtocol)
        if self._platform_config_service is None:
            from dynastore.modules.db_config.platform_config_service import PlatformConfigService
            self._platform_config_service = PlatformConfigService()
        return self._platform_config_service

    async def get_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> PluginConfig:
        """Retrieves configuration with a waterfall:
        1. Snapshot (if provided, overrides everything)
        2. Collection (if provided)
        3. Catalog (if provided)
        4. Platform (global)
        5. Code-level defaults (class default construction)
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None

        # Tier 0: Snapshot
        if config_snapshot is not None:
            if class_key in config_snapshot:
                data = config_snapshot[class_key]
                if data:
                    return cls.model_validate(data)
            # Authoritative snapshot: skip DB tiers, fall through to platform.
            catalog_id = None
            collection_id = None

        # Tier 1: Collection
        if catalog_id and collection_id:
            if not db_resource:
                data = await self.get_collection_config_internal_cached(
                    catalog_id, collection_id, class_key
                )
                if data:
                    return cls.model_validate(data)
            else:
                config = await self._get_collection_config_internal(
                    catalog_id, collection_id, cls, db_resource=db_resource
                )
                if config:
                    return config

        # Tier 2: Catalog
        if catalog_id:
            if not db_resource:
                data = await self.get_catalog_config_internal_cached(
                    catalog_id, class_key
                )
                if data:
                    return cls.model_validate(data)
            else:
                config = await self._get_catalog_config_internal(
                    catalog_id, cls, db_resource=db_resource
                )
                if config:
                    return config

        # Tier 3 & 4: Platform & defaults
        return await self._get_platform_config_service().get_config(
            cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    async def get_catalog_config_internal_cached(
        self, catalog_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"
        return await _catalog_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, class_key
        )

    async def get_collection_config_internal_cached(
        self, catalog_id: str, collection_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"
        return await _collection_config_cache(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, class_key
        )

    async def _get_catalog_config_internal(
        self,
        catalog_id: str,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> Optional[PluginConfig]:
        class_key = cls.class_key()
        if not db_resource:
            data = await self.get_catalog_config_internal_cached(catalog_id, class_key)
            return cls.model_validate(data) if data else None

        async with managed_transaction(db_resource) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                return None

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return None

            data = await _cq.select_catalog_config(phys_schema).execute(conn, class_key=class_key)

        return cls.model_validate(data) if data else None

    async def _get_collection_config_internal(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> Optional[PluginConfig]:
        class_key = cls.class_key()
        if not db_resource:
            data = await self.get_collection_config_internal_cached(
                catalog_id, collection_id, class_key
            )
            return cls.model_validate(data) if data else None

        async with managed_transaction(db_resource) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return None

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return None

            data = await _cq.select_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                class_key=class_key,
            )

        return cls.model_validate(data) if data else None

    async def set_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._set_collection_config(
                catalog_id, collection_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
        elif catalog_id is not None:
            await self._set_catalog_config(
                catalog_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
        else:
            await self._get_platform_config_service().set_config(
                cls, config,
                check_immutability=check_immutability,
                ctx=DriverContext(db_resource=db_resource) if db_resource else None,
            )

    async def _set_catalog_config(
        self,
        catalog_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )

            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            if check_immutability:
                current_data = await _cq.select_catalog_config_for_update(phys_schema).execute(
                    conn, class_key=class_key
                )
                if current_data:
                    current_config = cls.model_validate(current_data)
                    enforce_config_immutability(current_config, config)

            await _register_schema(conn, config)

            # secret_mode="db" → every Secret field serializes to its
            # encrypted envelope before jsonb persistence. See tools/secrets.py.
            config_data = (
                config.model_dump(mode="json", context={"secret_mode": "db"})
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_catalog_config(phys_schema).execute(
                conn,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, catalog_id, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply catalog configuration for '{class_key}' on catalog '{catalog_id}': {e}",
                        exc_info=True,
                    )

        _catalog_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, class_key
        )
        invalidate_router_cache(catalog_id, None)

    async def _set_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        class_key = cls.class_key()

        async with managed_transaction(db_resource or self.engine) as conn:
            await self._get_catalog_manager().ensure_catalog_exists(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )

            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )

            if not phys_schema:
                raise ValueError(
                    f"Could not resolve physical schema for catalog '{catalog_id}'."
                )

            # JIT-create the thin collection registry row if it doesn't yet
            # exist. Upfront config workflow: `PUT .../collections/{col}/configs/{key}`
            # always succeeds — every sub-config resolves from the waterfall,
            # so a minimal Collection can be materialised without the caller
            # first hitting `POST /collections`.
            await self._get_catalog_manager().ensure_collection_exists(
                catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
            )

            if check_immutability:
                current_data = await _cq.select_collection_config_for_update(phys_schema).execute(
                    conn,
                    collection_id=collection_id,
                    class_key=class_key,
                )
                if current_data:
                    current_config = cls.model_validate(current_data)
                    enforce_config_immutability(current_config, config)

            await _register_schema(conn, config)

            # secret_mode="db" → every Secret field serializes to its
            # encrypted envelope before jsonb persistence. See tools/secrets.py.
            config_data = (
                config.model_dump(mode="json", context={"secret_mode": "db"})
                if hasattr(config, "model_dump")
                else config
            )
            await _cq.upsert_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(config_data, cls=CustomJSONEncoder),
            )

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, catalog_id, collection_id, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply collection configuration for '{class_key}' on '{catalog_id}/{collection_id}': {e}",
                        exc_info=True,
                    )

        _collection_config_cache.cache_invalidate(
            self.engine, self._get_catalog_manager(), catalog_id, collection_id, class_key
        )
        invalidate_router_cache(catalog_id, collection_id)

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional[DriverContext] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")

            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows = await _cq.list_collection_configs_paginated(phys_schema).execute(
                    conn,
                    collection_id=collection_id,
                    limit=limit,
                    offset=offset,
                )

            total = rows[0]["total_count"] if rows else 0
            results = []
            for r in rows:
                ck: str = r["class_key"]
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping collection_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": cls.model_validate(r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}

        elif catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows = await _cq.list_catalog_configs_paginated(phys_schema).execute(
                    conn, limit=limit, offset=offset
                )

            total = rows[0]["total_count"] if rows else 0
            results = []
            for r in rows:
                ck: str = r["class_key"]
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping catalog_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": cls.model_validate(r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}
        else:
            configs = await self._get_platform_config_service().list_configs()
            all_results = [
                {"plugin_id": klass.class_key(), "config": cfg.model_dump()}
                for klass, cfg in configs.items()
            ]
            total = len(all_results)
            return {"total": total, "results": all_results[offset : offset + limit]}

    async def list_catalog_configs(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> Dict[str, PluginConfig]:
        validate_sql_identifier(catalog_id)
        db_resource = ctx.db_resource if ctx else None

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return {}

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return {}

            rows = await _cq.list_catalog_configs(phys_schema).execute(conn)

        configs: Dict[str, PluginConfig] = {}
        for row in rows:
            class_key: str = row["class_key"]
            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning("Skipping catalog_configs row for unknown class_key %r", class_key)
                continue
            configs[class_key] = cls.model_validate(row["config_data"])
        return configs

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if catalog_id:
            validate_sql_identifier(catalog_id)
        if collection_id:
            validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            if catalog_id:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                catalog_table_exists = await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema)
                collection_table_exists = await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema)
                if not catalog_table_exists and not collection_table_exists:
                    return {"total": 0, "results": []}

                if collection_id:
                    if not collection_table_exists:
                        return {"total": 0, "results": []}
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, 'collection' as level, collection_id, class_key, config_data
                    FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                    WHERE collection_id = :collection_id
                    """
                else:
                    parts = []
                    if catalog_table_exists:
                        parts.append(f"""
                        SELECT 'catalog' as level, NULL as collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{CATALOG_CONFIGS_TABLE}""")
                    if collection_table_exists:
                        parts.append(f"""
                        SELECT 'collection' as level, collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}""")
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, level, collection_id, class_key, config_data FROM (
                        {' UNION ALL '.join(parts)}
                    ) sub
                    """

                if query:
                    sql += " WHERE class_key ILIKE :query"

                sql += " ORDER BY level, collection_id, class_key LIMIT :limit OFFSET :offset;"

                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(
                    conn,
                    collection_id=collection_id,
                    query=f"%{query}%" if query else None,
                    limit=limit,
                    offset=offset,
                )

                total = rows[0]["total_count"] if rows else 0
                results = []
                for r in rows:
                    class_key: str = r["class_key"]
                    cls = resolve_config_class(class_key)
                    if cls is None:
                        continue
                    results.append(
                        {
                            "level": r["level"],
                            "catalog_id": catalog_id,
                            "collection_id": r.get("collection_id"),
                            "plugin_id": class_key,
                            "config": cls.model_validate(r["config_data"]).model_dump(),
                        }
                    )
                return {"total": total, "results": results}
            else:
                configs = await self._get_platform_config_service().list_configs()
                all_results = []
                for klass, cfg in configs.items():
                    ck = klass.class_key()
                    if not query or query.lower() in ck.lower():
                        all_results.append(
                            {
                                "level": "platform",
                                "plugin_id": ck,
                                "config": cfg.model_dump(),
                            }
                        )
                total = len(all_results)
                return {"total": total, "results": all_results[offset : offset + limit]}

    async def delete_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._delete_collection_config(
                catalog_id, collection_id, class_key, db_resource=db_resource
            )
        elif catalog_id is not None:
            await self._delete_catalog_config(
                catalog_id, class_key, db_resource=db_resource
            )
        else:
            await self._get_platform_config_service().delete_config(
                cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )

    async def _delete_catalog_config(
        self, catalog_id: str, class_key: str, db_resource: Optional[DbResource] = None
    ) -> bool:
        validate_sql_identifier(catalog_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_catalog_config(phys_schema).execute(
                conn, class_key=class_key
            )

            if rows_affected > 0:
                _catalog_config_cache.cache_invalidate(
                    self.engine, self.catalog_manager, catalog_id, class_key
                )
                invalidate_router_cache(catalog_id, None)
                return True
        return False

    async def _delete_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        class_key: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_collection_config(phys_schema).execute(
                conn,
                collection_id=collection_id,
                class_key=class_key,
            )

            if rows_affected > 0:
                _collection_config_cache.cache_invalidate(
                    self.engine,
                    self.catalog_manager,
                    catalog_id,
                    collection_id,
                    class_key,
                )
                invalidate_router_cache(catalog_id, collection_id)
                return True
        return False
