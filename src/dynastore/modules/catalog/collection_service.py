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
from typing import List, Optional, Any, Dict, Union, Tuple, Set, Callable
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    managed_nested_transaction,
)
from dynastore.modules.catalog.models import Collection, CollectionUpdate, Catalog
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    COLLECTION_PLUGIN_CONFIG_ID,
)
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol, AssetsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.async_utils import signal_bus
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext
from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.platform_config_service import ConfigRegistry

logger = logging.getLogger(__name__)


class CollectionService:
    """Service for collection-level operations."""

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine
        # Instance-bound caches (private)
        self._get_collection_model_cached = cached(maxsize=1024, namespace="collection_model")(
            self._get_collection_model_db
        )

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    async def _get_pg_driver(self):
        """Get the PostgreSQL storage driver instance."""
        from dynastore.modules.storage.drivers.postgresql import DriverRecordsPostgresql
        from dynastore.tools.discovery import get_protocols
        from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol

        for driver in get_protocols(CollectionStorageDriverProtocol):
            if isinstance(driver, DriverRecordsPostgresql):
                return driver
        return None

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Resolve physical table via the PG driver config."""
        pg_driver = await self._get_pg_driver()
        if not pg_driver:
            return None
        return await pg_driver.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Store physical table in PG driver config."""
        pg_driver = await self._get_pg_driver()
        if not pg_driver:
            raise RuntimeError("DriverRecordsPostgresql not available")
        await pg_driver.set_physical_table(
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    async def _get_collection_model_db(
        self, catalog_id: str, collection_id: str
    ) -> Optional[Collection]:
        async with managed_transaction(self.engine) as conn:
            return await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

    async def _get_collection_model_logic(
        self, catalog_id: str, collection_id: str, conn: DbResource
    ) -> Optional[Collection]:
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        if not phys_schema:
            return None

        # 1. Verify existence in thin PG registry (always PG — thin registry is authoritative).
        exists_sql = f'SELECT id FROM "{phys_schema}".collections WHERE id = :id AND deleted_at IS NULL;'
        exists = await DQLQuery(
            exists_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(conn, id=collection_id)
        if not exists:
            return None

        # 2. Read metadata via the metadata driver (ES preferred, PG fallback).
        meta_dict = None
        try:
            from dynastore.modules.catalog.metadata_router import get_metadata_driver

            meta_driver = await get_metadata_driver(catalog_id)
            if meta_driver is not None:
                meta_dict = await meta_driver.get_metadata(
                    catalog_id, collection_id, db_resource=conn,
                )
        except Exception:
            pass  # driver error — fall through to PG direct

        if meta_dict is None:
            # PG metadata fallback (always available)
            meta_sql = f'SELECT * FROM "{phys_schema}".metadata WHERE collection_id = :id;'
            meta_dict = await DQLQuery(
                meta_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=collection_id) or {}

        # Deserialize JSONB columns
        for key in ["title", "description", "keywords", "license", "links", "assets",
                    "extent", "providers", "summaries", "item_assets", "extra_metadata",
                    "stac_extensions"]:
            val = meta_dict.get(key)
            if isinstance(val, str):
                try:
                    meta_dict[key] = json.loads(val)
                except Exception:
                    meta_dict[key] = None

        data = {
            "id": collection_id,
            "title": meta_dict.get("title"),
            "description": meta_dict.get("description"),
            "keywords": meta_dict.get("keywords"),
            "license": meta_dict.get("license"),
            "links": meta_dict.get("links"),
            "assets": meta_dict.get("assets"),
            "extent": meta_dict.get("extent"),
            "providers": meta_dict.get("providers"),
            "summaries": meta_dict.get("summaries"),
            "item_assets": meta_dict.get("item_assets"),
            "extra_metadata": meta_dict.get("extra_metadata"),
        }

        # 3. CollectionMetadataEnricherProtocol pipeline (optional, priority-ordered).
        #    Each enricher can augment, filter, or transform the metadata dict.
        #    Enrichers are registered via register_plugin(); an empty registry is safe.
        try:
            from dynastore.tools.discovery import get_protocols
            from dynastore.models.protocols.enrichment import CollectionMetadataEnricherProtocol

            enrichers = sorted(
                get_protocols(CollectionMetadataEnricherProtocol),
                key=lambda e: e.priority,
            )
            for enricher in enrichers:
                try:
                    if enricher.can_enrich(catalog_id, collection_id):
                        data = await enricher.enrich(catalog_id, collection_id, data, context={})
                except Exception as _enrich_err:
                    logger.warning(
                        "CollectionMetadataEnricher '%s' failed for %s/%s: %s",
                        getattr(enricher, "enricher_id", repr(enricher)),
                        catalog_id,
                        collection_id,
                        _enrich_err,
                    )
        except Exception:
            pass  # discovery failure must not break the read path

        return Collection.model_validate(data)

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        """Retrieves a collection by ID, localized."""
        db_resource = ctx.db_resource if ctx else None
        collection_model = await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not collection_model:
            return None

        # Localize
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.localization import LocalizationProtocol
        loc = get_protocol(LocalizationProtocol)
        if loc:
            collection_model = loc.localize_model(collection_model, lang)
        return collection_model

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None
    ) -> CollectionPluginConfig:
        """Retrieves the active driver config for a collection (via routing)."""
        db_resource = ctx.db_resource if ctx else None
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        return await driver.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection_model(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Collection]:
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                return await self._get_collection_model_logic(
                    catalog_id, collection_id, conn
                )
        return await self._get_collection_model_cached(catalog_id, collection_id)

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        db_resource = ctx.db_resource if ctx else None
        phys_schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not phys_schema or not phys_table:
            return set()

        from dynastore.modules.db_config.shared_queries import get_table_column_names

        async def _execute(conn):
            return await get_table_column_names(conn, phys_schema, phys_table)

        if db_resource:
            return await _execute(db_resource)
        assert self.engine is not None, "engine required"
        from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine
        assert isinstance(self.engine, _AsyncEngine), "engine must be AsyncEngine for get_collection_column_names"
        async with self.engine.connect() as conn:
            return await _execute(conn)

    async def ensure_collection_exists(
        self,
        db_resource: DbResource,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
    ) -> None:
        if not await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            # If lang is not '*', we provide a simple string which create_collection will localize
            # If lang is '*', we provide the default 'en' dictionary
            title = {"en": collection_id} if lang == "*" else collection_id
            await self.create_collection(
                catalog_id,
                {"id": collection_id, "title": title},
                lang=lang,
                db_resource=db_resource,
            )

    async def create_collection(
        self,
        catalog_id: str,
        collection_definition: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> Collection:
        db_resource = ctx.db_resource if ctx else None
        if isinstance(collection_definition, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(collection_definition, lang)

        collection_model = (
            Collection.create_from_localized_input(collection_definition, lang)
            if isinstance(collection_definition, dict)
            else collection_definition
        )
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_model.id)

        async with managed_transaction(db_resource or self.engine) as conn:
            # Check catalog exists
            catalogs = get_protocol(CatalogsProtocol)
            assert catalogs is not None, "CatalogsProtocol not registered"
            if not await catalogs.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn)):
                raise ValueError(f"Catalog '{catalog_id}' does not exist.")

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                raise ValueError(f"No physical schema found for catalog '{catalog_id}'")

            logger.info(
                f"[LIFECYCLE] Creating collection '{catalog_id}:{collection_model.id}' in schema '{phys_schema}'"
            )

            # Get driver config (default/platform config only - collection doesn't exist yet,
            # so we must NOT pass db_resource here; querying collection_configs in a nested
            # transaction before the table may be ready would poison the outer transaction).
            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _meta_driver = await _get_driver(Operation.READ, catalog_id, collection_model.id)
            collection_config = await _meta_driver.get_driver_config(
                catalog_id, collection_model.id,
            )

            # Layer config override from input
            layer_config_override = None
            if isinstance(collection_definition, dict):
                layer_config_override = collection_definition.get("layer_config")
                if not layer_config_override and "sidecars" in collection_definition:
                    layer_config_override = {
                        "sidecars": collection_definition["sidecars"]
                    }
            else:
                if hasattr(collection_definition, "layer_config"):
                    layer_config_override = getattr(collection_definition, "layer_config")

                # Check for sidecars in Pydantic model (extra fields)
                if not layer_config_override and hasattr(
                    collection_definition, "sidecars"
                ):
                    # We wrap it in a dict to be compatible with CollectionPluginConfig input
                    layer_config_override = {"sidecars": getattr(collection_definition, "sidecars")}

            # Layer config override from input
            if layer_config_override and isinstance(layer_config_override, dict):
                from dynastore.modules.storage.driver_config import (
                    DriverRecordsPostgresqlConfig,
                )

                layer_config_override = DriverRecordsPostgresqlConfig.model_validate(
                    layer_config_override
                )

            # Registry-Based Auto-Injection
            from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

            # Aggregate context for injection (including kwargs like stac_context)
            injection_context = dict(kwargs)
            # Resolve effective collection_type (override takes precedence)
            effective_config = layer_config_override or collection_config
            injection_context.update(
                {
                    "catalog_id": catalog_id,
                    "collection_id": collection_model.id,
                    "schema": phys_schema,
                    "collection_type": getattr(effective_config, "collection_type", "VECTOR"),
                }
            )

            injected_configs = SidecarRegistry.get_injected_sidecar_configs(
                injection_context
            )
            if injected_configs:
                from typing import cast as _cast
                from dynastore.modules.storage.driver_config import (
                    DriverRecordsPostgresqlConfig,
                )
                if layer_config_override is None:
                    if isinstance(collection_config, DriverRecordsPostgresqlConfig):
                        layer_config_override = collection_config.model_copy()
                    else:
                        layer_config_override = DriverRecordsPostgresqlConfig()

                pg_override = _cast(DriverRecordsPostgresqlConfig, layer_config_override)
                current_sidecars: List[Any] = list(pg_override.sidecars or [])
                current_types = {s.sidecar_type for s in current_sidecars}

                modified = False
                for sc in injected_configs:
                    if sc.sidecar_type not in current_types:
                        # Append so injected sidecars run after core geometry/attributes sidecars.
                        # Registration order in SidecarRegistry determines relative order of
                        # injected sidecars: item_metadata is registered before stac_metadata,
                        # ensuring item_metadata publishes to context first.
                        current_sidecars.append(sc)
                        current_types.add(sc.sidecar_type)
                        modified = True

                if modified:
                    pg_override = pg_override.model_copy(update={"sidecars": current_sidecars})
                    layer_config_override = pg_override
                    logger.info(
                        f"Registry: Injected sidecars for {catalog_id}:{collection_model.id}: {list(current_types)}"
                    )

            # Resolved config for this collection creation
            init_config = layer_config_override or collection_config

            # Clean kwargs to avoid multiple values for arguments already passed positionally or explicitly
            init_kwargs = kwargs.copy()
            init_kwargs.pop("physical_table", None)
            init_kwargs.pop("layer_config", None)

            # 3. Insert thin registry row (id + catalog_id only)
            insert_sql = f"""
                INSERT INTO "{phys_schema}".collections (id, catalog_id)
                VALUES (:id, :catalog_id)
                RETURNING id;
            """
            await DQLQuery(insert_sql, result_handler=ResultHandler.SCALAR_ONE).execute(
                conn, id=collection_model.id, catalog_id=catalog_id,
            )

            # 4. Run infrastructure hooks (events partition, logs, proxy —
            #    hub/sidecars are handled by write_driver.ensure_storage() below).
            await lifecycle_registry.init_collection(
                conn,
                phys_schema,
                catalog_id,
                collection_model.id,
                layer_config=init_config,
                **init_kwargs,
            )

            # 5. Persist initial driver config (sidecars etc.) BEFORE ensure_storage.
            #    ensure_storage later saves the same config with physical_table set
            #    (check_immutability=False), which is allowed because WriteOnce starts
            #    at None here and is updated to a real value there.
            if layer_config_override:
                from dynastore.modules.db_config.platform_config_service import PluginConfig as _PluginConfig
                if isinstance(layer_config_override, _PluginConfig) and layer_config_override._plugin_id is not None:
                    configs = get_protocol(ConfigsProtocol)
                    assert configs is not None, "ConfigsProtocol not registered"
                    await configs.set_config(
                        layer_config_override._plugin_id,
                        layer_config_override,
                        catalog_id=catalog_id,
                        collection_id=collection_model.id,
                        ctx=DriverContext(db_resource=conn),
                    )

            # 6. Call write driver's ensure_storage() — creates hub + sidecar tables
            #    and pins physical_table in driver config (check_immutability=False).
            #    Skipped gracefully when no storage drivers are registered (e.g. tests
            #    that don't load StorageModule; PG-native tables are handled separately).
            from dynastore.modules.storage.router import get_driver
            try:
                write_driver = await get_driver(
                    "WRITE", catalog_id, collection_model.id
                )
                await write_driver.ensure_storage(
                    catalog_id,
                    collection_model.id,
                    db_resource=conn,
                    col_config=init_config,
                )
            except ValueError:
                pass

            # 6b. Pin the resolved routing config at collection level so future
            #     platform default changes don't silently re-route existing collections.
            try:
                from dynastore.modules.storage.routing_config import ROUTING_PLUGIN_CONFIG_ID, RoutingPluginConfig
                configs = get_protocol(ConfigsProtocol)
                if configs is None:
                    raise ValueError("ConfigsProtocol not registered")
                resolved_routing = await configs.get_config(
                    RoutingPluginConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )
                if resolved_routing:
                    await configs.set_config(
                        ROUTING_PLUGIN_CONFIG_ID,
                        resolved_routing,
                        catalog_id=catalog_id,
                        collection_id=collection_model.id,
                        ctx=DriverContext(db_resource=conn),
                    )
            except Exception as _routing_e:
                logger.warning(
                    "create_collection: failed to pin routing config for %s/%s: %s",
                    catalog_id, collection_model.id, _routing_e,
                )

            # 7. Store collection metadata — always write to PG metadata driver
            #    (authoritative), then sync to non-PG metadata driver if configured.
            metadata_payload = collection_model.model_dump(
                by_alias=True, exclude_none=True
            )
            from dynastore.modules.storage.drivers.metadata_postgresql import DriverMetadataPostgresql
            from dynastore.modules.catalog.metadata_router import get_metadata_driver

            pg_meta = DriverMetadataPostgresql()
            await pg_meta.upsert_metadata(
                catalog_id, collection_model.id, metadata_payload, db_resource=conn,
            )
            # Sync to non-PG metadata driver (e.g. ES) if configured
            try:
                meta_driver = await get_metadata_driver(catalog_id)
                if meta_driver is not None and not isinstance(meta_driver, DriverMetadataPostgresql):
                    await meta_driver.upsert_metadata(
                        catalog_id, collection_model.id, metadata_payload,
                    )
            except Exception as _meta_e:
                logger.warning(
                    "create_collection: metadata driver sync failed for %s/%s: %s",
                    catalog_id, collection_model.id, _meta_e,
                )

            # 8. Persist write_policy if provided in the collection definition.
            write_policy_input = None
            if isinstance(collection_definition, dict):
                write_policy_input = collection_definition.get("write_policy")
            elif hasattr(collection_definition, "write_policy"):
                write_policy_input = getattr(collection_definition, "write_policy", None)
            if write_policy_input:
                from dynastore.modules.storage.driver_config import (
                    CollectionWritePolicy,
                    WRITE_POLICY_PLUGIN_ID,
                )
                policy = (
                    CollectionWritePolicy.model_validate(write_policy_input)
                    if isinstance(write_policy_input, dict)
                    else write_policy_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    WRITE_POLICY_PLUGIN_ID,
                    policy,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )

            # 9. Persist feature_type if provided in the collection definition.
            feature_type_input = None
            if isinstance(collection_definition, dict):
                feature_type_input = collection_definition.get("feature_type")
            elif hasattr(collection_definition, "feature_type"):
                feature_type_input = getattr(collection_definition, "feature_type", None)
            if feature_type_input:
                from dynastore.modules.storage.driver_config import (
                    FeatureTypePluginConfig,
                    FEATURE_TYPE_PLUGIN_ID,
                )
                ft_def = (
                    FeatureTypePluginConfig.model_validate(feature_type_input)
                    if isinstance(feature_type_input, dict)
                    else feature_type_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    FEATURE_TYPE_PLUGIN_ID,
                    ft_def,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )

        # Resolve physical_table for async lifecycle context (PG driver only; None for others).
        # Use db_resource when available so uncommitted catalog/collection rows are visible.
        # Fall back to None gracefully if the table hasn't been registered yet.
        try:
            physical_table = await self.resolve_physical_table(
                catalog_id, collection_model.id, db_resource=db_resource
            )
        except (ValueError, Exception):
            physical_table = None

        # Invalidate caches
        self._get_collection_model_cached.cache_invalidate(
            catalog_id, collection_model.id
        )

        # Trigger async lifecycle
        config_snapshot = {}
        try:
            _cfg = get_protocol(ConfigsProtocol)
            if _cfg is not None:
                config_snapshot.update(await _cfg.list_catalog_configs(catalog_id))
        except Exception:
            pass

        lifecycle_registry.init_async_collection(
            catalog_id,
            collection_model.id,
            LifecycleContext(
                physical_schema=phys_schema,
                physical_table=physical_table,
                config=config_snapshot,
            ),
        )

        # Emit signal to wake up background tasks (Visibility Gap fix)
        await signal_bus.emit(
            "AFTER_COLLECTION_CREATION", identifier=collection_model.id
        )

        result = await self.get_collection_model(
            catalog_id, collection_model.id, db_resource=db_resource
        )
        assert result is not None, f"Collection '{collection_model.id}' not found after creation"
        return result

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
    ) -> List[Collection]:
        db_resource = ctx.db_resource if ctx else None
        # Delegate to metadata driver (ES preferred, PG fallback)
        from dynastore.modules.catalog.metadata_router import get_metadata_driver

        meta_driver = await get_metadata_driver(catalog_id)
        if meta_driver is not None:
            try:
                rows, _total = await meta_driver.search_metadata(
                    catalog_id,
                    q=q,
                    limit=limit,
                    offset=offset,
                    db_resource=db_resource,
                )
                return [Collection.model_validate(row) for row in rows]
            except Exception as e:
                logger.warning(
                    "Metadata driver search failed for %s, falling back to PG: %s",
                    catalog_id, e,
                )

        # PG direct fallback
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return []

            if not q:
                query_sql = (
                    f'SELECT c.id, m.title, m.description, m.keywords, m.license, '
                    f'm.links, m.assets, m.extent, m.providers, m.summaries, '
                    f'm.item_assets, m.extra_metadata '
                    f'FROM "{phys_schema}".collections c '
                    f'LEFT JOIN "{phys_schema}".metadata m ON m.collection_id = c.id '
                    f'WHERE c.deleted_at IS NULL '
                    f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
                )
                result_rows = await DQLQuery(
                    query_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, limit=limit, offset=offset)
            else:
                query_sql = (
                    f'SELECT c.id, m.title, m.description, m.keywords, m.license, '
                    f'm.links, m.assets, m.extent, m.providers, m.summaries, '
                    f'm.item_assets, m.extra_metadata '
                    f'FROM "{phys_schema}".collections c '
                    f'LEFT JOIN "{phys_schema}".metadata m ON m.collection_id = c.id '
                    f'WHERE c.deleted_at IS NULL '
                    f"AND (c.id ILIKE :q OR m.title->>'en' ILIKE :q OR m.description->>'en' ILIKE :q) "
                    f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
                )
                result_rows = await DQLQuery(
                    query_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, limit=limit, offset=offset, q=f"%{q}%")

            results = []
            for row_dict in result_rows:
                for key in ["title", "description", "keywords", "license", "links",
                            "assets", "extent", "providers", "summaries", "item_assets",
                            "extra_metadata"]:
                    val = row_dict.get(key)
                    if isinstance(val, str):
                        try:
                            row_dict[key] = json.loads(val)
                        except Exception:
                            row_dict[key] = None

                data = {
                    "id": row_dict["id"],
                    "title": row_dict.get("title"),
                    "description": row_dict.get("description"),
                    "keywords": row_dict.get("keywords"),
                    "license": row_dict.get("license"),
                    "links": row_dict.get("links"),
                    "assets": row_dict.get("assets"),
                    "extent": row_dict.get("extent"),
                    "providers": row_dict.get("providers"),
                    "summaries": row_dict.get("summaries"),
                    "item_assets": row_dict.get("item_assets"),
                    "extra_metadata": row_dict.get("extra_metadata"),
                }
                results.append(Collection.model_validate(data))
            return results

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Dict[str, Any],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        from dynastore.models.localization import validate_language_consistency

        validate_language_consistency(updates, lang)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return None

            existing_model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not existing_model:
                return None

            merged_model = existing_model.merge_localized_updates(updates, lang)

            # Store only user-provided extra_metadata content (no envelope)
            user_extra_metadata = (
                json.dumps(
                    merged_model.extra_metadata.model_dump(exclude_none=True),
                    cls=CustomJSONEncoder,
                )
                if merged_model.extra_metadata
                else None
            )

            # First verify the collection exists in thin registry
            exists_sql = f'SELECT id FROM "{phys_schema}".collections WHERE id = :id AND deleted_at IS NULL;'
            exists = await DQLQuery(
                exists_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(conn, id=collection_id)
            if not exists:
                return None

            # Upsert metadata via PG metadata driver (authoritative)
            from dynastore.modules.storage.drivers.metadata_postgresql import DriverMetadataPostgresql
            from dynastore.modules.catalog.metadata_router import get_metadata_driver

            metadata_payload = merged_model.model_dump(
                by_alias=True, exclude_none=True
            )
            pg_meta = DriverMetadataPostgresql()
            await pg_meta.upsert_metadata(
                catalog_id, collection_id, metadata_payload, db_resource=conn,
            )

            self._get_collection_model_cached.cache_invalidate(
                catalog_id, collection_id
            )

            # Sync to non-PG metadata driver (e.g. ES) if configured
            try:
                meta_driver = await get_metadata_driver(catalog_id)
                if meta_driver is not None and not isinstance(meta_driver, DriverMetadataPostgresql):
                    await meta_driver.upsert_metadata(
                        catalog_id, collection_id, metadata_payload,
                    )
            except Exception as _meta_e:
                logger.warning(
                    "update_collection: metadata driver sync failed for %s/%s: %s",
                    catalog_id, collection_id, _meta_e,
                )

            return await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            soft_delete_sql = f'UPDATE "{phys_schema}".collections SET deleted_at = NOW() WHERE id = :id AND deleted_at IS NULL;'
            await DQLQuery(
                soft_delete_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, id=collection_id)

            logger.info(
                f"[LIFECYCLE] Soft deleted collection '{catalog_id}:{collection_id}'"
            )

            config_snapshot = {}
            if force:
                logger.info(
                    f"[LIFECYCLE] Hard deleting collection '{catalog_id}:{collection_id}'"
                )
                phys_table = await self.resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )

                try:
                    configs = get_protocol(ConfigsProtocol)
                    config_snapshot: Dict[str, Any] = {
                        "catalog_id": catalog_id,
                        "collection_id": collection_id,
                    }
                    if configs is not None:
                        coll_config = await configs.get_config(
                            COLLECTION_PLUGIN_CONFIG_ID,
                            catalog_id,
                            collection_id,
                            ctx=DriverContext(db_resource=conn),
                        )
                        if coll_config:
                            config_snapshot["collection_config"] = coll_config.model_dump()
                except Exception as e:
                    logger.warning(
                        f"Failed to capture config snapshot for '{catalog_id}:{collection_id}: {e}"
                    )

                await lifecycle_registry.destroy_collection(
                    conn, phys_schema, catalog_id, collection_id
                )
                await lifecycle_registry.hard_destroy_collection(
                    conn, phys_schema, catalog_id, collection_id
                )

                if phys_table:
                    await shared_queries.delete_table_query.execute(
                        conn, schema=phys_schema, table=phys_table
                    )

                hard_delete_sql = (
                    f'DELETE FROM "{phys_schema}".collections WHERE id = :id;'
                )
                await DDLQuery(hard_delete_sql).execute(conn, id=collection_id)
                # Clean up metadata via PG metadata driver + driver config
                from dynastore.modules.storage.drivers.metadata_postgresql import DriverMetadataPostgresql
                from dynastore.modules.catalog.metadata_router import get_metadata_driver

                pg_meta = DriverMetadataPostgresql()
                await pg_meta.delete_metadata(
                    catalog_id, collection_id, db_resource=conn,
                )
                # Also delete from non-PG metadata driver if configured
                try:
                    meta_driver = await get_metadata_driver(catalog_id)
                    if meta_driver is not None and not isinstance(meta_driver, DriverMetadataPostgresql):
                        await meta_driver.delete_metadata(catalog_id, collection_id)
                except Exception as _meta_e:
                    logger.warning(
                        "delete_collection: metadata driver cleanup failed for %s/%s: %s",
                        catalog_id, collection_id, _meta_e,
                    )
                await DDLQuery(
                    f'DELETE FROM "{phys_schema}".collection_configs WHERE collection_id = :id;'
                ).execute(conn, id=collection_id)

                logger.info(
                    f"[LIFECYCLE] Hard deleted collection '{catalog_id}:{collection_id}' successfully"
                )

        self._get_collection_model_cached.cache_invalidate(catalog_id, collection_id)

        if force and phys_schema:
            lifecycle_registry.destroy_async_collection(
                catalog_id,
                collection_id,
                LifecycleContext(
                    physical_schema=phys_schema,
                    physical_table=phys_table,
                    config=config_snapshot,
                ),
            )

        return True

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Deletes a specific language variant from a collection."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not model:
                raise ValueError(
                    f"Collection '{catalog_id}:{collection_id}' not found."
                )

            can_delete = False
            fields_to_update = {}

            # Localizable fields in collections table are similar to catalogs
            for field in [
                "title",
                "description",
                "keywords",
                "license",
                "extra_metadata",
            ]:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(
                                f"Cannot delete language '{lang}' from field '{field}': it is the only language available."
                            )

                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            fields_to_update[field] = json.dumps(
                                data, cls=CustomJSONEncoder
                            )
                            can_delete = True

            params = {"id": collection_id, **fields_to_update}

            if not can_delete:
                return False

            set_clauses = [f"{k} = :{k}" for k in fields_to_update.keys()]
            sql = f'UPDATE "{phys_schema}".metadata SET {", ".join(set_clauses)} WHERE collection_id = :id;'
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, **params
            )

            self._get_collection_model_cached.cache_invalidate(
                catalog_id, collection_id
            )
            return True


