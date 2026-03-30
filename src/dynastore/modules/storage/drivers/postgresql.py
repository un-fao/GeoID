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
PostgreSQL Storage Driver — wraps existing ``ItemsProtocol`` services.

Zero SQL rewrite.  All complex PG logic (sidecars, query optimizer,
ON CONFLICT, RETURNING, PostGIS) stays exactly where it is.  This driver
is a thin adapter that maps ``CollectionStorageDriverProtocol``
to the existing PG-based service layer.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.location import PostgresStorageLocationConfig

logger = logging.getLogger(__name__)


class PostgresStorageDriver(ModuleProtocol):
    """PostgreSQL storage driver — delegates to existing ItemsProtocol.

    Satisfies ``CollectionStorageDriverProtocol`` by wrapping the existing
    PG-based item services, preserving all sidecar logic, query
    optimization, and streaming.
    """

    driver_id: str = "postgresql"
    priority: int = 10
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SPATIAL_FILTER,
        Capability.SORT,
        Capability.GROUP_BY,
        Capability.SOFT_DELETE,
        Capability.EXPORT,
        Capability.GEOSPATIAL,
        Capability.STATISTICS,
        Capability.SPATIAL_INDEX,
        Capability.ASSET_TRACKING,
        Capability.ATTRIBUTE_FILTER,
    })
    preferred_for: FrozenSet[str] = frozenset({"features", "write"})

    def is_available(self) -> bool:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.items import ItemsProtocol

        return get_protocol(ItemsProtocol) is not None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("PostgresStorageDriver: started (wraps existing ItemsProtocol)")
        yield
        logger.info("PostgresStorageDriver: stopped")

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        items_svc = self._get_crud_protocol()
        result = await items_svc.upsert(
            catalog_id, collection_id, entities, db_resource=db_resource
        )
        if isinstance(result, list):
            return result
        return [result]

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        if request is None:
            request = QueryRequest(limit=limit, offset=offset, item_ids=entity_ids)
        else:
            if entity_ids is not None and request.item_ids is None:
                request = request.model_copy(update={"item_ids": entity_ids})
            if request.limit is None:
                request = request.model_copy(update={"limit": limit})
            if request.offset is None:
                request = request.model_copy(update={"offset": offset})

        query_svc = self._get_query_protocol()
        response = await query_svc.stream_items(
            catalog_id, collection_id, request, db_resource=db_resource
        )

        async for feature in response.items:
            yield feature

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "PostgresStorageDriver: soft delete for individual entities "
                "is not yet implemented. Use drop_storage(soft=True) for "
                "collection-level soft deletion."
            )
        items_svc = self._get_crud_protocol()
        total = 0
        for eid in entity_ids:
            total += await items_svc.delete_item(
                catalog_id, collection_id, eid, db_resource=db_resource
            )
        return total

    async def _resolve_schema(self, catalog_id: str, db_resource=None) -> str:
        """Resolve the PG schema name for a catalog."""
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")
        schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)
        if not schema:
            raise ValueError(f"No physical schema found for catalog '{catalog_id}'")
        return schema

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[str]:
        """Resolve physical table name from pg_storage_locations."""
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        query_sql = f'SELECT physical_table FROM "{schema}".pg_storage_locations WHERE collection_id = :collection_id;'

        async def _query(conn):
            return await DQLQuery(
                query_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
            ).execute(conn, collection_id=collection_id)

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource=None,
        col_config=None,
    ) -> None:
        """Create PG hub table + sidecar tables for a collection.

        Creates ``pg_storage_locations`` and ``pg_collection_metadata``
        if they don't already exist, generates a unique ``physical_table``
        name, creates the hub table, creates sidecar tables, and registers
        the mapping in ``pg_storage_locations``.

        If ``collection_id`` is None, this is a no-op (catalog-level call).
        """
        if not collection_id:
            return

        from dynastore.modules.db_config.query_executor import (
            DDLQuery, DQLQuery, ResultHandler, managed_transaction, managed_nested_transaction,
        )
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.catalog.catalog_config import (
            CollectionPluginConfig,
            COLLECTION_PLUGIN_CONFIG_ID,
        )
        from dynastore.modules.catalog.catalog_service import generate_physical_name
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
        from dynastore.models.protocols.assets import AssetsProtocol

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)

        # Resolve col_config if not provided
        if col_config is None:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                col_config = await configs.get_config(
                    COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id
                )
        if col_config is None:
            col_config = CollectionPluginConfig()

        # --- Generate physical table name ---
        physical_table = generate_physical_name("t")

        # --- Partition context ---
        partition_keys = []
        partition_key_types = {
            "transaction_time": "TIMESTAMPTZ",
            "validity": "TSTZRANGE",
            "geoid": "UUID",
            "asset_id": "VARCHAR(255)",
        }

        if col_config.partitioning.enabled:
            partition_keys = col_config.partitioning.partition_keys
            for sc_config in col_config.sidecars:
                partition_key_types.update(sc_config.partition_key_types)

        # --- Build hub table DDL ---
        hub_cols_map = col_config.get_column_definitions()
        for key in partition_keys:
            if key not in hub_cols_map:
                col_type = partition_key_types.get(key, "TEXT")
                hub_cols_map[key] = f"{col_type} NOT NULL"

        has_validity = "validity" in partition_keys

        hub_columns_ddl = []
        for name, spec in hub_cols_map.items():
            clean_spec = spec.replace(" PRIMARY KEY", "")
            hub_columns_ddl.append(f'"{name}" {clean_spec}')

        pk_hub = ["geoid"]
        if has_validity:
            pk_hub.append("validity")
        pk_all = list(set(pk_hub) | set(partition_keys)) if partition_keys else list(pk_hub)
        quoted_pk = ", ".join(f'"{c}"' for c in pk_all)
        hub_columns_ddl.append(f"PRIMARY KEY ({quoted_pk})")

        partition_clause = ""
        if partition_keys:
            quoted_pk_keys = ", ".join(f'"{k}"' for k in partition_keys)
            partition_clause = f" PARTITION BY LIST ({quoted_pk_keys})"

        create_hub_sql = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{physical_table}" '
            f'({", ".join(hub_columns_ddl)}){partition_clause};'
        )
        await DDLQuery(create_hub_sql).execute(db_resource)

        # --- Create sidecar tables ---
        for sidecar_config in col_config.sidecars:
            try:
                sidecar_impl = SidecarRegistry.get_sidecar(sidecar_config)
                sc_has_validity = sidecar_impl.has_validity()
                ddl_statements = sidecar_impl.get_ddl(
                    physical_table=physical_table,
                    partition_keys=partition_keys,
                    partition_key_types=partition_key_types,
                    has_validity=sc_has_validity,
                )
                await DDLQuery(ddl_statements).execute(db_resource, schema=schema)
                await sidecar_impl.setup_lifecycle_hooks(
                    db_resource, schema, f"{physical_table}_{sidecar_impl.sidecar_id}"
                )
            except ValueError as e:
                logger.warning("Skipping sidecar table creation: %s", e)

        # --- Register physical table mapping ---
        insert_loc_sql = f"""
            INSERT INTO "{schema}".pg_storage_locations (collection_id, physical_table)
            VALUES (:collection_id, :physical_table)
            ON CONFLICT (collection_id) DO UPDATE SET physical_table = EXCLUDED.physical_table;
        """
        await DQLQuery(
            insert_loc_sql, result_handler=ResultHandler.NONE
        ).execute(db_resource, collection_id=collection_id, physical_table=physical_table)

        # --- Store schema hash ---
        try:
            import hashlib
            import json as _json
            config_dump = col_config.model_dump() if hasattr(col_config, "model_dump") else {}
            schema_hash = hashlib.sha256(
                _json.dumps(config_dump, sort_keys=True, default=str).encode()
            ).hexdigest()
            async with managed_nested_transaction(db_resource):
                await DQLQuery(
                    f'UPDATE "{schema}".collection_configs '
                    f'SET schema_hash = :schema_hash, updated_at = NOW() '
                    f'WHERE catalog_id = :catalog_id AND collection_id = :collection_id '
                    f"AND plugin_id = 'collection';",
                    result_handler=ResultHandler.NONE,
                ).execute(
                    db_resource,
                    schema_hash=schema_hash,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
        except Exception as e:
            logger.warning("Failed to store schema hash: %s", e)

        # --- Ensure asset cleanup trigger ---
        am = get_protocol(AssetsProtocol)
        if am:
            await am.ensure_asset_cleanup_trigger(schema, physical_table, db_resource=db_resource)

        logger.info(
            "PostgresStorageDriver.ensure_storage: created hub '%s' + sidecars for %s/%s",
            physical_table, catalog_id, collection_id,
        )

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata from pg_collection_metadata."""
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        import json

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        query_sql = f'SELECT * FROM "{schema}".pg_collection_metadata WHERE collection_id = :collection_id;'

        async def _query(conn):
            row = await DQLQuery(
                query_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, collection_id=collection_id)
            if not row:
                return None
            # Deserialize JSONB columns stored as strings
            for key in ["title", "description", "keywords", "license", "extent",
                        "providers", "summaries", "links", "assets", "item_assets",
                        "extra_metadata", "stac_extensions"]:
                val = row.get(key)
                if isinstance(val, str):
                    try:
                        row[key] = json.loads(val)
                    except Exception:
                        row[key] = None
            row.pop("collection_id", None)
            return row

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def set_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource=None,
    ) -> None:
        """Upsert collection metadata into pg_collection_metadata."""
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
        import json

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)

        def _serialize(val) -> Optional[str]:
            if val is None:
                return None
            if isinstance(val, str):
                return val
            return json.dumps(val, default=str)

        upsert_sql = f"""
            INSERT INTO "{schema}".pg_collection_metadata
                (collection_id, title, description, keywords, license,
                 extent, providers, summaries, links, assets, item_assets,
                 extra_metadata)
            VALUES
                (:collection_id, :title, :description, :keywords, :license,
                 :extent, :providers, :summaries, :links, :assets, :item_assets,
                 :extra_metadata)
            ON CONFLICT (collection_id) DO UPDATE SET
                title        = EXCLUDED.title,
                description  = EXCLUDED.description,
                keywords     = EXCLUDED.keywords,
                license      = EXCLUDED.license,
                extent       = EXCLUDED.extent,
                providers    = EXCLUDED.providers,
                summaries    = EXCLUDED.summaries,
                links        = EXCLUDED.links,
                assets       = EXCLUDED.assets,
                item_assets  = EXCLUDED.item_assets,
                extra_metadata = EXCLUDED.extra_metadata;
        """
        from dynastore.tools.json import CustomJSONEncoder

        def _to_json(val) -> Optional[str]:
            if val is None:
                return None
            if isinstance(val, str):
                return val
            return json.dumps(val, cls=CustomJSONEncoder)

        await DQLQuery(upsert_sql, result_handler=ResultHandler.NONE).execute(
            db_resource,
            collection_id=collection_id,
            title=_to_json(metadata.get("title")),
            description=_to_json(metadata.get("description")),
            keywords=_to_json(metadata.get("keywords")),
            license=_to_json(metadata.get("license")),
            extent=_to_json(metadata.get("extent")),
            providers=_to_json(metadata.get("providers")),
            summaries=_to_json(metadata.get("summaries")),
            links=_to_json(metadata.get("links")),
            assets=_to_json(metadata.get("assets")),
            item_assets=_to_json(metadata.get("item_assets")),
            extra_metadata=_to_json(metadata.get("extra_metadata")),
        )

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")

        if soft:
            logger.info(
                "PostgresStorageDriver.drop_storage(soft=True): "
                "catalog=%s collection=%s — marking as deleted via deleted_at",
                catalog_id, collection_id,
            )

        if collection_id:
            await catalogs.delete_collection(catalog_id, collection_id)
        else:
            await catalogs.delete_catalog(catalog_id)

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "PostgresStorageDriver.export_entities: use ExportFeaturesTask "
            "for async export via the task runner system."
        )

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> PostgresStorageLocationConfig:
        """Resolve PG storage coordinates via existing protocol methods."""
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.collections import CollectionsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")

        schema = await catalogs.resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        table = None
        if collection_id:
            collections = get_protocol(CollectionsProtocol)
            if collections:
                table = await collections.resolve_physical_table(
                    catalog_id, collection_id, db_resource=db_resource
                )

        return PostgresStorageLocationConfig(
            physical_schema=schema,
            physical_table=table,
        )

    # --- Internal helpers ---

    def _get_crud_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_crud import ItemCrudProtocol

        svc = get_protocol(ItemCrudProtocol)
        if not svc:
            raise RuntimeError("ItemCrudProtocol not available")
        return svc

    def _get_query_protocol(self):
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.item_query import ItemQueryProtocol

        svc = get_protocol(ItemQueryProtocol)
        if not svc:
            raise RuntimeError("ItemQueryProtocol not available")
        return svc
