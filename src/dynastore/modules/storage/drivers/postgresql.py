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
is a thin adapter that maps ``CollectionItemsStore``
to the existing PG-based service layer.
"""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import CollectionPostgresqlDriverConfig

logger = logging.getLogger(__name__)


class CollectionPostgresqlDriver(ModuleProtocol):
    """PostgreSQL storage driver — delegates to existing ItemsProtocol.

    Satisfies ``CollectionItemsStore`` by wrapping the existing
    PG-based item services, preserving all sidecar logic, query
    optimization, and streaming.
    """

    priority: int = 10
    preferred_chunk_size: int = 0
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
        Capability.INTROSPECTION,
        Capability.COUNT,
        Capability.AGGREGATION,
        Capability.REQUIRED_ENFORCEMENT,
        Capability.UNIQUE_ENFORCEMENT,
        Capability.PHYSICAL_ADDRESSING,
        Capability.SOFT_DELETE_ATOMIC,
        Capability.QUERY_FALLBACK_SOURCE,
        Capability.BULK_COPY,
    })
    preferred_for: FrozenSet[str] = frozenset({"features", "write"})
    supported_hints: FrozenSet[str] = frozenset({"features", "write", "metadata"})

    def is_available(self) -> bool:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.items import ItemsProtocol

        return get_protocol(ItemsProtocol) is not None

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> "CollectionPostgresqlDriverConfig":
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.storage.driver_config import CollectionPostgresqlDriverConfig

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return CollectionPostgresqlDriverConfig()
        config = await configs.get_config(
            CollectionPostgresqlDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        if not isinstance(config, CollectionPostgresqlDriverConfig):
            return CollectionPostgresqlDriverConfig()
        return config

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        logger.info("CollectionPostgresqlDriver: started (wraps existing ItemsProtocol)")
        yield
        logger.info("CollectionPostgresqlDriver: stopped")

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
            catalog_id, collection_id, entities, ctx=DriverContext(db_resource=db_resource) if db_resource else None
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
            catalog_id, collection_id, request, ctx=DriverContext(db_resource=db_resource) if db_resource else None
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
                "CollectionPostgresqlDriver: soft delete for individual entities "
                "is not yet implemented. Use drop_storage(soft=True) for "
                "collection-level soft deletion."
            )
        items_svc = self._get_crud_protocol()
        total = 0
        for eid in entity_ids:
            total += await items_svc.delete_item(
                catalog_id, collection_id, eid, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )
        return total

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        raise SoftDeleteNotSupportedError(
            "CollectionPostgresqlDriver: restore_entities not yet implemented."
        )

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Rename PG physical table and update driver config."""
        from dynastore.modules.db_config.query_executor import DDLQuery, managed_transaction

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        old_table = await self.resolve_physical_table(
            catalog_id, old_collection_id, db_resource=db_resource
        )
        if not old_table:
            raise ValueError(
                f"No physical table found for {catalog_id}:{old_collection_id}"
            )

        new_table = old_table  # Keep same physical table name, just update config mapping
        await self.set_physical_table(
            catalog_id, new_collection_id, new_table, db_resource=db_resource
        )

    async def _resolve_schema(self, catalog_id: str, db_resource=None) -> str:
        """Resolve the PG schema name for a catalog."""
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            raise RuntimeError("CatalogsProtocol not available")
        schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=db_resource))
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
        """Resolve physical table name from driver config."""
        config = await self.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        return config.physical_table

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        *,
        db_resource=None,
    ) -> None:
        """Store physical table name in driver config."""
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        from dynastore.modules.storage.driver_config import CollectionPostgresqlDriverConfig

        config = await self.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        updated_config = config.model_copy(update={"physical_table": physical_table})
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return
        await configs.set_config(
            CollectionPostgresqlDriverConfig,
            updated_config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            check_immutability=False,
            ctx=DriverContext(db_resource=db_resource),
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Create PG hub table + sidecar tables for a collection.

        Generates a unique ``physical_table`` name, creates the hub table,
        creates sidecar tables, and stores the mapping in the driver config.

        If ``collection_id`` is None, this is a no-op (catalog-level call).

        PG-specific kwargs:
            physical_table: Optional explicit table name. If not provided,
                one is generated automatically.
            layer_config: Optional config overlay merged on top of the
                resolved ``CollectionPostgresqlDriverConfig`` before creating storage.
        """
        if not collection_id:
            return

        db_resource = kwargs.get("db_resource")
        col_config = kwargs.get("col_config")
        physical_table = kwargs.get("physical_table")
        layer_config = kwargs.get("layer_config")
        if db_resource is None:
            raise ValueError("ensure_storage requires db_resource")

        from dynastore.modules.db_config.query_executor import (
            DDLQuery, DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            CollectionPostgresqlDriverConfig,
        )
        from dynastore.modules.catalog.catalog_service import generate_physical_name
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
        from dynastore.models.protocols.assets import AssetsProtocol

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)

        # Resolve col_config if not provided
        if col_config is None:
            col_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=db_resource,
            )

        # Apply layer_config overlay if provided
        if layer_config:
            base_dump = col_config.model_dump()
            layer_config_dict = (
                layer_config.model_dump()
                if hasattr(layer_config, "model_dump")
                else layer_config
            )

            def deep_update(d, u):
                for k, v in u.items():
                    if isinstance(v, dict):
                        d[k] = deep_update(d.get(k, {}), v)
                    else:
                        d[k] = v
                return d

            merged = deep_update(base_dump, layer_config_dict)
            try:
                col_config = CollectionPostgresqlDriverConfig.model_validate(merged)
            except Exception as e:
                logger.error(
                    "Failed to merge layer_config for %s:%s: %s",
                    catalog_id, collection_id, e,
                )

        # --- Generate physical table name if not provided ---
        # Prefer the name already stored in col_config (idempotent re-runs);
        # only generate a new name if the collection is truly new.
        if not physical_table and col_config and col_config.physical_table:
            physical_table = col_config.physical_table
        if not physical_table:
            physical_table = generate_physical_name("t")

        # --- Partition context ---
        partition_keys = []
        partition_key_types = {
            "transaction_time": "TIMESTAMPTZ",
            "validity": "TSTZRANGE",
            "geoid": "UUID",
            "asset_id": "VARCHAR(255)",
        }

        # Bridge CollectionSchema.fields → attributes sidecar so that
        # required=True / unique=True materialize as NOT NULL / UNIQUE in the
        # generated DDL (COLUMNAR mode).
        try:
            from dynastore.modules.storage.driver_config import (
                CollectionSchema,
            )
            from dynastore.modules.catalog.sidecars.attributes_config import (
                FeatureAttributeSidecarConfig,
            )
            from dynastore.modules.storage.field_constraints import (
                bridge_schema_to_attribute_sidecar,
            )

            configs = get_protocol(ConfigsProtocol)
            schema_cfg = None
            if configs is not None:
                schema_cfg = await configs.get_config(
                    CollectionSchema,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=db_resource),
                )
            if schema_cfg is not None and getattr(schema_cfg, "fields", None):
                new_sidecars = []
                for sc in col_config.sidecars:
                    if isinstance(sc, FeatureAttributeSidecarConfig):
                        new_sidecars.append(
                            bridge_schema_to_attribute_sidecar(schema_cfg, sc)
                        )
                    else:
                        new_sidecars.append(sc)
                col_config = col_config.model_copy(
                    update={"sidecars": new_sidecars}
                )
        except Exception as exc:
            logger.debug(
                "schema → attribute_schema bridge skipped for %s/%s: %s",
                catalog_id, collection_id, exc,
            )

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
                if sidecar_impl is None:
                    continue
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

        # --- Store physical_table in driver config ---
        from dynastore.modules.storage.driver_config import CollectionPostgresqlDriverConfig

        configs = get_protocol(ConfigsProtocol)
        updated_config = col_config.model_copy(update={"physical_table": physical_table})
        if configs is not None:
            await configs.set_config(
                CollectionPostgresqlDriverConfig,
                updated_config,
                catalog_id=catalog_id,
                collection_id=collection_id,
                check_immutability=False,
                ctx=DriverContext(db_resource=db_resource),
            )

        # --- Ensure asset cleanup trigger ---
        am = get_protocol(AssetsProtocol)
        if am:
            await am.ensure_asset_cleanup_trigger(schema, physical_table, ctx=DriverContext(db_resource=db_resource) if db_resource is not None else None)

        logger.info(
            "CollectionPostgresqlDriver.ensure_storage: created hub '%s' + sidecars for %s/%s",
            physical_table, catalog_id, collection_id,
        )

    async def get_collection_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Read collection metadata from metadata."""
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        import json

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        query_sql = f'SELECT * FROM "{schema}".metadata WHERE collection_id = :collection_id;'

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
        """Upsert collection metadata into metadata."""
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
        import json

        if db_resource is None:
            raise ValueError("set_collection_metadata requires db_resource")
        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)

        def _serialize(val) -> Optional[str]:
            if val is None:
                return None
            if isinstance(val, str):
                return val
            return json.dumps(val, default=str)

        upsert_sql = f"""
            INSERT INTO "{schema}".metadata
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
                "CollectionPostgresqlDriver.drop_storage(soft=True): "
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
            "CollectionPostgresqlDriver.export_entities: use ExportFeaturesTask "
            "for async export via the task runner system."
        )

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return 0

        query_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE deleted_at IS NULL;'

        async def _query(conn):
            return await DQLQuery(
                query_sql, result_handler=ResultHandler.SCALAR_ONE
            ).execute(conn)

        if db_resource is not None:
            return await _query(db_resource) or 0
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return 0
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn) or 0

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Any]:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )
        from dynastore.modules.db_config import shared_queries
        from dynastore.models.protocols.field_definition import FieldDefinition as ProtocolFieldDefinition

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return []

        # Map PG types to driver-agnostic type strings
        pg_type_map = {
            "integer": "integer", "bigint": "integer", "smallint": "integer",
            "numeric": "numeric", "real": "numeric", "double precision": "numeric",
            "boolean": "boolean",
            "character varying": "string", "text": "string", "character": "string",
            "uuid": "string", "varchar": "string",
            "timestamp with time zone": "datetime", "timestamp without time zone": "datetime",
            "date": "datetime", "time with time zone": "datetime",
            "tstzrange": "datetime",
            "jsonb": "json", "json": "json",
            "ARRAY": "array",
            "USER-DEFINED": "geometry",
        }

        query_sql = """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position;
        """

        async def _query(conn):
            # Check table exists first
            exists = await shared_queries.table_exists_query.execute(
                conn, schema=schema, table=table
            )
            if not exists:
                return []

            rows = await DQLQuery(
                query_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, schema=schema, table=table)

            # Also collect fields from attribute sidecar (JSONB keys)
            col_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            fields: list[ProtocolFieldDefinition] = []
            internal_cols = {"geoid", "deleted_at", "transaction_time", "geom", "bbox_geom", "asset_id"}
            for row in (rows or []):
                col_name = row["column_name"]
                if col_name in internal_cols:
                    continue
                data_type = row.get("data_type", "unknown")
                mapped = pg_type_map.get(data_type, "unknown")
                if mapped == "geometry":
                    mapped = "geometry"
                fields.append(ProtocolFieldDefinition(name=col_name, data_type=mapped))

            # Add sidecar attribute fields if available
            if col_config and col_config.sidecars:
                attr_table = f"{table}_attributes"
                attr_exists = await shared_queries.table_exists_query.execute(
                    conn, schema=schema, table=attr_table
                )
                if attr_exists:
                    attr_rows = await DQLQuery(
                        query_sql, result_handler=ResultHandler.ALL_DICTS
                    ).execute(conn, schema=schema, table=attr_table)
                    for row in (attr_rows or []):
                        col_name = row["column_name"]
                        if col_name in internal_cols or col_name == "attributes":
                            continue
                        data_type = row.get("data_type", "unknown")
                        mapped = pg_type_map.get(data_type, "unknown")
                        fields.append(ProtocolFieldDefinition(name=col_name, data_type=mapped))

                    # Also extract keys from JSONB attributes column via sample
                    try:
                        sample_sql = f"""
                            SELECT DISTINCT jsonb_object_keys(attributes) AS key
                            FROM "{schema}"."{attr_table}"
                            WHERE attributes IS NOT NULL
                            LIMIT 1000;
                        """
                        key_rows = await DQLQuery(
                            sample_sql, result_handler=ResultHandler.ALL
                        ).execute(conn)
                        existing_names = {f.name for f in fields}
                        for key_row in (key_rows or []):
                            key_name = key_row[0]
                            if key_name not in existing_names and key_name not in internal_cols:
                                fields.append(ProtocolFieldDefinition(name=key_name, data_type="string"))
                    except Exception:
                        pass

            return fields

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return []
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return FieldDefinition dict for the given entity level.

        For ``item`` level, wraps ``QueryOptimizer.get_all_queryable_fields()``.
        For ``collection``/``catalog``/``asset`` levels, returns STAC-standard
        fields enriched with driver-specific metadata.
        """
        from dynastore.models.protocols.field_definition import (
            FieldDefinition as ProtocolFieldDefinition,
            FieldCapability,
        )
        from dynastore.modules.db_config.query_executor import managed_transaction

        async def _resolve_item_fields(conn):
            if not collection_id:
                return {}
            col_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )
            if col_config and col_config.sidecars:
                try:
                    from dynastore.modules.catalog.query_optimizer import QueryOptimizer
                    from dynastore.modules.catalog.sidecars.attributes_config import (
                        FeatureAttributeSidecarConfig,
                    )

                    optimizer = QueryOptimizer(col_config)
                    sidecar_fields = optimizer.get_all_queryable_fields()

                    # Collect NOT NULL / UNIQUE flags from the attributes sidecar
                    # so introspection round-trips what ensure_storage materialised.
                    schema_flags: Dict[str, Dict[str, bool]] = {}
                    for sc in col_config.sidecars:
                        if isinstance(sc, FeatureAttributeSidecarConfig) and sc.attribute_schema:
                            for entry in sc.attribute_schema:
                                schema_flags[entry.name] = {
                                    "required": not entry.nullable,
                                    "unique": bool(entry.unique),
                                }

                    result = {}
                    for name, fd in sidecar_fields.items():
                        flags = schema_flags.get(name, {})
                        result[name] = ProtocolFieldDefinition(
                            name=fd.name,
                            alias=fd.alias,
                            title=fd.title if isinstance(fd.title, (str, dict, type(None))) else None,
                            description=fd.description if isinstance(fd.description, (str, dict, type(None))) else None,
                            capabilities=list(fd.capabilities),
                            data_type=fd.data_type,
                            expose=fd.expose,
                            required=flags.get("required", False),
                            unique=flags.get("unique", False),
                            aggregations=fd.aggregations,
                            transformations=fd.transformations,
                        )

                    # Overlay CollectionSchema-declared flags (authoritative).
                    try:
                        from dynastore.models.protocols.configs import ConfigsProtocol
                        from dynastore.modules.storage.driver_config import (
                            CollectionSchema,
                        )
                        from dynastore.modules.storage.field_constraints import (
                            overlay_schema_flags,
                        )
                        from dynastore.tools.discovery import get_protocol

                        configs = get_protocol(ConfigsProtocol)
                        if configs is not None:
                            schema_cfg = await configs.get_config(
                                CollectionSchema,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                ctx=DriverContext(db_resource=conn),
                            )
                            result = overlay_schema_flags(schema_cfg, result)
                    except Exception:
                        pass
                    return result
                except Exception:
                    pass

            # Fallback: use introspect_schema
            schema_fields = await self.introspect_schema(
                catalog_id, collection_id, db_resource=conn,
            )
            return {
                f["name"]: ProtocolFieldDefinition(
                    name=f["name"],
                    data_type=f.get("type", "string"),
                    capabilities=[FieldCapability.FILTERABLE],
                )
                for f in schema_fields
            }

        def _stac_base_fields(level: str) -> Dict[str, ProtocolFieldDefinition]:
            """Return STAC-standard fields for catalog/collection/asset levels."""
            if level == "catalog":
                names = ["id", "title", "description", "type", "conformsTo"]
            elif level == "collection":
                names = [
                    "id", "title", "description", "license", "keywords",
                    "extent", "providers", "summaries", "links",
                ]
            elif level == "asset":
                names = ["href", "type", "title", "description", "roles"]
            else:
                return {}
            return {
                n: ProtocolFieldDefinition(
                    name=n, data_type="string",
                    capabilities=[FieldCapability.FILTERABLE],
                )
                for n in names
            }

        if entity_level == "item":
            if db_resource is not None:
                return await _resolve_item_fields(db_resource)
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols.database import DatabaseProtocol
            db_proto = get_protocol(DatabaseProtocol)
            if not db_proto:
                return {}
            async with managed_transaction(db_proto.engine) as conn:
                return await _resolve_item_fields(conn)

        return _stac_base_fields(entity_level)

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return None

        async def _query(conn):
            layer_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )
            if not layer_config:
                return None

            geom_alias = "h"
            geom_col = "geom"
            storage_srid = 4326
            temporal_alias = "h"
            joins = []

            if layer_config.sidecars:
                from dynastore.modules.catalog.sidecars.geometries_config import (
                    GeometriesSidecarConfig,
                )
                from dynastore.modules.catalog.sidecars.attributes_config import (
                    FeatureAttributeSidecarConfig,
                )

                geom_sc = next(
                    (sc for sc in layer_config.sidecars if isinstance(sc, GeometriesSidecarConfig)),
                    None,
                )
                if geom_sc:
                    geom_source = f"{table}_{geom_sc.sidecar_id}"
                    geom_alias = "g"
                    storage_srid = geom_sc.target_srid
                    joins.append(
                        f'JOIN "{schema}"."{geom_source}" {geom_alias} ON h.geoid = {geom_alias}.geoid'
                    )

                attr_sc = next(
                    (sc for sc in layer_config.sidecars if isinstance(sc, FeatureAttributeSidecarConfig)),
                    None,
                )
                if attr_sc and attr_sc.enable_validity:
                    temporal_source = f"{table}_{attr_sc.sidecar_id}"
                    temporal_alias = "a"
                    if temporal_source != (f"{table}_{geom_sc.sidecar_id}" if geom_sc else ""):
                        joins.append(
                            f'JOIN "{schema}"."{temporal_source}" {temporal_alias} ON h.geoid = {temporal_alias}.geoid'
                        )
                    else:
                        temporal_alias = geom_alias

            if storage_srid == 4326:
                spatial_expr = f"ST_Extent({geom_alias}.{geom_col})"
            else:
                spatial_expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_alias}.{geom_col}),{storage_srid}), 4326)"

            join_clause = "\n" + "\n".join(joins) if joins else ""

            sql = f"""
                WITH calculated_extents AS (
                    SELECT
                        {spatial_expr} AS combined_geom,
                        MIN(lower({temporal_alias}.validity)) AS min_validity,
                        MAX(upper({temporal_alias}.validity)) AS max_validity
                    FROM "{schema}"."{table}" h
                    {join_clause}
                    WHERE h.deleted_at IS NULL AND {geom_alias}.{geom_col} IS NOT NULL
                )
                SELECT
                    ST_XMin(combined_geom),
                    ST_YMin(combined_geom),
                    ST_XMax(combined_geom),
                    ST_YMax(combined_geom),
                    CASE WHEN min_validity = '-infinity' THEN NULL ELSE min_validity END,
                    CASE WHEN max_validity = 'infinity' THEN NULL ELSE max_validity END
                FROM calculated_extents;
            """

            row = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)

            if row and row[0] is not None:
                bbox = [
                    max(-180.0, min(180.0, row[0])),
                    max(-90.0, min(90.0, row[1])),
                    max(-180.0, min(180.0, row[2])),
                    max(-90.0, min(90.0, row[3])),
                ]
            else:
                bbox = [-180.0, -90.0, 180.0, 90.0]

            min_time = row[4] if row else None
            max_time = row[5] if row else None

            return {
                "spatial": {"bbox": [list(bbox)]},
                "temporal": {"interval": [[min_time, max_time]]},
            }

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return None
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.modules.db_config.query_executor import (
            DQLQuery, ResultHandler, managed_transaction,
        )

        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource,
        )
        if not table:
            return None

        async def _query(conn):
            layer_config = await self.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            # Resolve attribute sidecar table
            attr_table = f"{table}_attributes"
            attr_alias = "s"
            attr_join = f'JOIN "{schema}"."{attr_table}" {attr_alias} ON h.geoid = {attr_alias}.geoid'

            # Resolve geometry sidecar table
            geom_table = f"{table}_geometries"
            geom_alias = "g"
            geom_join = f'JOIN "{schema}"."{geom_table}" {geom_alias} ON h.geoid = {geom_alias}.geoid'

            base_where = "h.deleted_at IS NULL"

            if aggregation_type == "terms" and field:
                sql = f"""
                    SELECT {attr_alias}.attributes->>'{field}' AS val, COUNT(*) AS cnt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where} AND {attr_alias}.attributes ? '{field}'
                    GROUP BY val
                    ORDER BY cnt DESC
                    LIMIT 100;
                """
                rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
                return [{"value": r["val"], "count": r["cnt"]} for r in (rows or [])]

            elif aggregation_type == "stats" and field:
                sql = f"""
                    SELECT
                        MIN(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS min_val,
                        MAX(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS max_val,
                        AVG(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS avg_val,
                        SUM(CAST({attr_alias}.attributes->>'{field}' AS NUMERIC)) AS sum_val,
                        COUNT(*) AS cnt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where}
                      AND {attr_alias}.attributes ? '{field}'
                      AND {attr_alias}.attributes->>'{field}' ~ '^-?[0-9]+(\\.[0-9]+)?$'
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn)
                return row

            elif aggregation_type == "datetime_range":
                sql = f"""
                    SELECT
                        MIN(lower({attr_alias}.validity)) AS min_dt,
                        MAX(upper({attr_alias}.validity)) AS max_dt
                    FROM "{schema}"."{table}" h
                    {attr_join}
                    WHERE {base_where};
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn)
                return row

            elif aggregation_type == "bbox":
                storage_srid = 4326
                geom_source = geom_alias
                join_sql = geom_join
                if layer_config and layer_config.sidecars:
                    from dynastore.modules.catalog.sidecars.geometries_config import (
                        GeometriesSidecarConfig,
                    )
                    geom_sc = next(
                        (sc for sc in layer_config.sidecars if isinstance(sc, GeometriesSidecarConfig)),
                        None,
                    )
                    if geom_sc:
                        storage_srid = geom_sc.target_srid

                if storage_srid == 4326:
                    expr = f"ST_Extent({geom_source}.geom)"
                else:
                    expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_source}.geom),{storage_srid}),4326)"

                sql = f"""
                    SELECT
                        ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
                    FROM (
                        SELECT {expr} AS ext
                        FROM "{schema}"."{table}" h
                        {join_sql}
                        WHERE {base_where} AND {geom_source}.geom IS NOT NULL
                    ) sub;
                """
                row = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)
                if row and row[0] is not None:
                    return [row[0], row[1], row[2], row[3]]
                return None

            elif aggregation_type == "count":
                sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE {base_where};'
                return await DQLQuery(sql, result_handler=ResultHandler.SCALAR_ONE).execute(conn)

            else:
                raise ValueError(f"Unsupported aggregation_type: {aggregation_type}")

        if db_resource is not None:
            return await _query(db_resource)
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        db_proto = get_protocol(DatabaseProtocol)
        if not db_proto:
            return None
        async with managed_transaction(db_proto.engine) as conn:
            return await _query(conn)

    async def resolve_storage_location(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> CollectionPostgresqlDriverConfig:
        """Resolve PG storage coordinates using internal methods."""
        schema = await self._resolve_schema(catalog_id, db_resource=db_resource)
        table = None
        if collection_id:
            table = await self.resolve_physical_table(
                catalog_id, collection_id, db_resource=db_resource,
            )

        return CollectionPostgresqlDriverConfig(
            physical_schema=schema,
            physical_table=table,
        )

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this collection."""
        from dynastore.modules.storage.storage_location import StorageLocation

        schema = await self._resolve_schema(catalog_id)
        table = await self.resolve_physical_table(catalog_id, collection_id)
        table_ref = table or collection_id
        return StorageLocation(
            backend="postgresql",
            canonical_uri=f"postgresql://{schema}.{table_ref}",
            identifiers={"schema": schema, "table": table_ref},
            display_label=f"{schema}.{table_ref}",
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
