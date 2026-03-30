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
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict, Union, Tuple, cast, AsyncIterator
from sqlalchemy import text

import inspect as _inspect
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    GeoDQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    is_async_resource,
)
from dynastore.modules.catalog.models import ItemDataForDB, Collection, Catalog
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    COLLECTION_PLUGIN_CONFIG_ID,
)
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.modules.catalog.sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.db_config import shared_queries
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
from dynastore.modules.catalog.sidecars.base import FeaturePipelineContext
from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.modules.catalog.item_distributed import ItemDistributedMixin

logger = logging.getLogger(__name__)


async def _run_query(conn, stmt, params=None):
    """Run a statement on either sync or async connection."""
    result = conn.execute(stmt, params or {})
    if _inspect.isawaitable(result):
        result = await result
    return result


# --- Specialized Queries for ItemService ---



soft_delete_item_query = DQLQuery(
    "UPDATE {catalog_id}.{collection_id} SET deleted_at = NOW() WHERE geoid = :geoid AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)


class ItemService(ItemQueryMixin, ItemDistributedMixin, ItemsProtocol):
    """Service for item-level operations.

    Explicitly declares conformance to ``ItemsProtocol`` so that static
    analysis tools (mypy) can verify method signatures stay in sync with
    the protocol definition.
    """

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )

    async def _resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def _get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        config_provider: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ) -> (
        Any
    ):  # Return type should be CollectionConfig from dynastore.modules.catalog.config
        """Helper to get collection configuration."""
        configs = config_provider or get_protocol(ConfigsProtocol)
        return await configs.get_config(
            COLLECTION_PLUGIN_CONFIG_ID,
            catalog_id,
            collection_id,
            db_resource=db_resource,
        )

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Optional[FeaturePipelineContext] = None,
    ) -> Feature:
        """
        Canonical row-to-Feature mapper. Runs each configured sidecar's
        ``map_row_to_feature`` in declaration order, producing a fully
        populated GeoJSON/STAC Feature.

        Pipeline (ordered by sidecar config):
          1. **Hub** (this initialiser): ``feature.id`` defaults to ``geoid``.
          2. **Geometry sidecar**: sets ``geometry``, ``bbox``, optional stats
             properties.
          3. **Attributes sidecar**: optionally overrides ``id`` with
             ``external_id``, populates schema-driven ``properties``.
          4. **STAC sidecar** (optional): transforms the GeoJSON Feature into
             a STAC Item — adds ``links``, converts timestamps, attaches the
             asset reference from context.

        Each sidecar receives the *same* ``FeaturePipelineContext`` so sidecars
        can share data (e.g. attributes → STAC via ``asset_id``) without
        direct coupling. Internal-field filtering is delegated to sidecars
        via ``get_internal_columns()``.
        """

        if not row:
            return Feature(type="Feature", geometry=None, properties={})

        row_dict = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

        # Hub contribution: initialise the feature with geoid as the default id.
        # Sidecars (e.g. Attributes) may override this later in the pipeline.
        geoid = row_dict.get("geoid")
        feature = Feature(
            type="Feature",
            geometry=None,
            properties={},
            id=str(geoid) if geoid is not None else None,
        )

        # Shared context propagated through the entire sidecar pipeline.
        if context is None:
            context = FeaturePipelineContext(lang=lang)

        if col_config and col_config.sidecars:
            from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
            # Gather all internal columns to prevent property leaking across sidecars
            all_internal = set()
            for sc_config in col_config.sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    all_internal.update(sidecar.get_internal_columns())
            context._all_internal_cols = all_internal

            for sc_config in col_config.sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    sidecar.map_row_to_feature(row_dict, feature, context=context)

            # Bridge context to feature model_extra for extension generators (e.g. STAC)
            if context:
                # model_extra is already initialized by Pydantic 'extra="allow"'
                sidecar_data = context._sidecar_store
                all_internal = context.all_internal_columns
                # Never overwrite defined model fields (id, geometry, etc.)
                # via __pydantic_extra__ — that would clobber values already
                # set by the sidecar pipeline.
                model_fields = set(Feature.model_fields)
                for sid, data in sidecar_data.items():
                    if isinstance(data, dict):
                        # Merge dicts if it's a standard sidecar publication,
                        # but skip raw internal columns (e.g. item_title,
                        # external_extensions) that are only meant for
                        # inter-sidecar communication.
                        for k, v in data.items():
                            if k not in all_internal and k not in model_fields:
                                feature.__pydantic_extra__[k] = v
                    else:
                        if sid not in model_fields:
                            feature.__pydantic_extra__[sid] = data
        else:
            logger.warning(
                "No sidecars configured for col_config; returning minimal "
                "feature with geoid-based id."
            )

        return feature

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        db_resource: Optional[DbResource] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """
        Create or update items (single or bulk).

        Args:
            catalog_id: Catalog identifier
            collection_id: Collection identifier
            items: Feature, FeatureCollection, STACItem, or raw dict/list
            db_resource: Optional database resource

        Returns:
            Created/Updated item(s) (single or list)
        """
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # Determine if single or bulk to return consistent type
        is_single = False
        items_list = []

        # Handle input types
        if isinstance(items, list):
            items_list = items
        elif isinstance(items, dict) and items.get("type") == "FeatureCollection":
            items_list = items.get("features", [])
        elif hasattr(items, "features") and items.type == "FeatureCollection":
            # Handle FeatureCollection Pydantic model
            items_list = items.features
        else:
            # Handle single item passed as list or other iterable
            # Single item (Feature, STACItem, dict)
            is_single = True
            items_list = [items]

        if not items_list:
            return [] if not is_single else {}

        async with managed_transaction(db_resource or self.engine) as conn:
            catalogs = get_protocol(CatalogsProtocol)
            col_config = await catalogs.get_collection_config(
                catalog_id, collection_id, db_resource=conn
            )

            # Resolve Physical Table (Hub)
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_table:
                # Fallback to promotion if missing (legacy)
                await self.ensure_physical_table_exists(
                    catalog_id, collection_id, col_config, db_resource=conn
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )

            # Instantiate Sidecars
            sidecars = []
            if col_config.sidecars:
                from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

                for sc_config in col_config.sidecars:
                    try:
                        sc = SidecarRegistry.get_sidecar(sc_config)
                        sidecars.append(sc)
                    except ValueError as e:
                        logger.warning(f"Skipping sidecar instantiation: {e}")

            created_rows = []

            for item_data in items_list:
                # 1. Normalize Input to Dict
                raw_item = {}
                if hasattr(item_data, "model_dump"):
                    raw_item = item_data.model_dump(by_alias=True, exclude_unset=True)
                elif isinstance(item_data, dict):
                    raw_item = item_data
                else:
                    raise ValueError(f"Unsupported item type: {type(item_data)}")

                # 2. Sidecar Processing
                sidecar_payloads: Dict[str, Dict[str, Any]] = {}
                from dynastore.tools.identifiers import generate_geoid

                geoid = generate_geoid()

                # Context for sidecars (Fresh for each item to avoid leakage)
                item_context = {
                    "geoid": geoid,
                    "operation": "insert",
                    "_raw_item": raw_item,  # Preserve for place stats and other sidecar extensions
                    **(processing_context or {}),
                }

                # Hub Payload (Base identity and temporal)
                hub_payload = {
                    "geoid": geoid,
                    "transaction_time": datetime.now(timezone.utc),
                    "deleted_at": None,
                }

                # Extract partition keys if any
                partition_values = {}

                for sidecar in sidecars:
                    # A. Validation
                    val_result = sidecar.validate_insert(raw_item, item_context)
                    if not val_result.valid:
                        raise ValueError(
                            f"Sidecar {sidecar.sidecar_id} rejected item: {val_result.error}"
                        )

                    # B. Prepare Payload
                    sc_payload = sidecar.prepare_upsert_payload(raw_item, item_context)
                    if sc_payload:
                        sidecar_payloads[sidecar.sidecar_id] = sc_payload

                        # C. Capture Partition Key matching (using protocol methods)
                        for pk in sidecar.get_partition_keys():
                            if pk in sc_payload:
                                partition_values[pk] = sc_payload[pk]
                                item_context[pk] = sc_payload[pk]

                # 4. Insert/Update Logic (Distributed)
                # Ensure Partitions exist for Hub
                if col_config.partitioning.enabled and partition_values:
                    # For simplicity, we assume one level of list partitioning for the tool helper
                    # If composite, we might need a more complex helper.
                    # Currently dynastore handles simple list/range.
                    p_val = (
                        list(partition_values.values())[0] if partition_values else None
                    )
                    await self.ensure_partition_exists(
                        catalog_id, collection_id, col_config, p_val, db_resource=conn
                    )

                # Resolve Generic Identity (from context, populated by sidecars)
                # Sidecars already populated the context in prepare_upsert_payload.
                # We no longer need to extract specific fields like 'external_id' here.
                pass

                # Add partition keys to hub if missing
                hub_payload.update(partition_values)

                # Perform Distributed Upsert
                new_row = await self.insert_or_update_distributed(
                    conn,
                    catalog_id,
                    collection_id,
                    hub_payload,
                    sidecar_payloads,
                    col_config=col_config,
                    sidecars=sidecars,
                    processing_context=item_context,
                )

                if new_row:
                    created_rows.append(new_row)

                    # 5. Post-Insert Hooks (Lifecycle)
                    # e.g. notifies, or side-effects
                    for sidecar in sidecars:
                        # await sidecar.on_item_created(...) # If exists
                        pass
                else:
                    # Generic error if upsert fails
                    logger.error(
                        f"FATAL: insert_or_update_distributed returned None for geoid: {geoid}"
                    )
                    raise RuntimeError(f"Failed to upsert item. Geoid: {geoid}")

            # created_rows already contains Feature objects from
            # _execute_distributed_insert / _execute_distributed_update
            # (which call map_row_to_feature internally).
            results = created_rows
            
            # Emit Item Events
            if results:
                try:
                    from dynastore.models.protocols.events import EventsProtocol
                    from dynastore.modules.catalog.event_service import CatalogEventType
                    events_protocol = get_protocol(EventsProtocol)
                    if events_protocol:
                        if is_single:
                            await events_protocol.emit(
                                event_type=CatalogEventType.ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                item_id=str(results[0].id) if results[0].id else None,
                                payload=results[0].model_dump(by_alias=True, exclude_unset=True)
                            )
                        else:
                            await events_protocol.emit(
                                event_type=CatalogEventType.BULK_ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                payload={"count": len(results), "items_subset": [r.model_dump(by_alias=True, exclude_none=True) for r in results[:10]]}
                            )
                except Exception as e:
                    logger.warning(f"Failed to emit item creation events: {e}")

            return results[0] if is_single else results

    # Query methods (get_features, get_item, search_items, stream_items, etc.)
    # are provided by ItemQueryMixin.

    # Distributed methods (insert_or_update_distributed, etc.)
    # are provided by ItemDistributedMixin.

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        db_resource: Optional[DbResource] = None,
    ) -> int:
        """
        Deletes a specific language variant from an item's attributes.
        Actually, it looks for localized fields in 'attributes' and removes the key.
        """
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return 0

            # Fetch the item to identify localized fields in attributes
            item = await self.get_item(
                catalog_id, collection_id, item_id, db_resource=conn
            )
            if not item:
                return 0

            attributes = item.get("attributes", {})
            if isinstance(attributes, str):
                attributes = json.loads(attributes)

            # Identifies fields that are potentially localized (e.g. title, description, or custom)
            # and removes the requested language.
            modified = False
            for key, value in attributes.items():
                if isinstance(value, dict):
                    # Check if it looks like a localized dict
                    from dynastore.models.localization import _LANGUAGE_METADATA

                    if any(k in _LANGUAGE_METADATA for k in value.keys()):
                        if lang in value:
                            if len(value) <= 1:
                                # We might skip deletion if it's the only language,
                                # but usually for items, we can either keep or remove.
                                # Let's follow the 'error if last language' rule if it makes sense.
                                # For STAC items, maybe it's less strict, but let's be consistent.
                                continue

                            del value[lang]
                            modified = True

            if not modified:
                return 0

            update_sql = f'UPDATE "{phys_schema}"."{phys_table}" SET attributes = :attr WHERE geoid = :geoid;'
            rows = await DQLQuery(
                update_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn, attr=json.dumps(attributes, cls=CustomJSONEncoder), geoid=item_id
            )
            return rows

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: CollectionPluginConfig,
        db_resource: Optional[DbResource] = None,
    ):
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.locking_tools import acquire_startup_lock

            async with acquire_startup_lock(
                conn, f"promote_{catalog_id}_{collection_id}"
            ):
                phys_schema = await self._resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )
                force_create = False
                if not phys_table:
                    logger.info(
                        f"Promoting collection {catalog_id}:{collection_id} to physical storage."
                    )
                    phys_table = collection_id
                    force_create = True
                    catalogs = get_protocol(CatalogsProtocol)
                    await catalogs.set_physical_table(
                        catalog_id, collection_id, phys_table, db_resource=conn
                    )
                else:
                    catalogs = get_protocol(CatalogsProtocol)

                if force_create or not await shared_queries.table_exists_query.execute(
                    conn, schema=phys_schema, table=phys_table
                ):
                    await catalogs.create_physical_collection(
                        conn,
                        phys_schema,
                        catalog_id,
                        collection_id,
                        physical_table=phys_table,
                        layer_config=col_config.model_dump(),
                    )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: CollectionPluginConfig,
        partition_value: Any,
        db_resource: Optional[DbResource] = None,
    ):
        partitioning = col_config.partitioning
        if not partitioning.enabled or not partitioning.partition_keys:
            return
        # Current simplify: we use the first partition key for the physical partition routing
        # In a fully composite world, partition_value should be a list/tuple.
        if partition_value is None:
            return
        if partition_value is None:
            return
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return
            # Determine strategy for the tool (simplified)
            # If the value is a date/datetime, use RANGE, else LIST
            from datetime import date, datetime

            tool_strategy = (
                "RANGE" if isinstance(partition_value, (date, datetime)) else "LIST"
            )
            interval = (
                None  # We might need to derive this if we had TimePartitionStrategy
            )

            from dynastore.modules.db_config.partition_tools import (
                ensure_partition_exists as ensure_partition_tool,
            )

            await ensure_partition_tool(
                conn=conn,
                table_name=phys_table,
                strategy=tool_strategy,
                partition_value=partition_value,
                schema=phys_schema,
                interval=interval,
                parent_table_name=phys_table,
                parent_table_schema=phys_schema,
            )

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns a dictionary of field definitions for the collection.
        Aggregates fields from the physical table introspection and configured sidecars.
        """
        # Resolve physical details internally
        schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

        fields = {}
        from dynastore.modules.catalog.sidecars.base import (
            FieldDefinition,
            FieldCapability,
        )

        # 1. Try to get config-based definitions if logical IDs provided
        if catalog_id and collection_id:
            try:
                col_config = await self._get_collection_config(
                    catalog_id, collection_id, db_resource=db_resource
                )

                if col_config:
                    optimizer = QueryOptimizer(col_config)
                    # A. Aggregate from all sidecars via QueryOptimizer
                    fields.update(optimizer.get_all_queryable_fields())

                    # B. Hub Fields (geoid, transaction_time, etc.)
                    hub_cols = col_config.get_column_definitions()
                    for name in hub_cols.keys():
                        if name not in fields:
                            # Infer simple type from SQL def
                            sql_type = hub_cols[name].upper()
                            d_type = "string"
                            if "TIMESTAMP" in sql_type:
                                d_type = "datetime"
                            elif "UUID" in sql_type:
                                d_type = "uuid"

                            fields[name] = FieldDefinition(
                                name=name,
                                sql_expression=f"h.{name}",
                                data_type=d_type,
                                capabilities=[
                                    FieldCapability.FILTERABLE,
                                    FieldCapability.SORTABLE,
                                    FieldCapability.GROUPABLE,
                                ],
                            )

            except (ValueError, KeyError, AttributeError, LookupError, OSError) as e:
                logger.warning(
                    f"Failed to resolve collection config for fields for {catalog_id}/{collection_id}: {e}"
                )

        # 2. Introspect Physical Table (Fallback & Validation)
        # Use a transaction for introspection
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.tools import _get_table_columns_query

            rows = await _get_table_columns_query.execute(
                conn, schema=schema, table=table
            )

            for row in rows:
                col_name = row.column_name
                # If already defined by config, skip (config is more authoritative for aliases/sql_expression)
                if col_name in fields:
                    continue

                dtype = str(row.data_type).lower()
                udt = str(row.udt_name).lower()

                # Normalize type name
                if dtype == "user-defined" and udt in ("geometry", "geography"):
                    dtype = "geometry"
                elif dtype in ("character varying", "character", "text"):
                    dtype = "string"
                elif dtype in ("integer", "smallint", "bigint"):
                    dtype = "integer"
                elif dtype in ("double precision", "real", "numeric"):
                    dtype = "float"
                elif dtype.startswith("timestamp"):
                    dtype = "datetime"
                elif dtype == "boolean":
                    dtype = "boolean"
                elif dtype == "jsonb":
                    dtype = "jsonb"
                elif dtype == "uuid":
                    dtype = "uuid"
                elif dtype == "tstzrange":
                    dtype = "tstzrange"

                # Simple object mocking FieldDefinition for introspection-only columns
                # We don't know the alias here, so we assume column name
                fields[col_name] = FieldDefinition(
                    name=col_name,
                    sql_expression=col_name,
                    data_type=dtype,
                    capabilities=[FieldCapability.FILTERABLE],
                )

        return fields

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns the composed JSON Schema for the collection's Feature output.
        Aggregated from all active sidecars via QueryOptimizer.
        """
        col_config = await self._get_collection_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not col_config:
            raise ValueError(f"Collection {catalog_id}/{collection_id} not found.")

        from dynastore.modules.catalog.query_optimizer import QueryOptimizer
        optimizer = QueryOptimizer(col_config)
        return optimizer.get_feature_type_schema()

    # NOTE: map_row_to_feature is defined once at the top of this class.
    # The canonical implementation (using the sidecar pipeline) is the single
    # source of truth — do not add a second definition here.

    @property
    def count_items_by_asset_id_query(self) -> DQLQuery:
        """Query builder for counting items by asset ID."""

        def _builder(conn, params):
            phys_schema = params["catalog_id"]
            phys_table = params["collection_id"]
            asset_id = params["asset_id"]

            sql = f'SELECT count(*) FROM "{phys_schema}"."{phys_table}" WHERE extra_metadata->\'assets\' ? :asset_id AND deleted_at IS NULL'
            return sql, {"asset_id": asset_id}

        return DQLQuery.from_builder(_builder, result_handler=ResultHandler.SCALAR_ONE)

