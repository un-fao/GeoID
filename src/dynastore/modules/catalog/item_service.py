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
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict, Union, Tuple, cast, AsyncIterator
from sqlalchemy import text

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    GeoDQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.catalog.models import ItemDataForDB, Collection, Catalog
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    COLLECTION_PLUGIN_CONFIG_ID,
)
from dynastore.modules.catalog.sidecars.attributes_config import VersioningBehaviorEnum
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.modules.catalog.sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.tools import (
    recalculate_and_update_extents,
)
from dynastore.modules.db_config import shared_queries
from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.catalog.query_orchestrator import CatalogQueryOrchestrator
from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
from dynastore.modules.catalog.sidecars.base import SidecarPipelineContext

logger = logging.getLogger(__name__)

# --- Specialized Queries for ItemService ---



soft_delete_item_query = DQLQuery(
    "UPDATE {catalog_id}.{collection_id} SET deleted_at = NOW() WHERE geoid = :geoid AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)


class ItemService(ItemsProtocol):
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
        context: Optional[SidecarPipelineContext] = None,
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

        Each sidecar receives the *same* ``SidecarPipelineContext`` so sidecars
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
            context = SidecarPipelineContext(lang=lang)

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
                for sid, data in sidecar_data.items():
                    if isinstance(data, dict):
                        # Merge dicts if it's a standard sidecar publication
                        for k, v in data.items():
                            feature.__pydantic_extra__[k] = v
                    else:
                        # Direct assignment for flat values
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

            # Return valid results (Sidecar-aware mapping)
            results = [self.map_row_to_feature(row, col_config) for row in created_rows]
            
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

    async def get_features(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[CollectionPluginConfig] = None,
        item_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        **kwargs,
    ) -> List[Feature]:
        """Retrieves a list of items via the QueryOptimizer path."""
        if not col_config:
            catalogs_svc = get_protocol(CatalogsProtocol)
            col_config = await catalogs_svc.get_collection_config(
                catalog_id, collection_id, db_resource=conn
            )

        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        if request is None:
            request = QueryRequest(item_ids=item_ids or None)

        optimizer = QueryOptimizer(col_config)
        sql, params = optimizer.build_optimized_query(request, phys_schema, phys_table)
        rows = await conn.execute(text(sql), params)
        return [self.map_row_to_feature(dict(row._mapping), col_config) for row in rows]

    async def get_features_query(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        col_config: CollectionPluginConfig,
        params: Dict[str, Any],
        param_suffix: str = "",
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Exposes the underlying query generation for features (used by Tiles/MVT).
        Now uses pluggable query transformations.
        """
        # 1. Build base QueryRequest from params
        query_request = self._build_base_query_request(params, col_config)

        # 2. Apply transformations and generate SQL
        context = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "col_config": col_config,
            **params,
        }
        sql, bind_params = await self._apply_query_transformations(
            query_request, context, catalog_id, collection_id, col_config
        )

        # 3. Apply param suffixing for multi-collection queries
        if param_suffix:
            sql, bind_params = self._apply_param_suffix(sql, bind_params, param_suffix)

        return sql, bind_params

    def _build_base_query_request(
        self, params: Dict[str, Any], col_config: CollectionPluginConfig
    ) -> QueryRequest:
        """Build base QueryRequest from params before transformations"""
        from dynastore.models.query_builder import QueryRequest, FieldSelection

        # Mandatory ID
        selects = [FieldSelection(field="geoid", alias="id")]

        # Geometry (if requested or by default)
        if params.get("with_geometry", True) and params.get("geom_format") != "MVT":
            selects.append(FieldSelection(field="geom"))

        # Attributes (JSONB)
        selects.append(FieldSelection(field="attributes"))

        query_req = QueryRequest(
            select=selects,
            limit=params.get("limit"),
            offset=params.get("offset"),
            raw_where=params.get("where"),
            raw_params=params.get("raw_params", {}),
        )

        # CQL Filter
        if params.get("cql_filter"):
            from dynastore.modules.tools.cql import parse_cql_filter

            cql_where_str, cql_params = parse_cql_filter(
                params["cql_filter"], field_mapping=None, parser_type="ecql"
            )
            if cql_where_str:
                if query_req.raw_where:
                    query_req.raw_where = (
                        f"({query_req.raw_where}) AND ({cql_where_str})"
                    )
                else:
                    query_req.raw_where = cql_where_str
                query_req.raw_params.update(cql_params)

        return query_req

    async def _apply_query_transformations(
        self,
        query_request: QueryRequest,
        context: Dict[str, Any],
        catalog_id: str,
        collection_id: str,
        col_config: CollectionPluginConfig,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Applies registered query transformations and generates optimized SQL.

        This centralizes the transformation logic used across get_features_query,
        _prepare_search, and stream_items.
        """
        from dynastore.models.protocols import QueryTransformProtocol
        from dynastore.tools.discovery import get_protocols

        # Get and sort transformers by priority
        transformers = sorted(
            get_protocols(QueryTransformProtocol),
            key=lambda t: getattr(t, "priority", 100),
        )

        # Apply QueryRequest-level transformations
        for transformer in transformers:
            if transformer.can_transform(context):
                logger.debug(f"Applying transformation: {transformer.transform_id}")
                query_request = transformer.transform_query(query_request, context)

        # Apply CQL Filter parsing if present
        if query_request.cql_filter:
            from dynastore.modules.tools.cql import parse_cql_filter

            # Use a temporary optimizer to get all available fields for validation
            temp_optimizer = QueryOptimizer(col_config)
            queryable_fields = temp_optimizer.get_all_queryable_fields()

            # Create mapping for CQL parser
            # We map field names to their SQL expressions
            field_mapping = {
                name: text(field_def.sql_expression)
                for name, field_def in queryable_fields.items()
            }
            # Add implicit fields
            field_mapping.update(
                {
                    "geoid": text("h.geoid"),
                    "deleted_at": text("h.deleted_at"),
                    "transaction_time": text("h.transaction_time"),
                    "validity": text("h.validity"),
                }
            )

            try:
                cql_where, cql_params = parse_cql_filter(
                    query_request.cql_filter,
                    field_mapping=field_mapping,
                    parser_type="cql2",  # OGC API Features uses CQL2 by default
                )

                if cql_where:
                    if query_request.raw_where:
                        query_request.raw_where = (
                            f"({query_request.raw_where}) AND ({cql_where})"
                        )
                    else:
                        query_request.raw_where = cql_where
                    query_request.raw_params.update(cql_params)
            except ValueError as e:
                # Re-raise as ValueError so it can be caught and returned as 400
                raise ValueError(f"Invalid CQL filter: {e}")

        # Resolve physical storage and generate SQL
        phys_schema = await self._resolve_physical_schema(catalog_id)
        phys_table = await self._resolve_physical_table(catalog_id, collection_id)
        if not phys_schema or not phys_table:
            raise ValueError(
                f"Could not resolve storage for {catalog_id}:{collection_id}"
            )

        optimizer = QueryOptimizer(col_config)
        sql, params = optimizer.build_optimized_query(
            query_request, schema=phys_schema, table=phys_table
        )

        # Apply SQL-level transformations
        for transformer in transformers:
            if transformer.can_transform(context):
                sql, params = transformer.post_process_sql(sql, params, context)

        return sql, params

    def _apply_param_suffix(
        self, sql: str, bind_params: Dict[str, Any], suffix: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Applies a suffix to bind parameters to avoid collisions."""
        import re

        new_params = {}
        sql_str = str(sql)
        for key, val in bind_params.items():
            new_key = f"{key}{suffix}"
            new_params[new_key] = val
            sql_str = re.sub(f":{key}(?=\\b)", f":{new_key}", sql_str)
        return sql_str, new_params

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[DbResource] = None,
        lang: str = "en",
        context: Optional[Any] = None,
    ) -> Optional[Feature]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(
                catalog_id, collection_id, db_resource=conn
            )
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return None

            request = QueryRequest(item_ids=[str(item_id)], limit=1)
            ctx: Dict[str, Any] = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
            }
            sql, params = await self._apply_query_transformations(
                request, ctx, catalog_id, collection_id, col_config
            )

            result = await conn.execute(text(sql), params)
            row = result.mappings().first()

            return (
                self.map_row_to_feature(dict(row), col_config, lang=lang, context=context)
                if row else None
            )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> int:
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

            configs = get_protocol(ConfigsProtocol)
            col_config = await configs.get_config(
                COLLECTION_PLUGIN_CONFIG_ID,
                catalog_id,
                collection_id,
                db_resource=conn,
            )

            # ID Resolution Logic
            target_geoid = item_id
            if col_config and col_config.sidecars:
                from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

                for sc in col_config.sidecars:
                    if sc.feature_id_field_name:
                        sidecar = SidecarRegistry.get_sidecar(sc)
                        ctx = {sc.feature_id_field_name: item_id}
                        existing = await sidecar.resolve_existing_item(
                            conn, phys_schema, phys_table, ctx
                        )
                        if existing:
                            target_geoid = str(existing["geoid"])
                            break

            rows = await soft_delete_item_query.execute(
                conn,
                catalog_id=phys_schema,
                collection_id=phys_table,
                geoid=target_geoid,
            )

            if rows > 0:
                await recalculate_and_update_extents(conn, catalog_id, collection_id)
                
                try:
                    from dynastore.models.protocols.events import EventsProtocol
                    from dynastore.modules.catalog.event_service import CatalogEventType
                    events_protocol = get_protocol(EventsProtocol)
                    if events_protocol:
                        await events_protocol.emit(
                            event_type=CatalogEventType.ITEM_DELETION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            item_id=target_geoid,
                            payload={"geoid": target_geoid, "original_id": item_id}
                        )
                except Exception as e:
                    logger.warning(f"Failed to emit item deletion event: {e}")

        return rows

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ) -> QueryResponse:
        """
        Stream search results using an async iterator (O(1) Memory).
        """
        # Metadata Resolution
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(catalog_id, collection_id, config, db_resource=conn)
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)

            if not phys_schema or not phys_table:
                raise ValueError(f"Collection '{catalog_id}/{collection_id}' not found.")

            # Apply transformations and generate SQL
            context = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
                **(request.raw_params or {}),
            }
            sql, params = await self._apply_query_transformations(
                request, context, catalog_id, collection_id, col_config
            )

            total_count = None
            if request.include_total_count:
                count_wrapper = f"SELECT count(*) FROM ({sql}) AS sub"
                total_count = await conn.scalar(text(count_wrapper), params)

        # Stream Generator (O(1) Memory)
        async def feature_stream():
            # Open a fresh connection/transaction for streaming to ensure isolation and avoid leaks
            async with managed_transaction(self.engine) as stream_conn:
                # Use a buffer for higher throughput but still O(1) memory
                stream = await stream_conn.stream(text(sql), params)
                async for row in stream:
                    yield self.map_row_to_feature(dict(row._mapping), col_config)

        return QueryResponse(
            items=feature_stream(),
            total_count=total_count,
            catalog_id=catalog_id,
            collection_id=collection_id,
            collection_config=col_config,
        )

    @asynccontextmanager
    async def _prepare_search(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ):
        """Standardized preparation for search results."""
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(catalog_id, collection_id, config, db_resource=conn)
            
            context = {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "col_config": col_config,
                **(request.raw_params or {}),
            }
            sql, params = await self._apply_query_transformations(
                request, context, catalog_id, collection_id, col_config
            )

            params.update({
                "col_config": col_config,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            })

            query = GeoDQLQuery(text(sql), result_handler=ResultHandler.ALL)
            yield query, conn, params

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search and retrieve items using optimized query generation.
        """
        async with self._prepare_search(
            catalog_id, collection_id, request, config, db_resource
        ) as (query, conn, params):
            rows = await query.execute(conn, **params)
            col_config = params.get("col_config")
            return [self.map_row_to_feature(row, col_config) for row in rows]

    async def stream_items_from_query(
        self,
        catalog_id: str,
        collection_id: str,
        where_clause: str,
        query_params: Optional[Dict[str, Any]] = None,
        select_columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream search results from a raw SQL WHERE clause, leveraging the QueryOrchestrator.

        This method builds a query with sidecar JOINs. The user provides the WHERE clause content.

        **NOTE**: The `where_clause` must use the correct table aliases:
        - `h` for the main items (hub) table.
        - `g` for the geometry sidecar.
        - `a` for the attributes sidecar.

        Example: `where_clause="a.external_id = :ext_id AND ST_Intersects(g.geom, ST_MakeEnvelope(...))"`

        WARNING: The caller is responsible for ensuring that if `select_columns` is provided,
        it includes all columns whose sidecars are referenced in the `where_clause`.
        If `select_columns` is None, all sidecars are joined, which is safer but may be less performant.

        Args:
            catalog_id: The catalog ID.
            collection_id: The collection ID.
            where_clause: The content of the SQL WHERE clause.
            query_params: A dictionary of parameters to bind to the query.
            select_columns: Optional list of columns to select. If None, selects all available fields.
            limit: Optional limit for the query.
            offset: Optional offset for the query.
            db_resource: Optional database resource.

        Yields:
            An async iterator of result dictionaries.
        """
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            catalogs = get_protocol(CatalogsProtocol)
            col_config = await catalogs.get_collection_config(catalog_id, collection_id)
            phys_schema = await self._resolve_physical_schema(catalog_id)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id)

            if not phys_schema or not phys_table:
                raise ValueError(
                    f"Collection '{catalog_id}/{collection_id}' not found."
                )

            orchestrator = CatalogQueryOrchestrator(col_config)

            columns_to_select = select_columns
            if not columns_to_select:
                from dynastore.modules.catalog.sidecars.registry import SidecarRegistry

                all_fields = {"geoid", "deleted_at"}
                if col_config.sidecars:
                    for sc_config in col_config.sidecars:
                        sidecar = SidecarRegistry.get_sidecar(sc_config)
                        all_fields.update(sidecar.get_field_definitions().keys())
                columns_to_select = list(all_fields)

            where_clauses = [where_clause] if where_clause else []

            query_string = orchestrator.build_select_query(
                schema=phys_schema,
                table=phys_table,
                columns=columns_to_select,
                where_clauses=where_clauses,
                limit=limit,
            )

            if offset is not None:
                query_string += f" OFFSET {offset}"

            query = GeoDQLQuery(text(query_string), result_handler=ResultHandler.ALL)
            async for item in await query.stream(conn, **(query_params or {})):
                yield self.map_row_to_feature(item, col_config)

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

    async def insert_or_update_distributed(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Dict[str, Dict[str, Any]],
        col_config: CollectionPluginConfig,
        sidecars: List[SidecarProtocol],
        processing_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Coordinates multi-table upsert for Hub and Sidecars."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            phys_table = collection_id

        logger.info(
            f"DISTRIBUTED UPSERT: collection={catalog_id}.{collection_id}, phys={phys_schema}.{phys_table}, sidecars={[s.sidecar_id for s in sidecars]}"
        )

        # Generic Identities are handled via SidecarProtocol.get_identity_payload()
        # and resolve_existing_item(). We no longer extract specific fields here.

        # 1. Versioning Logic Check on Hub
        ingestion_config = await get_protocol(ConfigsProtocol).get_config(
            COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
        )
        versioning_behavior = getattr(
            ingestion_config,
            "versioning_behavior",
            VersioningBehaviorEnum.ALWAYS_ADD_NEW,
        )

        # 1.5 Acceptance Check
        for sidecar in sidecars:
            if not sidecar.is_acceptable(hub_payload, processing_context):
                logger.warning(f"Feature rejected by sidecar {sidecar.sidecar_id}")
                return None

        # Standardized Identity Resolution via Sidecar Protocol
        # We iterate sidecars to find one that can resolve the existing item.
        # AttributeSidecar implements this for external_id.
        active_rec = None
        if versioning_behavior not in [
            VersioningBehaviorEnum.CREATE_NEW_VERSION,
            VersioningBehaviorEnum.ALWAYS_ADD_NEW,
        ]:
            for sidecar in sidecars:
                active_rec = await sidecar.resolve_existing_item(
                    conn, phys_schema, phys_table, processing_context
                )
                if active_rec:
                    logger.info(
                        f"DISTRIBUTED UPSERT: found active record geoid={active_rec.get('geoid')} (via {sidecar.sidecar_id})"
                    )
                    break

        # 1.6 Additional Checks: Identity/Asset/Unique Collision
        if versioning_behavior == VersioningBehaviorEnum.REFUSE_ON_ASSET_ID_COLLISION:
            # Re-mapped to generic identity collision detection
            for sidecar in sidecars:
                if await sidecar.check_upsert_collision(
                    conn, phys_schema, phys_table, processing_context
                ):
                    logger.warning(
                        f"Feature rejected: Identity/Unique collision found (via {sidecar.sidecar_id})"
                    )
                    return None

        result = None
        # 2. Execution Path
        if not active_rec or versioning_behavior in [
            VersioningBehaviorEnum.CREATE_NEW_VERSION,
            VersioningBehaviorEnum.ALWAYS_ADD_NEW,
        ]:
            # INSERT NEW
            result = await self._execute_distributed_insert(
                conn,
                phys_schema,
                phys_table,
                hub_payload,
                sidecar_payloads,
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context,
            )

        elif versioning_behavior == VersioningBehaviorEnum.REJECT_NEW_VERSION:
            logger.info(
                "DISTRIBUTED UPSERT: identity matched and REJECT_NEW_VERSION set. Skipping."
            )
            return None

        else:
            # 3. Update Validation Hooks
            processing_context["operation"] = "update"
            for sidecar in sidecars:
                val_result = sidecar.validate_update(
                    sidecar_payloads.get(sidecar.sidecar_id, {}),
                    active_rec,
                    processing_context,
                )
                if not val_result.valid:
                    raise ValueError(
                        f"Sidecar {sidecar.sidecar_id} rejected update: {val_result.error}"
                    )

            # B. Resolve Validity for Hub & Sidecars
            # We construct it here once and Inject into Hub payload if missing
            valid_from = processing_context.get("valid_from") or datetime.now(
                timezone.utc
            )
            valid_to = processing_context.get("valid_to")

            from asyncpg import Range

            validity = Range(valid_from, valid_to, lower_inc=True, upper_inc=False)

            if "validity" not in hub_payload:
                hub_payload["validity"] = validity
            if versioning_behavior == VersioningBehaviorEnum.UPDATE_EXISTING_VERSION:
                # UPDATE EXISTING (HUB + Sidecars)
                # Reuse existing validity to ensure we update the SAME row in sidecars
                if active_rec and "validity" in active_rec:
                    validity = active_rec["validity"]
                    hub_payload["validity"] = validity

                result = await self._execute_distributed_update(
                    conn,
                    phys_schema,
                    phys_table,
                    active_rec["geoid"],
                    hub_payload,
                    sidecar_payloads,
                    col_config=col_config,
                    sidecars=sidecars,
                    processing_context=processing_context,
                    active_rec=active_rec,
                )
            else:
                # 4. ARCHIVE + INSERT NEW
                expire_at = hub_payload.get("valid_from") or datetime.now(timezone.utc)
                for sidecar in sidecars:
                    await sidecar.expire_version(
                        conn,
                        phys_schema,
                        phys_table,
                        geoid=active_rec["geoid"],
                        expire_at=expire_at,
                    )

                result = await self._execute_distributed_insert(
                    conn,
                    phys_schema,
                    phys_table,
                    hub_payload,
                    sidecar_payloads,
                    col_config=col_config,
                    sidecars=sidecars,
                    processing_context=processing_context,
                )

        asset_id = processing_context.get("asset_id")
        # Asset linking is now handled via SidecarProtocol.on_item_created hook in FeatureAttributeSidecar

        return result

    async def _execute_distributed_insert(
        self,
        conn,
        schema,
        hub_table,
        hub_payload,
        sc_data_map,
        col_config,
        sidecars=None,
        processing_context=None,
    ) -> Dict[str, Any]:
        """Performs inserts across Hub and all sidecars."""
        # A. Insert Hub
        logger.warning(f"DEBUG: Inserting into Hub {schema}.{hub_table}")
        hub_row = await self._insert_table_raw(conn, schema, hub_table, hub_payload)
        hub_data = hub_row._mapping if hasattr(hub_row, "_mapping") else hub_row
        geoid = hub_data["geoid"]

        # B. Insert Sidecars
        # Identity and payload finalization is now delegated to sidecars via protocol.

        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # 1. Identity Columns (Conflict Target)
            conflict_cols = sidecar.get_identity_columns()

            # 2. Add partitioning keys to conflict target if enabled
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 3. Finalize Payload (Inject validity, geoid, etc.)
            # We start with what prepare_upsert_payload gave us, but we MUST have the geoid from the Hub.
            # IMPORTANT: We must use the original dictionary if sc_payload was passed from outside,
            # but finalize_upsert_payload returns a NEW dictionary usually.
            # However, if we modify sc_payload in place, it might affect subsequent logic? No.

            # Inject geoid into the PAYLOAD sent to finalize_upsert_payload
            # Note: prepare_upsert_payload might have returned None or empty dict if not relevant?
            # But earlier check: if sc_payload: sidecar_payloads[sidecar.sidecar_id] = sc_payload
            # So sc_payload exists if it was prepared. If not, we skip?
            # Wait, even if empty, some sidecars (like Attributes) might need to be created with just ID?
            # AttributeSidecar usually requires attributes.

            # If sc_payload is empty (was not in sc_data_map), should we skip?
            if sc_id not in sc_data_map and not sidecar.is_mandatory():
                continue

            if "geoid" not in sc_payload:
                sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            logger.info(f"DEBUG: Upserting sidecar {sc_table} for geoid {geoid}")
            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols
            )

            # JSON-FG Place Statistics: insert into <hub_table>_place if configured
            if hasattr(sidecar, "prepare_place_upsert_payload"):
                try:
                    place_payload = sidecar.prepare_place_upsert_payload(
                        processing_context.get("_raw_item", {}), processing_context
                    )
                    if place_payload:
                        place_table = f"{hub_table}_place"
                        if "geoid" not in place_payload:
                            place_payload["geoid"] = geoid
                        await self._upsert_sidecar_table_raw(
                            conn, schema, place_table, place_payload, conflict_cols=["geoid"]
                        )
                        logger.info(f"DEBUG: Upserted place stats into {schema}.{place_table} for geoid {geoid}")
                except Exception as e:
                    logger.warning(f"Place stats upsert skipped for geoid {geoid}: {e}")

        feature_id_provider = next(
            (sc for sc in sidecars if sc.provides_feature_id), None
        )
        res = await get_item_query.execute(
            conn,
            catalog_id=schema,
            collection_id=hub_table,
            where="h.geoid = :target_geoid",
            target_geoid=geoid,
            include_deleted=True,
            col_config=col_config,
            feature_id_provider=feature_id_provider,
        )
        logger.debug(f"FINAL RESULT FROM EXECUTOR: {res}")
        return res

    async def _execute_distributed_update(
        self,
        conn,
        schema,
        hub_table,
        geoid,
        hub_data,
        sc_data_map,
        col_config,
        sidecars=None,
        processing_context=None,
        active_rec=None,
    ) -> Dict[str, Any]:
        """Performs updates across Hub and all sidecars."""
        # A. Update Hub
        hub_row = await self._update_table_raw(conn, schema, hub_table, geoid, hub_data)
        # Handle cases where update might return None or a Row
        if not hub_row:
            return None

        row_data = hub_row._mapping if hasattr(hub_row, "_mapping") else hub_row
        res_geoid = row_data["geoid"]

        # B. Resolve Identity and Finalize Payloads for Sidecars
        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # 1. Identity Columns
            conflict_cols = sidecar.get_identity_columns()
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 2. Finalize Payload
            if "geoid" not in sc_payload:
                sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols
            )
        # Determine feature ID provider to ensure correct alias mapping
        feature_id_provider = next(
            (sc for sc in sidecars if sc.provides_feature_id), None
        )

        return await get_item_query.execute(
            conn,
            catalog_id=schema,
            collection_id=hub_table,
            where="h.geoid = :lookup_geoid",
            lookup_geoid=str(geoid),
            col_config=col_config,
            feature_id_provider=feature_id_provider,
        )

    async def _insert_table_raw(self, conn, schema, table, data) -> Dict[str, Any]:
        """Generic table insert (No special geometry handling here, already processed by sidecars)."""
        cols = []
        vals = []
        params = {}
        for k, v in data.items():
            # Skip internal keys if any
            cols.append(f'"{k}"')
            vals.append(f":{k}")
            params[k] = v

        sql = f'INSERT INTO "{schema}"."{table}" ({", ".join(cols)}) VALUES ({", ".join(vals)}) RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    async def _update_table_raw(
        self, conn, schema, table, geoid, data
    ) -> Dict[str, Any]:
        """Generic table update by geoid."""
        clauses = []
        params = {"geoid": geoid}
        for k, v in data.items():
            if k == "geoid":
                continue
            clauses.append(f'"{k}" = :{k}')
            params[k] = v

        sql = f'UPDATE "{schema}"."{table}" SET {", ".join(clauses)} WHERE geoid = :geoid RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    async def _upsert_sidecar_table_raw(
        self, conn, schema, table, data, conflict_cols: List[str] = ["geoid"]
    ):
        """Sidecar upsert with ON CONFLICT (conflict_cols)."""
        cols: list = []
        vals: list = []
        updates: list = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
            # Geometry columns: pass WKB hex through ST_GeomFromEWKB
            if k in ["geom", "bbox_geom", "centroid"] and isinstance(v, str):
                vals.append(f"ST_GeomFromEWKB(decode(:{k}, 'hex'))")
                params[k] = v
            # Range columns (e.g. validity TSTZRANGE): duck-type for any Range-like
            # object (asyncpg.Range, etc.) which psycopg2 cannot serialise directly.
            # Expand into lower/upper params and emit tstzrange() so both drivers work.
            elif hasattr(v, "lower") and hasattr(v, "upper") and hasattr(v, "lower_inc"):
                lk, uk = f"{k}_lower", f"{k}_upper"
                lb = "[" if v.lower_inc else "("
                ub = "]" if v.upper_inc else ")"
                vals.append(f"tstzrange(:{lk}, :{uk}, '{lb}{ub}')")
                params[lk] = v.lower
                params[uk] = v.upper
            else:
                vals.append(f":{k}")
                params[k] = v
            if k not in conflict_cols:
                updates.append(f'"{k}" = EXCLUDED."{k}"')

        # Handle composite PK in ON CONFLICT if partitioned?
        # Actually sidecar PK is (partition_keys, geoid).
        # We need the full PK in ON CONFLICT.
        # For now, we assume geoid is unique enough if we have partition pruning.
        # But Postgres requires the full index for ON CONFLICT.

        sql = f"""
INSERT INTO "{schema}"."{table}" ({", ".join(cols)}) 
VALUES ({", ".join(vals)})
ON CONFLICT ({", ".join([f'"{c}"' for c in conflict_cols])}) DO UPDATE SET {", ".join(updates)};
"""
        await DDLQuery(sql).execute(conn, **params)

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

            except Exception as e:
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

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Deletes a specific language translation for an item."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=db_resource)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

        async with managed_transaction(db_resource or self.engine) as conn:
            columns = await self.get_collection_column_names(
                catalog_id, collection_id, db_resource=conn
            )

            updates = []
            if "title" in columns:
                updates.append("title = title - :lang")
            if "description" in columns:
                updates.append("description = description - :lang")

            if not updates:
                return 0

            sql = f'UPDATE "{phys_schema}"."{phys_table}" SET {", ".join(updates)} WHERE external_id = :item_id AND deleted_at IS NULL;'
            return await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, item_id=item_id, lang=lang
            )

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

