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
    DDLQuery, DQLQuery, GeoDQLQuery, DbResource, ResultHandler, managed_transaction, BuilderResult
)
from dynastore.modules.catalog.models import ItemDataForDB, Collection, Catalog
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig, COLLECTION_PLUGIN_CONFIG_ID, VersioningBehaviorEnum
)
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.modules.catalog.sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.tools import recalculate_and_update_extents
from dynastore.modules.db_config import shared_queries
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.catalog.query_orchestrator import CatalogQueryOrchestrator

logger = logging.getLogger(__name__)

# --- Specialized Queries for ItemService ---

async def _get_item_by_geoid_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
    
    col_config = params['col_config']
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    
    select_fields = ["h.*"]
    from_clause = f'"{phys_schema}"."{phys_table}" h'
    joins = []
    
    # Delegate query generation to sidecars
    if col_config.sidecars:
        for sc_config in col_config.sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            select_fields.extend(sidecar.get_select_fields(hub_alias="h", include_all=True))
            joins.append(sidecar.get_join_clause(phys_schema, phys_table, hub_alias="h"))
    
    # Hub-level temporal filtering
    sql = f'SELECT {", ".join(select_fields)} FROM {from_clause} {" ".join(joins)} WHERE h.geoid = :geoid AND h.deleted_at IS NULL LIMIT 1;'
    return text(sql), params

get_item_by_geoid_query = GeoDQLQuery.from_builder(_get_item_by_geoid_builder, result_handler=ResultHandler.ONE)

async def _get_all_versions_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
    
    col_config = params['col_config']
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    ext_id = params['ext_id']
    
    select_fields = ["h.*"]
    from_clause = f'"{phys_schema}"."{phys_table}" h'
    joins = []
    where_conditions = []
    
    # Delegate to sidecars for SELECT and JOIN
    if col_config.sidecars:
        for sc_config in col_config.sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            select_fields.extend(sidecar.get_select_fields(hub_alias="h", include_all=True))
            joins.append(sidecar.get_join_clause(phys_schema, phys_table, hub_alias="h", join_type="INNER"))
            # Get WHERE conditions for external_id filtering
            sc_conditions = sidecar.get_where_conditions(external_id=ext_id)
            where_conditions.extend(sc_conditions)
    
    # Build WHERE clause
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Hub-level temporal filtering and ordering
    sql = f'SELECT {", ".join(select_fields)} FROM {from_clause} {" ".join(joins)} WHERE {where_clause} AND h.deleted_at IS NULL ORDER BY lower(h.validity) DESC;'
    return text(sql), params

get_all_versions_by_external_id_query = GeoDQLQuery.from_builder(_get_all_versions_builder, result_handler=ResultHandler.ALL)

async def _get_version_at_timestamp_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
    
    col_config = params['col_config']
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    ext_id = params['ext_id']
    timestamp = params['timestamp']
    
    select_fields = ["h.*"]
    from_clause = f'"{phys_schema}"."{phys_table}" h'
    joins = []
    where_conditions = []
    
    # 1. Identify Attribute Sidecar for Identity & Validity
    sc_attr = next((sc for sc in col_config.sidecars if sc.sidecar_type == "attributes"), None)
    
    # Delegate to sidecars for SELECT and JOIN
    if col_config.sidecars:
        for sc_config in col_config.sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            select_fields.extend(sidecar.get_select_fields(hub_alias="h", include_all=True))
            joins.append(sidecar.get_join_clause(phys_schema, phys_table, hub_alias="h", join_type="INNER"))
            # Get WHERE conditions for external_id filtering if this sidecar handles it
            if sidecar.config == sc_attr: # Only attributes sidecar handles identity (?)
                 sc_conditions = sidecar.get_where_conditions(external_id=ext_id)
                 where_conditions.extend(sc_conditions)
    
    # Validity Check: NOW in Attribute Sidecar
    if sc_attr and sc_attr.enable_validity:
        # We need alias for attribute sidecar. get_join_clause uses "sc_{sidecar_id}" by default.
        attr_alias = f"sc_{sc_attr.sidecar_id}" 
        where_conditions.append(f"{attr_alias}.validity @> :timestamp::timestamptz")
    else:
        # Fallback to Hub if validity not in sidecar (Legacy/Migration?)
        # Or if not enabled, maybe we don't check validity? 
        # But method is "get_version_at_timestamp".
        # Assuming Hub has it if sidecar doesn't? No, we removed it from Hub config.
        # If validity is disabled, this query might not make sense or acts as "latest"?
        # For now, if no validity col, we ignore timestamp? OR fail?
        pass

    # Build WHERE clause
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Hub-level temporal filtering (deleted_at is still on Hub)
    sql = f'SELECT {", ".join(select_fields)} FROM {from_clause} {" ".join(joins)} WHERE {where_clause} AND h.deleted_at IS NULL LIMIT 1;'
    return text(sql), params

get_version_at_timestamp_by_external_id_query = GeoDQLQuery.from_builder(_get_version_at_timestamp_builder, result_handler=ResultHandler.ONE)

async def _get_active_row_by_external_id_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
    
    col_config = params['col_config']
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    ext_id = params['ext_id']
    
    select_fields = ["h.*"]
    from_clause = f'"{phys_schema}"."{phys_table}" h'
    joins = []
    where_conditions = []
    
    # Delegate to sidecars for SELECT and JOIN
    if col_config.sidecars:
        for sc_config in col_config.sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            select_fields.extend(sidecar.get_select_fields(hub_alias="h", include_all=True))
            joins.append(sidecar.get_join_clause(phys_schema, phys_table, hub_alias="h", join_type="INNER"))
            # Get WHERE conditions for external_id filtering
            sc_conditions = sidecar.get_where_conditions(external_id=ext_id)
            where_conditions.extend(sc_conditions)
    
    # Build WHERE clause
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Hub-level temporal filtering and ordering
    sql = f'SELECT {", ".join(select_fields)} FROM {from_clause} {" ".join(joins)} WHERE {where_clause} AND h.deleted_at IS NULL ORDER BY lower(h.validity) DESC LIMIT 1;'
    return text(sql), params

get_active_row_by_external_id_query = GeoDQLQuery.from_builder(_get_active_row_by_external_id_builder, result_handler=ResultHandler.ONE_OR_NONE)

# Remove static queries that are now replaced by builders or unused
# get_active_record_by_external_id_query = ... (Already specific to legacy logic)

async def _soft_delete_by_external_id_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    col_config = params['col_config']
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    
    sc_attr = next((sc for sc in col_config.sidecars if sc.sidecar_type == "attributes" and getattr(sc, "enable_external_id", False)), None)
    
    if sc_attr:
        sc_table = f"{phys_table}_{sc_attr.sidecar_id}"
        # UPDATE Hub ... FROM Sidecar
        sql = f"""
            UPDATE "{phys_schema}"."{phys_table}" h
            SET deleted_at = NOW()
            FROM "{phys_schema}"."{sc_table}" s
            WHERE h.geoid = s.geoid 
              AND s.external_id = CAST(:ext_id AS TEXT) 
              AND h.deleted_at IS NULL
        """
        return text(sql), params
    
    # Fallback/Error if no external ID sidecar?
    # Cannot soft delete by external ID if we don't know where it is.
    raise ValueError("Cannot soft delete by external_id: No attributes sidecar with external_id enabled.") 

soft_delete_item_query = DQLQuery("UPDATE {catalog_id}.{collection_id} SET deleted_at = NOW() WHERE geoid = :geoid AND deleted_at IS NULL;", result_handler=ResultHandler.ROWCOUNT)

soft_delete_item_by_external_id_query = DQLQuery.from_builder(_soft_delete_by_external_id_builder, result_handler=ResultHandler.ROWCOUNT)

async def _expire_old_version_builder(conn: DbResource, params: Dict[str, Any]) -> BuilderResult:
    col_config = params.get('col_config')
    # If no config provided (should rely on caller), fallback or error?
    # Caller (insert_or_update_distributed) has col_config.
    
    phys_schema = params['catalog_id']
    phys_table = params['collection_id']
    
    # Locate attribute sidecar
    # We assume 'attributes' sidecar holds validity
    target_table = phys_table
    is_sidecar = False
    
    if col_config and col_config.sidecars:
        sc_attr = next((sc for sc in col_config.sidecars if sc.sidecar_type == "attributes"), None)
        if sc_attr and sc_attr.enable_validity:
            target_table = f"{phys_table}_{sc_attr.sidecar_id}"
            is_sidecar = True
    
    # If validity is not enabled or no attribute sidecar, where is it?
    # If removed from Hub, it MUST be in sidecar. 
    # If not found, we might fallback to Hub (legacy) or do nothing?
    # For now, let's assume if not in sidecar, it might still be in Hub during migration?
    # But user said "validity is a single column managed by the attribute sidecar".
    
    sql = f'UPDATE "{phys_schema}"."{target_table}" SET validity = tstzrange(lower(validity), :expire_at, \'[)\') WHERE geoid = :geoid'
    
    # If we are updating sidecar, we might need to check if row exists? 
    # Usually it does if we are expiring an old version.
    
    return text(sql + ";"), params

expire_old_version_query = GeoDQLQuery.from_builder(_expire_old_version_builder, result_handler=ResultHandler.ROWCOUNT)

class ItemService:
    """Service for item-level operations."""
    
    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine

    async def initialize(self, app_state: Any, db_resource: Optional[DbResource] = None):
        """Initializes the service with database connection."""
        if not self.engine:
            self.engine = db_resource

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(self, catalog_id: str, db_resource: Optional[DbResource] = None) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)

    async def _resolve_physical_table(self, catalog_id: str, collection_id: str, db_resource: Optional[DbResource] = None) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=db_resource)

    async def _get_collection_config(
        self, 
        catalog_id: str, 
        collection_id: str, 
        config_provider: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None
    ) -> Any: # Return type should be CollectionConfig from dynastore.modules.catalog.config
        """Helper to get collection configuration."""
        configs = config_provider or get_protocol(ConfigsProtocol)
        return await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=db_resource or self.engine)

    async def upsert(
        self, 
        catalog_id: str, 
        collection_id: str, 
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any], 
        db_resource: Optional[DbResource] = None,
        processing_context: Optional[Dict[str, Any]] = None
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
        elif isinstance(items, dict) and items.get('type') == 'FeatureCollection':
             items_list = items.get('features', [])
        elif hasattr(items, 'features') and items.type == 'FeatureCollection':
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
            col_config = await catalogs.get_collection_config(catalog_id, collection_id, db_resource=conn)
            
            # Resolve Physical Table (Hub)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_table:
                 # Fallback to promotion if missing (legacy)
                 await self.ensure_physical_table_exists(catalog_id, collection_id, col_config, db_resource=conn)
                 phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
                 
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            
            # Instantiate Sidecars
            sidecars = []
            if col_config.sidecars:
                from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
                for sc_config in col_config.sidecars:
                    try:
                        sidecars.append(SidecarRegistry.get_sidecar(sc_config))
                    except ValueError as e:
                        logger.warning(f"Skipping sidecar instantiation: {e}")

            created_rows = []
            
            for item_data in items_list:
                # 1. Normalize Input to Dict
                raw_item = {}
                if hasattr(item_data, 'model_dump'):
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
                    **(processing_context or {})
                }

                # Hub Payload (Base identity and temporal)
                hub_payload = {
                    "geoid": geoid,
                    "transaction_time": datetime.now(timezone.utc),
                    "deleted_at": None
                }
                
                # Extract partition keys if any
                partition_values = {}
                
                for sidecar in sidecars:
                    # A. Validation
                    val_result = sidecar.validate_insert(raw_item, item_context)
                    if not val_result.valid:
                        raise ValueError(f"Sidecar {sidecar.sidecar_id} rejected item: {val_result.error}")
                        
                    # B. Prepare Payload
                    sc_payload = sidecar.prepare_upsert_payload(raw_item, item_context)
                    if sc_payload:
                        sidecar_payloads[sidecar.sidecar_id] = sc_payload
                        
                        # C. Capture Partition Key matching
                        if col_config.partitioning.enabled and col_config.partitioning.partition_keys:
                            for pk in col_config.partitioning.partition_keys:
                                if pk in sc_payload:
                                    partition_values[pk] = sc_payload[pk]
                                    item_context[pk] = sc_payload[pk]

                # 4. Insert/Update Logic (Distributed)
                # Ensure Partitions exist for Hub
                if col_config.partitioning.enabled and partition_values:
                     # For simplicity, we assume one level of list partitioning for the tool helper
                     # If composite, we might need a more complex helper.
                     # Currently dynastore handles simple list/range.
                     p_val = list(partition_values.values())[0] if partition_values else None
                     await self.ensure_partition_exists(catalog_id, collection_id, col_config, p_val, db_resource=conn)
                
                # Resolve External ID (from context, populated by sidecars)
                external_id = item_context.get("external_id")
                
                # External ID is NOT stored in Hub. It is only used for lookup.
                # Attributes sidecar handles storage.
                
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
                    processing_context=item_context
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
                    raise RuntimeError(f"Failed to upsert item. External ID: {external_id}")
            
            # Recalculate extents once at the end
            await recalculate_and_update_extents(conn, catalog_id, collection_id)
            
            # Return valid results
            results = []
            for row in created_rows:
                if hasattr(row, '_mapping'):
                    results.append(dict(row._mapping))
                else:
                    results.append(dict(row))
            
            return results[0] if is_single else results

    async def get_item(self, catalog_id: str, collection_id: str, item_id: str, db_resource: Optional[DbResource] = None) -> Optional[Dict[str, Any]]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_schema or not phys_table:
                return None
            
            configs = get_protocol(ConfigsProtocol)
            col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
            
            return await get_item_by_geoid_query.execute(
                conn, 
                catalog_id=phys_schema, 
                collection_id=phys_table, 
                geoid=item_id,
                col_config=col_config
            )

    async def get_all_versions(self, catalog_id: str, collection_id: str, ext_id: str, db_resource: Optional[DbResource] = None) -> List[Dict[str, Any]]:
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_schema or not phys_table: return []
            
            configs = get_protocol(ConfigsProtocol)
            col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)

            return await get_all_versions_by_external_id_query.execute(
                conn, 
                catalog_id=phys_schema, 
                collection_id=phys_table, 
                ext_id=ext_id,
                col_config=col_config
            )

    async def get_version_at_timestamp_by_external_id(self, catalog_id: str, collection_id: str, ext_id: str, timestamp: datetime, db_resource: Optional[DbResource] = None) -> Optional[Dict[str, Any]]:
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_table: return None
            
            configs = get_protocol(ConfigsProtocol)
            col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)

            return await get_version_at_timestamp_by_external_id_query.execute(
                conn, 
                catalog_id=phys_schema, 
                collection_id=phys_table, 
                ext_id=ext_id, 
                timestamp=timestamp,
                col_config=col_config
            )

    @asynccontextmanager
    async def _prepare_search(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None
    ):
        """
        Shared context preparation for search and stream operations.
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            col_config = await self._get_collection_config(catalog_id, collection_id, config, db_resource=conn)
            
            # Resolve physical names for SQL generation
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            
            optimizer = QueryOptimizer(col_config)
            sql, params = optimizer.build_optimized_query(
                request, 
                schema=phys_schema, 
                table=phys_table
            )
            
            params.update({
                'col_config': col_config,
                'catalog_id': catalog_id,
                'collection_id': collection_id
            })
            
            # Create query object with GeoDQLQuery for spatial serialization
            query = GeoDQLQuery(text(sql), result_handler=ResultHandler.ALL)
            yield query, conn, params

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Search and retrieve items using optimized query generation.
        """
        async with self._prepare_search(catalog_id, collection_id, request, config, db_resource) as (query, conn, params):
            return await query.execute(conn, **params)

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream search results using an async iterator.
        """
        async with self._prepare_search(catalog_id, collection_id, request, config, db_resource) as (query, conn, params):
            async for item in await query.stream(conn, **params):
                yield item

    async def stream_items_from_query(
        self,
        catalog_id: str,
        collection_id: str,
        where_clause: str,
        query_params: Optional[Dict[str, Any]] = None,
        select_columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        db_resource: Optional[Any] = None
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
            col_config = await catalogs.get_collection_config(catalog_id, collection_id, db_resource=conn)
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)

            if not phys_schema or not phys_table:
                raise ValueError(f"Collection '{catalog_id}/{collection_id}' not found.")

            orchestrator = CatalogQueryOrchestrator(col_config)

            columns_to_select = select_columns
            if not columns_to_select:
                from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
                all_fields = {'geoid', 'validity', 'deleted_at'}
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
                limit=limit
            )

            if offset is not None:
                query_string += f" OFFSET {offset}"

            query = GeoDQLQuery(text(query_string), result_handler=ResultHandler.ALL)
            async for item in await query.stream(conn, **(query_params or {})):
                yield item

    async def get_active_row_by_external_id(self, phys_schema: str, phys_table: str, ext_id: str, logical_catalog_id: str, logical_collection_id: str, db_resource: Optional[DbResource] = None) -> Optional[Dict[str, Any]]:
        # Pass both physical and logical IDs to the query builder
        async with managed_transaction(db_resource or self.engine) as conn:
            configs = get_protocol(ConfigsProtocol)
            col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, logical_catalog_id, logical_collection_id, db_resource=conn)

            params = {
                'catalog_id': phys_schema,
                'collection_id': phys_table,
                'ext_id': ext_id,
                'col_config': col_config
            }
            return await get_active_row_by_external_id_query.execute(conn, **params)

    async def delete_item(self, catalog_id: str, collection_id: str, item_id: str, db_resource: Optional[DbResource] = None) -> int:
        validate_sql_identifier(catalog_id); validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_schema or not phys_table: return 0
            
            # Try deletion by geoid (if it looks like a UUID)
            import uuid
            is_uuid = False
            try:
                uuid.UUID(str(item_id))
                is_uuid = True
            except ValueError:
                pass
                
            rows = 0
            if is_uuid:
                rows = await soft_delete_item_query.execute(conn, catalog_id=phys_schema, collection_id=phys_table, geoid=item_id)
            
            # If not deleted by geoid (or not a UUID), try by external_id
            if rows == 0:
                configs = get_protocol(ConfigsProtocol)
                col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)

                rows = await soft_delete_item_by_external_id_query.execute(
                    conn, 
                    catalog_id=phys_schema, 
                    collection_id=phys_table, 
                    ext_id=item_id,
                    col_config=col_config
                )
    
            if rows > 0: await recalculate_and_update_extents(conn, catalog_id, collection_id)
        return rows

    async def delete_item_language(self, catalog_id: str, collection_id: str, item_id: str, lang: str, db_resource: Optional[DbResource] = None) -> int:
        """
        Deletes a specific language variant from an item's attributes.
        Actually, it looks for localized fields in 'attributes' and removes the key.
        """
        validate_sql_identifier(catalog_id); validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_schema or not phys_table: return 0
            
            # Fetch the item to identify localized fields in attributes
            item = await self.get_item(catalog_id, collection_id, item_id, db_resource=conn)
            if not item:
                return 0
            
            attributes = item.get('attributes', {})
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
            rows = await DQLQuery(update_sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, 
                attr=json.dumps(attributes, cls=CustomJSONEncoder), 
                geoid=item_id
            )
            return rows

    async def ensure_physical_table_exists(self, catalog_id: str, collection_id: str, col_config: CollectionPluginConfig, db_resource: Optional[DbResource] = None):
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.locking_tools import acquire_startup_lock
            async with acquire_startup_lock(conn, f"promote_{catalog_id}_{collection_id}"):
                phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
                phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
                force_create = False
                if not phys_table:
                    logger.info(f"Promoting collection {catalog_id}:{collection_id} to physical storage.")
                    phys_table = collection_id
                    force_create = True
                    catalogs = get_protocol(CatalogsProtocol)
                    await catalogs.set_physical_table(catalog_id, collection_id, phys_table, db_resource=conn)
                else:
                    catalogs = get_protocol(CatalogsProtocol)

                if force_create or not await shared_queries.table_exists_query.execute(conn, schema=phys_schema, table=phys_table):
                    await catalogs.create_physical_collection(
                        conn, phys_schema, catalog_id, collection_id, physical_table=phys_table, layer_config=col_config.model_dump()
                    )

    async def ensure_partition_exists(self, catalog_id: str, collection_id: str, col_config: CollectionPluginConfig, partition_value: Any, db_resource: Optional[DbResource] = None):
        partitioning = col_config.partitioning
        if not partitioning.enabled or not partitioning.partition_keys: return
        # Current simplify: we use the first partition key for the physical partition routing
        # In a fully composite world, partition_value should be a list/tuple.
        if partition_value is None: return
        if partition_value is None: return
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not phys_schema or not phys_table: return
            # Determine strategy for the tool (simplified)
            # If the value is a date/datetime, use RANGE, else LIST
            from datetime import date, datetime
            tool_strategy = 'RANGE' if isinstance(partition_value, (date, datetime)) else 'LIST'
            interval = None # We might need to derive this if we had TimePartitionStrategy
            
            from dynastore.modules.db_config.partition_tools import ensure_partition_exists as ensure_partition_tool
            await ensure_partition_tool(conn=conn, table_name=phys_table, strategy=tool_strategy, partition_value=partition_value, schema=phys_schema, interval=interval, parent_table_name=phys_table, parent_table_schema=phys_schema)

    async def insert_or_update_distributed(
        self, 
        conn: DbResource, 
        catalog_id: str, 
        collection_id: str, 
        hub_payload: Dict[str, Any],
        sidecar_payloads: Dict[str, Dict[str, Any]],
        col_config: CollectionPluginConfig, 
        sidecars: List[SidecarProtocol],
        processing_context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Coordinates multi-table upsert for Hub and Sidecars."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(catalog_id, collection_id, db_resource=conn)
        if not phys_table:
            phys_table = collection_id
            
        logger.info(f"DISTRIBUTED UPSERT: collection={catalog_id}.{collection_id}, phys={phys_schema}.{phys_table}, sidecars={[s.sidecar_id for s in sidecars]}")
        

        # Extract decentralized identities
        external_id = processing_context.get("external_id")
        asset_id = processing_context.get("asset_id")

        # 1. Versioning Logic Check on Hub
        ingestion_config = await get_protocol(ConfigsProtocol).get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
        versioning_behavior = getattr(ingestion_config, 'versioning_behavior', VersioningBehaviorEnum.ALWAYS_ADD_NEW)

        # 1.5 Acceptance Check
        for sidecar in sidecars:
            if not sidecar.is_acceptable(hub_payload, processing_context):
                logger.warning(f"Feature rejected by sidecar {sidecar.sidecar_id}")
                return None

        # Standardized Identity Resolution via Sidecar Protocol
        # We iterate sidecars to find one that can resolve the existing item.
        # AttributeSidecar implements this for external_id.
        active_rec = None
        if versioning_behavior not in [VersioningBehaviorEnum.CREATE_NEW_VERSION, VersioningBehaviorEnum.ALWAYS_ADD_NEW]:
             for sidecar in sidecars:
                 active_rec = await sidecar.resolve_existing_item(conn, phys_schema, phys_table, processing_context)
                 if active_rec:
                     logger.info(f"DISTRIBUTED UPSERT: found active record geoid={active_rec.get('geoid')} (via {sidecar.sidecar_id})")
                     break

        # 1.6 Additional Checks: Asset ID Collision
        if versioning_behavior == VersioningBehaviorEnum.REFUSE_ON_ASSET_ID_COLLISION:
            asset_id = processing_context.get("asset_id")
            if asset_id:
                # Check for collision across all sidecars that might manage asset_id
                for sidecar in sidecars:
                    if await sidecar.check_collision(conn, phys_schema, phys_table, "asset_id", asset_id):
                        logger.warning(f"Feature rejected: asset_id collision found for '{asset_id}'")
                        return None

        result = None
        # 2. Execution Path
        if not active_rec or versioning_behavior in [VersioningBehaviorEnum.CREATE_NEW_VERSION, VersioningBehaviorEnum.ALWAYS_ADD_NEW]:
            # INSERT NEW
            result = await self._execute_distributed_insert(
                conn, 
                phys_schema, 
                phys_table, 
                hub_payload, 
                sidecar_payloads, 
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context
            )
        
        elif versioning_behavior == VersioningBehaviorEnum.REJECT_NEW_VERSION:
            logger.info("DISTRIBUTED UPSERT: identity matched and REJECT_NEW_VERSION set. Skipping.")
            return None
            
        else:
            # 3. Update Validation Hooks
            processing_context["operation"] = "update"
            for sidecar in sidecars:
                val_result = sidecar.validate_update(sidecar_payloads.get(sidecar.sidecar_id, {}), active_rec, processing_context)
                if not val_result.valid:
                    raise ValueError(f"Sidecar {sidecar.sidecar_id} rejected update: {val_result.error}")

            # B. Resolve Validity for Hub & Sidecars
            # We construct it here once and Inject into Hub payload if missing
            valid_from = processing_context.get("valid_from") or datetime.now(timezone.utc)
            valid_to = processing_context.get("valid_to")
            
            # Using tuple for asyncpg/Postgres range compatibility: (Lower, Upper, Bounds)
            # asyncpg accepts a Range object or a tuple (lower, upper). 
            # For TSTZRANGE, we can also use a string representation.
            # But the most compatible way for both sync/async (if we share logic) 
            # is often a tuple or the native Range object of the driver.
            # However, ItemService is primarily async.
            
            from asyncpg import Range
            validity = Range(valid_from, valid_to, lower_inc=True, upper_inc=False)
            
            if "validity" not in hub_payload:
                 hub_payload["validity"] = validity
            if versioning_behavior == VersioningBehaviorEnum.UPDATE_EXISTING_VERSION:
                # UPDATE EXISTING (HUB + Sidecars)
                result = await self._execute_distributed_update(
                    conn, 
                    phys_schema, 
                    phys_table, 
                    active_rec["geoid"], 
                    hub_payload, 
                    sidecar_payloads, 
                    col_config=col_config,
                    processing_context=processing_context,
                    active_rec=active_rec
                )
            else:
                # 4. ARCHIVE + INSERT NEW
                expire_at = hub_payload.get('valid_from') or datetime.now(timezone.utc)
                await expire_old_version_query.execute(
                    conn, 
                    catalog_id=phys_schema, 
                    collection_id=phys_table, 
                    expire_at=expire_at, 
                    geoid=active_rec["geoid"], 
                    col_config=col_config
                )
                
                result = await self._execute_distributed_insert(
                    conn, 
                    phys_schema, 
                    phys_table, 
                    hub_payload, 
                    sidecar_payloads, 
                    col_config=col_config,
                    sidecars=sidecars,
                    processing_context=processing_context
                )

        # Link to asset if provided and valid result obtained
        if result and asset_id:
             try:
                 # Lazy import to avoid circular dependency
                 from dynastore.modules.catalog.catalog_module import get_asset_manager
                 await get_asset_manager().link_asset_to_feature(phys_schema, phys_table, feature_geoid=result['geoid'], asset_id=asset_id, db_resource=conn)
             except ImportError:
                 logger.warning("Could not link asset to feature: CatalogModule not available or circular import.")
             except Exception as e:
                 logger.error(f"Failed to link asset {asset_id} to feature {result.get('geoid')}: {e}")

        return result

    async def _execute_distributed_insert(self, conn, schema, hub_table, hub_data, sc_data_map, col_config, sidecars=None, processing_context=None) -> Dict[str, Any]:
        """Performs inserts across Hub and all sidecars."""
        # A. Insert Hub
        logger.warning(f"DEBUG: Inserting into Hub {schema}.{hub_table}")
        hub_row = await self._insert_table_raw(conn, schema, hub_table, hub_data)
        geoid = hub_row._mapping["geoid"]
        
        # B. Resolve Validity for Sidecar Inserts
        # Prefer validity from the INSERTed hub_row to ensure exact match with DB defaults/triggers
        validity = hub_row._mapping.get("validity")
        
        if not validity:
            # Fallback to hub_data or context if hub_row didn't return it (shouldn't happen if versioned)
            validity = hub_data.get("validity")
            if not validity:
                from asyncpg import Range
                # PostgreSQL TIMESTAMPTZ has microsecond precision.
                valid_from = (processing_context or {}).get("valid_from") or datetime.now(timezone.utc)
                valid_from = valid_from.replace(microsecond=(valid_from.microsecond // 1) * 1) 
                valid_to = (processing_context or {}).get("valid_to")
                if valid_to:
                     valid_to = valid_to.replace(microsecond=(valid_to.microsecond // 1) * 1)
                validity = Range(valid_from, valid_to, lower_inc=True, upper_inc=False)
        
        # B. Insert Sidecars
        conflict_cols = ["geoid"]
        if validity:
             conflict_cols.append("validity")
        
        # Partition keys MUST be included in the conflict target if partitioning is enabled,
        # as they are part of the composite primary key of the sidecar tables.
        if col_config.partitioning and col_config.partitioning.enabled:
            for key in col_config.partitioning.partition_keys:
                if key not in conflict_cols:
                    conflict_cols.insert(0, key)
                    
        # Verify if sidecar-specific conflict columns are needed (future-proofing)
        # For now, (partition_keys..., geoid, validity) covers current Attribute/Geometry sidecars.

        for sc_id, sc_payload in sc_data_map.items():
            sc_table = f"{hub_table}_{sc_id}"
            
            # Prepare payload - ALWAYS use the geoid returned by the Hub insert
            full_payload = {"geoid": geoid, **sc_payload}
            
            # Inject validity if sidecar needs it (Atttributes sidecar does)
            # We blindly inject it if not present?
            if "validity" not in full_payload:
                 full_payload["validity"] = validity
            
            logger.warning(f"DEBUG: Upserting sidecar {sc_table} with geoid {geoid} and validity {validity}")
            await self._upsert_sidecar_table_raw(conn, schema, sc_table, full_payload, conflict_cols=conflict_cols)
        
        # C. Return full combined row (Hub + Sidecars) using physical names
        return await get_item_by_geoid_query.execute(
            conn, 
            catalog_id=schema, 
            collection_id=hub_table, 
            geoid=geoid,
            col_config=col_config
        )

    async def _execute_distributed_update(self, conn, schema, hub_table, geoid, hub_data, sc_data_map, col_config, processing_context=None, active_rec=None) -> Dict[str, Any]:
        """Performs updates across Hub and all sidecars."""
        # A. Update Hub
        hub_row = await self._update_table_raw(conn, schema, hub_table, geoid, hub_data)
        # Handle cases where update might return None or a Row
        if not hub_row:
            return None
        
        row_data = hub_row._mapping if hasattr(hub_row, "_mapping") else hub_row
        res_geoid = row_data["geoid"]
        
        # B. Resolve Validity
        # Use validity from hub_data or fallback to context/active_rec
        validity = hub_data.get("validity")
        if not validity:
             if processing_context and (processing_context.get("valid_from") or processing_context.get("valid_to")):
                  from asyncpg import Range
                  v_from = processing_context.get("valid_from") or datetime.now(timezone.utc)
                  v_to = processing_context.get("valid_to")
                  validity = Range(v_from, v_to, lower_inc=True, upper_inc=False)
        elif active_rec and "validity" in active_rec:
             validity = active_rec["validity"]
        else:
             # If we can't determine validity, but we are updating sidecars...
             # The sidecar upsert might require it if it's the PK.
             # If validity is not changing, maybe we use the old one?
             # But if validity is in sidecar, we must provide it.
             # We assume default (NOW) if all else fails, but that might create a NEW version in partition logic?
             # Actually UPDATE implies modifying the EXISTING row.
             # If partitioning by validity, changing validity moves the row (new partition).
             # Postgres UPDATE handle this.
             # But we are doing _upsert_sidecar_table_raw which is INSERT ON CONFLICT DO UPDATE.
             # We need the PK.
             # If validity is PK, we need exact match to UPDATE.
             # If we pass a different validity, we INSERT a new row?
             # But we are in "UPDATE_EXISTING_VERSION" mode.
             # This implies we want to update the SAME version.
             # So we MUST use `active_rec["validity"]`.
             pass

        if not validity and active_rec:
             validity = active_rec.get("validity")
        
        # Ensure we have a validity range even if fallback
        if not validity:
            from asyncpg import Range
            valid_from = processing_context.get("valid_from") or datetime.now(timezone.utc)
            valid_to = processing_context.get("valid_to")
            validity = Range(valid_from, valid_to, lower_inc=True, upper_inc=False)

        # B. Update Sidecars (Upsert logic to be safe)
        conflict_cols = ["geoid"]
        if col_config.partitioning.enabled:
             conflict_cols = col_config.partitioning.partition_keys + conflict_cols
        
        # Determine if validity is part of conflict (PK)
        # If we use active_rec validity, we are targeting that specific row.
        # If validation is enabled, validity IS likely in conflict_cols.
        # We need to make sure we pass it.
        
        # If validity is in conflict_cols, we need to add it to conflict_cols list so upsert knows.
        # If validity is NOT in config partitioning keys but IS in sidecar PK... 
        # (AttributeSidecar puts it in PK if enable_validity is True).
        # We assume if we have a validity value, we might want to key on it.
        if validity and "validity" not in conflict_cols:
              # Only if we know sidecar expects it? 
              # Rough heuristic: if we have it, key on it.
              conflict_cols.append("validity")

        for sc_id, sc_payload in sc_data_map.items():
            sc_table = f"{hub_table}_{sc_id}"
            full_payload = {"geoid": geoid, **sc_payload}
            if validity:
                full_payload["validity"] = validity
            
            await self._upsert_sidecar_table_raw(conn, schema, sc_table, full_payload, conflict_cols=conflict_cols)
        
        return await get_item_by_geoid_query.execute(
            conn, 
            catalog_id=schema, 
            collection_id=hub_table, 
            geoid=geoid,
            col_config=col_config
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
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn, **params)

    async def _update_table_raw(self, conn, schema, table, geoid, data) -> Dict[str, Any]:
        """Generic table update by geoid."""
        clauses = []
        params = {"geoid": geoid}
        for k, v in data.items():
            if k == "geoid": continue
            clauses.append(f'"{k}" = :{k}')
            params[k] = v
            
        sql = f'UPDATE "{schema}"."{table}" SET {", ".join(clauses)} WHERE geoid = :geoid RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn, **params)

    async def _upsert_sidecar_table_raw(self, conn, schema, table, data, conflict_cols: List[str] = ["geoid"]):
        """Sidecar upsert with ON CONFLICT (conflict_cols)."""
        cols = []
        vals = []
        updates = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
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
