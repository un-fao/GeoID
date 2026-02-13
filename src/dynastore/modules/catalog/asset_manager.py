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

import uuid
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Union, Callable, Awaitable, Annotated
from sqlalchemy import text
from async_lru import alru_cache
from pydantic import BaseModel, Field, ConfigDict, AliasChoices

from dynastore.modules.db_config.query_executor import (
    DQLQuery, DDLQuery, ResultHandler, managed_transaction, DbResource, DbConnection, PydanticResultHandler, GeoDQLQuery
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.db_config.locking_tools import acquire_startup_lock
from dynastore.modules.db_config.partition_tools import (
    ensure_partition_exists as ensure_partition_tool,
    ensure_hierarchical_partitions_exist,
    PartitionDefinition
)
from dynastore.modules.catalog.models import EventType
from dynastore.models.protocols.assets import AssetsProtocol
from enum import Enum

logger = logging.getLogger(__name__)

# --- Asset-Specific Enums ---

CATALOG_LEVEL_COLLECTION_ID = "_catalog_"

class AssetTypeEnum(str, Enum):
    VECTORIAL = "VECTORIAL"
    RASTER = "RASTER"
    ASSET = "ASSET"

class AssetEventType(EventType):
    ASSET_CREATED = "asset_created"
    ASSET_UPDATED = "asset_updated"
    ASSET_DELETED = "asset_deleted"
    ASSET_HARD_DELETED = "asset_hard_deleted"
    ASSET_MAP_LINKED = "asset_map_linked"

# --- Asset Models ---

class AssetBase(BaseModel):
    """Core fields for an Asset."""
    asset_id: str = Field(..., description="Unique logical identifier for the asset.")
    uri: str = Field(..., description="URI pointing to the asset location (e.g., gs://bucket/path/to/asset.tif).")
    asset_type: AssetTypeEnum = Field(AssetTypeEnum.ASSET, description="Type of the asset. Could be VECTORIAL, RASTER, or generic ASSET.", examples=["VECTORIAL", "RASTER", "ASSET"])
    metadata: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Arbitrary metadata associated with the asset.",
        examples=[{"owner": "", "provider": ""}])

    model_config = ConfigDict(populate_by_name=True)

class AssetUpdate(BaseModel):
    """Mutable fields for an Asset."""
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary metadata for the asset.")

class AssetUploadDefinition(BaseModel):
    """A model for defining an asset during upload initiation, where the URI is not yet known."""
    asset_id: str = Field(..., description="Unique logical identifier for the asset.")
    asset_type: AssetTypeEnum = Field(AssetTypeEnum.ASSET, description="Type of the asset.")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary metadata for the asset.")

    model_config = ConfigDict(populate_by_name=True)

class Asset(AssetBase):
    """Fully formed Asset retrieved from DB."""
    # asset_id inherited from AssetBase (str)
    catalog_id: Annotated[str, Field(description="The catalog ID.")]
    collection_id: Annotated[Optional[str], Field(None, description="The collection ID.")]
    created_at: datetime
    deleted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

# --- Dynamic Query Helpers ---

class FilterOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    LIKE = "like"
    ILIKE = "ilike"
    IN = "in"

class AssetFilter(BaseModel):
    field: str  # asset_id, uri, metadata.path.to.key
    op: FilterOperator = FilterOperator.EQ
    value: Any

# --- Dynamic Query Helpers ---
# Moved inside methods to allow schema injection

# Obsolete global DDL removed.
# Assets correspond to {schema}.assets in the tenant schema.

class AssetManager(AssetsProtocol):
    """Manages Asset lifecycle with advanced search and event-driven architecture."""
    
    # Protocol attributes
    priority: int = 10  # Higher priority than CatalogModule
    
    def __init__(self, engine: Optional[DbResource] = None, event_emitter: Optional[Callable] = None):
        self.engine = engine
        self._event_emitter = event_emitter

        # Instance-bound cache for assets
        self.get_asset_cached = alru_cache(maxsize=128)(self._get_asset_db)
    
    def is_available(self) -> bool:
        """Returns True if the manager is initialized and ready."""
        return self.engine is not None

    async def initialize(self, app_state: Any, db_resource: Optional[DbResource] = None):
        """Initializes the manager with database connection."""
        from dynastore.modules.db_config.tools import get_any_engine
        from dynastore.modules.catalog.event_manager import emit_event
        
        # 1. Resolve Engine
        if not self.engine:
            self.engine = db_resource or get_any_engine(app_state)
            
        if not self.engine:
            logger.warning("AssetManager: No database engine available during initialization.")
            return

        # 2. Resolve Dependencies
        if not self._event_emitter:
            self._event_emitter = emit_event
            
        logger.info("AssetManager initialized.")
        # No global initialization required for cellular assets.
        # Tenant tables are created by catalog_module.create_catalog.
        pass

    async def _resolve_schema(self, catalog_id: str, db_resource: DbResource) -> Optional[str]:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)

    def _get_partition_name(self, catalog_id: str, collection_id: str) -> str:
        """Generates the partition name for a given catalog and collection."""
        # Standardize: remove dots or other non-identifier chars if necessary, but here we assume safe
        return f"assets_{catalog_id}_{collection_id}"


    # --- Retrieval & Advanced Search ---

    async def _get_asset_db(self, asset_id: str, catalog_id: str, collection_id: str) -> Optional[Asset]:
        async with managed_transaction(self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema: return None
            # asset_id in DB is VARCHAR now
            sql = f"SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata FROM \"{phys_schema}\".assets WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id AND deleted_at IS NULL;"
            return await DQLQuery(sql, result_handler=PydanticResultHandler.pydantic_one(Asset)).execute(
                conn, asset_id=asset_id, catalog_id=catalog_id, collection_id=collection_id
            )

    async def get_asset(self, asset_id: str, catalog_id: str, collection_id: Optional[str] = None, db_resource: Optional[DbResource] = None) -> Optional[Asset]:
        """
        Get asset by ID.
        """
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._resolve_schema(catalog_id, conn)
                if not phys_schema: return None
                sql = f"SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata FROM \"{phys_schema}\".assets WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id AND deleted_at IS NULL;"
                return await DQLQuery(sql, result_handler=PydanticResultHandler.pydantic_one(Asset)).execute(
                    conn, asset_id=asset_id, catalog_id=catalog_id, collection_id=target_col_id
                )
        return await self.get_asset_cached(asset_id, catalog_id, target_col_id)
    
    # Deprecated alias methods for backward compatibility if needed, but we should switch to get_asset using internal logic
    async def get_asset_by_code(self, catalog_id: str, asset_code: str, collection_id: Optional[str] = None, db_resource: Optional[DbResource] = None) -> Optional[Asset]:
        # 'code' is now 'asset_id'. This is just an alias.
        return await self.get_asset(asset_code, catalog_id, collection_id, db_resource)
    
    # _get_asset_by_code_cached removed as it's redundant with _get_asset_cached

    async def list_assets(self, catalog_id: str, collection_id: Optional[str] = None, limit: int = 10, offset: int = 0, db_resource: Optional[DbResource] = None) -> List[Asset]:
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema: return []

            sql = f"""
            SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata FROM "{phys_schema}".assets 
            WHERE catalog_id = :catalog_id 
              AND collection_id = :collection_id
              AND deleted_at IS NULL
            ORDER BY created_at DESC LIMIT :limit OFFSET :offset;
            """
            return await DQLQuery(sql, result_handler=PydanticResultHandler.pydantic_all(Asset)).execute(
                conn, catalog_id=catalog_id, collection_id=target_col_id, limit=limit, offset=offset
            )

    async def search_assets(
        self, 
        catalog_id: str, 
        filters: List[AssetFilter], 
        collection_id: Optional[str] = None,
        limit: int = 10, 
        offset: int = 0,
        db_resource: Optional[DbResource] = None
    ) -> List[Asset]:
        """
        Performs a granular search across assets using a list of filters.
        Supports metadata path extraction (e.g., 'metadata.provider.name').
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema: return []

            sql_base = f"SELECT asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata FROM \"{phys_schema}\".assets WHERE catalog_id = :catalog_id AND deleted_at IS NULL"
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}
            
            target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
            sql_base += " AND collection_id = :collection_id"
            params["collection_id"] = target_col_id

            op_map = {
                FilterOperator.EQ: "=", FilterOperator.NE: "!=", 
                FilterOperator.GT: ">", FilterOperator.GTE: ">=", 
                FilterOperator.LT: "<", FilterOperator.LTE: "<=",
                FilterOperator.LIKE: "LIKE", FilterOperator.ILIKE: "ILIKE",
                FilterOperator.IN: "IN"
            }

            for i, f in enumerate(filters):
                # Parse field (handle JSONB paths)
                if f.field.startswith("metadata."):
                    parts = f.field.split(".")
                    root = parts[0]
                    path = "->".join(f"'{p}'" for p in parts[1:-1])
                    leaf = f"->>'{parts[-1]}'"
                    field_expr = f"{root}{'->' + path if path else ''}{leaf}"
                elif f.field == "id":
                    field_expr = '"asset_id"' # Map 'id' filter to 'asset_id' column
                else:
                    # Basic fields: uri, asset_type, asset_id
                    validate_sql_identifier(f.field)
                    field_expr = f'"{f.field}"'

                val_key = f"val_{i}"
                sql_base += f" AND {field_expr} {op_map[f.op]} :{val_key}"
                
                # Formatting for LIKE/ILIKE
                if f.op in [FilterOperator.LIKE, FilterOperator.ILIKE] and "%" not in str(f.value):
                    params[val_key] = f"%{f.value}%"
                else:
                    params[val_key] = f.value

            sql_base += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset;"
            
            query = DQLQuery(sql_base, result_handler=PydanticResultHandler.pydantic_all(Asset))
            return await query.execute(conn, **params)

    # --- Lifecycle ---

    async def create_asset(self, catalog_id: str, asset: AssetBase, collection_id: Optional[str] = None, db_resource: Optional[DbResource] = None) -> Asset:
        target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
        
        async with managed_transaction(db_resource or self.engine) as conn:
            # Resolve physical schema
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema:
                raise ValueError(f"Catalog '{catalog_id}' does not exist or has no physical schema.")

            # Ensure partition exists for this collection in the tenant schema
            # Partitioning by collection_id
            await ensure_partition_tool(
                conn,
                table_name="assets",
                strategy="LIST",
                partition_value=target_col_id,
                schema=phys_schema,
                parent_table_name="assets",
                parent_table_schema=phys_schema
            )

            # Assign asset_id (it's mandatory from AssetBase)
            # if provided in AssetBase.asset_id, use it. But AssetBase must have asset_id.
            
            now = datetime.now(timezone.utc)
            
            insert_sql = text(f"""
                INSERT INTO "{phys_schema}".assets (asset_id, catalog_id, collection_id, asset_type, uri, created_at, metadata)
                VALUES (:asset_id, :catalog_id, :collection_id, :asset_type, :uri, :created_at, :metadata)
                ON CONFLICT (collection_id, asset_id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    metadata = EXCLUDED.metadata,
                    deleted_at = NULL
                RETURNING asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata;
            """)
            
            res_row = await DQLQuery(insert_sql, result_handler=ResultHandler.ONE_DICT).execute(conn, 
                asset_id=asset.asset_id,
                catalog_id=catalog_id,
                collection_id=target_col_id,
                asset_type=asset.asset_type.value,
                uri=asset.uri,
                created_at=now,
                metadata=json.dumps(asset.metadata, cls=CustomJSONEncoder)
            )
            
            created = Asset.model_validate(res_row)
            
            if created.collection_id == CATALOG_LEVEL_COLLECTION_ID:
                created.collection_id = None
            
            self._invalidate_cache(created.asset_id, catalog_id, target_col_id)

            if self._event_emitter:
                await self._event_emitter(AssetEventType.ASSET_CREATED, created.model_dump())
            return created

    async def update_asset(self, asset_id: str, update: AssetUpdate, catalog_id: str, collection_id: Optional[str] = None, db_resource: Optional[DbResource] = None) -> Asset:
        """
        Updates an existing asset's metadata. 
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            # 1. Fetch current asset
            current = await self.get_asset(asset_id=asset_id, catalog_id=catalog_id, collection_id=collection_id, db_resource=conn)
            if not current:
                raise ValueError(f"Asset '{asset_id}' not found in catalog '{catalog_id}'.")
            
            phys_schema = await self._resolve_schema(catalog_id, conn)
            
            # 2. Merge/Replace metadata (we'll replace for PUT)
            current.metadata = update.metadata
            
            # 3. Use direct UPDATE for metadata-only modification
            target_col_id = collection_id if collection_id else CATALOG_LEVEL_COLLECTION_ID
            
            update_sql = text(f"""
                UPDATE "{phys_schema}".assets 
                SET metadata = :metadata 
                WHERE asset_id = :asset_id AND catalog_id = :catalog_id AND collection_id = :collection_id
                RETURNING asset_id, catalog_id, collection_id, asset_type, uri, created_at, deleted_at, metadata;
            """)
            
            res_row = await DQLQuery(update_sql, result_handler=ResultHandler.ONE_DICT).execute(conn, 
                metadata=json.dumps(current.metadata, cls=CustomJSONEncoder),
                asset_id=current.asset_id,
                catalog_id=catalog_id,
                collection_id=target_col_id
            )
            updated = Asset.model_validate(res_row)
            
            if updated.collection_id == CATALOG_LEVEL_COLLECTION_ID:
                updated.collection_id = None
            
            self._invalidate_cache(updated.asset_id, catalog_id, target_col_id)
                
            if self._event_emitter:
                await self._event_emitter(AssetEventType.ASSET_UPDATED, updated.model_dump())
            return updated

    def _invalidate_cache(self, asset_id: str, catalog_id: str, collection_id: str):
        """Invalidates related cache entries."""
        self.get_asset_cached.cache_invalidate(asset_id, catalog_id, collection_id)

    async def delete_assets(self, catalog_id: str, asset_id: Optional[str] = None, collection_id: Optional[str] = None, hard: bool = False, propagate: bool = False, db_resource: Optional[DbResource] = None) -> int:
        now = datetime.now(timezone.utc)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_schema(catalog_id, conn)
            if not phys_schema: return 0

            where_clauses = ["catalog_id = :cat"]
            params = {"cat": catalog_id, "now": now}
            if asset_id:
                where_clauses.append("asset_id = :aid")
                params["aid"] = asset_id
            
            # Determine collection filter
            if collection_id:
                 where_clauses.append("collection_id = :coll")
                 params["coll"] = collection_id
            elif asset_id:
                 where_clauses.append("collection_id = :coll")
                 params["coll"] = CATALOG_LEVEL_COLLECTION_ID
            
            where_stmt = " AND ".join(where_clauses)
            
            fetch_sql = text(f"SELECT asset_id, catalog_id, collection_id FROM \"{phys_schema}\".assets WHERE {where_stmt}")

            assets = await DQLQuery(fetch_sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, **params)
            
            if not assets: return 0

            prefix = f"DELETE FROM \"{phys_schema}\".assets" if hard else f"UPDATE \"{phys_schema}\".assets SET deleted_at = :now"
            final_sql = text(f"{prefix} WHERE {where_stmt}")
            
            # Use DQLQuery with ROWCOUNT handler for UPDATE/DELETE to ensure sync/async compatibility
            # For HARD DELETE, DDLQuery could be used but DQL with ROWCOUNT is also fine and returns count
            rowcount = await DQLQuery(final_sql, result_handler=ResultHandler.ROWCOUNT).execute(conn, **params)
            
            # Invalidate cache
            for a in assets:
                self._invalidate_cache(a["asset_id"], a["catalog_id"], a["collection_id"])

            return rowcount

    async def soft_delete_asset(self, catalog_id: str, asset_id: str, collection_id: Optional[str] = None, db_resource: Optional[DbResource] = None) -> int:
        return await self.delete_assets(catalog_id, asset_id=asset_id, collection_id=collection_id, db_resource=db_resource)

    async def hard_delete_asset(self, catalog_id: str, asset_id: str, propagate: bool = False, db_resource: Optional[DbResource] = None) -> int:
        return await self.delete_assets(catalog_id, asset_id=asset_id, hard=True, propagate=propagate, db_resource=db_resource)

    async def soft_delete_collection_assets(self, catalog_id: str, collection_id: str, propagate: bool = False, db_resource: Optional[DbResource] = None) -> int:
        return await self.delete_assets(catalog_id, collection_id=collection_id, propagate=propagate, db_resource=db_resource)

    async def soft_delete_catalog_assets(self, catalog_id: str, propagate: bool = False, db_resource: Optional[DbResource] = None) -> int:
        return await self.delete_assets(catalog_id, propagate=propagate, db_resource=db_resource)

    async def ensure_asset_cleanup_trigger(self, catalog_id: str, collection_id: str, db_resource: Optional[DbResource] = None) -> None:
        # Trigger creation logic is complex in cellular mode and needs functions in tenant schema. 
        # Disabling for now to prevent errors with missing functions.
        pass