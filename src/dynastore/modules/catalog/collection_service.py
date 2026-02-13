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
from sqlalchemy import text
from async_lru import alru_cache

from dynastore.modules.db_config.query_executor import (
    DDLQuery, DQLQuery, DbResource, ResultHandler, managed_transaction
)
from dynastore.modules.catalog.models import Collection, CollectionUpdate, Catalog
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig, COLLECTION_PLUGIN_CONFIG_ID
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol, AssetsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.platform_config_manager import ConfigRegistry

logger = logging.getLogger(__name__)

class CollectionService:
    """Service for collection-level operations."""
    
    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine
        # Instance-bound caches (private)
        self._get_collection_model_cached = alru_cache(maxsize=1024)(self._get_collection_model_db)
        self._resolve_physical_table_cached = alru_cache(maxsize=1024)(self._resolve_physical_table_db)

    async def initialize(self, app_state: Any, db_resource: Optional[DbResource] = None):
        """Initializes the service with database connection."""
        if not self.engine:
            self.engine = db_resource

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(self, catalog_id: str, db_resource: Optional[DbResource] = None) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource)

    async def _resolve_physical_table_db(self, catalog_id: str, collection_id: str) -> Optional[str]:
        async with managed_transaction(self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return None
            query_sql = f'SELECT physical_table FROM "{phys_schema}".collections WHERE id = :collection_id AND deleted_at IS NULL;'
            return await DQLQuery(query_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE).execute(conn, collection_id=collection_id)

    async def resolve_physical_table(self, catalog_id: str, collection_id: str, db_resource: Optional[DbResource] = None) -> Optional[str]:
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
                if not phys_schema:
                    return None
                query_sql = f'SELECT physical_table FROM "{phys_schema}".collections WHERE id = :collection_id AND deleted_at IS NULL;'
                return await DQLQuery(query_sql, result_handler=ResultHandler.SCALAR_ONE_OR_NONE).execute(conn, collection_id=collection_id)
        return await self._resolve_physical_table_cached(catalog_id, collection_id)
        
    async def set_physical_table(self, catalog_id: str, collection_id: str, physical_table: str, db_resource: Optional[DbResource] = None) -> None:
        """Sets the physical table for a collection and invalidates the cache."""
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                raise ValueError(f"Physical schema not found for catalog '{catalog_id}'")
                
            await conn.execute(
                text(f'UPDATE "{phys_schema}".collections SET physical_table = :pt WHERE id = :colid'),
                {"pt": physical_table, "colid": collection_id}
            )
            # Invalidate cache
            self._resolve_physical_table_cached.cache_invalidate(catalog_id, collection_id)

    async def _get_collection_model_db(self, catalog_id: str, collection_id: str) -> Optional[Collection]:
        async with managed_transaction(self.engine) as conn:
            return await self._get_collection_model_logic(catalog_id, collection_id, conn)

    async def _get_collection_model_logic(self, catalog_id: str, collection_id: str, conn: DbResource) -> Optional[Collection]:
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        if not phys_schema:
            return None
        query_sql = f'SELECT * FROM "{phys_schema}".collections WHERE id = :id AND deleted_at IS NULL;'
        row_dict = await DQLQuery(query_sql, result_handler=ResultHandler.ONE_DICT).execute(conn, id=collection_id)
        if not row_dict:
            return None
        meta_blob = row_dict['extra_metadata'] or {}
        data = {
            "id": row_dict['id'],
            "title": row_dict['title'],
            "description": row_dict['description'],
            "keywords": row_dict['keywords'],
            "license": row_dict['license'],
            "type": meta_blob.get('type', "Collection"),
            "providers": meta_blob.get('providers'),
            "extent": meta_blob.get('extent'),
            "summaries": meta_blob.get('summaries'),
            "links": meta_blob.get('links', []),
            "extra_metadata": meta_blob.get('extra_metadata')
        }
        return Collection.model_validate(data)

    async def get_collection_model(self, catalog_id: str, collection_id: str, db_resource: Optional[DbResource] = None) -> Optional[Collection]:
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                return await self._get_collection_model_logic(catalog_id, collection_id, conn)
        return await self._get_collection_model_cached(catalog_id, collection_id)

    async def get_collection_column_names(self, catalog_id: str, collection_id: str, db_resource: Optional[DbResource] = None) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=db_resource)
        phys_table = await self.resolve_physical_table(catalog_id, collection_id, db_resource=db_resource)
        if not phys_schema or not phys_table:
            return set()
            
        from dynastore.modules.db_config.shared_queries import get_table_column_names
        async def _execute(conn):
            return await get_table_column_names(conn, phys_schema, phys_table)
            
        if db_resource:
            return await _execute(db_resource)
        async with self.engine.connect() as conn:
            return await _execute(conn)

    async def ensure_collection_exists(self, db_resource: DbResource, catalog_id: str, collection_id: str) -> None:
        if not await self.get_collection_model(catalog_id, collection_id, db_resource=db_resource):
            await self.create_collection(catalog_id, {"id": collection_id, "title": {"en": collection_id}}, db_resource=db_resource)

    async def create_collection(self, catalog_id: str, collection_definition: Union[Dict[str, Any], Collection], lang: str = "en", db_resource: Optional[DbResource] = None, **kwargs) -> Collection:
        if isinstance(collection_definition, dict):
            from dynastore.models.localization import validate_language_consistency
            validate_language_consistency(collection_definition, lang)
            
        collection_model = Collection.create_from_localized_input(collection_definition, lang) if isinstance(collection_definition, dict) else collection_definition
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_model.id)
        
        # We need a reference to the 'generate_physical_name' helper. 
        # I'll import it from catalog_service or redefine it. Redefining for now to avoid circularity.
        from dynastore.modules.catalog.catalog_service import generate_physical_name

        async with managed_transaction(db_resource or self.engine) as conn:
            # Check catalog exists
            catalogs = get_protocol(CatalogsProtocol)
            if not await catalogs.get_catalog_model(catalog_id, db_resource=conn):
                raise ValueError(f"Catalog '{catalog_id}' does not exist.")

            physical_table = kwargs.get("physical_table") or generate_physical_name("t")
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                raise ValueError(f"No physical schema found for catalog '{catalog_id}'")
            
            logger.info(f"[LIFECYCLE] Creating collection '{catalog_id}:{collection_model.id}' in schema '{phys_schema}'")
            
            # Get collection config
            configs = get_protocol(ConfigsProtocol)
            collection_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_model.id, db_resource=conn)
            
            # Layer config override from input
            layer_config_override = None
            if isinstance(collection_definition, dict):
                layer_config_override = collection_definition.get("layer_config")
            elif hasattr(collection_definition, "layer_config"):
                layer_config_override = collection_definition.layer_config

            # Layer config override from input
            if layer_config_override and isinstance(layer_config_override, dict):
                from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
                layer_config_override = CollectionPluginConfig.model_validate(layer_config_override)

            # Execute sync lifecycle initializers
            init_config = layer_config_override or collection_config
            await lifecycle_registry.init_collection(
                conn, 
                phys_schema, 
                catalog_id, 
                collection_model.id, 
                physical_table=physical_table,
                layer_config=init_config
            )
            extra_blob = {
                "type": collection_model.type,
                "providers": [p.model_dump() for p in collection_model.providers] if collection_model.providers else None,
                "extent": collection_model.extent.model_dump() if collection_model.extent else None,
                "summaries": collection_model.summaries,
                "extra_metadata": collection_model.extra_metadata.model_dump(exclude_none=True) if collection_model.extra_metadata else None
            }

            insert_sql = f"""
                INSERT INTO "{phys_schema}".collections 
                (id, catalog_id, physical_table, title, description, keywords, license, extra_metadata) 
                VALUES (:id, :catalog_id, :physical_table, :title, :description, :keywords, :license, :extra_metadata) 
                RETURNING id;
            """

            await DQLQuery(insert_sql, result_handler=ResultHandler.SCALAR_ONE).execute(
                conn,
                id=collection_model.id,
                catalog_id=catalog_id,
                physical_table=physical_table,
                title=json.dumps(collection_model.title.model_dump(exclude_none=True), cls=CustomJSONEncoder) if collection_model.title else None,
                description=json.dumps(collection_model.description.model_dump(exclude_none=True), cls=CustomJSONEncoder) if collection_model.description else None,
                keywords=json.dumps(collection_model.keywords.model_dump(exclude_none=True), cls=CustomJSONEncoder) if collection_model.keywords else None,
                license=json.dumps(collection_model.license.model_dump(exclude_none=True), cls=CustomJSONEncoder) if collection_model.license else None,
                extra_metadata=json.dumps(extra_blob, cls=CustomJSONEncoder)
            )

            # Persist config if override provided
            # Move AFTER metadata insert so set_config (which checks existence) succeeds
            if layer_config_override:
                # Ensure it's a model instance
                config_to_save = layer_config_override
                if isinstance(layer_config_override, dict):
                    config_to_save = CollectionPluginConfig.model_validate(layer_config_override)
                
                await configs.set_config(
                    COLLECTION_PLUGIN_CONFIG_ID, 
                    config_to_save, 
                    catalog_id=catalog_id, 
                    collection_id=collection_model.id, 
                    db_resource=conn
                )

        # Invalidate caches
        self._get_collection_model_cached.cache_invalidate(catalog_id, collection_model.id)
        self._resolve_physical_table_cached.cache_invalidate(catalog_id, collection_model.id)

        # Trigger async lifecycle
        config_snapshot = {}
        try:
            config_snapshot.update(await configs.list_catalog_configs(catalog_id))
        except Exception:
            pass
        
        lifecycle_registry.init_async_collection(phys_schema, catalog_id, collection_model.id, config_snapshot)
        
        return await self.get_collection_model(catalog_id, collection_model.id, db_resource=db_resource)

    async def list_collections(self, catalog_id: str, limit: int = 10, offset: int = 0, lang: str = "en", db_resource: Optional[DbResource] = None) -> List[Collection]:
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return []
            
            query_sql = f'SELECT * FROM "{phys_schema}".collections WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT :limit OFFSET :offset;'
            result_rows = await DQLQuery(query_sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, limit=limit, offset=offset)
            
            results = []
            for row_dict in result_rows:
                meta_blob = row_dict['extra_metadata'] or {}
                data = {
                    "id": row_dict['id'],
                    "title": row_dict['title'],
                    "description": row_dict['description'],
                    "keywords": row_dict['keywords'],
                    "license": row_dict['license'],
                    "type": meta_blob.get('type', "Collection"),
                    "providers": meta_blob.get('providers'),
                    "extent": meta_blob.get('extent'),
                    "summaries": meta_blob.get('summaries'),
                    "links": meta_blob.get('links', []),
                    "extra_metadata": meta_blob.get('extra_metadata')
                }
                results.append(Collection.model_validate(data))
            return results

    async def update_collection(self, catalog_id: str, collection_id: str, updates: Dict[str, Any], lang: str = "en", db_resource: Optional[DbResource] = None) -> Optional[Collection]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        
        from dynastore.models.localization import validate_language_consistency
        validate_language_consistency(updates, lang)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return None

            existing_model = await self._get_collection_model_logic(catalog_id, collection_id, conn)
            if not existing_model:
                return None

            merged_model = existing_model.merge_localized_updates(updates, lang)

            extra_blob = {
                "type": merged_model.type,
                "providers": [p.model_dump() for p in merged_model.providers] if merged_model.providers else None,
                "extent": merged_model.extent.model_dump() if merged_model.extent else None,
                "summaries": merged_model.summaries,
                "links": [l.model_dump() for l in merged_model.links],
                "extra_metadata": merged_model.extra_metadata.model_dump(exclude_none=True) if merged_model.extra_metadata else None
            }
            
            update_sql = f"""
                UPDATE "{phys_schema}".collections 
                SET title = :title, description = :description, keywords = :keywords,
                    license = :license, extra_metadata = :extra_metadata
                WHERE id = :id AND deleted_at IS NULL 
                RETURNING *;
            """
            
            rows = await DQLQuery(
                update_sql, 
                result_handler=ResultHandler.ALL_DICTS
            ).execute(conn,
                    id=collection_id,
                    title=json.dumps(merged_model.title.model_dump(exclude_none=True), cls=CustomJSONEncoder) if merged_model.title else None,
                    description=json.dumps(merged_model.description.model_dump(exclude_none=True), cls=CustomJSONEncoder) if merged_model.description else None,
                    keywords=json.dumps(merged_model.keywords.model_dump(exclude_none=True), cls=CustomJSONEncoder) if merged_model.keywords else None,
                    license=json.dumps(merged_model.license.model_dump(exclude_none=True), cls=CustomJSONEncoder) if merged_model.license else None,
                    extra_metadata=json.dumps(extra_blob, cls=CustomJSONEncoder)
            )
            
            if rows:
                self._get_collection_model_cached.cache_invalidate(catalog_id, collection_id)
                return await self._get_collection_model_logic(catalog_id, collection_id, conn)
            return None

    async def delete_collection(self, catalog_id: str, collection_id: str, force: bool = False, db_resource: Optional[DbResource] = None) -> bool:
        validate_sql_identifier(catalog_id); validate_sql_identifier(collection_id)
        
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return False
            
            soft_delete_sql = f'UPDATE "{phys_schema}".collections SET deleted_at = NOW() WHERE id = :id AND deleted_at IS NULL;'
            await DQLQuery(soft_delete_sql, result_handler=ResultHandler.ROWCOUNT).execute(conn, id=collection_id)
            
            logger.info(f"[LIFECYCLE] Soft deleted collection '{catalog_id}:{collection_id}'")

            config_snapshot = {}
            if force:
                logger.info(f"[LIFECYCLE] Hard deleting collection '{catalog_id}:{collection_id}'")
                phys_table = await self.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
                
                try:
                    configs = get_protocol(ConfigsProtocol)
                    config_snapshot = {"catalog_id": catalog_id, "collection_id": collection_id}
                    coll_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
                    if coll_config:
                        config_snapshot["collection_config"] = coll_config.model_dump()
                except Exception as e:
                    logger.warning(f"Failed to capture config snapshot for '{catalog_id}:{collection_id}: {e}")
                
                await lifecycle_registry.destroy_collection(conn, phys_schema, catalog_id, collection_id)
                
                if phys_table:
                    await shared_queries.delete_table_query.execute(conn, schema=phys_schema, table=phys_table)
                
                hard_delete_sql = f'DELETE FROM "{phys_schema}".collections WHERE id = :id;'
                await DDLQuery(hard_delete_sql).execute(conn, id=collection_id)
                
                logger.info(f"[LIFECYCLE] Hard deleted collection '{catalog_id}:{collection_id}' successfully")

        self._get_collection_model_cached.cache_invalidate(catalog_id, collection_id)
        self._resolve_physical_table_cached.cache_invalidate(catalog_id, collection_id)
        
        if force and phys_schema:
            lifecycle_registry.destroy_async_collection(phys_schema, catalog_id, collection_id, config_snapshot)
        
        return True

    async def delete_collection_language(self, catalog_id: str, collection_id: str, lang: str, db_resource: Optional[DbResource] = None) -> bool:
        """Deletes a specific language variant from a collection."""
        validate_sql_identifier(catalog_id); validate_sql_identifier(collection_id)
        
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return False
                
            model = await self._get_collection_model_logic(catalog_id, collection_id, conn)
            if not model:
                raise ValueError(f"Collection '{catalog_id}:{collection_id}' not found.")
            
            can_delete = False
            fields_to_update = {}
            
            # Localizable fields in collections table are similar to catalogs
            for field in ['title', 'description', 'keywords', 'license', 'extra_metadata']:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(f"Cannot delete language '{lang}' from field '{field}': it is the only language available.")
                        
                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            fields_to_update[field] = json.dumps(data, cls=CustomJSONEncoder)
                            can_delete = True
            
            if not can_delete:
                return False
                
            set_clauses = [f"{k} = :{k}" for k in fields_to_update.keys()]
            params = {"id": collection_id, **fields_to_update}
            
            sql = f'UPDATE "{phys_schema}".collections SET {", ".join(set_clauses)} WHERE id = :id AND deleted_at IS NULL;'
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(conn, **params)
            
            self._get_collection_model_cached.cache_invalidate(catalog_id, collection_id)
            return True

@lifecycle_registry.sync_collection_initializer
async def create_physical_collection_impl(
    conn: DbResource, 
    schema: str, 
    catalog_id: str, 
    collection_id: str, 
    physical_table: Optional[str] = None, 
    layer_config: Optional[Union[Dict[str, Any], Any]] = None
) -> None:
    """Orchestrates the creation of a data table and its indexes."""
    logger.info(f"LIFECYCLE: create_physical_collection_impl started for {catalog_id}:{collection_id} in schema {schema}")
    if not physical_table:
        logger.warning(f"LIFECYCLE: Skipping creation for {catalog_id}:{collection_id} - no physical_table name provided")
        return

    configs = get_protocol(ConfigsProtocol)
    col_config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
    
    if layer_config:
        base_dump = col_config.model_dump()
        layer_config_dict = layer_config.model_dump() if hasattr(layer_config, 'model_dump') else layer_config
        
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict):
                    d[k] = deep_update(d.get(k, {}), v)
                else:
                    d[k] = v
            return d
            
        merged = deep_update(base_dump, layer_config_dict)
        try:
             # Avoid circular import by using local import or registry
             from dynastore.modules.db_config.platform_config_manager import ConfigRegistry
             col_config = ConfigRegistry.validate_config("collection", merged)
        except Exception as e:
             logger.error(f"Failed to merge layer_config for {catalog_id}:{collection_id}: {e}")

    # 1. Resolve Partitioning Context
    partition_keys = []
    partition_key_types = {
        "transaction_time": "TIMESTAMPTZ",
        "validity": "TSTZRANGE",
        "geoid": "UUID",
        "asset_id": "VARCHAR(255)" # Standard fallback
    }
    
    if col_config.partitioning.enabled:
        partition_keys = col_config.partitioning.partition_keys
        
        # Aggregate types from all sidecars
        for sidecar in col_config.sidecars:
            if hasattr(sidecar, "partition_key_contributions"):
                partition_key_types.update(sidecar.partition_key_contributions)
    
    # 2. Create Hub Table
    hub_columns = col_config.get_column_definitions()
    
    # Ensure Hub has all partition keys
    for key in partition_keys:
        # Simple check if column is already defined (heuristic matching)
        if not any(c.split()[0] == key for c in hub_columns):
            col_type = partition_key_types.get(key, "TEXT")
            hub_columns.append(f'"{key}" {col_type} NOT NULL')
    
    # Add Primary Key to Hub (REQUIRED for sidecar Foreign Keys)
    pk_hub = ["geoid", "validity"] # Default composite identity
    if partition_keys:
        pk_hub = list(partition_keys)
        if "geoid" not in pk_hub: pk_hub.append("geoid")
        if "validity" not in pk_hub: pk_hub.append("validity")
    
    hub_columns.append(f"PRIMARY KEY ({', '.join([f'\"{c}\"' for c in pk_hub])})")

    partition_clause = ""
    if partition_keys:
         partition_clause = f" PARTITION BY LIST ({', '.join(partition_keys)})"
         
    create_hub_sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{physical_table}" ({", ".join(hub_columns)}){partition_clause};'
    await DDLQuery(create_hub_sql).execute(conn)
    
    # 3. Create Sidecar Tables
    if col_config.sidecars:
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
        
        for sidecar_config in col_config.sidecars:
            try:
                sidecar_impl = SidecarRegistry.get_sidecar(sidecar_config)
                
                # Versioning detection: Hub always has validity, so for FK compat we treat as versioned
                # unless specifically forced off (but Hub DDL has it by default).
                has_validity = True 
                
                ddl_statements = sidecar_impl.get_ddl(
                    physical_table=physical_table, 
                    partition_keys=partition_keys,
                    partition_key_types=partition_key_types,
                    has_validity=has_validity
                )
                
                # Split and execute individual statements to avoid "multiple commands" error
                for stmt in ddl_statements.split(';'):
                    if stmt.strip():
                        await DDLQuery(stmt).execute(conn, schema=schema)
                
                await sidecar_impl.setup_lifecycle_hooks(conn, schema, f"{physical_table}_{sidecar_impl.sidecar_id}")
            except ValueError as e:
                logger.warning(f"Skipping sidecar table creation: {e}")

    # Ensure asset cleanup trigger (using AssetsProtocol)
    am = get_protocol(AssetsProtocol)
    if am:
        await am.ensure_asset_cleanup_trigger(schema, physical_table, db_resource=conn)

