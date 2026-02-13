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
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import text

from dynastore.modules.db_config.query_executor import DbResource, DDLQuery, DQLQuery, ResultHandler, managed_transaction
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig, AttributeSchemaEntry, COLLECTION_PLUGIN_CONFIG_ID
)
from dynastore.modules.catalog.sidecars.geometry import GeometrySidecar, GeometrySidecarConfig
from dynastore.modules.catalog.sidecars.attributes import FeatureAttributeSidecar, FeatureAttributeSidecarConfig, AttributeStorageMode
from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

class OneShotMigrator:
    """
    Tool to migrate legacy monolithic collections to the new Sidecar Architecture.
    """

    def __init__(self, engine: DbResource):
        self.engine = engine

    async def migrate_collection(self, catalog_id: str, collection_id: str):
        """
        Performs the migration for a single collection.
        """
        logger.info(f"Starting migration for {catalog_id}.{collection_id}")
        
        async with managed_transaction(self.engine) as conn:
            # 1. Fetch Current Config
            configs = get_protocol(ConfigsProtocol)
            catalogs = get_protocol(CatalogsProtocol)
            
            orig_config: CollectionPluginConfig = await configs.get_config(
                COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
            )
            
            if orig_config.sidecars:
                logger.info(f"Collection {collection_id} already uses sidecars. Skipping.")
                return

            phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
            
            # SOURCE Table: In legacy, this WAS the collection_id
            # TARGET Hub Table: In sidecar architecture, we use a new physical name
            source_table = collection_id 
            
            # Determine if a physical table already exists (shouldn't if it's legacy monolithic)
            target_hub_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            if not target_hub_table or target_hub_table == source_table:
                 # Generate a new physical name for the Hub to comply with Sidecar Architecture
                 from dynastore.modules.catalog.catalog_service import generate_physical_name
                 target_hub_table = generate_physical_name("t") # Using 't' prefix for tables
                 await catalogs.set_physical_table(catalog_id, collection_id, target_hub_table, db_resource=conn)

            logger.info(f"DEBUG: Migrating {catalog_id}.{collection_id} from {source_table} to Hub {target_hub_table}")

            # 2. Convert Config to Sidecars
            new_sidecars = self._convert_legacy_to_sidecars(orig_config)
            
            # 3. Create Hub and Sidecar Tables
            # For migration, we assume the original table might be partitioned.
            partition_info = await self._get_table_partitioning(conn, phys_schema, source_table)
            partition_keys = partition_info.get("partition_keys", [])
            
            # A. Create Hub Table (if it's a new name)
            if target_hub_table != source_table:
                # We need to create the Hub table mirroring the source but ONLY core columns
                # For simplicity, we assume we want a standard Hub structure
                # Actually, CatalogService.create_physical_collection_impl can do this
                await catalogs.create_physical_collection(
                    conn, phys_schema, catalog_id, collection_id, 
                    physical_table=target_hub_table,
                    layer_config={"sidecars": new_sidecars}
                )
            
            # Sidecar tables are created by create_physical_collection_impl above!
            
            # 4. Data Distribution
            # We migrate from source_table to the new Hub and Sidecars
            # First, Hub data
            hub_cols = ["geoid", "transaction_time", "deleted_at", "validity"] # Core Hub columns
            # Add partition keys
            for pk in partition_keys:
                if pk not in hub_cols:
                    hub_cols.append(pk)
            
            # Identify which of these exist in source_table
            check_cols_sql = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table;
            """
            existing_cols_res = await DQLQuery(check_cols_sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, schema=phys_schema, table=source_table)
            existing_cols = {r["column_name"].lower() for r in existing_cols_res}
            
            available_hub_cols = [c for c in hub_cols if c.lower() in existing_cols]
            if "geoid" not in available_hub_cols:
                 # In some legacy tables it might be 'id' instead of 'geoid'
                 if "id" in existing_cols:
                      hub_select_cols = [f'id as geoid' if c == "geoid" else f'"{c}"' for c in available_hub_cols]
                      hub_target_cols = [f'"{c}"' for c in available_hub_cols]
                      if "geoid" not in hub_target_cols: available_hub_cols.append("geoid") # It will be mapped from 'id'
                 else:
                      raise ValueError(f"Source table {source_table} missing 'geoid' or 'id' column.")
            else:
                 hub_target_cols = [f'"{c}"' for c in available_hub_cols]
                 hub_select_cols = hub_target_cols

            col_str_target = ", ".join(hub_target_cols)
            col_str_select = ", ".join(hub_select_cols)
            
            logger.info(f"DEBUG: Migrating Hub data from {source_table} to {target_hub_table}. Columns: {available_hub_cols}")
            await DDLQuery(f'INSERT INTO "{phys_schema}"."{target_hub_table}" ({col_str_target}) SELECT {col_str_select} FROM "{phys_schema}"."{source_table}";').execute(conn)
            
            # Distribute to Sidecars
            for sidecar_config in new_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sidecar_config)
                await self._distribute_data(conn, phys_schema, source_table, target_hub_table, sidecar_config, sidecar)
            
            # 5. Cleanup Source Table? 
            # In one-shot migration, we might want to DROP the old table if it was renamed
            if target_hub_table != source_table:
                await DDLQuery(f'DROP TABLE IF EXISTS "{phys_schema}"."{source_table}" CASCADE;').execute(conn)
            
            # 6. Update Configuration
            updated_config = orig_config.model_copy(update={
                "sidecars": new_sidecars,
                "geometry_storage": None,
                "h3_resolutions": [],
                "s2_resolutions": [],
                "attribute_schema": []
            })
            
            await configs.set_config(
                COLLECTION_PLUGIN_CONFIG_ID,
                updated_config,
                catalog_id=catalog_id,
                collection_id=collection_id,
                check_immutability=False,
                db_resource=conn
            )
            
            logger.info(f"Successfully migrated {catalog_id}.{collection_id} to Sidecar Architecture.")

    def _convert_legacy_to_sidecars(self, config: CollectionPluginConfig) -> List[Any]:
        """Converts legacy fields to new sidecar configs."""
        sidecars = []
        
        # Access legacy fields via extra storage if class attributes were removed
        legacy_data = config.model_extra if hasattr(config, "model_extra") and config.model_extra else {}
        
        geometry_storage = getattr(config, "geometry_storage", legacy_data.get("geometry_storage"))
        h3_resolutions = getattr(config, "h3_resolutions", legacy_data.get("h3_resolutions", []))
        s2_resolutions = getattr(config, "s2_resolutions", legacy_data.get("s2_resolutions", []))

        # Geometry conversion
        if geometry_storage:
            if isinstance(geometry_storage, dict):
                from dynastore.modules.catalog.sidecars.geometry import GeometrySidecarConfig
                gs_config = GeometrySidecarConfig(**geometry_storage)
            else:
                gs_config = geometry_storage
            
            # Ensure resolutions are set
            gs_config.h3_resolutions = h3_resolutions or []
            gs_config.s2_resolutions = s2_resolutions or []
            sidecars.append(gs_config)
            
        # Attributes conversion
        # Legacy attributes were either in 'attributes' JSONB (default) or specified in schema
        attr_config = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            enable_external_id=True,
            enable_asset_id=True
        )
        sidecars.append(attr_config)
        
        return sidecars

    async def _get_table_partitioning(self, conn, schema, table) -> Dict[str, Any]:
        """Internal helper to detect existing partitioning."""
        # This is a simplified version. Dynamic detection of PK and partitions is complex.
        # For now, we rely on the fact that Hub and sidecars MUST match.
        # We can look up pg_partitioned_table if needed.
        sql = """
            SELECT
                array_agg(a.attname) as partition_keys,
                array_agg(format_type(a.atttypid, a.atttypmod)) as partition_key_types
            FROM pg_partitioned_table p
            JOIN pg_class c ON p.partrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = :schema AND c.relname = :table
            AND a.attnum = ANY(p.partattrs);
        """
        res = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, schema=schema, table=table)
        if res and res[0]["partition_keys"]:
            keys = res[0]["partition_keys"]
            types = res[0]["partition_key_types"]
            return {
                "partition_keys": keys,
                "partition_key_types": dict(zip(keys, types))
            }
        return {"partition_keys": [], "partition_key_types": {}}

    async def _distribute_data(self, conn, schema, source_table, target_hub, sc_config, sidecar):
        """Moves data from source table to sidecars."""
        sc_table = f"{target_hub}_{sidecar.sidecar_id}"
        
        if sc_config.sidecar_type == "geometry":
            # Geometry Migration
            cols = ["geoid", "geom", "geom_type", "validity"]
            if sc_config.write_bbox:
                cols.append("bbox_geom")
            for res in sc_config.h3_resolutions:
                cols.append(f"h3_res{res}")
            for res in sc_config.s2_resolutions:
                cols.append(f"s2_res{res}")
            
            # Legacy data might not have all index columns or even 'geom' if it was a data-only table
            # We should check which columns actually exist in the source table.
            check_cols_sql = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table;
            """
            existing_cols_res = await DQLQuery(check_cols_sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, schema=schema, table=source_table)
            existing_cols = {r["column_name"].lower() for r in existing_cols_res}
            
            available_cols = [c for c in cols if c.lower() in existing_cols]
            
            logger.warning(f"DEBUG: Migrating geometry sidecar {sc_table}. Source: {schema}.{source_table}")
            
            if not available_cols:
                logger.warning(f"No geometry columns found in source {schema}.{source_table} for migration to {sc_table}")
                return

            col_str = ", ".join(f'"{c}"' for c in available_cols)
            sql = f'INSERT INTO "{schema}"."{sc_table}" ({col_str}) SELECT {col_str} FROM "{schema}"."{source_table}";'
            logger.warning(f"DEBUG: Executing migration SQL: {sql}")
            await DDLQuery(sql).execute(conn)
            
        elif sc_config.sidecar_type == "attributes":
            # Attribute Migration (JSONB)
            # Check for existing columns
            check_cols_sql = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table;
            """
            existing_cols_res = await DQLQuery(check_cols_sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, schema=schema, table=source_table)
            existing_cols = {r["column_name"].lower() for r in existing_cols_res}

            target_cols = ["geoid", "validity", "external_id", "asset_id", "attributes"]
            available_cols = [c for c in target_cols if c.lower() in existing_cols]
            
            if "geoid" not in available_cols:
                logger.error(f"Required column 'geoid' missing in {schema}.{source_table}. Cannot migrate attributes.")
                return

            col_str = ", ".join(f'"{c}"' for c in available_cols)
            sql = f'INSERT INTO "{schema}"."{sc_table}" ({col_str}) SELECT {col_str} FROM "{schema}"."{source_table}";'
            await DDLQuery(sql).execute(conn)

    async def _cleanup_hub_table(self, conn, schema, table, config: CollectionPluginConfig):
        """Drops legacy columns from Hub table."""
        drops = []
        legacy_data = config.model_extra if hasattr(config, "model_extra") and config.model_extra else {}
        geometry_storage = getattr(config, "geometry_storage", legacy_data.get("geometry_storage"))
        h3_resolutions = getattr(config, "h3_resolutions", legacy_data.get("h3_resolutions", []))
        s2_resolutions = getattr(config, "s2_resolutions", legacy_data.get("s2_resolutions", []))

        if geometry_storage:
            drops.append('DROP COLUMN "geom"')
            drops.append('DROP COLUMN "geom_type"')
            if geometry_storage.write_bbox:
                 drops.append('DROP COLUMN "bbox_geom"')
            for res in h3_resolutions or []:
                 drops.append(f'DROP COLUMN "h3_res{res}"')
            for res in s2_resolutions or []:
                 drops.append(f'DROP COLUMN "s2_res{res}"')
        
        # Attributes and Identity moved to sidecar
        drops.append('DROP COLUMN "attributes"')
        drops.append('DROP COLUMN "external_id"')
        drops.append('DROP COLUMN "asset_id"')
        
        # Since we use asset_id for partitioning sometimes, we keep it in Hub if it's a partition key.
        # But external_id, content_hash etc stay in Hub as core metadata.
        
        if drops:
             sql = f'ALTER TABLE "{schema}"."{table}" {", ".join(drops)};'
             await DDLQuery(sql).execute(conn)
