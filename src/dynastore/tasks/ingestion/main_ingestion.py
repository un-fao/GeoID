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
import re
import os
import asyncio
import itertools
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Type

from dynastore.modules.catalog.asset_manager import Asset, AssetBase
from pydantic import BaseModel

from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.catalog.tools import recalculate_and_update_extents
from dynastore.modules.db_config.query_executor import DbEngine

# Import Ingestion Configuration
from dynastore.tasks.ingestion.ingestion_config import INGESTION_CONFIG_ID
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from dynastore.tasks.reporters import ReportingInterface
from .reporters_impl import DatabaseStatusReporter
from .reporters import _ingestion_reporter_registry
from dynastore.tasks.tools import initialize_reporters
from .operations import initialize_operations, run_pre_operations, run_post_operations

logger = logging.getLogger(__name__)

async def run_ingestion_task(
    engine: DbEngine,
    task_id: str,
    catalog_id: str,
    collection_id: str,
    task_request: TaskIngestionRequest,
    caller_id: Optional[str] = None
):
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    
    catalog_module = get_protocol(CatalogsProtocol)
    if not catalog_module:
        raise RuntimeError("CatalogsProtocol implementation not found.")
    
    pre_ops = []
    post_ops = []
    
    logger.info(f"Starting Ingestion Task '{task_id}' for collection '{catalog_id}:{collection_id}'. Source SRID: {task_request.source_srid}")
    
    # Resolve physical schema for task storage
    phys_schema = await catalog_module.resolve_physical_schema(catalog_id, engine)
    
    reporters = initialize_reporters(
        engine, 
        task_id, 
        task_request, 
        task_request.reporting, 
        registry=_ingestion_reporter_registry,
        schema=phys_schema,
        catalog_id=catalog_id,
        collection_id=collection_id
    )
    
    await asyncio.gather(*(reporter.task_started(task_id, collection_id, catalog_id, task_request.asset.asset_id or task_request.asset.uri) for reporter in reporters))

    # --- Ensure Logical Collection Exists ---
    await catalog_module.ensure_collection_exists(catalog_id, collection_id, db_resource=engine)

    logger.info(f"Task '{task_id}': Beginning main ingestion process.")
    try:
        # --- Fetch Physical Configuration (Immutable Storage) ---
        catalog_config = await catalog_module.get_collection_config(catalog_id, collection_id)
        
        # --- Fetch Ingestion Configuration (Mutable Logic) ---
        ingestion_config = await catalog_module.configs.get_config(INGESTION_CONFIG_ID, catalog_id, collection_id)
        
        # Initialize Operations
        pre_ops = initialize_operations(engine, task_id, task_request, task_request.pre_operations, catalog_config=catalog_config, ingestion_config=ingestion_config)
        post_ops = initialize_operations(engine, task_id, task_request, task_request.post_operations, catalog_config=catalog_config, ingestion_config=ingestion_config)

        # --- Ensure Physical Table Exists ---
        await catalog_module.ensure_physical_table_exists(
            catalog_id,
            collection_id,
            catalog_config,
            db_resource=engine
        )
      
        asset_manager = catalog_module.assets
        
        # --- Resolve or Create the Asset ---
        asset: Optional[Asset] = None
        if task_request.asset.asset_id:
            asset = await asset_manager.get_asset(task_request.asset.asset_id, catalog_id, collection_id)
            if not asset and not task_request.asset.uri:
                raise ValueError(f"Asset with asset_id '{task_request.asset.asset_id}' not found and no URI provided.")
        
        if not asset and task_request.asset.uri:
            logger.info(f"Creating asset from URI: {task_request.asset.uri}")
            asset_id_for_creation = task_request.asset.asset_id or re.sub(r'[^a-zA-Z0-9_\-]', '_', os.path.basename(task_request.asset.uri))
            
            asset_payload = AssetBase(
                asset_id=asset_id_for_creation, 
                uri=task_request.asset.uri,
                metadata=task_request.asset.metadata or {}
            )
            asset = await asset_manager.create_asset(catalog_id, asset_payload, collection_id, db_resource=engine)
            
        if not asset:
            raise ValueError("Could not find or create an asset.")

        # --- Run Pre-Operations ---
        if pre_ops:
            catalog = await catalog_module.get_catalog(catalog_id, db_resource=engine)
            collection = await catalog_module.get_collection(catalog_id, collection_id, db_resource=engine)
            asset = await run_pre_operations(pre_ops, catalog, collection, asset)

        source_file_path = asset.uri
        asset_id = asset.asset_id

        # --- Process and Ingest Features ---
        total_features = None
        try:
            import fiona
            with fiona.open(source_file_path, 'r', encoding=task_request.encoding) as source:
                total_features = len(source)
            logger.info(f"Source file contains {total_features} features.")
            await asyncio.gather(*(reporter.update_progress(0, total_features) for reporter in reporters))
        except Exception as e:
            logger.warning(f"Could not determine total feature count: {e}")

        batch_size = task_request.database_batch_size or 500
        current_batch = []
        rows_ingested = 0
        
        upsert_context = {"asset_id": asset_id}

        def prepare_record_for_upsert(raw: dict, request: TaskIngestionRequest) -> dict:
            mapping = request.column_mapping
            feature = {"properties": {}}
            
            def _get_raw_val(key):
                if not key: return None
                return raw.get(key) if key in raw else raw.get("properties", {}).get(key)

            # 1. Identity
            ext_id_field = mapping.external_id or "id"
            ext_id = _get_raw_val(ext_id_field)
            if ext_id:
                 feature["id"] = ext_id
            
            # 2. Geometry
            geometry = None
            if mapping.csv_lat_column and mapping.csv_lon_column:
                 lat = _get_raw_val(mapping.csv_lat_column)
                 lon = _get_raw_val(mapping.csv_lon_column)
                 if lat is not None and lon is not None:
                      coords = [float(lon), float(lat)]
                      elev = _get_raw_val(mapping.csv_elevation_column)
                      if elev is not None:
                           coords.append(float(elev))
                      geometry = {"type": "Point", "coordinates": coords}
            elif mapping.csv_wkt_column:
                 wkt = _get_raw_val(mapping.csv_wkt_column)
                 if wkt:
                      from shapely.wkt import loads
                      from shapely.geometry import mapping as shapely_mapping
                      try:
                           geometry = shapely_mapping(loads(wkt))
                      except Exception:
                           pass
            elif mapping.geometry_wkb:
                 wkb = _get_raw_val(mapping.geometry_wkb)
                 if wkb:
                      from shapely.wkb import loads
                      from shapely.geometry import mapping as shapely_mapping
                      try:
                           geometry = shapely_mapping(loads(wkb))
                      except Exception:
                           pass
            else:
                 # Fallback: check if 'geometry' is already present (e.g. GeoJSON/Fiona)
                 geometry = raw.get("geometry")
            
            if geometry:
                 feature["geometry"] = geometry
            
            # 3. Attributes
            raw_props = raw.get("properties", {})
            if mapping.attributes_source_type == 'explicit_list' and mapping.attribute_mapping:
                 for item in mapping.attribute_mapping:
                      val = _get_raw_val(item.source)
                      if val is not None:
                           feature["properties"][item.map_to] = val
            else:
                 reserved = {mapping.external_id, mapping.csv_lat_column, mapping.csv_lon_column, mapping.csv_elevation_column, mapping.csv_wkt_column, mapping.geometry_wkb}
                 # Take from properties first
                 for k, v in raw_props.items():
                      if k not in reserved:
                           feature["properties"][k] = v
                 # Also take from top-level if not already taken and not reserved
                 for k, v in raw.items():
                      if k not in ["geometry", "properties", "id"] and k not in reserved and k not in feature["properties"]:
                           feature["properties"][k] = v
            
            # 4. Temporal
            valid_from = _get_raw_val(request.time_validity_start_column)
            if valid_from:
                 feature["valid_from"] = valid_from
            valid_to = _get_raw_val(request.time_validity_end_column)
            if valid_to:
                 feature["valid_to"] = valid_to
            
            return feature

        import fiona
        with fiona.open(source_file_path, 'r', encoding=task_request.encoding) as reader:
            sliced_reader = itertools.islice(reader, task_request.offset, task_request.limit + task_request.offset if task_request.limit else None)
        
            for idx, raw_record in enumerate(sliced_reader, start=task_request.offset):
                feature = prepare_record_for_upsert(dict(raw_record), task_request)
                current_batch.append(feature)
                
                if len(current_batch) >= batch_size:
                    await catalog_module.upsert(
                        catalog_id, collection_id, current_batch, 
                        db_resource=engine, processing_context=upsert_context
                    )
                    rows_ingested += len(current_batch)
                    await asyncio.gather(*(reporter.update_progress(rows_ingested, total_features) for reporter in reporters))
                    current_batch = []
            
            if current_batch:
                await catalog_module.upsert(
                    catalog_id, collection_id, current_batch, 
                    db_resource=engine, processing_context=upsert_context
                )
                rows_ingested += len(current_batch)
                await asyncio.gather(*(reporter.update_progress(rows_ingested, total_features) for reporter in reporters))

        await recalculate_and_update_extents(engine, catalog_id, collection_id)
        await asyncio.gather(*(reporter.task_finished("COMPLETED") for reporter in reporters))
        
        # --- Run Post-Operations ---
        if post_ops:
            catalog = await catalog_module.get_catalog(catalog_id, db_resource=engine)
            collection = await catalog_module.get_collection(catalog_id, collection_id, db_resource=engine)
            await run_post_operations(post_ops, catalog, collection, asset, "COMPLETED")

    except Exception as e:
        logger.critical(f"Ingestion task {task_id} failed: {e}", exc_info=True)
        await asyncio.gather(*(reporter.task_finished("FAILED", error_message=str(e)) for reporter in reporters))
        if post_ops:
            try:
                catalog = await catalog_module.get_catalog(catalog_id, db_resource=engine)
                collection = await catalog_module.get_collection(catalog_id, collection_id, db_resource=engine)
                await run_post_operations(post_ops, catalog, collection, asset, "FAILED", error_message=str(e))
            except Exception:
                pass