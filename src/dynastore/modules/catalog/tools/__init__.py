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

# file: dynastore/modules/catalog/tools.py

import logging
from typing import Any, Dict, Optional, cast
from typing_extensions import deprecated
import uuid
from datetime import datetime, timezone
from shapely.geometry import shape

from dynastore.modules.catalog.models import SpatialExtent, TemporalExtent
from dynastore.modules.db_config.query_executor import (
    DQLQuery, DbResource, ResultHandler, managed_transaction
)
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.geospatial import (
    GeometryProcessingError, process_geometry, calculate_spatial_indices
)
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig, GeometryStorageConfig

logger = logging.getLogger(__name__)

async def recalculate_and_update_extents(db_resource: DbResource, catalog_id: str, collection_id: str):
    """
    Recalculates the spatial and temporal extents of a collection's data and
    updates its metadata record.
    This function dynamically transforms the extent to EPSG:4326 only if the
    collection's native SRID is different, ensuring compatibility with the
    WGS84 validation in the SpatialExtent model.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("CatalogsProtocol not found. Cannot recalculate extents.")
        return

    async with managed_transaction(db_resource) as conn:
        layer_config = await catalogs.get_collection_config(catalog_id, collection_id, db_resource=conn)
        if not layer_config:
            logger.warning(f"Cannot recalculate extents for collection '{catalog_id}:{collection_id}' as it has no config.")
            return
        
        # Resolve physical storage
        phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)

        if not phys_schema or not phys_table:
             logger.warning(f"Cannot recalculate extents for collection '{catalog_id}:{collection_id}': Physical storage not found.")
             return
             
        # Resolve geometry source and SRID
        geom_source_table = phys_table
        geom_col = "geom"
        storage_srid = 4326
        
        if layer_config.sidecars:
            # Find geometry sidecar config
            from dynastore.modules.catalog.sidecars.geometry_config import GeometrySidecarConfig
            geom_sc = next((sc for sc in layer_config.sidecars if isinstance(sc, GeometrySidecarConfig)), None)
            if geom_sc:
                geom_source_table = f"{phys_table}_geometry"
                # geom column name is always 'geom' in GeometrySidecar
                storage_srid = geom_sc.target_srid
        elif layer_config.geometry_storage:
             storage_srid = layer_config.geometry_storage.target_srid
        
        if storage_srid == 4326:
            spatial_extent_expr = f"ST_Extent({geom_col})"
        else:
            spatial_extent_expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_col}),{storage_srid}), 4326)"

        # The query template uses placeholders {phys_schema} and {phys_table}.
        # In sidecar mode, we might need a JOIN if temporal is on Hub but geom in Sidecar.
        # But 'validity' is currently on Hub. 'deleted_at' is on Hub.
        # So we JOIN Hub and Sidecar.
        
        if geom_source_table != phys_table:
            query_template = f"""
                WITH calculated_extents AS (
                    SELECT 
                        {spatial_extent_expr} AS combined_geom,
                        MIN(lower(h.validity)) AS min_validity,
                        MAX(upper(h.validity)) AS max_validity
                    FROM "{phys_schema}"."{phys_table}" h
                    JOIN "{phys_schema}"."{geom_source_table}" g ON h.geoid = g.geoid
                    WHERE h.deleted_at IS NULL AND g.{geom_col} IS NOT NULL
                ) 
            """
        else:
            query_template = f"""
                WITH calculated_extents AS (
                    SELECT 
                        {spatial_extent_expr} AS combined_geom,
                        MIN(lower(validity)) AS min_validity,
                        MAX(upper(validity)) AS max_validity
                    FROM "{phys_schema}"."{phys_table}"
                    WHERE deleted_at IS NULL AND {geom_col} IS NOT NULL
                ) 
            """
            
        query_template += """
            SELECT
                ST_XMin(combined_geom),
                ST_YMin(combined_geom),
                ST_XMax(combined_geom),
                ST_YMax(combined_geom),
                CASE WHEN min_validity = '-infinity' THEN NULL ELSE min_validity END,
                CASE WHEN max_validity = 'infinity' THEN NULL ELSE max_validity END
            FROM calculated_extents;
        """
        
        # We format the string directly because DQLQuery template substitution might conflict 
        # with our physical schema quoting needs if not careful.
        # However, DQLQuery handles params safely.
        # Let's use string formatting for schema/table to ensure correct quoting.
        final_sql = query_template.format(phys_schema=phys_schema, phys_table=phys_table)
        
        calculate_extents_query = DQLQuery(final_sql, result_handler=ResultHandler.ONE)
        row = await calculate_extents_query.execute(conn)

        new_bbox = [0, 0, 0, 0]
        if row and row[0] is not None:
            min_lon = max(-180.0, min(180.0, row[0]))
            min_lat = max(-90.0, min(90.0, row[1]))
            max_lon = max(-180.0, min(180.0, row[2]))
            max_lat = max(-90.0, min(90.0, row[3]))
            new_bbox = [min_lon, min_lat, max_lon, max_lat]
        
        new_spatial_extent = SpatialExtent(bbox=[list(new_bbox)])
        
        min_time = row[4] if row else None
        max_time = row[5] if row else None
        new_temporal_extent = TemporalExtent(interval=[[min_time, max_time]])
        
        # Update metadata using LOGICAL codes
        collection = await catalogs.get_collection(catalog_id, collection_id, db_resource=conn)
        if collection:
            # Ensure extent exists
            if not collection.extent:
                from dynastore.models.shared_models import Extent
                collection.extent = Extent(
                    spatial=new_spatial_extent,
                    temporal=new_temporal_extent
                )
            else:
                collection.extent.spatial = new_spatial_extent
                collection.extent.temporal = new_temporal_extent
            await catalogs.update_collection(catalog_id, collection_id, collection.model_dump(), lang="*", db_resource=conn)
            logger.info(f"Successfully updated extents for collection '{catalog_id}:{collection_id}'.")

def get_engine() -> DbResource:
    from dynastore.tools.protocol_helpers import get_engine as get_platform_engine
    return get_platform_engine()


@deprecated("Use CatalogsProtocol.upsert() instead.")
def prepare_item_for_db(
    feature_geometry: Dict[str, Any],
    feature_properties: Dict[str, Any],
    layer_config: CollectionPluginConfig,
    external_id: Optional[str] = None,
    asset_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validates and prepares a feature/item for database insertion.
    Centralizies logic for geometry processing, spatial indexing, and default asset codes.
    
    Args:
        feature_geometry: GeoJSON geometry dict.
        feature_properties: Feature properties dict.
        layer_config: Collection configuration.
        external_id: Optional external ID. If None, generated UUID.

    Returns:
        Dict matching ItemDataForDB fields.
    
    Raises:
        ValueError: If geometry is invalid or processing fails.
    """
    if not feature_geometry:
        raise ValueError("Item must have a valid geometry.")

    try:
        geom_wkb_hex = shape(feature_geometry).wkb.hex()
    except Exception as e:
        raise ValueError(f"Invalid GeoJSON geometry: {e}")

    # Resolve geometry storage config
    storage_config = None
    h3_res = []
    s2_res = []

    if layer_config.sidecars:
        from dynastore.modules.catalog.sidecars.geometry_config import GeometrySidecarConfig
        geom_sc = next((sc for sc in layer_config.sidecars if isinstance(sc, GeometrySidecarConfig)), None)
        if geom_sc:
            storage_config = geom_sc
            h3_res = geom_sc.h3_resolutions
            s2_res = geom_sc.s2_resolutions
    
    if not storage_config and layer_config.geometry_storage:
        storage_config = layer_config.geometry_storage
        h3_res = layer_config.h3_resolutions
        s2_res = layer_config.s2_resolutions

    if not storage_config:
        # Fallback to defaults if no config found
        from dynastore.modules.catalog.sidecars.geometry_config import GeometrySidecarConfig
        storage_config = GeometrySidecarConfig()

    try:
        # Since GeoJSON-derived input is always EPSG:4326 (RFC 7946), 
        # we explicitly set source_srid to prevent misinterpretation of 
        # standard WKB as EWKB with garbage SRIDs.
        geom_data = process_geometry(geom_wkb_hex, storage_config, source_srid=4326)
    except GeometryProcessingError as e:
        raise ValueError(f"Geometry validation failed: {e}")

    indices = calculate_spatial_indices(
        geom_data['shapely_geom'], 
        h3_res, 
        s2_res
    )

    final_external_id = external_id or str(uuid.uuid4())

    # Parse validity
    valid_from = feature_properties.get('valid_from') or feature_properties.get('datetime')
    if isinstance(valid_from, str):
        try:
            valid_from = datetime.fromisoformat(valid_from.replace('Z', '+00:00'))
        except ValueError:
             # Fallback or strict? Let's be lenient for now or default to now
             pass
    
    if not valid_from:
        valid_from = datetime.now(timezone.utc)

    valid_to = feature_properties.get('valid_to') or feature_properties.get('end_datetime')
    if isinstance(valid_to, str):
        try:
            valid_to = datetime.fromisoformat(valid_to.replace('Z', '+00:00'))
        except ValueError:
            pass

    record = {
        'external_id': final_external_id, 
        'attributes': feature_properties,
        'wkb_hex_processed': geom_data['wkb_hex_processed'], 
        'geom_type': geom_data['geom_type'],
        'was_geom_fixed': geom_data.get('was_geom_fixed', False),
        'content_hash': geom_data['content_hash'],
        'valid_from': valid_from,
        'valid_to': valid_to,
        'asset_id': asset_id,
        'bbox_coords': geom_data.get('bbox_coords'),
        **indices
    }

    return record

