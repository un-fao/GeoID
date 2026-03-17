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

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, cast
from shapely.geometry import shape, mapping
import logging

from dynastore.modules.catalog.models import SpatialExtent, TemporalExtent
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.geospatial import (
    GeometryProcessingError,
    process_geometry,
    calculate_spatial_indices,
)
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    GeometryStorageConfig,
)

logger = logging.getLogger(__name__)


async def recalculate_and_update_extents(
    db_resource: DbResource, catalog_id: str, collection_id: str
):
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
        layer_config = await catalogs.get_collection_config(
            catalog_id, collection_id, db_resource=conn
        )
        if not layer_config:
            logger.warning(
                f"Cannot recalculate extents for collection '{catalog_id}:{collection_id}' as it has no config."
            )
            return

        # Resolve physical storage
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, db_resource=conn
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        if not phys_schema or not phys_table:
            logger.warning(
                f"Cannot recalculate extents for collection '{catalog_id}:{collection_id}': Physical storage not found."
            )
            return

        # Resolve geometry source and alias
        geom_source_table = phys_table
        geom_alias = "h"
        geom_col = "geom"
        storage_srid = 4326

        # Resolve temporal source and alias
        temporal_source_table = phys_table
        temporal_alias = "h"

        joins = []

        if layer_config.sidecars:
            # Find geometry sidecar config
            from dynastore.modules.catalog.sidecars.geometries_config import (
                GeometriesSidecarConfig,
            )

            geom_sc_config = next(
                (
                    sc
                    for sc in layer_config.sidecars
                    if isinstance(sc, GeometriesSidecarConfig)
                ),
                None,
            )
            if geom_sc_config:
                geom_source_table = f"{phys_table}_{geom_sc_config.sidecar_id}"
                geom_alias = "g"
                storage_srid = geom_sc_config.target_srid
                joins.append(
                    f'JOIN "{phys_schema}"."{geom_source_table}" {geom_alias} ON h.geoid = {geom_alias}.geoid'
                )

            # Find attributes sidecar config (for validity)
            from dynastore.modules.catalog.sidecars.attributes_config import (
                FeatureAttributeSidecarConfig,
            )

            attr_sc_config = next(
                (
                    sc
                    for sc in layer_config.sidecars
                    if isinstance(sc, FeatureAttributeSidecarConfig)
                ),
                None,
            )
            if attr_sc_config and attr_sc_config.enable_validity:
                temporal_source_table = f"{phys_table}_{attr_sc_config.sidecar_id}"
                temporal_alias = "a"
                if temporal_source_table != geom_source_table:
                    joins.append(
                        f'JOIN "{phys_schema}"."{temporal_source_table}" {temporal_alias} ON h.geoid = {temporal_alias}.geoid'
                    )
                else:
                    # Rare case where both are in same table (not standard in our Hub-and-Spoke)
                    temporal_alias = geom_alias

        elif layer_config.geometry_storage:
            storage_srid = layer_config.geometry_storage.target_srid

        if storage_srid == 4326:
            spatial_extent_expr = f"ST_Extent({geom_alias}.{geom_col})"
        else:
            spatial_extent_expr = f"ST_Transform(ST_SetSRID(ST_Extent({geom_alias}.{geom_col}),{storage_srid}), 4326)"

        # 2. Build Query
        # Hub is the anchor (h) for deleted_at check
        join_clause = "\n" + "\n".join(joins) if joins else ""

        query_template = f"""
            WITH calculated_extents AS (
                SELECT 
                    {spatial_extent_expr} AS combined_geom,
                    MIN(lower({temporal_alias}.validity)) AS min_validity,
                    MAX(upper({temporal_alias}.validity)) AS max_validity
                FROM "{phys_schema}"."{phys_table}" h
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

        # We format the string directly because DQLQuery template substitution might conflict
        # with our physical schema quoting needs if not careful.
        final_sql = query_template.format(
            phys_schema=phys_schema, phys_table=phys_table
        )

        calculate_extents_query = DQLQuery(final_sql, result_handler=ResultHandler.ONE)
        row = await calculate_extents_query.execute(conn)

        if row and row[0] is not None:
            min_lon = max(-180.0, min(180.0, row[0]))
            min_lat = max(-90.0, min(90.0, row[1]))
            max_lon = max(-180.0, min(180.0, row[2]))
            max_lat = max(-90.0, min(90.0, row[3]))
            new_bbox = [min_lon, min_lat, max_lon, max_lat]
        else:
            new_bbox = [-180.0, -90.0, 180.0, 90.0]  # Default full extent if empty
            
        new_spatial_extent = SpatialExtent(bbox=[list(new_bbox)])

        min_time = row[4] if row else None
        max_time = row[5] if row else None
        new_temporal_extent = TemporalExtent(interval=[[min_time, max_time]])

        # Update metadata using LOGICAL codes
        collection = await catalogs.get_collection(
            catalog_id, collection_id, db_resource=conn
        )
        if collection:
            # Ensure extent exists
            if not collection.extent:
                from dynastore.models.shared_models import Extent
                collection.extent = Extent(
                    spatial=new_spatial_extent, temporal=new_temporal_extent
                )
            else:
                collection.extent.spatial = new_spatial_extent
                collection.extent.temporal = new_temporal_extent
            
            # Skip update if nothing changed
            # This check is crucial for performance - we don't need to bump transaction counts
            # or do unnecessary writes
            await catalogs.update_collection(
                catalog_id,
                collection_id,
                collection.model_dump(),
                lang="*",
                db_resource=conn,
            )
            logger.info(
                f"Successfully updated extents for collection '{catalog_id}:{collection_id}'."
            )


def get_engine() -> DbResource:
    from dynastore.tools.protocol_helpers import get_engine as get_platform_engine

    return get_platform_engine()


from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from shapely.geometry import mapping

