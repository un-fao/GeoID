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

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest, FieldSelection, SortOrder
from dynastore.modules.stac.stac_config import StacPluginConfig
import logging
logger = logging.getLogger(__name__)

from typing import Any, Dict, List, Optional, Tuple
async def get_stac_items_paginated(
    conn: Any, 
    catalog_id: str,
    collection_id: str, 
    limit: int, 
    offset: int, 
    stac_config: Optional[StacPluginConfig] = None
) -> Tuple[list, int]:
    """
    Fetches a paginated list of items for STAC using the optimized CatalogsProtocol.
    Geometries are simplified at the database level based on the StacPluginConfig.
    """
    catalogs: CatalogsProtocol = get_protocol(CatalogsProtocol)
    
    # Determine simplification SQL
    if stac_config and stac_config.simplification:
        simp_conf = stac_config.simplification
        case_sql = "CASE "
        # Sort thresholds descending by vertex count
        for points, tolerance in sorted(simp_conf.vertex_thresholds.items(), key=lambda item: item[0], reverse=True):
            case_sql += f"WHEN ST_NPoints(sc_geometry.geom) > {points} THEN {tolerance} "
        case_sql += f"ELSE {simp_conf.default_tolerance} END"
        simplification_sql = f"({case_sql})"
    else:
        simplification_sql = "0.0001"

    # Construct QueryRequest leveraging the sidecar-aware execution pipeline
    request = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=True,
        select=[
            FieldSelection(field="*"), # Includes HUB, attributes, and geometry
            # Explicitly format validity components using sidecar-supported transformations
            FieldSelection(field="validity", transformation="lower", alias="valid_from"),
            FieldSelection(field="validity", transformation="upper", alias="valid_to"),
            # BBOX components optimized via bbox_geom in GeometrySidecar
            FieldSelection(field="bbox_xmin"),
            FieldSelection(field="bbox_ymin"),
            FieldSelection(field="bbox_xmax"),
            FieldSelection(field="bbox_ymax"),
        ],
        raw_selects=[
            # Custom simplification requires raw access to GeometrySidecar alias 'sc_geometry'
            # (Alias choice is deterministic in QueryOptimizer)
            f"ST_AsEWKB(ST_SimplifyPreserveTopology(sc_geometry.geom, {simplification_sql})) AS geom"
        ],
        sort=[SortOrder(field="id", direction="ASC")]
    )
    
    # Execute through protocol which automatically joins sidecars
    rows = await catalogs.search_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        db_resource=conn
    )
    
    total_count = 0
    if rows:
        total_count = rows[0].get("_total_count", 0)
        # Keep internal metadata out of the final result list
        for r in rows:
            r.pop("_total_count", None)
            
    return rows, total_count