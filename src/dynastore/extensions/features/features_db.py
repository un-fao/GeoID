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

# dynastore/extensions/features/features_db.py

import logging
import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

from dynastore.modules.catalog.validation import get_valid_properties
from dynastore.models.query_builder import QueryRequest, FieldSelection, SortOrder

logger = logging.getLogger(__name__)

from dynastore.modules.tools.cql import parse_cql_filter


from dynastore.models.query_builder import QueryRequest, FieldSelection, SortOrder, FilterCondition

logger = logging.getLogger(__name__)

from dynastore.modules.tools.cql import parse_cql_filter
from fastapi import HTTPException, status


async def get_items_filtered(
    conn: AsyncConnection,
    schema: str,  # Kept for compatibility
    table: str,   # Kept for compatibility
    catalog_id: str, # Logical ID
    collection_id: str, # Logical ID
    limit: int,
    offset: int,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    datetime_str: Optional[str] = None,
    cql_filter: Optional[str] = None,
    bbox_crs_srid: Optional[int] = None,
    target_crs_srid: Optional[int] = None,
    sortby: Optional[str] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Fetches and filters items from a single collection table, leveraging the new ItemService search pipeline.
    Supports reprojection and CQL filters utilizing automated sidecar joins via the QueryOptimizer.
    """
    catalogs: CatalogsProtocol = get_protocol(CatalogsProtocol)
    
    # Construct robust QueryRequest utilizing the optimized execution pipeline
    request = QueryRequest(
        limit=limit,
        offset=offset,
        select=[FieldSelection(field="*")],
        include_total_count=True,
        raw_selects=[],
        filters=[],
        raw_params={}
    )
    
    raw_where_parts = []

    # --- 1. BBOX Filter ---
    if bbox:
        input_srid = bbox_crs_srid or 4326
        request.raw_params.update(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3], input_srid=input_srid)
        # We use a raw where part for complex spatial operations, using the deterministic sidecar alias
        # sc_geometry is the id of the GeometrySidecar
        raw_where_parts.append(f"sc_geometry.geom && ST_Transform(ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, :input_srid), ST_SRID(sc_geometry.geom))")

    # --- 2. Datetime Filter ---
    if datetime_str:
        if "/" in datetime_str:
            start_str, end_str = datetime_str.split('/')
            start_dt, end_dt = (start_str if start_str != '..' else None), (end_str if end_str != '..' else None)
            
            if start_dt and end_dt:
                raw_where_parts.append("h.validity && tstzrange(:start_dt, :end_dt, '[]')")
                request.raw_params.update(start_dt=start_dt, end_dt=end_dt)
            elif start_dt:
                raw_where_parts.append("lower(h.validity) >= :start_dt")
                request.raw_params['start_dt'] = start_dt
            elif end_dt:
                raw_where_parts.append("upper(h.validity) <= :end_dt")
                request.raw_params['end_dt'] = end_dt
        else:
            raw_where_parts.append("h.validity @> :dt::timestamptz")
            request.raw_params['dt'] = datetime_str

    # --- 3. CQL Filter ---
    if cql_filter:
        try:
            from sqlalchemy import text
            from sqlalchemy.sql import column as sql_column
 
            valid_props = await get_valid_properties(conn, catalog_id, collection_id)
            column_names = await catalogs.get_collection_column_names(catalog_id, collection_id, db_resource=conn)

            # Map fields safely using logical names; QueryOptimizer handles physical resolution
            field_mapping = {}
            for field_name in valid_props:
                if field_name in ["geoid", "validity", "deleted_at", "transaction_time", "content_hash"]:
                    field_mapping[field_name] = sql_column(f"h.{field_name}")
                elif field_name in ["geom", "bbox_geom"]:
                    field_mapping[field_name] = sql_column(f"sc_geometry.{field_name}")
                elif field_name in column_names:
                    field_mapping[field_name] = sql_column(f"sc_attributes.{field_name}") 
                else:
                    field_mapping[field_name] = text(f"sc_attributes.attributes->>'{field_name}'") 

            cql_where, cql_params = parse_cql_filter(
                cql_filter, 
                field_mapping=field_mapping, 
                valid_props=valid_props, 
                parser_type='cql2'
            )

            if cql_where:
                raw_where_parts.append(f"({cql_where})")
                request.raw_params.update(cql_params)

        except ValueError as ve:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
        except ImportError:
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="CQL filtering is not available on this server.")
        except Exception as e:
            logger.error(f"Failed to parse CQL2 filter: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid CQL2-TEXT filter: {e}")

    if raw_where_parts:
        request.raw_where = " AND ".join(raw_where_parts)

    # --- 4. Sort Order ---
    if sortby:
        request.sort = []
        for field in sortby.split(','):
            field = field.strip()
            if not field: continue
            direction = "DESC" if field.startswith('-') else "ASC"
            field = field.lstrip('+-')
            if not re.match(r'^[a-zA-Z0-9_]+$', field):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid sort field: '{field}'")
            request.sort.append(SortOrder(field=field, direction=direction))

    # --- 5. Projection (Reprojection) ---
    if target_crs_srid:
        # Reprojection transformation: QueryOptimizer will skip redundant ST_Transform internally
        request.select.append(FieldSelection(
            field="geom", 
            transformation="ST_Transform", 
            transform_args={"srid": target_crs_srid},
            alias="geom"
        ))

    # --- Execute Single Query utilizing the Optimized Services ---
    rows_proxy = await catalogs.search_items(
        catalog_id=catalog_id, 
        collection_id=collection_id, 
        request=request, 
        db_resource=conn
    )

    rows = []
    total_count = 0
    if rows_proxy:
        total_count = rows_proxy[0].get("_total_count", 0)
        for row in rows_proxy:
            r = dict(row)
            r.pop("_total_count", None)
            rows.append(r)

    return total_count, rows