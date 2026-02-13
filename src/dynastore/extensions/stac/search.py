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
from datetime import datetime
from sqlalchemy import text
import asyncio
from typing import List, Optional, Tuple, Dict, Any, Union, cast
from pydantic import BaseModel, Field
from enum import Enum

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules import get_protocol
from dynastore.modules.stac.stac_config import StacPluginConfig, SimplificationConfig
from dynastore.modules.catalog.models import Collection
from dynastore.modules.db_config.query_executor import managed_transaction, DbResource, DQLQuery, ResultHandler
from dynastore.modules.db_config.shared_queries import build_filter_clause, get_table_column_names

class FilterOperator(str, Enum):
    EQ = "eq"; NEQ = "neq"; LT = "lt"; LTE = "lte"; GT = "gt"; GTE = "gte"; LIKE = "like"; ILIKE = "ilike"

from dynastore.modules.stac.stac_config import AggregationRule

class AttributeFilter(BaseModel):
    field: str; operator: FilterOperator; value: Any

class QueryFilter(BaseModel):
    op: str = Field("and"); args: List[Union['QueryFilter', AttributeFilter]]

class ItemSearchRequest(BaseModel):
    catalog_id: str
    collections: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    datetime: Optional[str] = None
    intersects: Optional[Dict[str, Any]] = None
    filter: Optional[Union[AttributeFilter, QueryFilter]] = None
    filter_lang: str = "cql2-json"
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    aggregations: Optional[List[AggregationRule]] = Field(None, alias="aggregate")

class CollectionSearchRequest(BaseModel):
    catalog_id: Optional[str] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    datetime: Optional[str] = None
    keywords: Optional[List[str]] = None
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)

logger = logging.getLogger(__name__)

def _get_simplification_sql(config: Optional[StacPluginConfig]) -> str:
    """Builds the dynamic simplification SQL CASE statement from configuration."""
    if not config or not config.simplification:
        return "0.0001"
    
    simp = config.simplification
    case_sql = "CASE "
    for points, tolerance in sorted(simp.vertex_thresholds.items(), key=lambda item: item[0], reverse=True):
        case_sql += f"WHEN ST_NPoints(geom) > {points} THEN {tolerance} "
    case_sql += f"ELSE {simp.default_tolerance} END"
    return f"({case_sql})"

def _build_attribute_filter_sql(filt: AttributeFilter, params: dict) -> str:
    param_name = f"filter_{filt.field}_{filt.operator.value}"
    field_accessor = f"attributes->>'{filt.field}'"
    type_cast = "::numeric" if filt.operator in ["lt", "lte", "gt", "gte"] else ""
    sql_operator_map = {"eq": "=", "neq": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "like": "LIKE", "ilike": "ILIKE"}
    sql_op = sql_operator_map[filt.operator.value]
    params[param_name] = filt.value
    return f"({field_accessor}{type_cast} {sql_op} :{param_name})"

def _build_complex_filter_sql(query_filter: QueryFilter, params: dict) -> str:
    if not query_filter.args: return ""
    op_sql = f" {query_filter.op.upper()} "
    conditions = []
    for arg in query_filter.args:
        if isinstance(arg, QueryFilter): conditions.append(_build_complex_filter_sql(arg, params))
        else: conditions.append(_build_attribute_filter_sql(arg, params))
    return f"({op_sql.join(filter(None, conditions))})"

async def search_items(
    db_resource: DbResource, 
    search_request: ItemSearchRequest, 
    stac_config: StacPluginConfig,
    hierarchy_sql: Optional[str] = None,
    hierarchy_params: Optional[Dict[str, Any]] = None
) -> Tuple[list, int, Optional[Dict[str, Any]]]:
    from dynastore.modules.db_config.tools import get_any_engine
    engine = get_any_engine(db_resource)

    catalogs = get_protocol(CatalogsProtocol)
    initial_collection_ids = search_request.collections or [c.id for c in await catalogs.list_collections(search_request.catalog_id, limit=1000, db_resource=db_resource)]
    
    async def _check_layer_def(cid):
        # FIX: Use get_collection_config instead of cached version.
        # The ConfigManager handles caching internally.
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.get_collection_config(search_request.catalog_id, cid, db_resource=db_resource)
    
    results = await asyncio.gather(*[_check_layer_def(cid) for cid in initial_collection_ids])
    target_collections = [initial_collection_ids[i] for i, config in enumerate(results) if config is not None]

    if not target_collections: return [], 0, None

    async with engine.connect() as conn:
        catalogs = get_protocol(CatalogsProtocol)
        table_columns = await catalogs.get_collection_column_names(search_request.catalog_id, target_collections[0], db_resource=conn)
        where_sql, params = build_filter_clause(table_columns=table_columns, datetime_str=search_request.datetime, bbox=search_request.bbox, intersects=search_request.intersects, ids=search_request.ids)
    
    if search_request.filter:
        cql_filter_sql = None
        if isinstance(search_request.filter, QueryFilter): cql_filter_sql = _build_complex_filter_sql(search_request.filter, params)
        elif isinstance(search_request.filter, AttributeFilter): cql_filter_sql = _build_attribute_filter_sql(search_request.filter, params)
        if cql_filter_sql: where_sql += f" AND {cql_filter_sql}"

    if hierarchy_sql:
        where_sql += f" AND {hierarchy_sql}"
    if hierarchy_params:
        params.update(hierarchy_params)

    # Resolve physical schema
    catalogs = get_protocol(CatalogsProtocol)
    phys_schema = await catalogs.resolve_physical_schema(search_request.catalog_id, db_resource=db_resource)

    select_fragments = []
    for collection_id in target_collections:
        catalogs = get_protocol(CatalogsProtocol)
        phys_table = await catalogs.resolve_physical_table(search_request.catalog_id, collection_id, db_resource=db_resource)
        if not phys_table: continue

        fragment = f"""
            SELECT '{search_request.catalog_id}' as catalog_id, '{collection_id}' as collection_id, id, external_id, geoid, transaction_time, lower(validity) as valid_from, upper(validity) as valid_to, geom_type, attributes, asset_id, content_hash, geom
            FROM "{phys_schema}"."{phys_table}" WHERE {where_sql}
        """
        select_fragments.append(fragment)

    full_union_query = " UNION ALL ".join(select_fragments)
    count_query = f"SELECT COUNT(*) FROM ({full_union_query}) AS items"
    
    simplification_sql = _get_simplification_sql(stac_config)
    data_query = f""" 
        WITH matched_items AS ({full_union_query})
        SELECT *,
            ST_AsEWKB(ST_SimplifyPreserveTopology(geom, {simplification_sql})) AS simplified_geom,
            ST_XMin(geom) as bbox_xmin, ST_YMin(geom) as bbox_ymin, ST_XMax(geom) as bbox_xmax, ST_YMax(geom) as bbox_ymax
        FROM matched_items ORDER BY valid_from DESC, id LIMIT :limit OFFSET :offset
    """
    
    final_params = {"limit": search_request.limit, "offset": search_request.offset, **params}
    final_params = {"limit": search_request.limit, "offset": search_request.offset, **params}
    
    # Execute sequentially using DQLQuery to handle both sync and async engines safely
    count_result = await DQLQuery(count_query, result_handler=ResultHandler.SCALAR_ONE).execute(db_resource, **params)
    rows_result = await DQLQuery(data_query, result_handler=ResultHandler.ALL_DICTS).execute(db_resource, **final_params)
    total_count = count_result

    aggregation_results = None
    if search_request.aggregations:
        from dynastore.extensions.stac.stac_aggregations import execute_aggregations
        async with engine.connect() as agg_conn:
            aggregation_results = await execute_aggregations(
                agg_conn, search_request.catalog_id, target_collections, search_request.aggregations, where_sql, params
            )

    rows = []
    for row_dict in rows_result:
        row_dict['geom'] = row_dict.pop('simplified_geom')
        rows.append(row_dict)
    return rows, total_count, aggregation_results

async def search_collections(db_resource: DbResource, search_request: CollectionSearchRequest) -> Tuple[List[Collection], int]:
    # Resolve physical schema if catalog_id is provided
    schema = "catalog"
    if search_request.catalog_id:
        from dynastore.models.protocols import CatalogsProtocol
        catalogs = get_protocol(CatalogsProtocol)
        schema = await catalogs.resolve_physical_schema(search_request.catalog_id, db_resource)
    
    where_clauses = ["deleted_at IS NULL"]; params = {}
    if search_request.catalog_id: where_clauses.append("catalog_id = :catalog_id"); params["catalog_id"] = search_request.catalog_id
    if search_request.ids: where_clauses.append("id = ANY(:ids)"); params["ids"] = search_request.ids
    if search_request.keywords: where_clauses.append("metadata->'keywords' @> :keywords::jsonb"); params["keywords"] = str(search_request.keywords).replace("'", '"')
    if search_request.bbox:
        where_clauses.append("""EXISTS (SELECT 1 FROM jsonb_array_elements(metadata->'extent'->'spatial'->'bbox') AS bbox WHERE ST_Intersects(ST_MakeEnvelope((bbox->>0)::float, (bbox->>1)::float, (bbox->>2)::float, (bbox->>3)::float, 4326), ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326)))""")
        params.update({"xmin": search_request.bbox[0], "ymin": search_request.bbox[1], "xmax": search_request.bbox[2], "ymax": search_request.bbox[3]})
    
    where_sql = " AND ".join(where_clauses)
    base_query = f"FROM {schema}.collections WHERE {where_sql}"
    count_query = f"SELECT count(*) {base_query}"
    data_query = f"SELECT metadata {base_query} ORDER BY catalog_id, id LIMIT :limit OFFSET :offset"
    final_params = {"limit": search_request.limit, "offset": search_request.offset, **params}

    final_params = {"limit": search_request.limit, "offset": search_request.offset, **params}

    # Execute sequentially using DQLQuery safety wrapper
    count_result = await DQLQuery(count_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE).execute(db_resource, **params)
    rows_result = await DQLQuery(data_query, result_handler=ResultHandler.ALL_DICTS).execute(db_resource, **final_params)
    total_count = count_result or 0
    collections = [Collection.model_validate(row['metadata']) for row in rows_result]
    return collections, total_count