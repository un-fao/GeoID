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
import re
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
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config.shared_queries import (
    build_filter_clause,
    get_table_column_names,
)
from dynastore.models.query_builder import (
    QueryRequest,
    FieldSelection,
    FilterCondition,
    SortOrder,
    FilterOperator,
)
from dynastore.modules.catalog.query_optimizer import QueryOptimizer


from dynastore.modules.stac.stac_config import AggregationRule


class AttributeFilter(BaseModel):
    field: str
    operator: FilterOperator
    value: Any


class QueryFilter(BaseModel):
    op: str = Field("and")
    args: List[Union["QueryFilter", AttributeFilter]]


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
    sortby: Optional[str] = Field(
        None,
        description=(
            "Sort field. Prefix with '+' for ascending, '-' for descending. "
            "Aliases: 'code'=id, 'label'=title. E.g. '+code', '-label'."
        ),
    )
    lang: Optional[str] = Field(
        None,
        description="Language code for multilingual sort (e.g. 'en', 'fr'). Default: 'en'.",
    )


logger = logging.getLogger(__name__)


def _get_simplification_sql(config: Optional[StacPluginConfig]) -> str:
    """Builds the dynamic simplification SQL CASE statement from configuration."""
    if not config or not config.simplification:
        return "0.0001"

    simp = config.simplification
    case_sql = "CASE "
    for points, tolerance in sorted(
        simp.vertex_thresholds.items(), key=lambda item: item[0], reverse=True
    ):
        case_sql += f"WHEN ST_NPoints(geom) > {points} THEN {tolerance} "
    case_sql += f"ELSE {simp.default_tolerance} END"
    return f"({case_sql})"


_SAFE_ATTRIBUTE_FIELD_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

_COLLECTION_SORT_ALIASES: dict = {"code": "id", "label": "title"}
_COLLECTION_DIRECT_SORT_COLUMNS = frozenset({"id", "catalog_id"})


def _parse_collection_sort_sql(sortby: Optional[str], lang: Optional[str] = None) -> str:
    """
    Convert a sortby string to a safe SQL ORDER BY clause for collection search.

    Aliases: code → id, label → title
    Multilingual: title → (metadata->'title'->>'{lang}')
    Custom fields: (metadata->>'field') validated against _SAFE_ATTRIBUTE_FIELD_RE
    """
    if not sortby:
        return "catalog_id ASC, id ASC"
    direction = "DESC" if sortby.startswith("-") else "ASC"
    raw_field = sortby.lstrip("+-")
    field = _COLLECTION_SORT_ALIASES.get(raw_field, raw_field)
    if field in _COLLECTION_DIRECT_SORT_COLUMNS:
        return f"{field} {direction}"
    if field == "title":
        safe_lang = lang if (lang and re.match(r"^[a-z]{2,3}$", lang)) else "en"
        return f"(metadata->'title'->>'{safe_lang}') {direction}"
    if _SAFE_ATTRIBUTE_FIELD_RE.match(field):
        return f"(metadata->>'{field}') {direction}"
    raise ValueError(f"Invalid sort field: {field!r}")


def _build_attribute_filter_sql(filt: AttributeFilter, params: dict) -> str:
    if not _SAFE_ATTRIBUTE_FIELD_RE.match(filt.field):
        raise ValueError(
            f"Invalid attribute field name '{filt.field}': "
            "must start with a letter or underscore and contain only alphanumeric characters and underscores."
        )
    op: FilterOperator = (
        filt.operator if isinstance(filt.operator, FilterOperator)
        else FilterOperator.from_str(str(filt.operator))
    )
    sql_op = op.to_sql()
    type_cast = "::numeric" if op.needs_numeric_cast else ""
    param_name = f"filter_{filt.field}_{op.value}"
    field_accessor = f"attributes->>'{filt.field}'"
    params[param_name] = filt.value
    return f"({field_accessor}{type_cast} {sql_op} :{param_name})"


def _build_complex_filter_sql(query_filter: QueryFilter, params: dict) -> str:
    if not query_filter.args:
        return ""
    op_sql = f" {query_filter.op.upper()} "
    conditions = []
    for arg in query_filter.args:
        if isinstance(arg, QueryFilter):
            conditions.append(_build_complex_filter_sql(arg, params))
        else:
            conditions.append(_build_attribute_filter_sql(arg, params))
    return f"({op_sql.join(filter(None, conditions))})"


async def search_items(
    db_resource: DbResource,
    search_request: ItemSearchRequest,
    stac_config: StacPluginConfig,
    hierarchy_sql: Optional[str] = None,
    hierarchy_params: Optional[Dict[str, Any]] = None,
) -> Tuple[list, int, Optional[Dict[str, Any]]]:
    from sqlalchemy.ext.asyncio import AsyncEngine

    catalogs = get_protocol(CatalogsProtocol)

    # Helper to execute with a connection (reusing if present, connecting if engine)
    async def get_columns_with_conn(resource):
        if isinstance(resource, AsyncEngine):
            async with resource.connect() as conn:
                return await catalogs.get_collection_column_names(
                    search_request.catalog_id, target_collections[0], db_resource=conn
                )
        else:
            return await catalogs.get_collection_column_names(
                search_request.catalog_id, target_collections[0], db_resource=resource
            )

    # Resolve layer config in parallel
    initial_collection_ids = search_request.collections or [
        c.id
        for c in await catalogs.list_collections(
            search_request.catalog_id, limit=1000, db_resource=db_resource
        )
    ]

    async def _check_layer_def(cid):
        catalogs = get_protocol(CatalogsProtocol)
        return await catalogs.get_collection_config(
            search_request.catalog_id, cid, db_resource=db_resource
        )

    results = await asyncio.gather(
        *[_check_layer_def(cid) for cid in initial_collection_ids]
    )

    # Store configs map
    collection_configs = {
        initial_collection_ids[i]: config
        for i, config in enumerate(results)
        if config is not None
    }
    target_collections = list(collection_configs.keys())

    if not target_collections:
        return [], 0, None

    # Get column names for filtering
    table_columns = await get_columns_with_conn(db_resource)

    where_sql, params = build_filter_clause(
        table_columns=table_columns,
        datetime_str=search_request.datetime,
        bbox=None,  # Handled via raw_where in loop
        intersects=None,  # Handled via raw_where in loop
        ids=None,  # Handled via QueryOptimizer filters
    )

    if search_request.filter:
        cql_filter_sql = None
        if isinstance(search_request.filter, QueryFilter):
            cql_filter_sql = _build_complex_filter_sql(search_request.filter, params)
        elif isinstance(search_request.filter, AttributeFilter):
            cql_filter_sql = _build_attribute_filter_sql(search_request.filter, params)
        if cql_filter_sql:
            where_sql += f" AND {cql_filter_sql}"

    if hierarchy_sql:
        where_sql += f" AND {hierarchy_sql}"
    if hierarchy_params:
        params.update(hierarchy_params)

    # Resolve physical schema
    catalogs = get_protocol(CatalogsProtocol)
    phys_schema = await catalogs.resolve_physical_schema(
        search_request.catalog_id, db_resource=db_resource
    )

    # --- Query Planner: Dependency Analysis ---
    # Detect which sidecars are strictly required by the filter criteria

    req_geometry = bool(
        search_request.bbox
        or search_request.intersects
        or (hierarchy_sql and "geom" in hierarchy_sql)
        or "geom" in where_sql
    )

    req_stac = bool(
        "stac_" in where_sql
        or "external_extensions" in where_sql
        or "external_assets" in where_sql
    )

    # "Attributes" sidecar is needed if we filter on properties, IDs, or datetime (validity)
    req_attributes = bool(
        search_request.ids
        or search_request.datetime
        or search_request.filter
        or "attributes" in where_sql
        or "external_id" in where_sql
    )

    # --- Query Planner: Sort Strategy Optimization ---
    # If we are in "Spatial Only" mode (Geometry query, no attribute filters),
    # we can optimize by sorting on Hub columns (transaction_time) instead of joining Attributes (validity).
    # Otherwise, we default to standard STAC sorting (validity, external_id) which requires Attributes.

    if req_geometry and not req_attributes and not req_stac:
        # Optimization: Spatial Only Mode
        # Sort by Hub columns to avoid extra join
        sort_mode = "hub"
        sort_col = "h.transaction_time"
        sort_id = "h.geoid::text"
    else:
        # Standard Mode
        # Sort by Attributes (validity)
        sort_mode = "attributes"
        sort_col = "lower(s.validity)"  # Assumes 's' alias for attributes
        sort_id = "s.external_id"
        req_attributes = True  # Force attributes join for sorting

    candidate_fragments = []

    # Store table mapping for hydration: collection_id -> (phys_table, sidecar_table, geom_table, stac_meta_table)
    collection_table_map = {}

    for collection_id in target_collections:
        config = collection_configs[collection_id]

        # Instantiate QueryOptimizer for this collection
        optimizer = QueryOptimizer(config)

        # --- Query Request Construction ---

        query_selects = []
        raw_selects = [
            f"'{search_request.catalog_id}' as catalog_id",
            f"'{collection_id}' as collection_id",
        ]

        # We always need geoid
        query_selects.append(FieldSelection(field="geoid"))

        # --- Filter Logic Mapping ---
        query_filters = []
        raw_where_clauses = []

        # Start with global params copy to avoid pollution
        query_params = params.copy()

        # Spatial Filters -> Geometry Sidecar (via QueryOptimizer)
        # We use raw_where to handle spatial operators cleanly (avoiding casting issues in QueryOptimizer '&&')
        # BUT QueryOptimizer only joins sidecars if fields are used in SELECT/FILTER/SORT/GROUP.
        # So we MUST force Geometry sidecar usage if we have spatial filters.
        # We do this by selecting a cheap property: ST_SRID(geometry).

        has_spatial_filter = False

        if search_request.bbox:
            # Construct WKT Polygon for BBOX (if needed) but here we use ST_MakeEnvelope
            # Use 'geom' instead of 'geometry' to avoid ambiguity with attributes sidecar
            raw_where_clauses.append(
                "ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326) && geom"
            )
            query_params.update(
                {
                    "xmin": search_request.bbox[0],
                    "ymin": search_request.bbox[1],
                    "xmax": search_request.bbox[2],
                    "ymax": search_request.bbox[3],
                }
            )
            has_spatial_filter = True

        if search_request.intersects:
            # Use ST_Intersects(ST_GeomFromGeoJSON(:intersects), geom)
            raw_where_clauses.append(
                "ST_Intersects(ST_GeomFromGeoJSON(:intersects), geom)"
            )
            query_params["intersects"] = json.dumps(search_request.intersects)
            has_spatial_filter = True

        # Force Geometry Sidecar if needed
        if has_spatial_filter:
            # We use ST_SRID as a transformation to trigger sidecar join in QueryOptimizer
            # alias starts with _ to indicate internal use
            # Use 'geom' to avoid ambiguity with attributes.geometry JSONB
            query_selects.append(
                FieldSelection(field="geom", transformation="ST_SRID", alias="_srid")
            )

        # Hierarchy SQL (recursive CTE usually) -> Handled via raw_where below if present

        # Attribute Filters -> Attributes Sidecar
        if search_request.ids:
            # Use "id" field (aliased external_id)
            # Operator "IN" with tuple.
            query_filters.append(
                FilterCondition(
                    field="id",
                    operator=FilterOperator.IN,
                    value=list(search_request.ids),
                )
            )

        # Datetime is usually handled in where_sql (from build_filter_clause).
        # We added where_sql to raw_where.
        # But we need to ensure Attributes sidecar is joined if datetime filter exists.

        # Use the pre-computed where_sql from build_filter_clause as raw_where
        # This includes datetime, cql_filter logic.

        combined_raw_where = []
        raw_where_clauses_str = " AND ".join(raw_where_clauses)
        if raw_where_clauses_str:
            combined_raw_where.append(raw_where_clauses_str)

        if where_sql:
            # Prefixes deleted_at to avoid ambiguity
            where_sql = where_sql.replace("deleted_at IS NULL", "h.deleted_at IS NULL")
            combined_raw_where.append(where_sql)

        # --- Optimization Mode: Spatial Only ---

        # Check if we need Attributes Sidecar based on filters
        req_attributes = bool(
            search_request.ids
            or search_request.datetime
            or search_request.filter
            or "attributes" in where_sql
            or "external_id" in where_sql
            or "validity" in where_sql
        )

        req_stac = bool(
            "stac_" in where_sql
            or "external_extensions" in where_sql
            or "external_assets" in where_sql
        )

        # Select & Sort Strategy
        if req_attributes:
            # Standard Mode: Sort by Attributes (validity)
            query_selects.append(FieldSelection(field="validity", alias="valid_from"))
            # Use raw select for ID coalescence
            # QueryOptimizer uses sc_attributes for the attributes sidecar
            raw_selects.append(
                "COALESCE(sc_attributes.external_id, h.geoid::text) as id"
            )

        elif not req_stac:
            # Optimization: Spatial Only Mode (and no STAC)
            # Sort by Hub columns
            query_selects.append(
                FieldSelection(field="transaction_time", alias="valid_from")
            )  # Hub field
            raw_selects.append("h.geoid::text as id")  # Hub field
        else:
            # Fallback
            query_selects.append(
                FieldSelection(field="transaction_time", alias="valid_from")
            )
            raw_selects.append("h.geoid::text as id")

        # Construct QueryRequest
        query_req = QueryRequest(
            select=query_selects,
            raw_selects=raw_selects,
            filters=query_filters,
            raw_where=" AND ".join(combined_raw_where) if combined_raw_where else None,
            raw_params=query_params,
            limit=None,
            offset=None,
        )

        catalogs = get_protocol(CatalogsProtocol)
        phys_table = await catalogs.resolve_physical_table(
            search_request.catalog_id, collection_id, db_resource=db_resource
        )
        if not phys_table:
            continue

        # Build SQL
        # Use collection_id as param_prefix to avoid collisions in UNION ALL
        # We sanitize the prefix because it's used in SQL param names (must be alphanumeric)
        safe_prefix = re.sub(r"[^a-zA-Z0-9]", "_", collection_id)

        try:
            sql_fragment, fragment_params = optimizer.build_optimized_query(
                query_req,
                schema=phys_schema,
                table=phys_table,
                param_prefix=safe_prefix,
            )

            # Correct any hardcoded 's.' aliases in the fragment if they survived (should be sc_attributes)
            # Actually we fixed them in raw_selects above.

            candidate_fragments.append(sql_fragment)

            # Merge fragment params into the main execution params
            if fragment_params:
                params.update(fragment_params)

            # Update mapping for hydration
            sidecar_table = f"{phys_table}_attributes"
            geom_table = f"{phys_table}_geometries"
            stac_meta_table = f"{phys_table}_stac_metadata"

            has_attr_sidecar = False
            has_geom_sidecar = False
            has_stac_sidecar = False
            if config and config.sidecars:
                for sc in config.sidecars:
                    if not sc.enabled:
                        continue
                    stype = getattr(sc, "sidecar_type", "")
                    if stype in ["attributes", "feature_attributes"]:
                        has_attr_sidecar = True
                    elif stype in ["geometries", "geometry"]:
                        has_geom_sidecar = True
                    elif stype in ["stac_metadata"]:
                        has_stac_sidecar = True

            collection_table_map[collection_id] = (
                phys_table,
                sidecar_table,
                geom_table,
                stac_meta_table,
                has_attr_sidecar,
                has_geom_sidecar,
                has_stac_sidecar,
            )

        except Exception as e:
            logger.error(
                f"Failed to build optimized query for collection {collection_id}: {e}"
            )
            import traceback

            logger.error(traceback.format_exc())
            continue

    if not candidate_fragments:
        return [], 0, None

    full_union_query = " UNION ALL ".join(candidate_fragments)

    # Merged paging + count using a window function (1 query instead of 2)
    paging_query = f"""
        WITH candidate_matches AS ({full_union_query})
        SELECT catalog_id, collection_id, geoid, valid_from, id,
               COUNT(*) OVER() AS _total_count
        FROM candidate_matches
        ORDER BY valid_from DESC, id
        LIMIT :limit OFFSET :offset
    """

    final_params = {
        "limit": search_request.limit,
        "offset": search_request.offset,
        **params,
    }

    logger.debug(f"EXECUTE PAGING+COUNT QUERY: {paging_query}")
    page_rows = await DQLQuery(
        paging_query, result_handler=ResultHandler.ALL_DICTS
    ).execute(db_resource, **final_params)

    if not page_rows:
        return [], 0, None

    total = int(page_rows[0]["_total_count"])

    # Hydration Phase: Single UNION ALL across all matching collections
    # Build one fragment per collection, combined into one query.
    simplification_sql = _get_simplification_sql(stac_config)

    from collections import defaultdict

    ids_by_collection: dict = defaultdict(list)
    for row in page_rows:
        ids_by_collection[row["collection_id"]].append(row["geoid"])

    hydration_fragments = []
    hydration_params: dict = {}

    for coll_idx, (coll_id, geoids) in enumerate(ids_by_collection.items()):
        if coll_id not in collection_table_map:
            continue

        p_tab, s_tab, g_tab, sm_tab, has_attr, has_geom, has_stac = (
            collection_table_map[coll_id]
        )

        geoid_param = f"geoids_{coll_idx}"
        hydration_params[geoid_param] = geoids

        select_parts = [
            f"SELECT '{search_request.catalog_id}' as catalog_id,"
            f" '{coll_id}' as collection_id,"
            f" h.geoid, h.transaction_time, h.content_hash"
        ]
        joins = [f'FROM "{phys_schema}"."{p_tab}" h']

        if has_attr:
            select_parts.append(
                ", COALESCE(s.external_id, h.geoid::text) as id"
                ", s.external_id"
                ", lower(s.validity) as valid_from"
                ", upper(s.validity) as valid_to"
                ", s.attributes, s.asset_id"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{s_tab}" s ON h.geoid = s.geoid')
        else:
            select_parts.append(
                ", h.geoid::text as id, h.geoid::text as external_id"
                ", h.transaction_time as valid_from, null as valid_to"
                ", '{}'::jsonb as attributes, null as asset_id"
            )

        if has_geom:
            select_parts.append(
                f", ST_AsEWKB(ST_SimplifyPreserveTopology(g.geom, {simplification_sql})) AS simplified_geom"
                f", ST_XMin(g.geom) as bbox_xmin, ST_YMin(g.geom) as bbox_ymin"
                f", ST_XMax(g.geom) as bbox_xmax, ST_YMax(g.geom) as bbox_ymax"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{g_tab}" g ON h.geoid = g.geoid')
        else:
            select_parts.append(
                ", null as simplified_geom"
                ", null as bbox_xmin, null as bbox_ymin"
                ", null as bbox_xmax, null as bbox_ymax"
            )

        if has_stac:
            select_parts.append(
                ", sm.title as stac_title, sm.description as stac_description"
                ", sm.keywords as stac_keywords, sm.external_extensions"
                ", sm.external_assets, sm.extra_fields as stac_extra_fields"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{sm_tab}" sm ON h.geoid = sm.geoid')
        else:
            select_parts.append(
                ", null as stac_title, null as stac_description"
                ", null as stac_keywords, null as external_extensions"
                ", null as external_assets, null as stac_extra_fields"
            )

        fragment = (
            "".join(select_parts)
            + " " + " ".join(joins)
            + f" WHERE h.geoid = ANY(:{geoid_param})"
        )
        hydration_fragments.append(fragment)

    hydrated_items = {}
    if hydration_fragments:
        hydration_sql = " UNION ALL ".join(hydration_fragments)
        logger.debug(f"EXECUTE HYDRATION QUERY: {hydration_sql}")
        all_results = await DQLQuery(
            hydration_sql, result_handler=ResultHandler.ALL_DICTS
        ).execute(db_resource, **hydration_params)
        for item in all_results:
            hydrated_items[(item["collection_id"], item["geoid"])] = item


    # Reconstruct sorted list
    rows = []
    
    from dynastore.models.protocols import ItemsProtocol
    items_svc = get_protocol(ItemsProtocol)
    
    for row in page_rows:
        key = (row["collection_id"], row["geoid"])
        if key in hydrated_items:
            item_data = hydrated_items[key]
            if item_data.get("simplified_geom"):
                item_data["geom"] = item_data.pop("simplified_geom")
            else:
                item_data["geom"] = None
            
            if items_svc:
                col_config = collection_configs.get(item_data["collection_id"])
                # We need to preserve original `item_data` values for sidecars (e.g., stac_title/stac_description)
                # ItemsProtocol will place these nicely into `feature.properties` or `feature.stac_extensions` etc.
                feature = items_svc.map_row_to_feature(item_data, col_config=col_config)
                if feature:
                    if feature.properties is None:
                        feature.properties = {}
                    feature.properties["_catalog_id"] = item_data["catalog_id"]
                    feature.properties["_collection_id"] = item_data["collection_id"]
                    rows.append(feature)
            else:
                rows.append(item_data)

    # Aggregations
    aggregation_results = None
    if search_request.aggregations:
        from dynastore.extensions.stac.stac_aggregations import execute_aggregations

        # Hints for joins in aggregation queries (frozenset for deterministic cache keys)
        _hints: set = set()
        # If we have attribute filters, we likely need attributes sidecar
        if "attributes" in where_sql or "properties" in str(search_request.filter):
            _hints.add("attributes")
        # If we have spatial filters, we likely need geometry sidecar
        if "geom" in where_sql or search_request.bbox or search_request.intersects:
            _hints.add("geometry")
        hints = frozenset(_hints)

        if isinstance(db_resource, AsyncEngine):
            async with db_resource.connect() as agg_conn:
                aggregation_results = await execute_aggregations(
                    agg_conn,
                    search_request.catalog_id,
                    target_collections,
                    search_request.aggregations,
                    where_sql,
                    params,
                    filter_hints=hints,
                )
        else:
            aggregation_results = await execute_aggregations(
                db_resource,
                search_request.catalog_id,
                target_collections,
                search_request.aggregations,
                where_sql,
                params,
                filter_hints=hints,
            )

    return rows, total, aggregation_results


async def search_collections(
    db_resource: DbResource, search_request: CollectionSearchRequest
) -> Tuple[List[Collection], int]:
    # Determine which schemas to search
    target_schemas = []

    if search_request.catalog_id:
        from dynastore.models.protocols import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        # Check if catalog exists and get schema
        schema = await catalogs.resolve_physical_schema(
            search_request.catalog_id, db_resource
        )
        if schema:
            target_schemas.append(schema)
        # If catalog_id provided but not found, filtering by it implies no results
    else:
        # Search all catalogs
        # Retrieve all physical schemas from catalog.catalogs
        # We query the catalog registry directly to find all active physical schemas
        catalog_query = (
            "SELECT physical_schema FROM catalog.catalogs WHERE deleted_at IS NULL"
        )
        try:
            target_schemas = await DQLQuery(
                catalog_query, result_handler=ResultHandler.ALL_SCALARS
            ).execute(db_resource)
        except Exception as e:
            logger.warning(f"Failed to retrieve catalog schemas for global search: {e}")
            return [], 0

    if not target_schemas:
        return [], 0

    where_clauses = ["deleted_at IS NULL"]
    params = {}

    # Common filters
    if search_request.catalog_id:
        where_clauses.append("catalog_id = :catalog_id")
        params["catalog_id"] = search_request.catalog_id

    if search_request.ids:
        where_clauses.append("id = ANY(:ids)")
        params["ids"] = search_request.ids

    if search_request.keywords:
        where_clauses.append("metadata->'keywords' @> CAST(:keywords AS jsonb)")
        params["keywords"] = str(search_request.keywords).replace("'", '"')

    if search_request.bbox:
        where_clauses.append(
            """EXISTS (SELECT 1 FROM jsonb_array_elements(metadata->'extent'->'spatial'->'bbox') AS bbox WHERE ST_Intersects(ST_MakeEnvelope((bbox->>0)::float, (bbox->>1)::float, (bbox->>2)::float, (bbox->>3)::float, 4326), ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326)))"""
        )
        params.update(
            {
                "xmin": search_request.bbox[0],
                "ymin": search_request.bbox[1],
                "xmax": search_request.bbox[2],
                "ymax": search_request.bbox[3],
            }
        )

    where_sql = " AND ".join(where_clauses)

    # Build UNION ALL query
    # effectively: SELECT metadata, catalog_id, id FROM schema1.collections WHERE ... UNION ALL ...
    union_queries = []
    for schema in target_schemas:
        # Double quote schema to handle special characters or keywords
        union_queries.append(
            f'SELECT metadata, catalog_id, id FROM "{schema}".collections WHERE {where_sql}'
        )

    full_union_query = " UNION ALL ".join(union_queries)

    # use CTE to encapsulate the union for COUNT and paging
    base_query_cte = f"WITH all_matches AS ({full_union_query})"

    count_query = f"{base_query_cte} SELECT count(*) FROM all_matches"

    order_by = _parse_collection_sort_sql(search_request.sortby, search_request.lang)
    # We select metadata from the matches
    data_query = minified_data_query = (
        f"{base_query_cte} SELECT metadata FROM all_matches ORDER BY {order_by} LIMIT :limit OFFSET :offset"
    )

    final_params = {
        "limit": search_request.limit,
        "offset": search_request.offset,
        **params,
    }

    # Execute sequentially
    try:
        count_result = await DQLQuery(
            count_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(db_resource, **params)

        if not count_result:
            return [], 0

        rows_result = await DQLQuery(
            data_query, result_handler=ResultHandler.ALL_DICTS
        ).execute(db_resource, **final_params)

        total_count = count_result
        collections = [
            Collection.model_validate(row["metadata"]) for row in rows_result
        ]
        return collections, total_count
    except Exception as e:
        logger.error(f"Error executing collection search: {e}")
        # In case of partial schema failures (e.g. missing tables), we might want to return empty or raise
        # For now, safer to re-raise to see the error
        raise
