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

"""Build QueryRequest objects for DGGS zone or bbox-based PostGIS queries."""

from typing import Optional, Tuple

from dynastore.models.query_builder import QueryRequest
from dynastore.modules.dggs.h3_indexer import (
    cell_str_to_int,
    cell_to_geojson_polygon,
    get_resolution,
    is_valid_cell,
)


def bbox_for_zone(zone_id: str) -> Tuple[float, float, float, float]:
    """Return (xmin, ymin, xmax, ymax) bounding box for an H3 cell.

    Derives the bbox from the cell's boundary vertices.
    """
    if not is_valid_cell(zone_id):
        raise ValueError(f"Invalid H3 zone ID: {zone_id!r}")
    polygon = cell_to_geojson_polygon(zone_id)
    coords = polygon["coordinates"][0]
    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return min(lngs), min(lats), max(lngs), max(lats)


def build_query_for_zone(
    zone_id: str,
    datetime_str: Optional[str] = None,
    limit: int = 10_000,
) -> QueryRequest:
    """Build a QueryRequest filtered to the bbox of a specific H3 zone.

    The bbox filter lets the PostGIS driver apply a spatial index scan.
    The caller is responsible for further filtering to the exact H3 boundary
    via the aggregator.
    """
    xmin, ymin, xmax, ymax = bbox_for_zone(zone_id)
    return _build_query(
        xmin=xmin,
        ymin=ymin,
        xmax=xmax,
        ymax=ymax,
        datetime_str=datetime_str,
        limit=limit,
    )


def build_query_for_bbox(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    datetime_str: Optional[str] = None,
    limit: int = 10_000,
) -> QueryRequest:
    """Build a QueryRequest filtered to the given WGS-84 bounding box."""
    return _build_query(
        xmin=xmin,
        ymin=ymin,
        xmax=xmax,
        ymax=ymax,
        datetime_str=datetime_str,
        limit=limit,
    )


def build_query_for_zone_indexed(
    zone_id: str,
    datetime_str: Optional[str] = None,
    limit: int = 10_000,
) -> QueryRequest:
    """Build a QueryRequest using an exact H3 cell equality filter on the pre-computed index.

    This is the **preferred** path when the geometry sidecar has ``h3_res{N}``
    pre-computed (i.e. ``GeometriesSidecarConfig.h3_resolutions`` includes the
    zone's resolution).  It uses a B-tree EQ lookup on the BIGINT index column
    instead of a GIST bbox scan, which is:

    * Exact — no bbox overselection; every returned feature is guaranteed to
      belong to ``zone_id``.
    * Fast — integer B-tree equality vs. spatial index overlap.

    The caller must verify that the collection actually has ``h3_res{resolution}``
    as a queryable field before calling this function (see
    :meth:`DGGSService._has_h3_field`).  Use :func:`build_query_for_zone` as the
    fallback when the index column is not available.
    """
    if not is_valid_cell(zone_id):
        raise ValueError(f"Invalid H3 zone ID: {zone_id!r}")
    resolution = get_resolution(zone_id)
    cell_int = cell_str_to_int(zone_id)
    return _build_query(
        h3_field=f"h3_res{resolution}",
        h3_cell_int=cell_int,
        datetime_str=datetime_str,
        limit=limit,
    )


def build_global_query(
    datetime_str: Optional[str] = None,
    limit: int = 10_000,
) -> QueryRequest:
    """Build an unfiltered QueryRequest (no spatial constraint)."""
    return _build_query(datetime_str=datetime_str, limit=limit)


def _build_query(
    xmin: Optional[float] = None,
    ymin: Optional[float] = None,
    xmax: Optional[float] = None,
    ymax: Optional[float] = None,
    h3_field: Optional[str] = None,
    h3_cell_int: Optional[int] = None,
    datetime_str: Optional[str] = None,
    limit: int = 10_000,
) -> QueryRequest:
    from dynastore.models.query_builder import FilterCondition, FilterOperator

    filters = []

    if h3_field is not None and h3_cell_int is not None:
        # Preferred path: exact B-tree equality on pre-computed sidecar column.
        filters.append(
            FilterCondition(
                field=h3_field,
                operator=FilterOperator.EQ,
                value=h3_cell_int,
            )
        )
    elif all(v is not None for v in [xmin, ymin, xmax, ymax]):
        # Fallback path: GIST bbox scan (may overselect near hexagon edges).
        ewkt = (
            f"SRID=4326;POLYGON(({xmin} {ymin},{xmax} {ymin},"
            f"{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
        )
        filters.append(
            FilterCondition(
                field="geom",
                operator=FilterOperator.BBOX,
                value=ewkt,
                spatial_op=True,
            )
        )

    if datetime_str:
        filters.append(
            FilterCondition(
                field="datetime",
                operator=FilterOperator.EQ,
                value=datetime_str,
            )
        )

    return QueryRequest(limit=limit, filters=filters)
