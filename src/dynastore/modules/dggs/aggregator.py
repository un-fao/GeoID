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

"""Aggregate PostGIS features into H3 cells on-the-fly."""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from dynastore.modules.dggs.h3_indexer import (
    cell_int_to_str,
    cell_to_geojson_polygon,
    latlng_to_cell,
)
from dynastore.modules.dggs.models import DGGSFeature, DGGSFeatureCollection, ZoneProperties


def _cell_from_stored_index(feature: Any, resolution: int) -> Optional[str]:
    """Read a pre-computed H3 cell from the geometry sidecar column ``h3_res{resolution}``.

    The geometry sidecar stores H3 indices as BIGINT (``int(cell_hex, 16)``).
    This function checks ``feature.properties`` for the key ``h3_res{resolution}``
    and converts the integer back to the canonical hex string cell ID.

    Returns None if the key is absent or the value is not a valid integer,
    triggering the fallback centroid-computation path.
    """
    key = f"h3_res{resolution}"
    props = _get_properties(feature)
    val = props.get(key)
    if val is None:
        return None
    try:
        int_val = int(val)
        return cell_int_to_str(int_val)
    except (TypeError, ValueError):
        return None


def _extract_centroid(feature: Any) -> Optional[tuple]:
    """Extract (lat, lng) from a GeoJSON feature.

    Supports Point geometry and features that expose centroid via properties.
    Returns None if the centroid cannot be determined.
    """
    geom = None
    if hasattr(feature, "geometry"):
        geom = feature.geometry
    elif isinstance(feature, dict):
        geom = feature.get("geometry")

    if not geom:
        return None

    # Normalise Pydantic geometry to a plain dict
    if hasattr(geom, "model_dump"):
        geom = geom.model_dump(by_alias=True, exclude_none=True)
    elif hasattr(geom, "dict"):
        geom = geom.dict(by_alias=True, exclude_none=True)
    elif not isinstance(geom, dict):
        return None

    geom_type = geom.get("type")
    coords = geom.get("coordinates")

    if geom_type == "Point" and coords and len(coords) >= 2:
        # GeoJSON Point: [lng, lat]
        return coords[1], coords[0]

    if geom_type == "Polygon" and coords:
        ring = coords[0]
        if ring:
            lngs = [v[0] for v in ring]
            lats = [v[1] for v in ring]
            return sum(lats) / len(lats), sum(lngs) / len(lngs)

    if geom_type == "MultiPolygon" and coords:
        all_coords = [v for ring in coords[0] for v in ring]
        if all_coords:
            lngs = [v[0] for v in all_coords]
            lats = [v[1] for v in all_coords]
            return sum(lats) / len(lats), sum(lngs) / len(lngs)

    return None


def _get_properties(feature: Any) -> Dict[str, Any]:
    """Extract properties dict from a GeoJSON feature."""
    if hasattr(feature, "properties"):
        props = feature.properties
        if props is None:
            return {}
        if hasattr(props, "model_dump"):
            return props.model_dump(by_alias=True, exclude_none=True) or {}
        return dict(props) if props else {}
    if isinstance(feature, dict):
        return dict(feature.get("properties") or {})
    return {}


def aggregate_features(
    features: List[Any],
    resolution: int,
    parameter_names: Optional[Set[str]] = None,
    dggs_id: str = "H3",
) -> DGGSFeatureCollection:
    """Aggregate a list of GeoJSON features into H3 cells at *resolution*.

    For each feature:
    1. Extract centroid (lat, lng)
    2. Compute H3 cell at *resolution*
    3. Count features per cell and compute per-property numeric averages

    Args:
        features: List of GeoJSON feature objects (Pydantic models or dicts).
        resolution: H3 resolution level (0-15).
        parameter_names: If set, only include these property names in aggregation.
        dggs_id: DGGRS identifier to embed in the response.

    Returns:
        DGGSFeatureCollection with one feature per occupied H3 cell.
    """
    cell_counts: Dict[str, int] = defaultdict(int)
    cell_sums: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    cell_numeric_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for feature in features:
        # Fast path: read pre-computed H3 index from the geometry sidecar column
        # (stored as BIGINT in h3_res{N}; only available if the collection's
        # GeometriesSidecarConfig includes the requested resolution).
        cell = _cell_from_stored_index(feature, resolution)
        if cell is None:
            # Slow path: extract centroid and compute H3 on-the-fly.
            centroid = _extract_centroid(feature)
            if centroid is None:
                continue
            lat, lng = centroid
            try:
                cell = latlng_to_cell(lat, lng, resolution)
            except Exception:
                continue

        cell_counts[cell] += 1
        props = _get_properties(feature)
        for key, value in props.items():
            if parameter_names and key not in parameter_names:
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cell_sums[cell][key] += float(value)
                cell_numeric_counts[cell][key] += 1

    dggs_features: List[DGGSFeature] = []
    for cell, count in cell_counts.items():
        avgs: Dict[str, Any] = {}
        for key, total in cell_sums[cell].items():
            n = cell_numeric_counts[cell][key]
            avgs[key] = round(total / n, 6) if n > 0 else None

        dggs_features.append(
            DGGSFeature(
                id=cell,
                geometry=cell_to_geojson_polygon(cell),
                properties=ZoneProperties(
                    **{"zone-id": cell, "resolution": resolution, "count": count, "values": avgs}
                ),
            )
        )

    return DGGSFeatureCollection(
        features=dggs_features,
        numberMatched=len(dggs_features),
        numberReturned=len(dggs_features),
        dggsId=dggs_id,
        zoneLevel=resolution,
    )
