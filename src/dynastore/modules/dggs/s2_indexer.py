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

"""S2 indexing utilities: coordinate → cell token, cell → GeoJSON geometry."""

from typing import Any, Dict, List, Optional, Set, Tuple

S2_MIN_LEVEL = 0
S2_MAX_LEVEL = 30


def _require_s2():
    try:
        import s2sphere
        return s2sphere
    except ImportError:
        raise ImportError(
            "The 's2sphere' package is required for S2 DGGS support. "
            "Install it with: pip install 'dynastore[extension_dggs]'"
        )


def latlng_to_cell(lat: float, lng: float, level: int) -> str:
    """Convert a WGS-84 coordinate to an S2 cell token at *level*."""
    s2 = _require_s2()
    if not (S2_MIN_LEVEL <= level <= S2_MAX_LEVEL):
        raise ValueError(
            f"S2 level must be between {S2_MIN_LEVEL} and {S2_MAX_LEVEL}, got {level}"
        )
    latlng = s2.LatLng.from_degrees(lat, lng)
    return s2.CellId.from_lat_lng(latlng).parent(level).to_token()


def cell_to_geojson_polygon(token: str) -> Dict[str, Any]:
    """Return a GeoJSON Polygon dict for the S2 cell boundary.

    S2 cells are quadrilaterals (4 vertices). Vertices are returned in CCW order
    in geographic space; GeoJSON requires [lng, lat] coordinate order.
    The polygon is closed (first == last vertex).
    """
    s2 = _require_s2()
    cell = s2.Cell(s2.CellId.from_token(token))
    coords: List[List[float]] = []
    for i in range(4):
        vertex = cell.get_vertex(i)
        ll = s2.LatLng.from_point(vertex)
        coords.append([ll.lng().degrees, ll.lat().degrees])
    coords.append(coords[0])  # close ring
    return {"type": "Polygon", "coordinates": [coords]}


def cell_to_center(token: str) -> Tuple[float, float]:
    """Return (lat, lng) of the S2 cell centre."""
    s2 = _require_s2()
    cell = s2.Cell(s2.CellId.from_token(token))
    ll = s2.LatLng.from_point(cell.get_center())
    return ll.lat().degrees, ll.lng().degrees


def get_level(token: str) -> int:
    """Return the level of an S2 cell token."""
    s2 = _require_s2()
    return s2.CellId.from_token(token).level()


def is_valid_cell(token: str) -> bool:
    """Return True if *token* is a valid S2 cell token."""
    if not token:
        return False
    s2 = _require_s2()
    try:
        cell_id = s2.CellId.from_token(token)
        return cell_id.is_valid()
    except Exception:
        return False


def cell_str_to_int(token: str) -> int:
    """Convert an S2 cell token to the BIGINT stored in the geometries sidecar.

    The geometries sidecar stores S2 indices as ``BIGINT`` (``CellId.id()``).
    Use this when building ``FilterCondition(EQ, s2_res{N}, cell_str_to_int(zone_id))``.
    """
    if not is_valid_cell(token):
        raise ValueError(f"Not a valid S2 cell token: {token!r}")
    s2 = _require_s2()
    return s2.CellId.from_token(token).id()


def cell_int_to_str(val: int) -> str:
    """Convert a BIGINT S2 value (as stored in the geometries sidecar) to a cell token.

    Reverse of :func:`cell_str_to_int`.
    """
    s2 = _require_s2()
    return s2.CellId(val).to_token()


def bbox_to_cells(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    level: int,
) -> Set[str]:
    """Return the set of S2 cells that cover the given WGS-84 bounding box at *level*."""
    s2 = _require_s2()
    p1 = s2.LatLng.from_degrees(ymin, xmin)
    p2 = s2.LatLng.from_degrees(ymax, xmax)
    rect = s2.LatLngRect.from_point_pair(p1, p2)
    coverer = s2.RegionCoverer()
    coverer.min_level = level
    coverer.max_level = level
    covering = coverer.get_covering(rect)
    return {c.to_token() for c in covering}


def parse_bbox(bbox_str: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parse a comma-separated bbox string into (xmin, ymin, xmax, ymax).

    Returns None if the string is empty or None.
    Raises ValueError on malformed input.
    """
    if not bbox_str:
        return None
    parts = [p.strip() for p in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"bbox must have exactly 4 comma-separated values (xmin,ymin,xmax,ymax), got {len(parts)}"
        )
    try:
        xmin, ymin, xmax, ymax = (float(p) for p in parts)
    except ValueError:
        raise ValueError(f"bbox values must be numeric, got: {bbox_str!r}")
    if xmin >= xmax or ymin >= ymax:
        raise ValueError(
            f"bbox is degenerate: xmin={xmin} >= xmax={xmax} or ymin={ymin} >= ymax={ymax}"
        )
    return xmin, ymin, xmax, ymax
