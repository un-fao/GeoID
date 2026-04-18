"""BigQuery row → Feature adapter and paged streamer."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from dynastore.models.ogc import Feature


def row_to_feature(
    row: Dict[str, Any],
    *,
    id_column: str,
    geometry_column: Optional[str],
) -> Feature:
    if id_column not in row:
        raise KeyError(f"id column {id_column!r} not in row keys {list(row)}")
    feat_id = row[id_column]
    geometry: Optional[Dict[str, Any]] = None
    if geometry_column and geometry_column in row:
        raw = row[geometry_column]
        geometry = _coerce_geometry(raw)
    properties = {
        k: v
        for k, v in row.items()
        if k != id_column and k != geometry_column
    }
    return Feature(
        type="Feature",
        id=feat_id,
        geometry=geometry,
        properties=properties,
    )


def _coerce_geometry(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict) and "type" in raw and "coordinates" in raw:
        return raw
    if isinstance(raw, str):
        return _wkt_to_geojson(raw)
    return None


def _wkt_to_geojson(wkt: str) -> Dict[str, Any]:
    """Minimal WKT → GeoJSON for POINT / LINESTRING / POLYGON."""
    wkt = wkt.strip()
    m = re.match(r"^POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)$", wkt, re.I)
    if m:
        return {
            "type": "Point",
            "coordinates": [float(m.group(1)), float(m.group(2))],
        }
    m = re.match(r"^LINESTRING\s*\((.+)\)$", wkt, re.I)
    if m:
        coords = [
            [float(p) for p in pair.strip().split()]
            for pair in m.group(1).split(",")
        ]
        return {"type": "LineString", "coordinates": coords}
    m = re.match(r"^POLYGON\s*\(\s*\((.+)\)\s*\)$", wkt, re.I)
    if m:
        ring = [
            [float(p) for p in pair.strip().split()]
            for pair in m.group(1).split(",")
        ]
        return {"type": "Polygon", "coordinates": [ring]}
    raise ValueError(f"Unsupported WKT geometry: {wkt!r}")
