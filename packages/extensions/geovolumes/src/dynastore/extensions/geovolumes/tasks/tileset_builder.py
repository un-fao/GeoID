#    Copyright 2026 FAO
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

"""Pure-Python 3D Tiles 1.1 generation.

Functions in this module are pure (no DB, no I/O) and build on:
- trimesh  >= 4  — scene/mesh assembly and GLB export
- mapbox_earcut  — polygon triangulation
- pyproj         — CRS transformation to ECEF (EPSG:4978)

Offline alternatives for large datasets (>100 MB cities):
  - tyler 0.4.1  (https://github.com/nicholasgasior/tyler)
  - pg2b3dm      (https://github.com/Geodan/pg2b3dm)
Neither is a runtime dependency of this module.
"""

from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np
import pyproj

try:
    import trimesh
    import mapbox_earcut
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "trimesh and mapbox_earcut are required for 3D Tiles generation. "
        "Install them with: pip install 'trimesh>=4' mapbox_earcut"
    ) from exc

from dynastore.extensions.geovolumes.cityjson_ingest import (
    CityJsonHeader,
    dequantize,
    _extract_surfaces,
)

# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------


def dequantize_feature(
    feature: dict[str, Any], header: CityJsonHeader
) -> list[tuple[float, float, float]]:
    """Return real-world 3-D coordinates for all vertices in one feature."""
    return dequantize(feature.get("vertices", []), header)


def triangulate_surface(
    ring: Sequence[tuple[float, float, float]],
) -> list[int]:
    """Triangulate a planar polygon ring (outer boundary only).

    Uses mapbox_earcut via a 2-D projection onto the dominant plane.
    Returns a flat list of vertex indices into ``ring``.
    """
    pts = np.array(ring, dtype=np.float64)
    if len(pts) < 3:
        return []

    # Project to 2-D by dropping the axis with the smallest variance
    variances = pts.var(axis=0)
    drop_axis = int(np.argmin(variances))
    axes = [i for i in range(3) if i != drop_axis]
    # ring_end_indices: one entry per ring = index of the last vertex + 1
    ring_end_indices = np.array([len(pts)], dtype=np.uint32)
    triangles = mapbox_earcut.triangulate_float64(
        pts[:, axes], ring_end_indices
    )
    return list(triangles.tolist())


def _feature_to_mesh(
    feature: dict[str, Any],
    header: CityJsonHeader,
    transformer: pyproj.Transformer,
    center: np.ndarray,
    lod_filter: str | None,
) -> trimesh.Trimesh | None:
    """Convert one CityJSONFeature to a Trimesh in the local ECEF frame."""
    real_verts = dequantize_feature(feature, header)
    if not real_verts:
        return None

    all_vertices: list[tuple[float, float, float]] = []
    all_faces: list[tuple[int, int, int]] = []
    offset = 0

    for obj in feature.get("CityObjects", {}).values():
        for geom in obj.get("geometry", []):
            lod_val = geom.get("lod")
            if lod_filter is not None and str(lod_val) != lod_filter:
                continue
            for ring_indices in _extract_surfaces(geom):
                ring_3d = [real_verts[i] for i in ring_indices]
                if len(ring_3d) < 3:
                    continue
                # Transform to ECEF
                ecef_verts = [
                    transformer.transform(v[0], v[1], v[2])
                    for v in ring_3d
                ]
                tri_indices = triangulate_surface(
                    [(x, y, z) for x, y, z in ecef_verts]
                )
                if not tri_indices:
                    continue
                for v in ecef_verts:
                    all_vertices.append(v)
                n = len(ecef_verts)
                for k in range(0, len(tri_indices), 3):
                    i0, i1, i2 = (
                        tri_indices[k] + offset,
                        tri_indices[k + 1] + offset,
                        tri_indices[k + 2] + offset,
                    )
                    all_faces.append((i0, i1, i2))
                offset += n

    if not all_vertices or not all_faces:
        return None

    verts_np = np.array(all_vertices, dtype=np.float32) - center.astype(np.float32)
    faces_np = np.array(all_faces, dtype=np.int32)
    return trimesh.Trimesh(vertices=verts_np, faces=faces_np, process=False)


def _compute_center(
    features: list[dict[str, Any]],
    header: CityJsonHeader,
    transformer: pyproj.Transformer,
) -> np.ndarray:
    """Compute the ECEF centroid of all feature vertices (for the local frame)."""
    xs: list[float] = []
    ys: list[float] = []
    zs: list[float] = []
    for feature in features:
        for v in dequantize_feature(feature, header):
            ex, ey, ez = transformer.transform(v[0], v[1], v[2])
            xs.append(ex)
            ys.append(ey)
            zs.append(ez)
    if not xs:
        return np.zeros(3, dtype=np.float64)
    return np.array(
        [sum(xs) / len(xs), sum(ys) / len(ys), sum(zs) / len(zs)],
        dtype=np.float64,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_glb(
    features: list[dict[str, Any]],
    header: CityJsonHeader,
    lod_filter: str | None = None,
) -> bytes:
    """Build a GLB binary from a list of CityJSONFeature dicts.

    Transforms coordinates to ECEF (EPSG:4978), subtracts the centroid to
    keep a well-conditioned local frame, then exports as GLB.

    Args:
        features:    CityJSONFeature dicts (as stored by the ingest phase).
        header:      CityJsonHeader carrying transform + EPSG.
        lod_filter:  If given, only geometries with matching 'lod' are included.

    Returns:
        Raw GLB bytes (binary glTF).
    """
    if header.epsg is None:
        raise ValueError("CityJsonHeader.epsg is required for GLB generation.")

    transformer = pyproj.Transformer.from_crs(
        header.epsg, 4978, always_xy=True
    )
    center = _compute_center(features, header, transformer)

    scene = trimesh.Scene()
    for feature in features:
        mesh = _feature_to_mesh(feature, header, transformer, center, lod_filter)
        if mesh is not None and len(mesh.vertices) > 0:
            fid = feature.get("id", "mesh")
            scene.add_geometry(mesh, geom_name=str(fid))

    glb_bytes: bytes = scene.export(file_type="glb")
    return glb_bytes


def build_tileset_json(
    bbox_wgs84: tuple[float, float, float, float],
    glb_refs: list[str],
    *,
    geometric_error_root: float = 500.0,
    geometric_error_leaf: float = 0.0,
    min_height: float = 0.0,
    max_height: float = 500.0,
) -> dict[str, Any]:
    """Build a 3D Tiles 1.1 tileset.json dict (no file I/O).

    Args:
        bbox_wgs84:            (west, south, east, north) in degrees.
        glb_refs:              List of GLB filenames/relative URIs.
        geometric_error_root:  geometricError at the tileset root.
        geometric_error_leaf:  geometricError at leaf tiles (0 = highest detail).
        min_height:            Minimum terrain height in metres.
        max_height:            Maximum terrain height in metres.

    Returns:
        dict suitable for ``json.dumps`` as tileset.json.
    """
    west, south, east, north = bbox_wgs84
    region = [
        math.radians(west),
        math.radians(south),
        math.radians(east),
        math.radians(north),
        min_height,
        max_height,
    ]
    bounding_volume = {"region": region}

    if len(glb_refs) == 1:
        root_tile: dict[str, Any] = {
            "boundingVolume": bounding_volume,
            "geometricError": geometric_error_leaf,
            "content": {"uri": glb_refs[0]},
        }
    else:
        children = [
            {
                "boundingVolume": bounding_volume,
                "geometricError": geometric_error_leaf,
                "content": {"uri": ref},
            }
            for ref in glb_refs
        ]
        root_tile = {
            "boundingVolume": bounding_volume,
            "geometricError": geometric_error_root,
            "children": children,
        }

    return {
        "asset": {"version": "1.1"},
        "geometricError": geometric_error_root,
        "root": root_tile,
    }


def export_tileset_bytes(tileset: dict[str, Any]) -> bytes:
    """Serialise a tileset dict to UTF-8 JSON bytes."""
    import json

    return json.dumps(tileset, ensure_ascii=False, indent=2).encode("utf-8")
