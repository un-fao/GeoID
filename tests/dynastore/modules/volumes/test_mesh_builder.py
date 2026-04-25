"""Tests for modules/volumes/mesh_builder.py."""

from __future__ import annotations

import struct

import pytest

pytest.importorskip("shapely", reason="shapely required for mesh_builder tests")

from shapely.geometry import Polygon, MultiPolygon
import shapely.wkb

from dynastore.models.protocols.geometry_fetcher import FeatureGeometry
from dynastore.modules.volumes.mesh_builder import (
    MeshBuffers,
    _MeshAccumulator,
    _empty_buffers,
    _fan_triangulate,
    _extrude_ring,
    _extrude_polygon,
    build_mesh_from_geometries,
)


def _square_wkb(x0=0.0, y0=0.0, size=1.0) -> bytes:
    poly = Polygon([
        (x0, y0), (x0 + size, y0), (x0 + size, y0 + size), (x0, y0 + size),
    ])
    return shapely.wkb.dumps(poly)


# ---------------------------------------------------------------------------
# MeshAccumulator
# ---------------------------------------------------------------------------


def test_accumulator_empty_produces_empty_buffers():
    acc = _MeshAccumulator()
    buf = acc.to_buffers()
    assert buf.vertex_count == 0
    assert buf.index_count == 0


def test_accumulator_single_triangle():
    acc = _MeshAccumulator()
    a = acc.add_vertex(0.0, 0.0, 0.0)
    b = acc.add_vertex(1.0, 0.0, 0.0)
    c = acc.add_vertex(0.0, 1.0, 0.0)
    acc.add_triangle(a, b, c)

    buf = acc.to_buffers()
    assert buf.vertex_count == 3
    assert buf.index_count == 3
    assert buf.triangle_count == 1
    assert len(buf.positions) == 3 * 3 * 4  # 3 verts * 3 floats * 4 bytes
    assert len(buf.indices) == 3 * 4        # 3 indices * 4 bytes


# ---------------------------------------------------------------------------
# _fan_triangulate
# ---------------------------------------------------------------------------


def test_fan_triangulate_square_produces_four_triangles():
    acc = _MeshAccumulator()
    square = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    _fan_triangulate(square, z=0.0, acc=acc)
    # 4-vertex ring → 4 fan triangles (centroid + each edge)
    assert acc.to_buffers().triangle_count == 4


def test_fan_triangulate_degenerate_ring_skipped():
    acc = _MeshAccumulator()
    _fan_triangulate([(0.0, 0.0), (1.0, 0.0)], z=0.0, acc=acc)
    assert acc.to_buffers().triangle_count == 0


def test_fan_triangulate_flip_winding():
    acc_normal = _MeshAccumulator()
    acc_flipped = _MeshAccumulator()
    ring = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    _fan_triangulate(ring, z=0.0, acc=acc_normal, flip_winding=False)
    _fan_triangulate(ring, z=0.0, acc=acc_flipped, flip_winding=True)
    # Triangle vertex order must differ.
    assert acc_normal.tris[0] != acc_flipped.tris[0]


# ---------------------------------------------------------------------------
# _extrude_ring (side walls)
# ---------------------------------------------------------------------------


def test_extrude_ring_square_produces_eight_triangles():
    acc = _MeshAccumulator()
    square = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]
    _extrude_ring(square, z_bottom=0.0, z_top=5.0, acc=acc)
    # 4 edges × 2 triangles each = 8
    assert acc.to_buffers().triangle_count == 8


# ---------------------------------------------------------------------------
# _extrude_polygon (full prism: top + bottom + sides)
# ---------------------------------------------------------------------------


def test_extrude_polygon_unit_square():
    acc = _MeshAccumulator()
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    _extrude_polygon(poly, z_base=0.0, extrusion_height=3.0, acc=acc)
    buf = acc.to_buffers()
    # top cap: 4 tris, bottom cap: 4 tris, sides: 8 tris = 16
    assert buf.triangle_count == 16
    # Z range: 0 → 3
    assert buf.min_pos[2] == pytest.approx(0.0)
    assert buf.max_pos[2] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# build_mesh_from_geometries
# ---------------------------------------------------------------------------


def test_build_mesh_empty_input():
    buf = build_mesh_from_geometries([])
    assert buf.vertex_count == 0


def test_build_mesh_single_feature():
    wkb = _square_wkb(0, 0, 1)
    fg = FeatureGeometry(feature_id="f1", geom_wkb=wkb, height=5.0)
    buf = build_mesh_from_geometries([fg], default_extrusion_height=10.0)
    assert buf.vertex_count > 0
    assert buf.triangle_count > 0
    # height=5 used as extrusion
    assert buf.max_pos[2] == pytest.approx(5.0)


def test_build_mesh_uses_default_extrusion_when_height_zero():
    wkb = _square_wkb(0, 0, 1)
    fg = FeatureGeometry(feature_id="f1", geom_wkb=wkb, height=0.0)
    buf = build_mesh_from_geometries([fg], default_extrusion_height=7.0)
    assert buf.max_pos[2] == pytest.approx(7.0)


def test_build_mesh_multiple_features():
    wkb1 = _square_wkb(0, 0, 1)
    wkb2 = _square_wkb(2, 0, 1)
    feats = [
        FeatureGeometry("a", wkb1, height=3.0),
        FeatureGeometry("b", wkb2, height=3.0),
    ]
    buf = build_mesh_from_geometries(feats)
    assert buf.vertex_count > 0
    # Should contain vertices from both features.
    assert buf.max_pos[0] >= 3.0  # rightmost X of second square


def test_build_mesh_skips_bad_wkb():
    bad = FeatureGeometry("bad", b"\x00" * 5, height=1.0)
    good_wkb = _square_wkb(0, 0, 1)
    good = FeatureGeometry("good", good_wkb, height=1.0)
    buf = build_mesh_from_geometries([bad, good])
    assert buf.vertex_count > 0


def test_build_mesh_multipolygon():
    mp = MultiPolygon([
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
    ])
    wkb = shapely.wkb.dumps(mp)
    fg = FeatureGeometry("mp", wkb, height=2.0)
    buf = build_mesh_from_geometries([fg])
    assert buf.triangle_count > 0
    assert buf.max_pos[0] >= 3.0
