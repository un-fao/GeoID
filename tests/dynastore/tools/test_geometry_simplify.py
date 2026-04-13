"""Tests for `dynastore.tools.geometry_simplify.simplify_to_fit`."""

from shapely.geometry import mapping, Polygon

from dynastore.tools.geometry_simplify import (
    MODE_BBOX,
    MODE_NONE,
    MODE_TOLERANCE,
    simplify_to_fit,
)


def _ring(n_vertices: int) -> list[tuple[float, float]]:
    """Build a large closed ring with `n_vertices` densely sampled points."""
    import math

    return [
        (math.cos(2 * math.pi * i / n_vertices), math.sin(2 * math.pi * i / n_vertices))
        for i in range(n_vertices)
    ] + [(1.0, 0.0)]


def test_under_budget_returns_unchanged():
    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = simplify_to_fit(doc, max_bytes=10_000_000)
    assert out is doc
    assert factor == 1.0
    assert mode == MODE_NONE


def test_simplifies_to_fit_under_budget():
    poly = Polygon(_ring(50_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    # Pick a tight budget that the original busts but a simplified
    # geometry can satisfy.
    out, factor, mode = simplify_to_fit(doc, max_bytes=100_000, max_iterations=3)
    assert mode == MODE_TOLERANCE
    assert 0.0 < factor < 1.0


def test_falls_back_to_bbox_when_iterations_exhausted():
    poly = Polygon(_ring(200_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    # Budget below any possible simplified-polygon serialization forces
    # the bbox fallback after 3 iterations.
    out, factor, mode = simplify_to_fit(doc, max_bytes=30, max_iterations=3)
    assert mode == MODE_BBOX
    assert factor == 0.0
    # bbox geometry has exactly one ring of 5 coords.
    coords = out["geometry"]["coordinates"][0]
    assert len(coords) == 5


def test_no_geometry_returns_unchanged_even_if_oversized():
    doc = {"id": "x", "blob": "x" * 20_000}
    out, factor, mode = simplify_to_fit(doc, max_bytes=1000)
    assert mode == MODE_NONE
    assert factor == 1.0
    assert out is doc
