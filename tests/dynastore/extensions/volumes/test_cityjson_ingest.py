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

"""Unit tests for CityJSONSeq parsing and item ingestion (Task 1.1 + 1.2)."""

from __future__ import annotations

import json
import pathlib
from unittest.mock import AsyncMock, MagicMock

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Task 1.1 — parse_cityjsonseq
# ---------------------------------------------------------------------------


def test_parse_header_and_features():
    from dynastore.extensions.volumes.cityjson_ingest import parse_cityjsonseq

    header, features = parse_cityjsonseq(FIXTURES / "minimal.city.jsonl")

    assert header.version == "2.0"
    assert header.epsg == 7415
    assert len(features) == 3
    assert features[0]["type"] == "CityJSONFeature"


def test_dequantize():
    from dynastore.extensions.volumes.cityjson_ingest import (
        CityJsonHeader,
        dequantize,
    )

    header = CityJsonHeader(
        version="2.0",
        transform_scale=[0.001, 0.001, 0.001],
        transform_translate=[85000.0, 446000.0, 0.0],
        reference_system="https://www.opengis.net/def/crs/EPSG/0/7415",
        epsg=7415,
        metadata={},
    )
    vertices = [[10, 20, 12500]]
    result = dequantize(vertices, header)

    assert len(result) == 1
    x, y, z = result[0]
    assert abs(x - 85000.01) < 1e-6
    assert abs(y - 446000.02) < 1e-6
    assert abs(z - 12.5) < 1e-6


def test_feature_to_item_input():
    from dynastore.extensions.volumes.cityjson_ingest import (
        CityJsonHeader,
        feature_to_item_input,
    )

    header = CityJsonHeader(
        version="2.0",
        transform_scale=[0.001, 0.001, 0.001],
        transform_translate=[85000.0, 446000.0, 0.0],
        reference_system="https://www.opengis.net/def/crs/EPSG/0/7415",
        epsg=7415,
        metadata={},
    )
    feature = {
        "type": "CityJSONFeature",
        "id": "bldg-1",
        "CityObjects": {
            "bldg-1": {
                "type": "Building",
                "attributes": {"height": 12.5},
                "geometry": [
                    {
                        "type": "Solid",
                        "lod": "2",
                        # Solid: boundaries[shell][surface][ring][vertex_idx]
                        "boundaries": [
                            [
                                [[0, 1, 2, 3]],
                                [[4, 5, 1, 0]],
                                [[5, 6, 2, 1]],
                                [[6, 7, 3, 2]],
                                [[7, 4, 0, 3]],
                                [[7, 6, 5, 4]],
                            ]
                        ],
                    }
                ],
            }
        },
        "vertices": [
            [0, 0, 0],
            [10, 0, 0],
            [10, 10, 0],
            [0, 10, 0],
            [0, 0, 12500],
            [10, 0, 12500],
            [10, 10, 12500],
            [0, 10, 12500],
        ],
    }

    item = feature_to_item_input(feature, header)

    assert item["id"] == "bldg-1"

    # Geometry must be GeoJSON MultiPolygon in WGS84
    geom = item["geometry"]
    assert geom["type"] == "MultiPolygon"
    # All coords should be in Netherlands lon/lat range
    coords = geom["coordinates"]
    # Flatten to get all [lon, lat] pairs
    flat_coords = []
    for polygon in coords:
        for ring in polygon:
            flat_coords.extend(ring)
    for lon, lat in flat_coords:
        assert 3.0 < lon < 8.0, f"lon {lon} out of Netherlands range"
        assert 50.0 < lat < 54.0, f"lat {lat} out of Netherlands range"

    props = item["properties"]
    assert props["lod"] == ["2"]
    assert props["citygml_type"] == "Building"
    assert props["zmin"] < props["zmax"]
    assert props["height"] == 12.5

    # cityjson must be the exact input dict
    # The raw CityJSONFeature must ride inside properties — free-form
    # top-level keys are dropped by the item write path.
    assert item["properties"]["cityjson"] is feature
    assert "cityjson" not in item


def test_feature_to_item_input_raises_without_epsg():
    from dynastore.extensions.volumes.cityjson_ingest import (
        CityJsonHeader,
        feature_to_item_input,
    )

    header = CityJsonHeader(
        version="2.0",
        transform_scale=[0.001, 0.001, 0.001],
        transform_translate=[0.0, 0.0, 0.0],
        reference_system=None,
        epsg=None,
        metadata={},
    )
    feature = {
        "type": "CityJSONFeature",
        "id": "x",
        "CityObjects": {"x": {"type": "Building", "attributes": {}, "geometry": []}},
        "vertices": [],
    }
    with pytest.raises(ValueError):
        feature_to_item_input(feature, header)


def test_parse_single_cityjson_object(tmp_path):
    """A plain CityJSON (type=CityJSON) is split into CityJSONFeature dicts."""
    from dynastore.extensions.volumes.cityjson_ingest import parse_cityjsonseq

    doc = {
        "type": "CityJSON",
        "version": "2.0",
        "transform": {
            "scale": [0.001, 0.001, 0.001],
            "translate": [85000.0, 446000.0, 0.0],
        },
        "metadata": {"referenceSystem": "https://www.opengis.net/def/crs/EPSG/0/7415"},
        "CityObjects": {
            "bldg-A": {
                "type": "Building",
                "attributes": {"height": 10.0},
                "geometry": [
                    {
                        "type": "Solid",
                        "lod": "1",
                        # Solid: boundaries[shell][surface][ring][vertex_idx]
                        "boundaries": [
                            [
                                [[0, 1, 2, 3]],
                                [[4, 5, 1, 0]],
                                [[5, 6, 2, 1]],
                                [[6, 7, 3, 2]],
                                [[7, 4, 0, 3]],
                                [[7, 6, 5, 4]],
                            ]
                        ],
                    }
                ],
            },
            "bldg-A--part": {
                "type": "BuildingPart",
                "parents": ["bldg-A"],
                "attributes": {},
                "geometry": [
                    {
                        "type": "Solid",
                        "lod": "1",
                        # Solid: boundaries[shell][surface][ring][vertex_idx]
                        "boundaries": [
                            [
                                [[8, 9, 10, 11]],
                                [[12, 13, 9, 8]],
                                [[13, 14, 10, 9]],
                                [[14, 15, 11, 10]],
                                [[15, 12, 8, 11]],
                                [[15, 14, 13, 12]],
                            ]
                        ],
                    }
                ],
            },
        },
        "vertices": [
            [0, 0, 0],
            [10, 0, 0],
            [10, 10, 0],
            [0, 10, 0],
            [0, 0, 10000],
            [10, 0, 10000],
            [10, 10, 10000],
            [0, 10, 10000],
            # Part vertices (separate box)
            [20, 20, 0],
            [30, 20, 0],
            [30, 30, 0],
            [20, 30, 0],
            [20, 20, 5000],
            [30, 20, 5000],
            [30, 30, 5000],
            [20, 30, 5000],
        ],
    }
    path = tmp_path / "test.city.json"
    path.write_text(json.dumps(doc))

    header, features = parse_cityjsonseq(path)

    assert header.version == "2.0"
    assert header.epsg == 7415
    # Only one root building (bldg-A); its part is bundled inside
    assert len(features) == 1
    feat = features[0]
    assert feat["type"] == "CityJSONFeature"
    assert feat["id"] == "bldg-A"
    # Both building and part are present in this feature's CityObjects
    assert "bldg-A" in feat["CityObjects"]
    assert "bldg-A--part" in feat["CityObjects"]
    # Local vertices must be compact — max index < len(local vertices)
    local_verts = feat["vertices"]
    all_indices = []
    for co in feat["CityObjects"].values():
        for geom in co.get("geometry", []):
            for shell in geom.get("boundaries", []):
                for face in shell:
                    if isinstance(face[0], list):
                        for ring in face:
                            all_indices.extend(ring)
                    else:
                        all_indices.extend(face)
    if all_indices:
        assert max(all_indices) < len(local_verts)


def test_terrain_relief_root_is_skipped(tmp_path):
    """A top-level TINRelief (terrain) must NOT become a feature.

    Real CityJSON city tiles (e.g. 3DBAG / 3D BK Delft Den Haag) carry a single
    TINRelief alongside the buildings. The footprint-extrusion model unions all
    surface rings into one 2D polygon and extrudes it by the z-extent — for a
    terrain TIN that produces one tile-spanning slab that swallows the whole
    scene. Only volumetric objects (Building/BuildingPart) should be ingested.
    """
    from dynastore.extensions.volumes.cityjson_ingest import parse_cityjsonseq

    doc = {
        "type": "CityJSON",
        "version": "2.0",
        "transform": {"scale": [0.001, 0.001, 0.001], "translate": [0.0, 0.0, 0.0]},
        "metadata": {"referenceSystem": "https://www.opengis.net/def/crs/EPSG/0/7415"},
        "CityObjects": {
            "bldg-A": {
                "type": "Building",
                "attributes": {"height": 10.0},
                "geometry": [
                    {
                        "type": "Solid",
                        "lod": "1",
                        "boundaries": [
                            [
                                [[0, 1, 2, 3]],
                                [[4, 5, 1, 0]],
                                [[5, 6, 2, 1]],
                                [[6, 7, 3, 2]],
                                [[7, 4, 0, 3]],
                                [[7, 6, 5, 4]],
                            ]
                        ],
                    }
                ],
            },
            "terrain": {
                "type": "TINRelief",
                "attributes": {},
                "geometry": [
                    {
                        "type": "CompositeSurface",
                        "lod": "1",
                        "boundaries": [[[0, 1, 2]], [[0, 2, 3]]],
                    }
                ],
            },
        },
        "vertices": [
            [0, 0, 0],
            [10, 0, 0],
            [10, 10, 0],
            [0, 10, 0],
            [0, 0, 10000],
            [10, 0, 10000],
            [10, 10, 10000],
            [0, 10, 10000],
        ],
    }
    path = tmp_path / "terrain.city.json"
    path.write_text(json.dumps(doc))

    _header, features = parse_cityjsonseq(path)

    # Only the building survives; the terrain relief is dropped.
    assert [f["id"] for f in features] == ["bldg-A"]


# ---------------------------------------------------------------------------
# Task 1.2 — ingest_cityjson_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_cityjson_file():
    from dynastore.extensions.volumes.cityjson_ingest import ingest_cityjson_file

    item_service = MagicMock()
    item_service.upsert = AsyncMock(return_value=[{}, {}, {}])

    catalog_service = MagicMock()
    catalog_service.update_collection = AsyncMock(return_value=MagicMock())

    summary = await ingest_cityjson_file(
        "test-cat",
        "test-col",
        FIXTURES / "minimal.city.jsonl",
        item_service=item_service,
        catalog_service=catalog_service,
        batch_size=500,
    )

    assert summary["items"] == 3
    assert isinstance(summary["warnings"], list)

    # update_collection called twice: header provenance, then extent/zrange
    assert catalog_service.update_collection.call_count == 2
    args, kwargs = catalog_service.update_collection.call_args_list[0]
    assert "test-cat" in args or kwargs.get("catalog_id") == "test-cat"
    assert "test-col" in args or kwargs.get("collection_id") == "test-col"
    # Third positional argument is the extras payload
    extras_payload = args[2] if len(args) > 2 else kwargs.get("extras", {})
    meta = extras_payload.get("extras", {})
    assert meta.get("cityjson:version") == "2.0", "cityjson:version must be set"
    assert isinstance(meta.get("cityjson:transform"), dict), "cityjson:transform must be a dict"
    assert "scale" in meta["cityjson:transform"] and "translate" in meta["cityjson:transform"]
    # minimal.city.jsonl carries a referenceSystem
    assert "cityjson:referenceSystem" in meta, "cityjson:referenceSystem must be present when set in header"
    # PG read path only returns extra_metadata — meta must be mirrored there
    assert extras_payload.get("extra_metadata") == meta

    # Second call carries the computed extent + vertical range
    args2, kwargs2 = catalog_service.update_collection.call_args_list[1]
    final_payload = args2[2] if len(args2) > 2 else kwargs2.get("extras", {})
    extent = final_payload.get("extent", {})
    bbox = extent.get("spatial", {}).get("bbox", [[]])[0]
    assert len(bbox) == 4
    assert bbox[0] <= bbox[2] and bbox[1] <= bbox[3]
    zrange = final_payload.get("extras", {}).get("geovolumes:zrange", {})
    assert "zmin" in zrange and "zmax" in zrange
    # The 2D bbox must also be stamped into extras — the extent column only
    # persists where the optional STAC collection sidecar is materialized.
    stamped = final_payload.get("extras", {}).get("geovolumes:bbox")
    assert stamped == [bbox[0], bbox[1], bbox[2], bbox[3]]
    assert final_payload.get("extra_metadata", {}).get("geovolumes:bbox") == stamped

    # item_service.upsert called with 3 items total
    item_service.upsert.assert_called_once()
    upsert_args, upsert_kwargs = item_service.upsert.call_args
    items_passed = upsert_args[2] if len(upsert_args) > 2 else upsert_kwargs.get("items", [])
    assert len(items_passed) == 3


# ---------------------------------------------------------------------------
# Fix 2 — parse_epsg authority-aware parsing
# ---------------------------------------------------------------------------


def testparse_epsg_uri_epsg_authority():
    """URI with EPSG authority returns the numeric code."""
    from dynastore.extensions.volumes.cityjson_ingest import parse_epsg

    assert parse_epsg("https://www.opengis.net/def/crs/EPSG/0/7415") == 7415


def testparse_epsg_uri_ogc_authority_returns_none():
    """URI with OGC authority (e.g. CRS84) must return None, not extract 84."""
    from dynastore.extensions.volumes.cityjson_ingest import parse_epsg

    assert parse_epsg("http://www.opengis.net/def/crs/OGC/1.3/CRS84") is None


def testparse_epsg_none_and_empty_return_none():
    """None and empty string both return None without raising."""
    from dynastore.extensions.volumes.cityjson_ingest import parse_epsg

    assert parse_epsg(None) is None
    assert parse_epsg("") is None


def testparse_epsg_urn_form():
    """URN form ``EPSG::<code>`` returns the numeric code."""
    from dynastore.extensions.volumes.cityjson_ingest import parse_epsg

    assert parse_epsg("urn:ogc:def:crs:EPSG::28992") == 28992


# ---------------------------------------------------------------------------
# Fix 1 — fail-fast when header lacks a reference system
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_raises_when_no_reference_system(tmp_path):
    """ingest_cityjson_file raises ValueError before any DB call when header has no CRS."""
    from dynastore.extensions.volumes.cityjson_ingest import ingest_cityjson_file

    # Build a minimal CityJSONSeq file without metadata.referenceSystem
    header_line = {
        "type": "CityJSONSeq",
        "version": "2.0",
        "transform": {
            "scale": [0.001, 0.001, 0.001],
            "translate": [85000.0, 446000.0, 0.0],
        },
        # Intentionally no "metadata" key
    }
    feature_line = {
        "type": "CityJSONFeature",
        "id": "bldg-x",
        "CityObjects": {
            "bldg-x": {
                "type": "Building",
                "attributes": {},
                "geometry": [],
            }
        },
        "vertices": [],
    }
    fixture = tmp_path / "no_crs.city.jsonl"
    fixture.write_text(
        json.dumps(header_line) + "\n" + json.dumps(feature_line) + "\n"
    )

    item_service = MagicMock()
    item_service.upsert = AsyncMock()
    catalog_service = MagicMock()
    catalog_service.update_collection = AsyncMock()

    with pytest.raises(ValueError, match="no usable EPSG"):
        await ingest_cityjson_file(
            "cat",
            "col",
            fixture,
            item_service=item_service,
            catalog_service=catalog_service,
        )

    # No DB writes should have been attempted
    item_service.upsert.assert_not_called()


# ---------------------------------------------------------------------------
# Fix 3 — best-effort batch failure policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_best_effort_batch_failure():
    """Batch failure is recorded as a warning; subsequent batches still run."""
    from dynastore.extensions.volumes.cityjson_ingest import ingest_cityjson_file

    # First upsert call raises; second succeeds
    item_service = MagicMock()
    item_service.upsert = AsyncMock(
        side_effect=[RuntimeError("DB down"), None]
    )
    catalog_service = MagicMock()
    catalog_service.update_collection = AsyncMock()

    # minimal.city.jsonl has 3 features; batch_size=2 → 2 batches (2 + 1)
    summary = await ingest_cityjson_file(
        "test-cat",
        "test-col",
        FIXTURES / "minimal.city.jsonl",
        item_service=item_service,
        catalog_service=catalog_service,
        batch_size=2,
    )

    # Second batch (1 item) succeeded; first batch (2 items) failed
    assert summary["items"] == 1, "Only the second batch's item should be counted"
    assert summary["failed"] == 2, "Two items in the failed first batch"
    # A batch-failure warning must be present
    batch_warnings = [w for w in summary["warnings"] if "Batch" in w and "failed" in w]
    assert batch_warnings, "Expected at least one batch-failure warning"
    # Both batches were attempted (2 calls total)
    assert item_service.upsert.call_count == 2


# ---------------------------------------------------------------------------
# Geometry repair: make_valid + polygonal coercion (issue #2044)
# ---------------------------------------------------------------------------


def test_coerce_to_multipolygon_valid_polygon():
    """A valid Polygon is wrapped into a one-element MultiPolygon."""
    from shapely.geometry import Polygon

    from dynastore.extensions.volumes.cityjson_ingest import _coerce_to_multipolygon

    p = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    result = _coerce_to_multipolygon(p)
    assert result.geom_type == "MultiPolygon"
    assert result.is_valid
    assert not result.is_empty


def test_coerce_to_multipolygon_valid_multipolygon():
    """A valid MultiPolygon is returned unchanged."""
    from shapely.geometry import MultiPolygon, Polygon

    from dynastore.extensions.volumes.cityjson_ingest import _coerce_to_multipolygon

    mp = MultiPolygon([
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
    ])
    result = _coerce_to_multipolygon(mp)
    assert result.geom_type == "MultiPolygon"
    assert result.is_valid
    assert not result.is_empty


def test_coerce_to_multipolygon_self_intersecting_bowtie():
    """A self-intersecting (bow-tie) Polygon is repaired and the result is:
    (a) valid per shapely,
    (b) polygonal (Polygon or MultiPolygon, NOT GeometryCollection),
    (c) non-empty — area is preserved from both lobes of the bow-tie.
    """
    from shapely.geometry import Polygon

    from dynastore.extensions.volumes.cityjson_ingest import _coerce_to_multipolygon

    # Classic bow-tie: two triangles sharing a single crossing point.
    # buffer(0) collapses this to empty; make_valid preserves both lobes.
    bow_tie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
    assert not bow_tie.is_valid, "precondition: bow-tie must be invalid"

    result = _coerce_to_multipolygon(bow_tie)

    # (a) valid
    assert result.is_valid, f"result is not valid: {result}"
    # (b) polygonal — never a GeometryCollection
    assert result.geom_type in ("Polygon", "MultiPolygon"), (
        f"expected Polygon or MultiPolygon, got {result.geom_type}"
    )
    # (c) non-empty — both lobes survive
    assert not result.is_empty
    assert result.area > 0


def test_coerce_to_multipolygon_geometrycollection_with_lines():
    """A GeometryCollection mixing polygons and lines is coerced to MultiPolygon.

    ES geo_shape rejects GeometryCollection and LineString. ``make_valid``
    can return these for pathological inputs (e.g. collinear self-touching edges).
    The coercion must extract only the polygon-area components.
    """
    from shapely.geometry import GeometryCollection, LineString, Polygon

    from dynastore.extensions.volumes.cityjson_ingest import _coerce_to_multipolygon

    # Build a GeometryCollection directly (simulates make_valid output)
    gc = GeometryCollection([
        Polygon([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]),
        LineString([(0, 4), (2, 2)]),
    ])
    assert gc.geom_type == "GeometryCollection"

    result = _coerce_to_multipolygon(gc)

    assert result.geom_type in ("Polygon", "MultiPolygon"), (
        f"expected polygonal type, got {result.geom_type}"
    )
    assert result.is_valid
    assert not result.is_empty
    # The polygon area must be preserved; the line contributes no area
    assert abs(result.area - 16.0) < 1e-9


def test_coerce_to_multipolygon_es_serializable():
    """The repaired MultiPolygon must serialize to valid GeoJSON that an ES
    geo_shape mapping accepts — i.e. only type=MultiPolygon, well-formed
    coordinates, no GeometryCollection wrapper.
    """
    import json

    from shapely.geometry import Polygon, mapping

    from dynastore.extensions.volumes.cityjson_ingest import _coerce_to_multipolygon

    bow_tie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
    result = _coerce_to_multipolygon(bow_tie)

    geojson = mapping(result)
    # JSON-serialisable
    dumped = json.dumps(geojson)
    reloaded = json.loads(dumped)

    assert reloaded["type"] == "MultiPolygon", (
        f"GeoJSON type must be MultiPolygon for ES geo_shape, got {reloaded['type']}"
    )
    assert isinstance(reloaded["coordinates"], list)
    assert len(reloaded["coordinates"]) > 0


def test_build_footprint_produces_multipolygon_for_bowtie_ring(tmp_path):
    """_build_footprint_geojson must produce a valid MultiPolygon GeoJSON even
    when the 2D-projected surface rings are self-intersecting (bow-tie).

    This is the regression test for issue #2044: previously ``buffer(0)`` on
    self-intersecting rings could produce an empty geometry; ``make_valid``
    preserves the polygon area. The returned GeoJSON must have type=MultiPolygon
    so ES geo_shape accepts it.
    """
    import pyproj
    from shapely.geometry import shape

    from dynastore.extensions.volumes.cityjson_ingest import (
        CityJsonHeader,
        _build_footprint_geojson,
        dequantize,
    )

    # Craft a CityJSON feature whose single surface ring will project to a
    # self-intersecting polygon in 2D. We use EPSG:28992 (Dutch RD) with
    # a trivial identity-like transform (scale=1, translate=0).
    # The surface ring crosses itself: first two points and last two points
    # form a bow-tie when projected.
    header = CityJsonHeader(
        version="2.0",
        transform_scale=[1.0, 1.0, 1.0],
        transform_translate=[0.0, 0.0, 0.0],
        reference_system="https://www.opengis.net/def/crs/EPSG/0/28992",
        epsg=28992,
        metadata={},
    )
    transformer = pyproj.Transformer.from_crs(28992, 4326, always_xy=True)

    # Two valid triangles slightly offset so when projected they both survive.
    # We use a known valid Dutch RD coordinate region (near Amsterdam ~[120000, 487000]).
    base_x, base_y = 120000, 487000
    feature = {
        "type": "CityJSONFeature",
        "id": "bowtie-bldg",
        "CityObjects": {
            "bowtie-bldg": {
                "type": "Building",
                "attributes": {},
                "geometry": [
                    {
                        "type": "MultiSurface",
                        "lod": "2",
                        # A self-intersecting ring: vertices wind in a bow-tie pattern
                        # (0->2->1->3 instead of 0->1->2->3)
                        "boundaries": [
                            # surface 0: bow-tie ring
                            [[0, 2, 1, 3]],
                        ],
                    }
                ],
            }
        },
        "vertices": [
            [base_x, base_y, 0],
            [base_x + 10, base_y, 0],
            [base_x + 10, base_y + 10, 0],
            [base_x, base_y + 10, 0],
        ],
    }

    vertices_3d = dequantize(feature["vertices"], header)
    geojson = _build_footprint_geojson(feature, transformer, vertices_3d)

    # Must be a valid GeoJSON MultiPolygon
    assert geojson["type"] == "MultiPolygon", (
        f"Expected MultiPolygon for ES geo_shape, got {geojson['type']}"
    )
    # Must be shapely-valid
    shapely_geom = shape(geojson)
    assert shapely_geom.is_valid, f"Repaired geometry is not valid: {shapely_geom}"
    assert not shapely_geom.is_empty
