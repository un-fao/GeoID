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
    assert item["cityjson"] is feature


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


# ---------------------------------------------------------------------------
# Task 1.2 — ingest_cityjson_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_cityjson_file():
    from dynastore.extensions.volumes.cityjson_ingest import ingest_cityjson_file

    item_service = MagicMock()
    item_service.upsert_bulk = AsyncMock(return_value=[{}, {}, {}])

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

    # update_collection called once with header provenance metadata
    catalog_service.update_collection.assert_called_once()
    args, kwargs = catalog_service.update_collection.call_args
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

    # item_service.upsert_bulk called with 3 items total
    item_service.upsert_bulk.assert_called_once()
    upsert_args, upsert_kwargs = item_service.upsert_bulk.call_args
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
    item_service.upsert_bulk = AsyncMock()
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
    item_service.upsert_bulk.assert_not_called()


# ---------------------------------------------------------------------------
# Fix 3 — best-effort batch failure policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ingest_best_effort_batch_failure():
    """Batch failure is recorded as a warning; subsequent batches still run."""
    from dynastore.extensions.volumes.cityjson_ingest import ingest_cityjson_file

    # First upsert_bulk call raises; second succeeds
    item_service = MagicMock()
    item_service.upsert_bulk = AsyncMock(
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
    assert item_service.upsert_bulk.call_count == 2
