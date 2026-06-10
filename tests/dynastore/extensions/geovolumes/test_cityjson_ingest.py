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
    from dynastore.extensions.geovolumes.cityjson_ingest import parse_cityjsonseq

    header, features = parse_cityjsonseq(FIXTURES / "minimal.city.jsonl")

    assert header.version == "2.0"
    assert header.epsg == 7415
    assert len(features) == 3
    assert features[0]["type"] == "CityJSONFeature"


def test_dequantize():
    from dynastore.extensions.geovolumes.cityjson_ingest import (
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
    from dynastore.extensions.geovolumes.cityjson_ingest import (
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
    from dynastore.extensions.geovolumes.cityjson_ingest import (
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
    from dynastore.extensions.geovolumes.cityjson_ingest import parse_cityjsonseq

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
    from dynastore.extensions.geovolumes.cityjson_ingest import ingest_cityjson_file

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

    # update_collection called once with header info
    catalog_service.update_collection.assert_called_once()
    call_kwargs = catalog_service.update_collection.call_args
    # Check catalog_id and collection_id passed positionally or as kwargs
    args, kwargs = call_kwargs
    assert "test-cat" in args or kwargs.get("catalog_id") == "test-cat"
    assert "test-col" in args or kwargs.get("collection_id") == "test-col"

    # item_service.upsert_bulk called with 3 items total
    item_service.upsert_bulk.assert_called_once()
    upsert_args, upsert_kwargs = item_service.upsert_bulk.call_args
    items_passed = upsert_args[2] if len(upsert_args) > 2 else upsert_kwargs.get("items", [])
    assert len(items_passed) == 3
