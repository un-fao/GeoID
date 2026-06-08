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

import pytest
from unittest.mock import MagicMock
from dynastore.modules.edr.collection_metadata import build_edr_collection, _build_extent


class _FakeExtent:
    def model_dump(self, **_kwargs):
        return {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["2020-01-01", None]]},
        }


class _FakeCollection:
    id = "rainfall"
    title = "Monthly Rainfall"
    description = "Global monthly rainfall dataset"
    extent = _FakeExtent()


def test_build_edr_collection_basic():
    col = _FakeCollection()
    result = build_edr_collection("fao", col, base_url="https://host")
    assert result["id"] == "rainfall"
    assert result["title"] == "Monthly Rainfall"
    assert "position" in result["data_queries"]
    assert "area" in result["data_queries"]
    assert "cube" in result["data_queries"]
    assert result["crs"] == ["CRS84"]
    assert result["output_formats"] == ["CoverageJSON", "GeoJSON"]
    pos_href = result["data_queries"]["position"]["link"]["href"]
    assert "fao" in pos_href
    assert "rainfall" in pos_href


def test_build_extent_spatial():
    extent = _build_extent({
        "spatial": {"bbox": [[-10.0, -20.0, 30.0, 40.0]]},
    })
    assert extent["spatial"]["bbox"] == [[-10.0, -20.0, 30.0, 40.0]]
    assert "temporal" not in extent


def test_build_extent_temporal():
    extent = _build_extent({
        "temporal": {"interval": [["2020-01-01", "2024-12-31"]]},
    })
    assert "spatial" not in extent
    assert extent["temporal"]["interval"] == [["2020-01-01", "2024-12-31"]]
