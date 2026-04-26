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
