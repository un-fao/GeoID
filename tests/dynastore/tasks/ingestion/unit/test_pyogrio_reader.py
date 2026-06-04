"""PyogrioReader — the pyogrio-backed fallback reader (replaces FionaReader).

Verifies the reader yields GeoJSON-shaped records and reports a feature
count, using a tiny on-disk GeoJSON so the test needs only pyogrio (no
system osgeo bindings).
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pyogrio")
pytest.importorskip("geopandas")

from dynastore.tasks.ingestion.readers.pyogrio_reader import PyogrioReader


_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "a", "val": 1},
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        },
        {
            "type": "Feature",
            "properties": {"name": "b", "val": 2},
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        },
    ],
}


@pytest.fixture()
def geojson_path(tmp_path):
    p = tmp_path / "sample.geojson"
    p.write_text(json.dumps(_GEOJSON))
    return str(p)


def test_open_yields_geojson_records(geojson_path):
    reader = PyogrioReader()
    with reader.open(geojson_path) as records:
        feats = list(records)
    assert len(feats) == 2
    names = {f["properties"]["name"] for f in feats}
    assert names == {"a", "b"}
    for f in feats:
        assert f["type"] == "Feature"
        assert f["geometry"]["type"] == "Point"


def test_feature_count(geojson_path):
    assert PyogrioReader().feature_count(geojson_path) == 2


def test_feature_count_bad_uri_returns_none():
    assert PyogrioReader().feature_count("/nonexistent/path/nope.geojson") is None


def test_can_read_matches_extensions():
    assert PyogrioReader.can_read("gs://b/x/file.geojson")
    assert PyogrioReader.can_read("gs://b/x/file.gpkg")
    assert not PyogrioReader.can_read("gs://b/x/file.parquet")


def test_priority_is_tail_fallback():
    # Strictly behind GdalOsgeoReader (priority=10); higher number = later.
    assert PyogrioReader.priority == 100
    assert PyogrioReader.reader_id == "pyogrio"


def test_named_fid_column_surfaced_into_properties(tmp_path):
    """A format with a named FID column (GeoPackage's ``fid``) surfaces it as a
    property so a collection declaring it required resolves it (#1820).

    GeoJSON has no FID column, so write a GeoPackage — its primary key ``fid``
    is the OGR FID and is otherwise dropped by ``iterfeatures``.
    """
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"]},
        geometry=[Point(0.0, 0.0), Point(1.0, 2.0)],
        crs="EPSG:4326",
    )
    gpkg = tmp_path / "sample.gpkg"
    gdf.to_file(gpkg, driver="GPKG", engine="pyogrio")

    with PyogrioReader().open(str(gpkg)) as records:
        feats = list(records)

    assert len(feats) == 2
    # Every feature carries the GeoPackage ``fid`` primary key in properties.
    assert all("fid" in f["properties"] for f in feats)
    assert {f["properties"]["name"] for f in feats} == {"a", "b"}
