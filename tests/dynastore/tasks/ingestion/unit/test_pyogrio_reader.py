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

"""PyogrioReader — the pyogrio-backed fallback reader (replaces FionaReader).

Verifies the reader yields GeoJSON-shaped records and reports a feature
count, using a tiny on-disk GeoJSON so the test needs only pyogrio (no
system osgeo bindings).
"""

from __future__ import annotations

import json
import os
import tempfile

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
