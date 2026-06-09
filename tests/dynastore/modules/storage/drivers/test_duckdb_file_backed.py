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

"""Unit tests for the file-backed DuckDB driver logic (#374).

These exercise the pure helpers (source-expression selection and deterministic
geoid stamping) without requiring the optional ``duckdb`` extra to be installed —
the driver module imports ``duckdb`` lazily inside its connection helpers.
"""
from __future__ import annotations

from uuid import UUID


def _driver_cls():
    from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
    return ItemsDuckdbDriver


def test_vector_formats_use_st_read():
    D = _driver_cls()
    for fmt in ("gpkg", "geopackage", "shp", "shapefile", "geojson", "fgb"):
        assert D._source_expr(fmt, "/data/x.file") == "ST_Read('/data/x.file')"


def test_tabular_formats_use_native_reader():
    D = _driver_cls()
    assert D._source_expr("parquet", "/d/x.parquet") == "read_parquet('/d/x.parquet')"
    assert D._source_expr("csv", "/d/x.csv") == "read_csv_auto('/d/x.csv')"
    assert D._source_expr("json", "/d/x.json") == "read_json_auto('/d/x.json')"
    # Unknown format degrades to parquet reader, never ST_Read.
    assert D._source_expr(None, "/d/x") == "read_parquet('/d/x')"


def test_is_vector_format_case_insensitive():
    D = _driver_cls()
    assert D._is_vector_format("GPKG") is True
    assert D._is_vector_format("parquet") is False
    assert D._is_vector_format(None) is False


def test_file_geoid_uses_id_column_value():
    D = _driver_cls()
    from dynastore.tools.identifiers import derive_file_geoid
    row = {"fid": 42, "name": "Rome"}
    got = D._file_geoid_for_row("cat1", "col1", row, "fid")
    assert got == derive_file_geoid("cat1", "col1", 42)
    assert UUID(got).version == 5


def test_file_geoid_content_hash_fallback_is_stable():
    D = _driver_cls()
    row1 = {"name": "Rome", "pop": 2800000}
    row2 = {"pop": 2800000, "name": "Rome"}  # key order must not matter
    a = D._file_geoid_for_row("cat1", "col1", dict(row1), None)
    b = D._file_geoid_for_row("cat1", "col1", dict(row2), None)
    assert a == b
    # Different content → different geoid.
    c = D._file_geoid_for_row("cat1", "col1", {"name": "Milan"}, None)
    assert a != c


def test_row_to_feature_file_backed_stamps_geoid_and_keeps_fid():
    D = _driver_cls()
    from dynastore.tools.identifiers import derive_file_geoid
    row = {"id": "raw-source-id", "fid": "F7", "name": "Rome",
           "geometry": {"type": "Point", "coordinates": [12.0, 41.9]}}
    feat = D._row_to_feature(
        dict(row), "geometry",
        catalog_id="cat1", collection_id="col1", id_column="fid", file_backed=True,
    )
    # Wire id is the deterministic geoid, NOT the file's native id/fid.
    assert feat.id == derive_file_geoid("cat1", "col1", "F7")
    assert feat.id != "raw-source-id"
    # The native fid stays queryable in properties.
    assert (feat.properties or {}).get("fid") == "F7"


def test_row_to_feature_legacy_mode_preserves_row_id():
    """Non-file-backed reads (analytical parquet with an existing id) keep the
    pre-existing behaviour: the row's id column becomes the feature id."""
    D = _driver_cls()
    row = {"id": "existing-123", "name": "x",
           "geometry": {"type": "Point", "coordinates": [0, 0]}}
    feat = D._row_to_feature(dict(row), "geometry")
    assert feat.id == "existing-123"
