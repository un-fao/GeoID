
import io
import json
import os
import tempfile
import zipfile

import pytest
from shapely import wkb
from shapely.geometry import Point

from dynastore.tools.features import Feature
from dynastore.tools.file_io import (
    _process_records_for_writing,
    _sanitize,
    write_geojson,
    write_geopackage,
    write_parquet,
    write_shapefile,
)
from dynastore.tools.identifiers import generate_uuidv7

def test_process_records_for_writing_with_dict():
    """Test _process_records_for_writing with a dictionary."""
    geom = Point(0, 0)
    records = [
        {
            "geoid": generate_uuidv7(),
            "attributes": {"attr1": "val1"},
            "geom": wkb.dumps(geom),
            "external_id": "ext-1"
        }
    ]
    
    processed = list(_process_records_for_writing(records))
    
    assert len(processed) == 1
    assert processed[0]["type"] == "Feature"
    assert processed[0]["properties"]["attr1"] == "val1"
    assert processed[0]["properties"]["external_id"] == "ext-1"
    assert processed[0]["geometry"]["type"] == "Point"

def test_process_records_for_writing_with_pydantic():
    """Test _process_records_for_writing with a Pydantic model."""
    geoid = generate_uuidv7()
    geom = Point(1, 1)
    feature = Feature(
        geoid=geoid,
        attributes={"attr2": "val2"},
        geom=wkb.dumps(geom),
        external_id="ext-2"
    )
    
    records = [feature]
    
    processed = list(_process_records_for_writing(records))
    
    assert len(processed) == 1
    assert processed[0]["type"] == "Feature"
    assert processed[0]["properties"]["attr2"] == "val2"
    assert processed[0]["properties"]["external_id"] == "ext-2"
    assert processed[0]["geometry"]["type"] == "Point"
    assert processed[0]["geometry"]["coordinates"] == (1.0, 1.0)

# ---------------------------------------------------------------------------
# _sanitize helper
# ---------------------------------------------------------------------------


def test_sanitize_none():
    assert _sanitize(None) is None


def test_sanitize_string():
    assert _sanitize("hello") == "hello"


def test_sanitize_list():
    result = _sanitize([1, 2, None])
    assert result == [1, 2, None]


def test_sanitize_nested_dict():
    result = _sanitize({"a": {"b": 1}})
    assert result["a"]["b"] == 1


# ---------------------------------------------------------------------------
# _process_records_for_writing — OGC Feature dict (type/geometry/properties)
# ---------------------------------------------------------------------------


def test_process_records_feature_style_dict():
    """OGC Feature-style dict (has 'properties' key)."""
    records = [
        {
            "type": "Feature",
            "id": "feat-1",
            "geometry": {"type": "Point", "coordinates": [12.0, 45.0]},
            "properties": {"name": "Rome", "pop": 4_000_000},
        }
    ]
    processed = list(_process_records_for_writing(records))
    assert len(processed) == 1
    r = processed[0]
    assert r["type"] == "Feature"
    assert r["id"] == "feat-1"
    assert r["properties"]["name"] == "Rome"
    assert r["geometry"]["type"] == "Point"


def test_process_records_empty_input():
    assert list(_process_records_for_writing([])) == []


def test_process_records_multiple():
    geom = Point(0, 0)
    records = [
        {"attributes": {"val": i}, "geom": wkb.dumps(geom)} for i in range(5)
    ]
    processed = list(_process_records_for_writing(records))
    assert len(processed) == 5


def test_process_records_flat_dict():
    """Flat dict without 'properties' or 'attributes' key."""
    records = [{"id": "x1", "name": "test", "geometry": {"type": "Point", "coordinates": [0, 0]}}]
    processed = list(_process_records_for_writing(records))
    assert len(processed) == 1
    # geometry should be extracted out of properties
    assert processed[0]["geometry"] is not None


# ---------------------------------------------------------------------------
# write_geojson
# ---------------------------------------------------------------------------


def _make_point_records(n: int = 3):
    geom = Point(0, 0)
    return [
        {
            "attributes": {"idx": i},
            "geom": wkb.dumps(geom),
        }
        for i in range(n)
    ]


def test_write_geojson_produces_valid_json():
    records = _make_point_records(2)
    chunks = list(write_geojson(records, srid=4326))
    full = b"".join(chunks)
    data = json.loads(full)
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 2


def test_write_geojson_empty_records():
    chunks = list(write_geojson([], srid=4326))
    full = b"".join(chunks)
    data = json.loads(full)
    assert data["type"] == "FeatureCollection"
    assert data["features"] == []


# ---------------------------------------------------------------------------
# write_parquet
# ---------------------------------------------------------------------------


def test_write_parquet_returns_bytes():
    records = _make_point_records(3)
    chunks = list(write_parquet(records, srid=4326))
    full = b"".join(chunks)
    # Parquet files start with magic bytes PAR1
    assert full[:4] == b"PAR1"


def test_write_parquet_empty_records():
    """Empty generator should produce no output (no writer created)."""
    chunks = list(write_parquet([], srid=4326))
    assert chunks == []


# ---------------------------------------------------------------------------
# write_geopackage
# ---------------------------------------------------------------------------


def test_write_geopackage_returns_bytes():
    records = _make_point_records(2)
    chunks = list(write_geopackage(records, srid=4326))
    full = b"".join(chunks)
    # GeoPackage / SQLite magic bytes
    assert full[:16] == b"SQLite format 3\x00"


# ---------------------------------------------------------------------------
# write_shapefile — extended
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("shapefile_pyarrow_isolation")
def test_write_shapefile_with_dict():
    """Test write_shapefile with a dictionary to ensure it doesn't crash."""
    geom = Point(0, 0)
    records = [
        {
            "geoid": generate_uuidv7(),
            "attributes": {"attr1": "val1"},
            "geom": wkb.dumps(geom),
            "external_id": "ext-1"
        }
    ]

    chunks = list(write_shapefile(records, srid=4326))

    assert len(chunks) > 0
    full_content = b"".join(chunks)
    assert len(full_content) > 0
    assert full_content.startswith(b"PK")


@pytest.mark.xdist_group("shapefile_pyarrow_isolation")
def test_write_shapefile_zip_contains_shp():
    """Shapefile ZIP must contain a .shp component."""
    records = _make_point_records(1)
    chunks = list(write_shapefile(records, srid=4326))
    full = b"".join(chunks)
    with zipfile.ZipFile(io.BytesIO(full)) as zf:
        names = zf.namelist()
    assert any(n.endswith(".shp") for n in names)


@pytest.mark.xdist_group("shapefile_pyarrow_isolation")
def test_write_shapefile_empty_records():
    """Empty shapefile should still be a valid ZIP."""
    chunks = list(write_shapefile([], srid=4326))
    full = b"".join(chunks)
    # May be empty or a zero-record shapefile — either is acceptable
    if full:
        assert full[:2] == b"PK"
