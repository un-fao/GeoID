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

"""Unit and integration tests for GeoParquet format support in the DuckDB driver.

Three test layers:
1. SQL-generation unit tests — verify ``_source_expr`` emits the correct
   ST_GeomFromWKB decode subquery without touching DuckDB at all.
2. Hermetic decode test — creates a tiny GeoParquet fixture using DuckDB's own
   spatial extension (offline), reads it back through the geoparquet path, and
   asserts the geometry comes back as a usable GeoJSON point.
3. Optional live test — fetches the OGC GeoParquet example file over HTTPS;
   skipped gracefully if the network is unavailable.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _driver_cls():
    from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
    return ItemsDuckdbDriver


def _try_import_duckdb():
    """Return the duckdb module or None if unavailable."""
    try:
        import duckdb
        return duckdb
    except ImportError:
        return None


def _spatial_available() -> bool:
    """Return True when DuckDB + the spatial extension can both load."""
    duckdb = _try_import_duckdb()
    if duckdb is None:
        return False
    try:
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Layer 1: SQL-generation unit tests (no DuckDB required)
# ---------------------------------------------------------------------------

class TestSourceExprGeoparquet:
    """_source_expr generates the correct SQL for each format."""

    def test_geoparquet_format_uses_st_geom_from_wkb(self):
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "/data/places.parquet")
        assert "ST_GeomFromWKB" in sql
        assert "read_parquet" in sql
        assert "/data/places.parquet" in sql
        # Default geometry column
        assert '"geometry"' in sql

    def test_gpq_alias_behaves_like_geoparquet(self):
        D = _driver_cls()
        sql = D._source_expr("gpq", "/data/x.parquet")
        assert "ST_GeomFromWKB" in sql
        assert "read_parquet" in sql

    def test_geoparquet_geometry_column_override(self):
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "/data/x.parquet", geometry_column="geom")
        assert '"geom"' in sql
        assert "ST_GeomFromWKB" in sql
        # Should NOT contain the default "geometry" column name as the decode target
        # (it may appear in path but not as the column being decoded)
        # The REPLACE clause should reference "geom"
        assert 'ST_GeomFromWKB("geom")' in sql

    def test_geoparquet_decode_subquery_is_parenthesised(self):
        """The returned expression must be a parenthesised subquery for use in FROM."""
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "s3://bucket/data.parquet")
        assert sql.startswith("(")
        assert sql.endswith(")")

    def test_geoparquet_remote_path_passes_through_to_read_parquet(self):
        """Cloud paths are embedded inside read_parquet — DuckDB httpfs handles them."""
        D = _driver_cls()
        for scheme in ("gs://", "s3://", "https://", "http://"):
            path = f"{scheme}bucket/data.parquet"
            sql = D._source_expr("geoparquet", path)
            assert path in sql
            assert "read_parquet" in sql

    def test_plain_parquet_no_decode(self):
        """Plain ``parquet`` format must NOT emit ST_GeomFromWKB."""
        D = _driver_cls()
        sql = D._source_expr("parquet", "/data/table.parquet")
        assert "ST_GeomFromWKB" not in sql
        assert sql == "read_parquet('/data/table.parquet')"

    def test_gpkg_still_uses_st_read(self):
        """Vector formats must still go through ST_Read, not ST_GeomFromWKB."""
        D = _driver_cls()
        sql = D._source_expr("gpkg", "/data/layer.gpkg")
        assert sql == "ST_Read('/data/layer.gpkg')"
        assert "ST_GeomFromWKB" not in sql

    def test_is_geoparquet_format_recognises_aliases(self):
        D = _driver_cls()
        assert D._is_geoparquet_format("geoparquet") is True
        assert D._is_geoparquet_format("gpq") is True
        assert D._is_geoparquet_format("GEOPARQUET") is True
        assert D._is_geoparquet_format("GPQ") is True
        assert D._is_geoparquet_format("parquet") is False
        assert D._is_geoparquet_format("gpkg") is False
        assert D._is_geoparquet_format(None) is False

    def test_geoparquet_not_classified_as_vector(self):
        """geoparquet must NOT fall into the ST_Read path."""
        D = _driver_cls()
        assert D._is_vector_format("geoparquet") is False
        assert D._is_vector_format("gpq") is False


# ---------------------------------------------------------------------------
# Layer 2: Hermetic decode test (requires duckdb + spatial extension)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not _spatial_available(),
    reason="duckdb spatial extension not available in this environment",
)
class TestGeoparquetHermeticDecode:
    """End-to-end decode test using a locally generated GeoParquet fixture."""

    @pytest.fixture(scope="class")
    def geoparquet_fixture(self, tmp_path_factory) -> Path:
        """Create a minimal GeoParquet file using DuckDB's spatial extension.

        Uses ``ST_AsWKB(ST_Point(lon, lat))`` to write WKB bytes into the
        geometry column — exactly what real GeoParquet files contain.
        """
        import duckdb

        fixture_dir = tmp_path_factory.mktemp("geoparquet_fixtures")
        fixture_path = fixture_dir / "test_points.parquet"

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.execute(
            f"""
            COPY (
                SELECT
                    1 AS id,
                    'Rome' AS name,
                    ST_AsWKB(ST_Point(12.5, 41.9)) AS geometry
                UNION ALL
                SELECT
                    2 AS id,
                    'Paris' AS name,
                    ST_AsWKB(ST_Point(2.35, 48.85)) AS geometry
            ) TO '{fixture_path}' (FORMAT PARQUET)
            """
        )
        conn.close()
        assert fixture_path.exists(), "GeoParquet fixture was not created"
        return fixture_path

    def test_raw_parquet_returns_bytes(self, geoparquet_fixture: Path):
        """Sanity check: plain read_parquet returns the geometry as bytes, not GEOMETRY."""
        import duckdb

        conn = duckdb.connect(":memory:")
        row = conn.execute(
            f"SELECT geometry FROM read_parquet('{geoparquet_fixture}') LIMIT 1"
        ).fetchone()
        conn.close()
        # Raw WKB should come back as bytes
        assert row is not None
        assert isinstance(row[0], (bytes, bytearray)), (
            f"Expected bytes from plain read_parquet, got {type(row[0])}"
        )

    def test_geoparquet_path_decodes_geometry(self, geoparquet_fixture: Path):
        """Reading via the geoparquet _source_expr yields a decoded GEOMETRY column."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(geoparquet_fixture))

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        # The geometry column must now be a GEOMETRY type, not BLOB/bytes.
        schema = conn.execute(
            f"DESCRIBE SELECT * FROM {source} LIMIT 0"
        ).fetchall()
        geom_col_type = {row[0]: row[1] for row in schema}.get("geometry", "")
        assert "GEOMETRY" in str(geom_col_type).upper(), (
            f"Expected GEOMETRY column after decode, got: {geom_col_type}"
        )
        conn.close()

    def test_geoparquet_st_x_works_after_decode(self, geoparquet_fixture: Path):
        """After WKB decode, ST_X / ST_Y work on the geometry column."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(geoparquet_fixture))

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_X(geometry), ST_Y(geometry) FROM {source} WHERE id = 1"
        ).fetchone()
        conn.close()

        assert row is not None, "No rows returned from GeoParquet decode"
        lon, lat = row
        assert abs(lon - 12.5) < 0.01, f"Unexpected longitude: {lon}"
        assert abs(lat - 41.9) < 0.01, f"Unexpected latitude: {lat}"

    def test_geoparquet_geojson_serialisation(self, geoparquet_fixture: Path):
        """ST_AsGeoJSON round-trip produces a valid Point GeoJSON."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(geoparquet_fixture))

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_AsGeoJSON(geometry) FROM {source} WHERE id = 2"
        ).fetchone()
        conn.close()

        assert row is not None
        geom_json = json.loads(row[0])
        assert geom_json["type"] == "Point"
        coords = geom_json["coordinates"]
        assert abs(coords[0] - 2.35) < 0.01
        assert abs(coords[1] - 48.85) < 0.01

    def test_geometry_column_override(self, tmp_path):
        """geometry_column override redirects the WKB decode to a non-default column."""
        import duckdb

        fixture_path = tmp_path / "custom_geom_col.parquet"
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.execute(
            f"""
            COPY (
                SELECT
                    99 AS id,
                    ST_AsWKB(ST_Point(0.0, 51.5)) AS geom
            ) TO '{fixture_path}' (FORMAT PARQUET)
            """
        )

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(fixture_path), geometry_column="geom")

        assert '"geom"' in source
        row = conn.execute(
            f"SELECT ST_X(geom), ST_Y(geom) FROM {source} LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        assert abs(row[0] - 0.0) < 0.01
        assert abs(row[1] - 51.5) < 0.01


# ---------------------------------------------------------------------------
# Layer 3: Optional live test — OGC example.parquet from HTTPS
# ---------------------------------------------------------------------------

_OGC_EXAMPLE_URL = (
    "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
)


def _fetch_url_to_tmp(url: str) -> Optional[Path]:
    """Fetch *url* via curl into a temp file; return the path or None on failure."""
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.close()
        result = subprocess.run(
            ["curl", "-fsSL", "--max-time", "20", url, "-o", tmp.name],
            capture_output=True,
            timeout=25,
        )
        if result.returncode == 0 and Path(tmp.name).stat().st_size > 0:
            return Path(tmp.name)
        os.unlink(tmp.name)
        return None
    except Exception:
        return None


@pytest.mark.skipif(
    not _spatial_available(),
    reason="duckdb spatial extension not available",
)
class TestGeoparquetLiveOgcExample:
    """Live test against the OGC reference GeoParquet file.

    Skipped when the file cannot be downloaded (offline / CI without egress).
    """

    @pytest.fixture(scope="class")
    def ogc_fixture(self) -> Path:
        path = _fetch_url_to_tmp(_OGC_EXAMPLE_URL)
        if path is None:
            pytest.skip("OGC GeoParquet example not reachable; skipping live test")
        return path

    def test_live_file_has_features(self, ogc_fixture: Path):
        import duckdb

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(ogc_fixture))

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        count = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
        conn.close()

        assert count > 0, "OGC example GeoParquet returned 0 features"

    def test_live_file_geometry_is_decodable(self, ogc_fixture: Path):
        import duckdb

        D = _driver_cls()
        source = D._source_expr("geoparquet", str(ogc_fixture))

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        # All rows must return a non-null ST_AsGeoJSON
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM {source} WHERE ST_AsGeoJSON(geometry) IS NULL"
        ).fetchone()[0]
        conn.close()

        assert null_count == 0, (
            f"{null_count} rows have null geometry after WKB decode"
        )

    def test_cleanup(self, ogc_fixture: Path):
        """Remove the downloaded fixture after the live tests."""
        try:
            ogc_fixture.unlink(missing_ok=True)
        except Exception:
            pass
