"""
Integration test: GeoParquet-backed collection with DuckDB reader.

Exercises the full pipeline documented in
examples/geoparquet-duckdb-opensearch/README.md:

  1. Create catalog + collection via STAC API.
  2. Set driver:duckdb config (parquet path) and routing config
     (READ → duckdb, WRITE → postgresql) via Configs API.
  3. Create a collection-level asset pointing to the GeoParquet.
  4. Import 5 Natural Earth country features via STAC Items API.
  5. Verify OGC Features API returns all 5 items (PostgreSQL read path).
  6. Verify DuckDB driver reads directly from the parquet file.
  7. Verify DuckDB attribute filter (iso_a3 equality).
"""

import json
import uuid
from pathlib import Path

import pytest

# Absolute path to the example parquet; resolved at module load so tests are
# skipped cleanly if the file is missing.
_PARQUET_PATH = (
    Path(__file__).parents[5]
    / "examples"
    / "geoparquet-duckdb-opensearch"
    / "data"
    / "countries.parquet"
)

# Expected iso_a3 codes in the example parquet (Natural Earth 5-country sample).
_EXPECTED_ISO = {"FJI", "TZA", "ESH", "CAN", "USA"}

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stats", "storage_duckdb"
    ),
    pytest.mark.enable_extensions("stac", "features", "assets", "configs"),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def run_id():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def catalog_id(run_id):
    return f"geoparquet-test-{run_id}"


@pytest.fixture
def collection_id():
    return "countries"


@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "type": "Catalog",
        "stac_version": "1.0.0",
        "title": "GeoParquet Test Catalog",
        "description": "Integration test catalog for the GeoParquet/DuckDB use case.",
    }


@pytest.fixture
def collection_data(collection_id):
    return {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.0.0",
        "description": "Natural Earth countries from GeoParquet via DuckDB",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
    }


def _build_stac_item(iso_a3: str, name: str, continent: str, pop_est, gdp_md_est,
                     geometry: dict, bbox: list) -> dict:
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": iso_a3,
        "geometry": geometry,
        "bbox": bbox,
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "name": name,
            "continent": continent,
            "pop_est": pop_est,
            "gdp_md_est": gdp_md_est,
            "iso_a3": iso_a3,
        },
        "links": [],
        "assets": {
            "source": {
                "href": f"file://{_PARQUET_PATH}",
                "type": "application/geo+parquet",
                "title": "GeoParquet source",
                "roles": ["data"],
            }
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_geoparquet_duckdb_pipeline(
    in_process_client, catalog_id, collection_id, catalog_data, collection_data
):
    """Full end-to-end pipeline: configure → import → query (OGC + DuckDB)."""
    if not _PARQUET_PATH.exists():
        pytest.skip(f"Example parquet not found: {_PARQUET_PATH}")

    try:
        import duckdb  # noqa: F401
    except ImportError:
        pytest.skip("duckdb not installed")

    parquet_str = str(_PARQUET_PATH)

    # ------------------------------------------------------------------
    # 1. Create catalog
    # ------------------------------------------------------------------
    r = await in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code in (200, 201), f"Catalog create failed: {r.text}"

    # ------------------------------------------------------------------
    # 2. Set driver:duckdb config (before collection exists — allowed by design)
    # ------------------------------------------------------------------
    duckdb_config = {
        "path": parquet_str,
        "format": "parquet",
    }
    r = await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/configs/driver:duckdb",
        json=duckdb_config,
    )
    assert r.status_code in (200, 201), f"DuckDB config failed: {r.text}"

    # ------------------------------------------------------------------
    # 3. Set routing config: READ → duckdb, WRITE → postgresql
    # ------------------------------------------------------------------
    routing_config = {
        "operations": {
            "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "duckdb", "hints": [], "on_failure": "fatal"}],
        }
    }
    r = await in_process_client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}/configs/routing",
        json=routing_config,
    )
    assert r.status_code in (200, 201), f"Routing config failed: {r.text}"

    # ------------------------------------------------------------------
    # 4. Create collection
    # ------------------------------------------------------------------
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code in (200, 201), f"Collection create failed: {r.text}"

    # ------------------------------------------------------------------
    # 5. Register GeoParquet as a collection-level asset
    # ------------------------------------------------------------------
    asset_payload = {
        "asset_id": "countries-geoparquet",
        "uri": f"file://{parquet_str}",
        "asset_type": "ASSET",
        "owned_by": "local",
        "metadata": {
            "type": "application/geo+parquet",
            "title": "Natural Earth Countries GeoParquet",
            "roles": ["data"],
        },
    }
    r = await in_process_client.post(
        f"/assets/catalogs/{catalog_id}/collections/{collection_id}",
        json=asset_payload,
    )
    assert r.status_code in (200, 201), f"Asset create failed: {r.text}"

    # ------------------------------------------------------------------
    # 6. Import features from parquet via DuckDB → STAC Items API
    # ------------------------------------------------------------------
    import duckdb as _duckdb

    con = _duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    rows = con.execute(f"""
        SELECT
            iso_a3, name, continent, pop_est, gdp_md_est,
            ST_AsGeoJSON(geometry)::VARCHAR AS geometry_json,
            ST_XMin(geometry) AS xmin, ST_YMin(geometry) AS ymin,
            ST_XMax(geometry) AS xmax, ST_YMax(geometry) AS ymax
        FROM read_parquet('{parquet_str}')
        WHERE iso_a3 IS NOT NULL
          AND iso_a3 != '-99'
          AND TRIM(iso_a3) != ''
    """).fetchall()
    con.close()

    cols = ["iso_a3", "name", "continent", "pop_est", "gdp_md_est",
            "geometry_json", "xmin", "ymin", "xmax", "ymax"]
    features_data = [dict(zip(cols, row)) for row in rows]

    assert len(features_data) == 5, f"Expected 5 features, got {len(features_data)}"

    for fd in features_data:
        geom = json.loads(fd["geometry_json"])
        bbox = [
            round(fd["xmin"], 6), round(fd["ymin"], 6),
            round(fd["xmax"], 6), round(fd["ymax"], 6),
        ]
        item = _build_stac_item(
            iso_a3=fd["iso_a3"],
            name=fd["name"],
            continent=fd["continent"],
            pop_est=fd["pop_est"],
            gdp_md_est=fd["gdp_md_est"],
            geometry=geom,
            bbox=bbox,
        )
        r = await in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )
        assert r.status_code in (200, 201), (
            f"Item {fd['iso_a3']} create failed ({r.status_code}): {r.text[:300]}"
        )

    # ------------------------------------------------------------------
    # 7. Verify OGC Features API returns all 5 items (PostgreSQL path)
    # ------------------------------------------------------------------
    r = await in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items"
    )
    assert r.status_code == 200, f"Features list failed: {r.text}"
    fc = r.json()
    assert fc.get("type") == "FeatureCollection"
    returned_ids = {f["id"] for f in fc.get("features", [])}
    assert returned_ids == _EXPECTED_ISO, (
        f"Expected ISO codes {_EXPECTED_ISO}, got {returned_ids}"
    )

    # ------------------------------------------------------------------
    # 8. Verify DuckDB driver reads directly from the parquet file
    #    via the storage router (tests the full READ routing path)
    # ------------------------------------------------------------------
    from dynastore.modules.storage import get_driver, Operation

    driver = await get_driver(Operation.READ, catalog_id, collection_id)
    assert driver.driver_id == "duckdb", (
        f"Expected duckdb driver via routing, got '{driver.driver_id}'"
    )

    features_from_duckdb = []
    async for feat in driver.read_entities(catalog_id, collection_id, limit=10):
        features_from_duckdb.append(feat)

    assert len(features_from_duckdb) == 5, (
        f"DuckDB read_entities returned {len(features_from_duckdb)}, expected 5"
    )

    # ------------------------------------------------------------------
    # 9. Verify DuckDB attribute filter (iso_a3 == 'CAN')
    # ------------------------------------------------------------------
    from dynastore.models.query_builder import FilterCondition, QueryRequest

    canada_filter = QueryRequest(
        filters=[FilterCondition(field="iso_a3", operator="eq", value="CAN")]
    )
    canada_features = []
    async for feat in driver.read_entities(
        catalog_id, collection_id, request=canada_filter, limit=5
    ):
        canada_features.append(feat)

    assert len(canada_features) == 1, (
        f"Expected 1 feature for CAN, got {len(canada_features)}"
    )
    canada = canada_features[0]
    assert canada.properties.get("iso_a3") == "CAN" or canada.id == "CAN"
