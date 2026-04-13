#!/usr/bin/env python3
"""
Import Natural Earth countries from GeoParquet into the dynastore catalog.

Reads from countries.parquet via DuckDB read_parquet() + ST_AsGeoJSON(),
then POSTs each feature to the STAC Items API.

Usage:
    python3 examples/geoparquet-duckdb-opensearch/import_countries.py

Requirements:
    pip install duckdb requests
"""

import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PARQUET_PATH = str(SCRIPT_DIR / "data" / "countries.parquet")

API = "http://localhost:80"
CATALOG_ID = "natural-earth"
COLLECTION_ID = "countries"


def load_duckdb():
    try:
        import duckdb
        return duckdb
    except ImportError:
        sys.exit("duckdb not installed -- run: pip install duckdb")


def load_requests():
    try:
        import requests
        return requests
    except ImportError:
        sys.exit("requests not installed -- run: pip install requests")


def read_parquet_features(duckdb, path: str):
    """Read all country features from the GeoParquet file via DuckDB."""
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    rows = con.execute(f"""
        SELECT
            iso_a3,
            name,
            continent,
            pop_est,
            gdp_md_est,
            ST_AsGeoJSON(geometry)::VARCHAR AS geometry_json,
            ST_XMin(geometry)              AS xmin,
            ST_YMin(geometry)              AS ymin,
            ST_XMax(geometry)              AS xmax,
            ST_YMax(geometry)              AS ymax
        FROM read_parquet('{path}')
        WHERE iso_a3 IS NOT NULL
          AND iso_a3 != '-99'
          AND TRIM(iso_a3) != ''
    """).fetchall()

    cols = ["iso_a3", "name", "continent", "pop_est", "gdp_md_est",
            "geometry_json", "xmin", "ymin", "xmax", "ymax"]
    return [dict(zip(cols, row)) for row in rows]


def build_stac_item(row: dict) -> dict:
    """Convert a row dict to a STAC Item payload."""
    geometry = json.loads(row["geometry_json"])
    item_id = row["iso_a3"]

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "geometry": geometry,
        "bbox": [
            round(row["xmin"], 6),
            round(row["ymin"], 6),
            round(row["xmax"], 6),
            round(row["ymax"], 6),
        ],
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "name": row["name"],
            "continent": row["continent"],
            "pop_est": row["pop_est"],
            "gdp_md_est": row["gdp_md_est"],
            "iso_a3": item_id,
        },
        "links": [],
        "assets": {
            "source": {
                "href": "file:///data/countries.parquet",
                "type": "application/geo+parquet",
                "title": "GeoParquet source",
                "roles": ["data"],
            }
        },
    }


def post_item(requests, catalog_id: str, collection_id: str, item: dict) -> bool:
    """POST a STAC item. Returns True on success."""
    url = f"{API}/stac/catalogs/{catalog_id}/collections/{collection_id}/items"
    resp = requests.post(url, json=item, headers={"Content-Type": "application/json"})
    if resp.status_code in (200, 201):
        return True
    print(f"    FAILED {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
    return False


def main():
    duckdb = load_duckdb()
    requests = load_requests()

    print(f"Reading from: {PARQUET_PATH}")
    rows = read_parquet_features(duckdb, PARQUET_PATH)
    print(f"Found {len(rows)} features\n")

    imported, failed = 0, 0
    for row in rows:
        item = build_stac_item(row)
        ok = post_item(requests, CATALOG_ID, COLLECTION_ID, item)
        if ok:
            imported += 1
            print(f"  [{imported:3d}] {item['id']:6s}  {row['name']:30s}  {row['continent']}")
        else:
            failed += 1

    print(f"\nDone: {imported} imported, {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
