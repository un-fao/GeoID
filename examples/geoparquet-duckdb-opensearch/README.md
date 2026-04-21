# Use Case: GeoParquet Collection with DuckDB Reader + OpenSearch Index

End-to-end walkthrough: register a GeoParquet file as a **collection asset**,
import its features into PostgreSQL via DuckDB, index them into OpenSearch,
and query both the OGC Features API and the Search API.

**Source file**: [OGC GeoParquet example](https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet)
-- Natural Earth countries with columns `pop_est`, `continent`, `name`,
`iso_a3`, `gdp_md_est`, and a WKB `geometry` column (Polygon/MultiPolygon,
CRS84).

## Architecture

```
                                 +-------------------+
                                 | countries.parquet  |
                                 | (Natural Earth)    |
                                 +--------+----------+
                                          |
                              DuckDB read_entities()
                              (via resolve_datasource())
                                          |
                    +---------+-----------+----------+
                    |                                |
            Import Script                    Custom pipeline
       (POST to STAC Items API)          (direct driver access)
                    |
                    v
          +---------+----------+
          |    PostgreSQL       |
          |  (items table)      |
          +---------+----------+
                    |
          +---------+----------+
          |                    |
   OGC Features API    ITEM_CREATION event
   (reads from PG)             |
                               v
                     +---------+----------+
                     |   OpenSearch        |
                     |   (dynastore-items) |
                     +----------+---------+
                                |
                      POST /search
                      (q, bbox, datetime)
```

**Two distinct read paths:**
- **DuckDB** (`read_entities()`): reads directly from `.parquet` via
  `resolve_datasource()` — used in import scripts and enrichment pipelines.
- **OGC Features API** (`/features/.../items`): always reads from PostgreSQL —
  populated by the import step.

**OpenSearch** is populated automatically when items are POSTed (event-driven),
or on-demand via the bulk reindex endpoint.

## About the Source File

| Column       | Type              | Description                          |
|--------------|-------------------|--------------------------------------|
| `pop_est`    | int64             | Estimated population                 |
| `continent`  | string            | Continent name                       |
| `name`       | string            | Country name                         |
| `iso_a3`     | string            | ISO 3166-1 alpha-3 code              |
| `gdp_md_est` | float64          | GDP estimate (millions USD)          |
| `geometry`   | WKB (Polygon/MultiPolygon) | Country boundary, CRS84     |

> The file has no `id` column. The DuckDB driver handles this gracefully
> (`Feature.id = None`). The import script uses `iso_a3` as the item ID.

---

## Prerequisites

### 1. Download the GeoParquet file

```bash
mkdir -p examples/geoparquet-duckdb-opensearch/data

curl -sL -o examples/geoparquet-duckdb-opensearch/data/countries.parquet \
  'https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet'
```

### 2. Start the services

```bash
docker compose -f src/dynastore/docker/docker-compose.yml up -d db catalog worker elasticsearch
```

### 3. Copy parquet into containers (or add a bind-mount)

The fastest approach for a running stack is `docker cp`:

```bash
docker exec geoid_catalog mkdir -p /data
docker exec geoid_worker  mkdir -p /data

docker cp examples/geoparquet-duckdb-opensearch/data/countries.parquet \
  geoid_catalog:/data/countries.parquet

docker cp examples/geoparquet-duckdb-opensearch/data/countries.parquet \
  geoid_worker:/data/countries.parquet
```

For a persistent setup add a bind-mount in `docker-compose.yml`:

```yaml
services:
  catalog:
    volumes:
      - ../examples/geoparquet-duckdb-opensearch/data:/data:ro
  worker:
    volumes:
      - ../examples/geoparquet-duckdb-opensearch/data:/data:ro
```

> **Ports**: `80` = catalog API, `81` = worker, `9200` = OpenSearch.

---

## Step 1 -- Create a Catalog

```bash
curl -s -X POST http://localhost:80/features/catalogs \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "natural-earth",
    "title": "Natural Earth",
    "description": "GeoParquet-backed Natural Earth datasets"
  }' | python3 -m json.tool
```

---

## Step 2 -- Disable GCP Provisioning (local dev)

Locally there is no GCP Cloud Run / Pub/Sub infrastructure. The
`gcp_provision_catalog` task will fail with
`Cannot determine self URL: K_SERVICE environment variable is not set`
and **cascade-delete the catalog row**.

Disable bucket provisioning **before creating the catalog** so the
lifecycle hook marks the catalog as `ready` immediately:

### 2a -- Disable GCP bucket provisioning (platform level)

```bash
curl -s -X PUT \
  http://localhost:80/configs/gcp_catalog_bucket \
  -H 'Content-Type: application/json' \
  -d '{"enabled": false}' | python3 -m json.tool
```

### 2b -- Disable Pub/Sub eventing (platform level)

```bash
curl -s -X PUT \
  http://localhost:80/configs/gcp_eventing \
  -H 'Content-Type: application/json' \
  -d '{
    "managed_eventing": {"enabled": false},
    "custom_subscriptions": [],
    "action_templates": {}
  }' | python3 -m json.tool
```

With both disabled, catalog creation completes synchronously (no async
provisioning task). Assets must be registered manually (Step 5) since
there is no upload completion webhook.

---

## Step 3 -- Pre-configure the Collection (before it exists)

Configs do **not** require the collection to exist. The config service stores
them keyed by `(catalog_id, collection_id)` with no FK constraint, so you can
set up driver and routing configs before creating the collection.

### 3a -- DuckDB driver config

```bash
curl -s -X PUT \
  http://localhost:80/configs/catalogs/natural-earth/collections/countries/configs/driver:duckdb \
  -H 'Content-Type: application/json' \
  -d '{
    "path": "/data/countries.parquet",
    "format": "parquet"
  }' | python3 -m json.tool
```

This config is used when code calls `resolve_datasource("READ")` or
`get_driver("READ", ...)` on this collection — for example the import script
in Step 6.

### 3b -- Routing config

```bash
curl -s -X PUT \
  http://localhost:80/configs/catalogs/natural-earth/collections/countries/configs/storage:collections \
  -H 'Content-Type: application/json' \
  -d '{
    "operations": {
      "WRITE":  [{"driver_id": "postgresql"}],
      "READ":   [{"driver_id": "duckdb"}],
      "SEARCH": [{"driver_id": "elasticsearch"}]
    }
  }' | python3 -m json.tool
```

| Operation | Driver         | Used by                                               |
|-----------|----------------|-------------------------------------------------------|
| WRITE     | postgresql     | STAC/OGC item creation, update, delete                |
| READ      | duckdb         | Custom tasks calling `resolve_datasource("READ")`     |
| SEARCH    | elasticsearch  | `resolve_datasource("SEARCH")`, search indexer check  |

> The OGC Features API (`/features/.../items`) always reads from **PostgreSQL**
> via the `ItemService` layer and does NOT consult the routing config.
> The routing config's `READ` entry is for explicit driver access in custom
> pipelines (import scripts, enrichment tasks).

---

## Step 4 -- Create the Collection

```bash
curl -s -X POST \
  http://localhost:80/features/catalogs/natural-earth/collections \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "countries",
    "title": "Countries",
    "description": "Natural Earth country boundaries from GeoParquet",
    "extent": {
      "spatial":  {"bbox": [[-180, -18.28, 180, 83.23]]},
      "temporal": {"interval": [["2024-01-01T00:00:00Z", null]]}
    }
  }' | python3 -m json.tool
```

---

## Step 5 -- Register the GeoParquet as a Collection Asset (manual)

Assets are scoped to the **collection** (not just the catalog). Because
Pub/Sub is disabled we create the asset directly via POST:

```bash
curl -s -X POST \
  http://localhost:80/assets/catalogs/natural-earth/collections/countries \
  -H 'Content-Type: application/json' \
  -d '{
    "asset_id": "countries-geoparquet",
    "uri": "file:///data/countries.parquet",
    "asset_type": "VECTORIAL",
    "metadata": {
      "media_type": "application/geo+parquet",
      "title": "Natural Earth Countries GeoParquet",
      "description": "OGC GeoParquet example - country boundaries (Polygon/MultiPolygon, CRS84)",
      "roles": ["data"],
      "source": "https://github.com/opengeospatial/geoparquet",
      "columns": ["pop_est", "continent", "name", "iso_a3", "gdp_md_est", "geometry"]
    },
    "owned_by": "local"
  }' | python3 -m json.tool
```

`owned_by: "local"` activates the deletion guard: the asset cannot be
hard-deleted while non-cascading references point to it.

---

## Step 6 -- Read from GeoParquet via DuckDB (direct driver access)

The DuckDB driver's `read_entities()` is accessed via the storage router:
`get_driver("READ", catalog_id, collection_id)`. This is used in custom
pipelines and enrichment tasks running inside the server process.

### 6a -- Python (server-side / custom task)

```python
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules import get_protocol
from dynastore.models.protocols import CatalogsProtocol

catalog_svc = get_protocol(CatalogsProtocol)  # CatalogService instance

# Resolve READ driver -> DuckDB (from routing config)
driver = await catalog_svc.resolve_datasource(
    "natural-earth",
    "countries",
    operation="READ",
)
print(f"Driver: {driver.driver_id}")  # -> "duckdb"

# Stream features directly from /data/countries.parquet
async for feature in driver.read_entities("natural-earth", "countries"):
    props = feature.properties
    print(f"{props.get('iso_a3'):5s} | {props.get('name'):30s} | {props.get('continent')}")
```

### 6b -- Equivalent HTTP call (via the enrichment API or custom endpoint)

There is no default HTTP endpoint that proxies the storage driver.
Direct DuckDB reads are intended for server-side import pipelines and
enrichment tasks. The import script in Step 7 shows how to use this.

---

## Step 7 -- Import GeoParquet Features into PostgreSQL

Read features from the parquet file (DuckDB) and POST each one to the STAC
Items API. This writes to PostgreSQL and automatically fires the
`ITEM_CREATION` event → Elasticsearch module indexes each item into OpenSearch.

### Import script

```python
#!/usr/bin/env python3
"""
Import Natural Earth countries from GeoParquet into the dynastore catalog.

Reads from /data/countries.parquet via DuckDB read_parquet(), then POSTs
each feature to the STAC Items API.
"""
import json
import sys
import duckdb
import requests

API = "http://localhost:80"
CATALOG_ID = "natural-earth"
COLLECTION_ID = "countries"
PARQUET_PATH = "examples/geoparquet-duckdb-opensearch/data/countries.parquet"

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
        ST_AsGeoJSON(geometry) AS geometry_json
    FROM read_parquet('{PARQUET_PATH}')
    WHERE iso_a3 IS NOT NULL AND iso_a3 != '-99'
""").fetchall()

cols = ["iso_a3", "name", "continent", "pop_est", "gdp_md_est", "geometry_json"]

imported = 0
for row in rows:
    r = dict(zip(cols, row))
    item_id = r["iso_a3"]
    geometry = json.loads(r["geometry_json"])

    stac_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "geometry": geometry,
        "bbox": [
            geometry.get("bbox", [-180, -90, 180, 90])
        ] if "bbox" in geometry else None,
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "name": r["name"],
            "continent": r["continent"],
            "pop_est": r["pop_est"],
            "gdp_md_est": r["gdp_md_est"],
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

    resp = requests.post(
        f"{API}/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
        json=stac_item,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code in (200, 201):
        imported += 1
        print(f"  [{imported:3d}] {item_id} OK")
    else:
        print(f"  [{imported:3d}] {item_id} FAILED {resp.status_code}: {resp.text[:120]}", file=sys.stderr)

print(f"\nImported {imported}/{len(rows)} features")
```

Run it:

```bash
cd /Users/ccancellieri/work/code/geoid
python3 examples/geoparquet-duckdb-opensearch/import_countries.py
```

> **What happens automatically:**
> Each POST to the STAC Items API fires an `ITEM_CREATION` event. The
> Elasticsearch module listener serializes the item and dispatches an
> `elasticsearch_index` task. The **worker** picks it up and indexes the
> document into the `dynastore-items` index. No manual reindex needed.

---

## Step 8 -- Read Features via the OGC Features API (from PostgreSQL)

After the import, the standard OGC Features endpoint reads from PostgreSQL:

### 8a -- Get all items (paginated)

```bash
curl -s 'http://localhost:80/features/catalogs/natural-earth/collections/countries/items?limit=5' \
  | python3 -m json.tool
```

### 8b -- Spatial filter (bbox) -- Europe

```bash
curl -s 'http://localhost:80/features/catalogs/natural-earth/collections/countries/items?limit=50&bbox=-25,35,45,72' \
  | python3 -m json.tool
```

### 8c -- Attribute filter -- by continent

```bash
curl -s 'http://localhost:80/features/catalogs/natural-earth/collections/countries/items?limit=100&filter=continent%3D%3DAfrica' \
  | python3 -m json.tool
```

### 8d -- Single feature by ID (iso_a3)

```bash
curl -s 'http://localhost:80/features/catalogs/natural-earth/collections/countries/items/CAN' \
  | python3 -m json.tool
```

---

## Step 9 -- (Optional) Manual Bulk Reindex into OpenSearch

If the automatic event-driven indexing was not active during import (e.g. the
worker was down), trigger a manual bulk reindex:

```bash
curl -s -X POST \
  'http://localhost:80/search/catalogs/natural-earth/collections/countries/reindex' \
  | python3 -m json.tool
```

Response (202 Accepted):
```json
{
  "task_id": "...",
  "catalog_id": "natural-earth",
  "collection_id": "countries",
  "mode": "catalog",
  "status": "queued"
}
```

The worker streams all items from PostgreSQL and bulk-indexes them into the
`dynastore-items` OpenSearch index.

Verify:
```bash
curl -s 'http://localhost:9200/dynastore-items/_count?q=catalog_id:natural-earth' \
  | python3 -m json.tool
```

---

## Step 10 -- Search via OpenSearch

### 10a -- Free-text search

```bash
curl -s -X POST http://localhost:80/search \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "Canada",
    "catalog_id": "natural-earth",
    "collections": ["countries"],
    "limit": 10
  }' | python3 -m json.tool
```

### 10b -- Spatial search (bbox) -- Africa

```bash
curl -s -X POST http://localhost:80/search \
  -H 'Content-Type: application/json' \
  -d '{
    "bbox": [-20, -35, 55, 40],
    "catalog_id": "natural-earth",
    "collections": ["countries"],
    "limit": 100
  }' | python3 -m json.tool
```

### 10c -- Free-text + bbox combined

```bash
curl -s -X POST http://localhost:80/search \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "Oceania",
    "bbox": [100, -50, 180, 0],
    "catalog_id": "natural-earth",
    "collections": ["countries"],
    "limit": 50,
    "sortby": "-properties.pop_est"
  }' | python3 -m json.tool
```

### 10d -- Cursor-based pagination

```bash
# First page
curl -s -X POST http://localhost:80/search \
  -H 'Content-Type: application/json' \
  -d '{"catalog_id": "natural-earth", "limit": 5}' | python3 -m json.tool

# Next page -- use token from previous response's `next` link body
curl -s -X POST http://localhost:80/search \
  -H 'Content-Type: application/json' \
  -d '{"catalog_id": "natural-earth", "limit": 5, "token": "[...]"}' | python3 -m json.tool
```

---

## Capabilities Summary

| Capability              | OGC Features API (PG)      | OpenSearch Search           | DuckDB `read_entities()`     |
|-------------------------|----------------------------|-----------------------------|------------------------------|
| **Data source**         | PostgreSQL items table      | OpenSearch index            | `.parquet` file (direct)     |
| **Spatial filter**      | `bbox` param, PostGIS       | `bbox` / `intersects` body  | `ST_Intersects()` DuckDB SQL |
| **Text search**         | Not supported               | Full-text, fuzziness        | SQL LIKE / exact             |
| **Attribute filter**    | `filter=field==value`       | Multi-field, boosted        | SQL `WHERE field = ?`        |
| **Pagination**          | `limit`/`offset`            | Cursor (`search_after`)     | `LIMIT`/`OFFSET` SQL         |
| **Response format**     | OGC GeoJSON                 | STAC ItemCollection         | Python `Feature` objects     |
| **Data freshness**      | Real-time (transactional)   | Eventual (event + reindex)  | Real-time (file read)        |
| **HTTP endpoint**       | `/features/.../items`       | `POST /search`              | No HTTP — server-side only   |

---

## Order of Operations (recap)

```
1.  PUT  /configs/gcp_catalog_bucket    -- disable GCP bucket provisioning (local dev)
2.  PUT  /configs/gcp_eventing          -- disable Pub/Sub eventing (local dev)
3.  POST /features/catalogs             -- create catalog (now completes synchronously)
4.  PUT  .../configs/driver:duckdb      -- point DuckDB at parquet (before collection exists!)
5.  PUT  .../configs/storage:collections -- READ->duckdb, WRITE->postgresql, SEARCH->elasticsearch
6.  POST /features/.../collections      -- create collection
7.  POST /assets/.../collections/{cid}  -- register parquet as collection asset (manual)
8.  python3 import_countries.py         -- read parquet via DuckDB, POST each feature to STAC API
9.  GET  /features/.../items            -- OGC Features reads from PostgreSQL
10. (automatic) worker indexes each item into OpenSearch on ITEM_CREATION event
11. POST /search                        -- OpenSearch serves discovery queries
```

Steps 4-5 work before the collection exists because the config service stores
configs keyed by `(catalog_id, collection_id)` with **no FK constraint** on
the collections table.

Step 10 is automatic when the worker is running; use
`POST /search/.../reindex` to trigger it manually if the worker was offline
during import.
