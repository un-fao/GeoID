"""Generator for three new notebooks introduced with the role-based-driver
naming-harmonisation refactor.

Keeps the handwritten cell content here (plain Python strings) rather than in
the raw .ipynb JSON — much easier to review in a code diff, and the script
re-generates the notebooks idempotently.  Run once, then the emitted .ipynb
files are the source of record; this script is kept for future edits.
"""

from __future__ import annotations

import json
import os
import textwrap
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def md(*chunks: str) -> Dict[str, Any]:
    """Build a markdown cell from one or more text chunks (joined with "\n")."""
    body = "\n".join(chunks)
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": body.splitlines(keepends=True),
    }


def code(body: str) -> Dict[str, Any]:
    """Build a code cell from a dedented body."""
    body = textwrap.dedent(body).strip("\n") + "\n"
    return {
        "cell_type": "code",
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": body.splitlines(keepends=True),
    }


def write_nb(path: str, cells: List[Dict[str, Any]]) -> None:
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(f"wrote {path}")


# ---------------------------------------------------------------------------
# Shared bootstrap / teardown cells
# ---------------------------------------------------------------------------

BOOTSTRAP_IMPORTS = """
import os
import json
import uuid as _uuid
import time as _t

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL    = os.environ.get("DYNASTORE_BASE_URL", "http://localhost:8080")
ADMIN_TOKEN = (
    os.environ.get("DYNASTORE_ADMIN_TOKEN")
    or os.environ.get("DYNASTORE_SYSADMIN_TOKEN")
    or ""
)

headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"} if ADMIN_TOKEN else {}
client  = httpx.Client(base_url=BASE_URL, headers=headers, timeout=60.0)
"""


def nb_records_no_stac() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []

    cells.append(md(
        "# Records — 01: RECORDS-type collection without STAC\n",
        "**Persona:** Metadata librarian / catalogue operator.\n",
        "**Goal:** Run a pure OGC API — Records catalogue (no STAC fields, no geometry)\n"
        "through DynaStore's generic storage pipeline.  Demonstrates that the role-based\n"
        "driver architecture supports metadata-only catalogues by setting\n"
        "``ItemsPostgresqlDriverConfig.collection_type=\"RECORDS\"`` — the PG driver\n"
        "then skips the geometry sidecar automatically, and the Records extension\n"
        "serves ``/records`` endpoints against the same storage row.\n",
        "## What's different from STAC\n",
        "- `collection_type=\"RECORDS\"` → no geometry column, no GeometrySidecar DDL.\n"
        "- Only `item_metadata` + `attributes` sidecars — records carry title, description,\n"
        "  keywords, and arbitrary typed attributes.\n"
        "- Served under `/records/...` endpoints (see `extensions/records/records_service.py`).\n"
        "- No `/stac/*` conformance URIs on this collection.\n",
        "## Prerequisites\n",
        "- DynaStore is running and reachable at `DYNASTORE_BASE_URL`.\n"
        "- Admin token in `DYNASTORE_ADMIN_TOKEN` (or `DYNASTORE_SYSADMIN_TOKEN`).\n"
        "- The `records` extension is loaded (scope_catalog / api_open / …).",
    ))
    cells.append(code(BOOTSTRAP_IMPORTS + """
CATALOG_ID    = f"rec01-{_uuid.uuid4().hex[:8]}"
COLLECTION_ID = "thesaurus-descriptors"
print(f"Using catalog={CATALOG_ID} collection={COLLECTION_ID}")
"""))

    cells.append(code("""
# Bootstrap: create ephemeral catalog (via STAC catalog endpoint — STAC serves
# as the catalog-creation API; individual collections can then live under it
# with any collection_type).

payload = {
    "id": CATALOG_ID,
    "type": "Catalog",
    "title": "Records demo catalog",
    "description": "Harvested metadata records — no STAC features.",
    "stac_version": "1.1.0",
    "conformsTo": [],
    "links": [],
}
for _attempt in range(3):
    r = client.post("/stac/catalogs", content=json.dumps(payload))
    if r.status_code in (200, 201, 409):
        break
    _t.sleep(1)
assert r.status_code in (200, 201, 409), r.text[:400]
print("Catalog created" if r.status_code != 409 else "Catalog exists")
"""))

    cells.append(md(
        "## Step 1 — Create a RECORDS-type collection\n",
        "Collection creation is where we opt out of geometry.  The storage driver\n"
        "config carries ``collection_type=\"RECORDS\"``; the PG driver's\n"
        "``_effective_sidecars`` helper sees this and injects only the sidecars\n"
        "relevant for record-only collections (`item_metadata` + `attributes`).\n",
    ))
    cells.append(code("""
# Create a RECORDS collection via /records endpoint (the records extension
# registers its own POST handler for collection creation with RECORDS-typed
# storage pre-wired).

rec_collection = {
    "id": COLLECTION_ID,
    "type": "Collection",
    "title": "Thesaurus descriptors",
    "description": "Controlled-vocabulary records; no spatial component.",
    "keywords": ["thesaurus", "records", "metadata"],
    "license": "CC-BY-4.0",
}
r = client.post(f"/catalogs/{CATALOG_ID}/collections", content=json.dumps(rec_collection))
# Falls back to STAC's collection-creation if records-specific route is absent.
if r.status_code >= 400:
    r = client.post(
        f"/stac/catalogs/{CATALOG_ID}/collections",
        content=json.dumps({**rec_collection, "stac_version": "1.1.0", "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}}, "links": []}),
    )
assert r.status_code in (200, 201), r.text[:400]
print("Collection created")
"""))

    cells.append(code("""
# Step 2 — Pin the storage driver config to RECORDS mode.
# This is what removes geometry from the pipeline for this collection.

driver_cfg = {
    "class_key": "ItemsPostgresqlDriverConfig",
    "collection_type": "RECORDS",
    "sidecars": [
        {"sidecar_type": "item_metadata"},
        {"sidecar_type": "attributes"},
    ],
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/ItemsPostgresqlDriverConfig",
    content=json.dumps(driver_cfg),
)
assert r.status_code in (200, 201, 204), r.text[:400]
print("Driver config pinned: collection_type=RECORDS")
"""))

    cells.append(md(
        "## Step 2 — Route WRITE + READ through PostgreSQL\n",
        "A minimal routing config: PG for both WRITE and READ, no fan-out, no INDEX, no BACKUP.\n"
        "Notice the use of the canonical class name `ItemsPostgresqlDriver` (post-Phase-1 rename)\n"
        "— legacy routing entries with `CollectionPostgresqlDriver` still validate through the\n"
        "`config_rewriter` primitive, but new deployments pin to the canonical name.",
    ))
    cells.append(code("""
routing_cfg = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [{"driver_id": "ItemsPostgresqlDriver", "on_failure": "fatal"}],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig",
    content=json.dumps(routing_cfg),
)
assert r.status_code in (200, 201, 204), r.text[:400]
print("Routing pinned: WRITE/READ → ItemsPostgresqlDriver")
"""))

    cells.append(md(
        "## Step 3 — Write a few records\n",
        "Records are `Feature(geometry=None, properties={…})` as far as the storage pipeline\n"
        "is concerned.  The PG driver skips the geometry sidecar for RECORDS collections,\n"
        "so the insert path only touches the hub + item_metadata + attributes tables.",
    ))
    cells.append(code("""
records = [
    {
        "type": "Feature",
        "id": f"desc-{i:03d}",
        "geometry": None,
        "properties": {
            "title": f"Descriptor {i}",
            "description": "Example thesaurus entry",
            "broader": f"root-cat-{i % 3}",
            "language": "en",
            "keyword_count": i,
        },
    }
    for i in range(1, 6)
]
payload = {"type": "FeatureCollection", "features": records}
r = client.post(
    f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
    content=json.dumps(payload),
)
assert r.status_code in (200, 201, 204), r.text[:400]
print(f"Inserted {len(records)} records")
"""))

    cells.append(md(
        "## Step 4 — Read back via `/records` (no STAC envelope)\n",
        "The Records extension returns OGC API — Records resource shapes (`type: Record`),\n"
        "which carry only the OGC Records core + record-core conformance classes; no STAC\n"
        "`assets`, no `stac_extensions`.",
    ))
    cells.append(code("""
r = client.get(f"/records/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items?limit=3")
if r.status_code == 404:
    # Some deployments only expose records endpoints under a different prefix.
    r = client.get(f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items?limit=3")
assert r.status_code == 200, r.text[:400]
out = r.json()
print(f"Returned {len(out.get('features', []))} records; first:")
print(json.dumps(out.get("features", [])[:1], indent=2)[:1000])
"""))

    cells.append(md(
        "## Teardown\n",
        "Keeps the per-collection configs in place but removes the ephemeral catalog\n"
        "(and its collections) so re-running the notebook starts clean.",
    ))
    cells.append(code("""
for path in [
    f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
    f"/stac/catalogs/{CATALOG_ID}",
]:
    r = client.delete(path)
    print(f"DELETE {path} → {r.status_code}")
client.close()
"""))

    return cells


def nb_routing_pg_es_parquet() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []

    cells.append(md(
        "# Routing — 01: PG primary · ES INDEX · Parquet BACKUP\n",
        "**Persona:** Platform engineer wiring a multi-sink ingestion pipeline.\n\n",
        "**Goal:** Demonstrate the role-based driver refactor's routing composition:\n"
        "- PostgreSQL as the **Primary** for WRITE and READ (source of truth).\n"
        "- Elasticsearch as an **INDEX** target — async search/mirror, fed the raw envelope.\n"
        "- DuckDB (or the file-sink of choice) as a **BACKUP** target with `fmt=parquet`.\n\n"
        "All three live under a single `CollectionRoutingConfig`.  INDEX and BACKUP\n"
        "are declared under `metadata.operations` so the ReindexWorker / backup endpoints\n"
        "pick them up without blocking the synchronous WRITE path.\n",
        "## Architecture\n",
        "```\n"
        "           POST /items\n"
        "               │\n"
        "        ┌──────▼──────┐\n"
        "        │   Router    │\n"
        "        └──────┬──────┘\n"
        "               │ WRITE (sync)\n"
        "        ┌──────▼──────┐\n"
        "        │  PG primary │ ───────► source of truth (READ also here)\n"
        "        └──────┬──────┘\n"
        "               │ post-commit notify\n"
        "       ┌───────┴───────┐\n"
        "       ▼               ▼\n"
        "  INDEX (ES)     BACKUP (Parquet)\n"
        "   async           async\n"
        "```\n",
    ))
    cells.append(code(BOOTSTRAP_IMPORTS + """
CATALOG_ID    = f"rtng01-{_uuid.uuid4().hex[:8]}"
COLLECTION_ID = "events"
print(f"Using catalog={CATALOG_ID} collection={COLLECTION_ID}")
"""))

    cells.append(code("""
# Bootstrap: create the ephemeral catalog+collection.

catalog = {"id": CATALOG_ID, "type": "Catalog", "title": "Routing demo", "description": "PG+ES+Parquet fan-out example", "stac_version": "1.1.0", "conformsTo": [], "links": []}
for _ in range(3):
    r = client.post("/stac/catalogs", content=json.dumps(catalog))
    if r.status_code in (200, 201, 409):
        break
assert r.status_code in (200, 201, 409), r.text[:400]

collection = {
    "id": COLLECTION_ID, "type": "Collection",
    "title": "Event stream", "description": "Ingest events fanned out to ES + Parquet",
    "stac_version": "1.1.0",
    "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
    "license": "proprietary", "keywords": ["events"], "links": [],
}
r = client.post(
    f"/stac/catalogs/{CATALOG_ID}/collections",
    content=json.dumps(collection),
)
assert r.status_code in (200, 201, 409), r.text[:400]
print("Catalog + collection ready")
"""))

    cells.append(md(
        "## Step 1 — Pin the PostgreSQL driver as the primary\n",
        "Default `ItemsPostgresqlDriverConfig` — attribute + geometry sidecars are\n"
        "injected automatically via the registry (no explicit `sidecars` needed for\n"
        "default VECTOR collections).",
    ))
    cells.append(code("""
pg_cfg = {
    "class_key": "ItemsPostgresqlDriverConfig",
    # default VECTOR collection → geometry + attributes sidecars auto-injected
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/ItemsPostgresqlDriverConfig",
    content=json.dumps(pg_cfg),
)
assert r.status_code in (200, 201, 204), r.text[:400]
"""))

    cells.append(md(
        "## Step 2 — Wire ES (obfuscated) as an INDEX target\n",
        "The Elasticsearch obfuscated driver (`ItemsElasticsearchObfuscatedDriver`) is a\n"
        "search-tier mirror.  Registering it under `metadata.operations.INDEX` makes the\n"
        "ReindexWorker fan out on post-commit; `transformed=false` means the raw envelope\n"
        "(PG's primary shape) is forwarded — no TRANSFORM chain is applied.",
    ))
    cells.append(code("""
es_cfg = {
    "class_key": "ItemsElasticsearchObfuscatedDriverConfig",
    "target": {"index_prefix": "rtng-"},
}
# Many deployments accept the PUT even when the driver module isn't loaded; the
# _validate_routing_entries apply-handler catches that, with a clearer 400 than
# the raw "module missing" error.
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/ItemsElasticsearchObfuscatedDriverConfig",
    content=json.dumps(es_cfg),
)
print(f"ES config PUT → {r.status_code}")
"""))

    cells.append(md(
        "## Step 3 — Wire DuckDB as a BACKUP target emitting Parquet\n",
        "DuckDB's BACKUP-role behaviour uses its `EXPORT` capability.  The routing entry\n"
        "declares `fmt=\"parquet\"` so ingestion events carry the sink into the backup\n"
        "endpoint (`GET .../backup?format=parquet`).",
    ))
    cells.append(code("""
duck_cfg = {
    "class_key": "ItemsDuckdbDriverConfig",
    "warehouse": "/tmp/duckdb-backups",
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/ItemsDuckdbDriverConfig",
    content=json.dumps(duck_cfg),
)
print(f"DuckDB config PUT → {r.status_code}")
"""))

    cells.append(md(
        "## Step 4 — Build the composite routing config\n",
        "One `CollectionRoutingConfig` ties it all together.  `operations` covers the\n"
        "top-level WRITE/READ path; `metadata.operations` carries the async INDEX / BACKUP\n"
        "entries.  Fan-out is async-by-role: the ReindexWorker consumes INDEX entries and\n"
        "the backup endpoint consumes BACKUP entries.\n",
    ))
    cells.append(code("""
routing_cfg = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [{"driver_id": "ItemsPostgresqlDriver", "on_failure": "fatal"}],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
    "metadata": {
        "operations": {
            # INDEX — search mirror; transformed=False means "feed the raw envelope"
            "INDEX": [{
                "driver_id": "ItemsElasticsearchObfuscatedDriver",
                "transformed": False,
                "on_failure": "warn",
            }],
            # BACKUP — file-sink; fmt binds the sink to the ?format=parquet query.
            "BACKUP": [{
                "driver_id": "ItemsDuckdbDriver",
                "transformed": False,
                "fmt": "parquet",
                "on_failure": "warn",
            }],
        },
    },
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig",
    content=json.dumps(routing_cfg),
)
assert r.status_code in (200, 201, 204), r.text[:400]
print("Routing configured: PG primary · ES INDEX · DuckDB BACKUP(fmt=parquet)")
"""))

    cells.append(md(
        "## Step 5 — Verify effective routing (read the resolved config)\n",
        "The config waterfall merges platform → catalog → collection — verifying the\n"
        "collection scope resolves correctly is a sanity check before pushing data.",
    ))
    cells.append(code("""
r = client.get(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig/effective"
)
assert r.status_code == 200, r.text[:400]
effective = r.json()
print(json.dumps(effective, indent=2)[:1200])
"""))

    cells.append(md(
        "## Step 6 — Write a feature; observe WRITE returns immediately after PG commits\n",
        "INDEX and BACKUP are async — the user request returns on PG success and the\n"
        "other sinks catch up on the ReindexWorker / backup endpoint read, respectively.\n",
    ))
    cells.append(code("""
feat = {
    "type": "Feature",
    "id": f"evt-{_uuid.uuid4().hex[:8]}",
    "geometry": {"type": "Point", "coordinates": [12.4924, 41.8902]},  # Rome
    "properties": {"event_type": "notebook_demo", "severity": "info"},
}
r = client.post(
    f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
    content=json.dumps({"type": "FeatureCollection", "features": [feat]}),
)
assert r.status_code in (200, 201, 204), r.text[:400]
print(f"WRITE → {r.status_code}; PG commit done (ES/DuckDB propagation is async)")
"""))

    cells.append(md(
        "## Teardown",
    ))
    cells.append(code("""
r = client.delete(f"/stac/catalogs/{CATALOG_ID}")
print(f"DELETE catalog → {r.status_code}")
client.close()
"""))

    return cells


def nb_routing_corner_cases() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []

    cells.append(md(
        "# Routing — 02: Corner cases (failure policies, hints, TRANSFORM chain, BQ reporter)\n",
        "**Persona:** Operations engineer debugging multi-driver writes.\n\n",
        "Four independent recipes in one notebook:\n\n"
        "1. **`on_failure` semantics** — `fatal` vs `warn` on a fan-out WRITE.\n"
        "2. **Routing hints** — filter a WRITE fan-out down to one driver using `hints`.\n"
        "3. **TRANSFORM chain** — enrich READ output by chaining an asset-stats driver after PG.\n"
        "4. **BigQuery reporter** — `ItemsBigQueryDriver` as an async ingestion mirror\n"
        "   using the new Phase-3 reporter mode (`flat` / `batch_summary`).\n",
        "All recipes share the bootstrap cell below; each section is runnable standalone\n"
        "once the catalog+collection exist.",
    ))
    cells.append(code(BOOTSTRAP_IMPORTS + """
CATALOG_ID    = f"rtng02-{_uuid.uuid4().hex[:8]}"
COLLECTION_ID = "sensors"
print(f"Using catalog={CATALOG_ID} collection={COLLECTION_ID}")

# Minimal catalog+collection setup (no driver config yet — recipes pin their own)
catalog = {"id": CATALOG_ID, "type": "Catalog", "title": "Routing corner cases",
           "description": "fatal/warn, hints, TRANSFORM, BQ reporter",
           "stac_version": "1.1.0", "conformsTo": [], "links": []}
for _ in range(3):
    r = client.post("/stac/catalogs", content=json.dumps(catalog))
    if r.status_code in (200, 201, 409):
        break
collection = {"id": COLLECTION_ID, "type": "Collection", "title": "Sensors",
              "description": "Demo", "stac_version": "1.1.0",
              "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]},
                         "temporal": {"interval": [[None, None]]}},
              "license": "proprietary", "keywords": [], "links": []}
r = client.post(f"/stac/catalogs/{CATALOG_ID}/collections", content=json.dumps(collection))
assert r.status_code in (200, 201, 409), r.text[:400]
print("Catalog + collection ready")
"""))

    # --- Recipe 1: on_failure semantics -----------------------------------
    cells.append(md(
        "## Recipe 1 — `on_failure`: fatal vs warn\n",
        "Two drivers on the WRITE path, second one is known-misconfigured.\n"
        "- `fatal` first-driver failure = whole WRITE aborts.\n"
        "- `warn` second-driver failure = logs + continues; first-driver commit holds.",
    ))
    cells.append(code("""
# Routing: PG primary (fatal) + DuckDB with an unreachable warehouse (warn).
# The unreachable path makes DuckDB write fail — but the WRITE succeeds overall
# because DuckDB is pinned as on_failure=warn.

routing = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [
            {"driver_id": "ItemsPostgresqlDriver", "on_failure": "fatal"},
            {"driver_id": "ItemsDuckdbDriver",     "on_failure": "warn"},
        ],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
}
duck = {"class_key": "ItemsDuckdbDriverConfig", "warehouse": "/nonexistent/disallowed"}
for (cls_key, body) in [("ItemsPostgresqlDriverConfig", {"class_key": "ItemsPostgresqlDriverConfig"}),
                        ("ItemsDuckdbDriverConfig", duck),
                        ("CollectionRoutingConfig", routing)]:
    r = client.put(
        f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/{cls_key}",
        content=json.dumps(body),
    )
    print(f"PUT {cls_key} → {r.status_code}")
"""))
    cells.append(code("""
feat = {"type": "Feature", "id": f"s-{_uuid.uuid4().hex[:6]}", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {"kind": "demo"}}
r = client.post(f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items", content=json.dumps({"type": "FeatureCollection", "features": [feat]}))
# Expected: 2xx — PG commits, DuckDB fails-and-warns, WRITE returns success.
print(f"fan-out WRITE (warn) → {r.status_code}")
"""))

    # --- Recipe 2: hints --------------------------------------------------
    cells.append(md(
        "## Recipe 2 — Routing hints: filter fan-out to a single driver\n",
        "Two drivers registered; a hint on the POST narrows WRITE to one of them.\n"
        "Useful for replays / targeted re-indexing without touching the other sinks.",
    ))
    cells.append(code("""
routing = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [
            {"driver_id": "ItemsPostgresqlDriver", "hints": ["features"]},
            {"driver_id": "ItemsElasticsearchObfuscatedDriver", "hints": ["obfuscated"], "on_failure": "warn"},
        ],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig",
    content=json.dumps(routing),
)
print(f"routing put → {r.status_code}")

# Send a write hinting only "features" → PG only, ES skipped.
headers_hint = {**headers, "X-Dynastore-Hint": "features"}
c2 = httpx.Client(base_url=BASE_URL, headers=headers_hint, timeout=60.0)
r = c2.post(
    f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
    content=json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "id": "h-1", "geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {}}
    ]}),
)
c2.close()
print(f"hinted WRITE (features) → {r.status_code}")
"""))

    # --- Recipe 3: TRANSFORM chain ---------------------------------------
    cells.append(md(
        "## Recipe 3 — TRANSFORM chain on READ\n",
        "`metadata.operations.TRANSFORM` is an ordered list of drivers that enrich the\n"
        "collection descriptor at READ time.  AssetPostgresqlDriver is a natural fit —\n"
        "it publishes derived summaries (asset counts, last ingestion timestamp) into\n"
        "the collection envelope that MetadataPostgresqlDriver would otherwise miss.",
    ))
    cells.append(code("""
routing_with_transform = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [{"driver_id": "ItemsPostgresqlDriver", "on_failure": "fatal"}],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
    "metadata": {
        "operations": {
            "READ":      [{"driver_id": "MetadataPostgresqlDriver"}],
            "TRANSFORM": [{"driver_id": "AssetPostgresqlDriver"}],
        },
    },
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig",
    content=json.dumps(routing_with_transform),
)
print(f"TRANSFORM-chain routing put → {r.status_code}")

# A GET /collections/{id} should now include asset-derived summaries in the
# envelope if any assets are registered for the collection.
r = client.get(f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}")
print(f"collection GET → {r.status_code}")
print(json.dumps(r.json(), indent=2)[:800])
"""))

    # --- Recipe 4: BigQuery reporter --------------------------------------
    cells.append(md(
        "## Recipe 4 — BigQuery reporter (Phase 3 WRITE capability)\n",
        "`ItemsBigQueryDriver` now implements WRITE in three modes:\n"
        "- `off` (default) — WRITE is a no-op even when pinned in a routing config.\n"
        "- `flat`          — one BQ row per feature (entity_id, payload, ingested_at).\n"
        "- `batch_summary` — one BQ row per write call (row_count, first/last id).\n\n"
        "Partial/complete failures are logged as warnings; the PG primary WRITE is\n"
        "unaffected.  Use `on_failure=warn` on the BQ entry.",
    ))
    cells.append(code("""
# 4a. Set the reporter config.
bq_cfg = {
    "class_key": "ItemsBigQueryDriverConfig",
    "target":         {"project_id": "demo-project", "dataset_id": "catalog", "table_name": "ingest_mirror"},
    "report_target":  {"project_id": "demo-project", "dataset_id": "catalog", "table_name": "ingest_mirror"},
    "reporter_mode":  "flat",
    "include_payload": True,
    "exclude_fields": ["secret_key"],
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/ItemsBigQueryDriverConfig",
    content=json.dumps(bq_cfg),
)
print(f"BQ reporter config put → {r.status_code}")

# 4b. Pin the routing with BQ as a warn-level WRITE fan-out target.
routing_bq = {
    "class_key": "CollectionRoutingConfig",
    "operations": {
        "WRITE": [
            {"driver_id": "ItemsPostgresqlDriver", "on_failure": "fatal"},
            {"driver_id": "ItemsBigQueryDriver",   "on_failure": "warn"},
        ],
        "READ":  [{"driver_id": "ItemsPostgresqlDriver"}],
    },
}
r = client.put(
    f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/configs/CollectionRoutingConfig",
    content=json.dumps(routing_bq),
)
print(f"routing (BQ reporter) put → {r.status_code}")
"""))
    cells.append(code("""
# 4c. Write a feature — PG commits synchronously; BQ reporter fires (best-effort).
# If BQ is not reachable from this environment (no credentials), the warn-level
# entry logs on the server side and does NOT abort the PG commit.

feat = {"type": "Feature", "id": f"bq-{_uuid.uuid4().hex[:6]}",
        "geometry": {"type": "Point", "coordinates": [10, 20]},
        "properties": {"sensor": "rain", "reading": 12.5, "secret_key": "SHOULD-NOT-APPEAR"}}
r = client.post(
    f"/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
    content=json.dumps({"type": "FeatureCollection", "features": [feat]}),
)
print(f"WRITE (fan-out) → {r.status_code}")
print("If BigQueryService is unregistered or BQ returns errors, check the server logs")
print("for 'BQ reporter WRITE' entries — the WRITE still returns 2xx thanks to on_failure=warn.")
"""))

    cells.append(md(
        "## Teardown",
    ))
    cells.append(code("""
r = client.delete(f"/stac/catalogs/{CATALOG_ID}")
print(f"DELETE catalog → {r.status_code}")
client.close()
"""))

    return cells


# ---------------------------------------------------------------------------
# Write the three notebooks
# ---------------------------------------------------------------------------


def main() -> None:
    notebooks_dir = os.path.join(HERE)
    write_nb(
        os.path.join(notebooks_dir, "records", "01_records_collection_no_stac.ipynb"),
        nb_records_no_stac(),
    )
    write_nb(
        os.path.join(notebooks_dir, "routing", "01_pg_primary_es_index_parquet_backup.ipynb"),
        nb_routing_pg_es_parquet(),
    )
    write_nb(
        os.path.join(notebooks_dir, "routing", "02_corner_cases_failure_policies_and_hints.ipynb"),
        nb_routing_corner_cases(),
    )


if __name__ == "__main__":
    main()
