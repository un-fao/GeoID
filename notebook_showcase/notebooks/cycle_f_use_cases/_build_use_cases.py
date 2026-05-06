"""Generator for the four cycle_f_use_cases notebooks.

Run once; the emitted .ipynb files are the source of record. Keep this
file for future edits — plain-Python cell content is much easier to
review than raw .ipynb JSON.

Pattern matches storage_drivers/_build_nb04_engines_multi_instance.py
and ui_walkthrough notebooks: env-var-driven base URL + optional Bearer
token, ephemeral catalog id keyed off RUN_ID, teardown at the end.
"""

from __future__ import annotations

import json
import os
import textwrap
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))


def md(*chunks: str) -> Dict[str, Any]:
    body = "".join(chunks)
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": body.splitlines(keepends=True),
    }


def code(body: str) -> Dict[str, Any]:
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
# Shared cells (header + bootstrap + helpers)
# ---------------------------------------------------------------------------


def _bootstrap_cell(uc_short: str) -> Dict[str, Any]:
    return code(f"""
        import json
        import os
        import time
        import uuid

        import httpx
        from dotenv import load_dotenv

        load_dotenv()

        BASE_URL = os.environ.get("DYNASTORE_BASE_URL", "http://localhost:8080")
        TOKEN = (
            os.environ.get("DYNASTORE_TOKEN")
            or os.environ.get("DYNASTORE_SYSADMIN_TOKEN")
            or os.environ.get("DYNASTORE_ADMIN_TOKEN")
            or ""
        )
        RUN_ID = os.environ.get("RUN_ID", uuid.uuid4().hex[:8])
        CATALOG_ID = os.environ.get("CATALOG_ID", f"cf_{uc_short}_{{RUN_ID}}")
        COLLECTION_ID = os.environ.get("COLLECTION_ID", f"col_{{RUN_ID}}")

        IS_LOCAL = "localhost" in BASE_URL or "127.0.0.1" in BASE_URL

        headers = {{"Content-Type": "application/json"}}
        if TOKEN:
            headers["Authorization"] = f"Bearer {{TOKEN}}"

        client = httpx.Client(base_url=BASE_URL, headers=headers, timeout=120.0)

        print(f"BASE_URL      : {{BASE_URL}}")
        print(f"CATALOG_ID    : {{CATALOG_ID}}")
        print(f"COLLECTION_ID : {{COLLECTION_ID}}")
        print(f"AUTH          : {{'token set' if TOKEN else 'anonymous'}}")
        if not TOKEN:
            print("\\nWARNING: no Bearer token set — config writes will 401.")
            print("Set DYNASTORE_TOKEN before running write cells.")
        """)


def _create_catalog_collection_cell() -> Dict[str, Any]:
    return code("""
        # Create catalog (idempotent: 409 = already exists)
        catalog_payload = {
            "id": CATALOG_ID,
            "type": "Catalog",
            "title": f"Cycle F UC walkthrough {RUN_ID}",
            "description": "Ephemeral catalog for cycle_f_use_cases notebook.",
            "stac_version": "1.0.0",
        }
        r = client.post("/stac/catalogs", content=json.dumps(catalog_payload))
        if r.status_code in (200, 201):
            print(f"Catalog '{CATALOG_ID}' created.")
        elif r.status_code == 409:
            print(f"Catalog '{CATALOG_ID}' already exists.")
        else:
            raise RuntimeError(f"Catalog create failed: {r.status_code}: {r.text}")

        # Create collection (idempotent)
        collection_payload = {
            "id": COLLECTION_ID,
            "type": "Collection",
            "stac_version": "1.0.0",
            "title": f"UC collection {RUN_ID}",
            "description": "Walkthrough collection — defaults inherited then PATCHed.",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
            "links": [],
        }
        r = client.post(
            f"/stac/catalogs/{CATALOG_ID}/collections",
            content=json.dumps(collection_payload),
        )
        if r.status_code in (200, 201):
            print(f"Collection '{COLLECTION_ID}' created.")
        elif r.status_code == 409:
            print(f"Collection '{COLLECTION_ID}' already exists.")
        else:
            raise RuntimeError(f"Collection create failed: {r.status_code}: {r.text}")
        """)


def _show_delta_helper_cell() -> Dict[str, Any]:
    return code("""
        # Helper — show explicit-vs-effective config delta for a plugin
        def show_config_delta(plugin_id: str, level: str = "collection") -> None:
            if level == "collection":
                base = f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}"
            elif level == "catalog":
                base = f"/configs/catalogs/{CATALOG_ID}"
            else:
                raise ValueError(level)
            rx = client.get(f"{base}/plugins/{plugin_id}")
            re_ = client.get(f"{base}/plugins/{plugin_id}/effective")
            print(f"\\n=== {plugin_id} @ {level} ===")
            print("EXPLICIT:")
            if rx.status_code == 200:
                print(json.dumps(rx.json(), indent=2)[:600])
            else:
                print(f"  ({rx.status_code} — none stored, every field inherited)")
            if re_.status_code != 200:
                print(f"  effective unavailable ({re_.status_code})")
                return
            eff = re_.json()
            resolved = eff.get("resolved", eff.get("config", {}))
            sources = eff.get("sources", {})
            print("EFFECTIVE (resolved + per-field source):")
            for field in sorted(resolved.keys()):
                val = resolved[field]
                src = sources.get(field, "?")
                vs = json.dumps(val) if not isinstance(val, str) else val
                if len(str(vs)) > 70:
                    vs = str(vs)[:67] + "..."
                marker = "*" if src == level else " "
                print(f"  {marker} {field:<30} {vs:<60} [{src}]")
        """)


def _put_helper_cell() -> Dict[str, Any]:
    return code("""
        def put_config(plugin_id: str, body: dict, level: str = "collection") -> None:
            if level == "collection":
                url = f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/plugins/{plugin_id}"
            else:
                url = f"/configs/catalogs/{CATALOG_ID}/plugins/{plugin_id}"
            r = client.put(url, content=json.dumps(body), timeout=60.0)
            print(f"PUT {plugin_id}: {r.status_code}")
            if r.status_code not in (200, 201, 204):
                print(f"  body: {r.text[:300]}")
                if r.status_code == 401:
                    raise RuntimeError("Unauthorized — set DYNASTORE_TOKEN.")
                raise RuntimeError(f"PUT failed: {r.status_code}")
        """)


def _teardown_cell() -> Dict[str, Any]:
    return code("""
        # Teardown — delete the ephemeral catalog. Comment out to keep state.
        r = client.delete(
            f"/stac/catalogs/{CATALOG_ID}",
            params={"force": "true"},
            timeout=60.0,
        )
        print(f"teardown DELETE /stac/catalogs/{CATALOG_ID}: {r.status_code}")
        client.close()
        """)


# ---------------------------------------------------------------------------
# UC1 — PG with 4 sidecars + multi-version + dual-search (ES public + PG)
# ---------------------------------------------------------------------------


def nb_uc1() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []
    cells.append(md(
        "# UC1 — PG with all 4 sidecars + multi-version + dual-search\n",
        "\n",
        "**Persona:** platform builder ingesting versioned vector features.\n",
        "\n",
        "**Goal:** stand up a collection that:\n",
        "1. stores items on PostgreSQL with all four sidecars (`geometries`,\n",
        "   `attributes`, `item_metadata`, `stac_metadata`) — full default surface;\n",
        "2. enables 2D geometry statistics (area, length, centroid) as columns,\n",
        "   indexed and exposed on the feature type;\n",
        "3. uses an `ItemsWritePolicy` keyed on `properties.code` so re-ingesting\n",
        "   the same `code` from a different asset creates a **new version** rather\n",
        "   than overwriting (`asset_id` tracking is enabled);\n",
        "4. routes WRITE to PG (fatal) + ES public (async) and SEARCH/READ to\n",
        "   ES public with `geometry_simplified` + PG with `geometry_exact`,\n",
        "   so callers can pick approximate-fast vs exact-slow at query time.\n",
        "\n",
        "**Critical:** `enable_validity=true` is required for `on_conflict=new_version`\n",
        "to actually create version rows; without it the policy silently degrades to\n",
        "`update`.  We set it explicitly.\n",
        "\n",
        "**Critical:** sidecars are immutable post-creation — the four-sidecar set\n",
        "below MUST be applied before any feature is ingested.\n",
    ))
    cells.append(_bootstrap_cell("uc1"))
    cells.append(_create_catalog_collection_cell())
    cells.append(_show_delta_helper_cell())
    cells.append(_put_helper_cell())

    cells.append(md(
        "## Step 1 — PATCH `items_postgresql_driver_config` with all 4 sidecars\n",
        "\n",
        "Sidecars discriminated by `sidecar_type`.  The `geometries` sidecar's\n",
        "`statistics` block defaults to area/length/centroid all `enabled=true,\n",
        "index=true`; we set it explicitly here for documentation value.\n",
    ))
    cells.append(code("""
        items_pg_driver = {
            "engine_ref": "postgresql_engine_config",
            "sidecars": [
                {
                    "sidecar_type": "geometries",
                    "target_srid": 4326,
                    "target_dimension": "FORCE_2D",
                    "geom_column": "geom",
                    "bbox_column": "bbox_geom",
                    "geohash_precision": 9,
                    "store_bbox": True,
                    "statistics": {
                        "enabled": True,
                        "storage_mode": "COLUMNAR",
                        "area":     {"enabled": True, "index": True},
                        "length":   {"enabled": True, "index": True},
                        "centroid_type": "geometric",
                        "index_centroid": True,
                    },
                },
                {
                    "sidecar_type": "attributes",
                    "storage_mode": "AUTOMATIC",
                    "enable_external_id": True,
                    "external_id_field": "properties.code",
                    "index_external_id": True,
                    "enable_asset_id": True,
                    "index_asset_id": True,
                    "enable_validity": True,
                },
                {"sidecar_type": "item_metadata"},
                {"sidecar_type": "stac_metadata"},
            ],
        }
        put_config("items_postgresql_driver_config", items_pg_driver)
        show_config_delta("items_postgresql_driver_config")
        """))

    cells.append(md(
        "## Step 2 — PATCH `items_write_policy` for multi-version on `code`\n",
    ))
    cells.append(code("""
        write_policy = {
            "on_conflict": "new_version",
            "identity_matchers": ["external_id"],
            "external_id_field": "properties.code",
            "track_asset_id": True,
            "enable_validity": True,
        }
        put_config("items_write_policy", write_policy)
        show_config_delta("items_write_policy")
        """))

    cells.append(md(
        "## Step 3 — PATCH `items_routing_config` for dual SEARCH dispatch\n",
        "\n",
        "WRITE → PG (fatal, primary) + ES public (async, async outbox).  \n",
        "SEARCH/READ → ES public with `geometry_simplified` (warn) + PG with\n",
        "`geometry_exact` (fatal).  Caller chooses via `?hint=…`.\n",
    ))
    cells.append(code("""
        routing = {
            "operations": {
                "WRITE": [
                    {"driver_ref": "items_postgresql_driver",     "on_failure": "fatal"},
                    {"driver_ref": "items_elasticsearch_driver",  "write_mode": "async", "on_failure": "outbox"},
                ],
                "READ": [
                    {"driver_ref": "items_elasticsearch_driver",  "hints": ["geometry_simplified"], "on_failure": "warn"},
                    {"driver_ref": "items_postgresql_driver",     "hints": ["geometry_exact"],     "on_failure": "fatal"},
                ],
                "SEARCH": [
                    {"driver_ref": "items_elasticsearch_driver",  "hints": ["geometry_simplified"], "on_failure": "warn"},
                    {"driver_ref": "items_postgresql_driver",     "hints": ["geometry_exact"],     "on_failure": "fatal"},
                ],
            },
        }
        put_config("items_routing_config", routing)
        show_config_delta("items_routing_config")
        """))

    cells.append(md(
        "## Step 4 — Confirm slim view shows everything\n",
        "\n",
        "`strict=true&resolved=true` against the collection scope returns only\n",
        "the configs owned at this scope (the four PUTs we just made).\n",
    ))
    cells.append(code("""
        r = client.get(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/",
            params={"strict": "true", "resolved": "true", "meta": "none"},
        )
        configs = r.json().get("configs", {})
        items = configs.get("platform", {}).get("catalog", {}).get("collection", {}).get("items", {})
        print("items.* keys:", list(items.keys()))
        for sub, val in items.items():
            print(f"  {sub}:", list(val.keys()) if isinstance(val, dict) else type(val).__name__)
        """))

    cells.append(md(
        "## Step 5 — Ingest sample features\n",
        "\n",
        "Three features with distinct `code` values; routing fans out to PG\n",
        "(synchronous) + ES (async, outbox-backed).\n",
    ))
    cells.append(code("""
        sample_features = []
        for i, code_val in enumerate(["AREA-001", "AREA-002", "AREA-003"]):
            sample_features.append({
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": f"feat-{i}-{RUN_ID}",
                "collection": COLLECTION_ID,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [12.4 + i*0.1, 41.85], [12.55 + i*0.1, 41.85],
                        [12.55 + i*0.1, 41.95], [12.4 + i*0.1, 41.95],
                        [12.4 + i*0.1, 41.85],
                    ]],
                },
                "bbox": [12.4 + i*0.1, 41.85, 12.55 + i*0.1, 41.95],
                "properties": {
                    "datetime": "2024-01-10T00:00:00Z",
                    "code": code_val,
                    "name": f"Demo region {code_val}",
                },
                "assets": {},
                "links": [],
            })

        ingest_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        for feat in sample_features:
            r = client.post(ingest_url, content=json.dumps(feat))
            print(f"  POST {feat['id']}: {r.status_code}")
            assert r.status_code in (200, 201, 207), f"ingest failed: {r.status_code} {r.text[:200]}"
        """))

    cells.append(md(
        "## Step 6 — Verify dual-search hint dispatch\n",
        "\n",
        "Same query with two different hints — geometries returned by ES are\n",
        "simplified (lower vertex count); PG returns exact stored polygons.\n",
    ))
    cells.append(code("""
        search_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        # Approximated path — ES with simplified geometry
        r_es = client.get(search_url, params={"limit": 5, "hint": "geometry_simplified"})
        print(f"SEARCH hint=geometry_simplified: HTTP {r_es.status_code}")
        es_features = r_es.json().get("features", []) if r_es.status_code == 200 else []
        print(f"  features returned: {len(es_features)}")

        # Exact path — PG with stored polygon
        r_pg = client.get(search_url, params={"limit": 5, "hint": "geometry_exact"})
        print(f"SEARCH hint=geometry_exact: HTTP {r_pg.status_code}")
        pg_features = r_pg.json().get("features", []) if r_pg.status_code == 200 else []
        print(f"  features returned: {len(pg_features)}")

        if es_features and pg_features:
            es_vertex_count = sum(len(c) for c in es_features[0]["geometry"]["coordinates"][0])
            pg_vertex_count = sum(len(c) for c in pg_features[0]["geometry"]["coordinates"][0])
            print(f"  vertex count: ES={es_vertex_count}, PG={pg_vertex_count}")
            print(f"  same? {es_vertex_count == pg_vertex_count} — equal is fine for tiny geoms,"
                  f" different proves hint dispatch")
        """))

    cells.append(md("## Teardown"))
    cells.append(_teardown_cell())
    return cells


# ---------------------------------------------------------------------------
# UC2 — Schema enforcement + multi-version from 2 assets
# ---------------------------------------------------------------------------


def nb_uc2() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []
    cells.append(md(
        "# UC2 — Schema enforcement + multi-version from 2 assets\n",
        "\n",
        "**Persona:** data ingestion pipeline owner enforcing input contracts.\n",
        "\n",
        "**Goal:**\n",
        "1. PATCH `items_schema` to declare `code` and `name` as mandatory.\n",
        "2. Ingest a feature missing `code` → expect **HTTP 207 Multi-Status**\n",
        "   with an `IngestionReport.rejections[*]` describing the missing field\n",
        "   and a `policy_source` URL pointing back to the schema config.\n",
        "3. Ingest features from **two different assets** with the same `code`\n",
        "   value; the `ItemsWritePolicy.on_conflict=new_version` plus\n",
        "   `enable_validity=true` create a new version row; the previous row's\n",
        "   `valid_to` is bounded.\n",
    ))
    cells.append(_bootstrap_cell("uc2"))
    cells.append(_create_catalog_collection_cell())
    cells.append(_show_delta_helper_cell())
    cells.append(_put_helper_cell())

    cells.append(md(
        "## Step 1 — PATCH driver with attributes sidecar (external_id_field)\n",
        "\n",
        "Sidecars before any rows; same immutability rule as UC1.\n",
    ))
    cells.append(code("""
        items_pg_driver = {
            "engine_ref": "postgresql_engine_config",
            "sidecars": [
                {"sidecar_type": "geometries"},
                {
                    "sidecar_type": "attributes",
                    "enable_external_id": True,
                    "external_id_field": "properties.code",
                    "index_external_id": True,
                    "enable_asset_id": True,
                    "enable_validity": True,
                },
            ],
        }
        put_config("items_postgresql_driver_config", items_pg_driver)
        """))

    cells.append(md("## Step 2 — PATCH `items_schema` with mandatory fields"))
    cells.append(code("""
        schema_patch = {
            "fields": [
                {"name": "code", "type": "text", "required": True},
                {"name": "name", "type": "text", "required": True},
                {"name": "description", "type": "text"},
            ],
            "strict_unknown_fields": False,
        }
        put_config("items_schema", schema_patch)
        show_config_delta("items_schema")
        """))

    cells.append(md("## Step 3 — PATCH `items_write_policy` for multi-version"))
    cells.append(code("""
        write_policy = {
            "on_conflict": "new_version",
            "identity_matchers": ["external_id"],
            "external_id_field": "properties.code",
            "track_asset_id": True,
            "enable_validity": True,
        }
        put_config("items_write_policy", write_policy)
        """))

    cells.append(md(
        "## Step 4 — Ingest a feature missing `code` → 207 Multi-Status\n",
        "\n",
        "Look at the `rejections` array — each entry includes `reason`,\n",
        "`message`, `matcher` (which sidecar matcher fired), and a\n",
        "`policy_source` URL pointing back to the schema config.\n",
    ))
    cells.append(code("""
        bad = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": f"bad-{RUN_ID}",
            "collection": COLLECTION_ID,
            "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
            "bbox": [12.5, 41.9, 12.5, 41.9],
            "properties": {
                "datetime": "2024-01-10T00:00:00Z",
                # NB: NO "code" — should be rejected
                "name": "Missing code field",
            },
            "assets": {},
            "links": [],
        }
        ingest_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        r = client.post(ingest_url, content=json.dumps(bad))
        print(f"POST bad feature: {r.status_code}")
        if r.status_code == 207:
            report = r.json()
            print("rejections:")
            for rej in report.get("rejections", []):
                print(f"  - reason={rej.get('reason')!r} matcher={rej.get('matcher')!r}")
                print(f"    message={rej.get('message')!r}")
                print(f"    policy_source={rej.get('policy_source')!r}")
        elif r.status_code in (400, 422):
            print(f"  body: {r.text[:400]}")
            print("  (single-feature ingest may surface 400/422 instead of 207)")
        else:
            print(f"  unexpected: {r.text[:300]}")
        """))

    cells.append(md(
        "## Step 5 — Ingest 2 features with same `code` from different assets\n",
        "\n",
        "First create two STAC assets (asset_id `pack-A` and `pack-B`).  Each\n",
        "asset contributes a feature with `properties.code = \"K42\"`.  The first\n",
        "ingest creates the row; the second triggers the `new_version` policy.\n",
    ))
    cells.append(code("""
        # Helper to upload a tiny GeoJSON asset blob
        def upload_asset(asset_id: str) -> str:
            url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/assets/{asset_id}"
            blob = {
                "type": "FeatureCollection",
                "features": [],
            }
            files = {"file": (f"{asset_id}.geojson", json.dumps(blob), "application/geo+json")}
            r = client.put(url, files=files, headers={k: v for k, v in client.headers.items()
                                                      if k.lower() != "content-type"})
            print(f"  asset PUT {asset_id}: {r.status_code}")
            return asset_id

        asset_a = upload_asset("pack-A")
        asset_b = upload_asset("pack-B")

        def ingest_with_asset(asset_id: str, idx: int) -> int:
            feat = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": f"k42-via-{asset_id}-{RUN_ID}",
                "collection": COLLECTION_ID,
                "geometry": {"type": "Point", "coordinates": [12.5 + 0.01*idx, 41.9]},
                "bbox": [12.5 + 0.01*idx, 41.9, 12.5 + 0.01*idx, 41.9],
                "properties": {
                    "datetime": "2024-01-10T00:00:00Z",
                    "code": "K42",
                    "name": f"From {asset_id}",
                    "asset_id": asset_id,
                },
                "assets": {},
                "links": [],
            }
            r = client.post(ingest_url, content=json.dumps(feat))
            print(f"  ingest via {asset_id}: {r.status_code}")
            return r.status_code

        s1 = ingest_with_asset(asset_a, 0)
        s2 = ingest_with_asset(asset_b, 1)

        # Read back items filtered by external_id="K42" — expect 2 rows when
        # the new_version policy created a version row for the second asset.
        search_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        r = client.get(search_url, params={"limit": 10, "external_id": "K42"})
        rows = r.json().get("features", []) if r.status_code == 200 else []
        print(f"\\nK42 versions found: {len(rows)}")
        for row in rows:
            props = row.get("properties", {})
            print(f"  geoid={row.get('id')} asset_id={props.get('asset_id')} valid_from={props.get('valid_from')} valid_to={props.get('valid_to')}")
        """))

    cells.append(md("## Teardown"))
    cells.append(_teardown_cell())
    return cells


# ---------------------------------------------------------------------------
# UC3 — PG + private ES + privacy verification
# ---------------------------------------------------------------------------


def nb_uc3() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []
    cells.append(md(
        "# UC3 — PG + private ES + dual-search + privacy probe\n",
        "\n",
        "**Persona:** publisher of restricted-access vector data.\n",
        "\n",
        "**Goal:**\n",
        "1. Configure WRITE to PG (primary) + private ES (async) — no public ES.\n",
        "2. Configure SEARCH/READ to private ES (`geometry_simplified`) + PG\n",
        "   (`geometry_exact`).\n",
        "3. Verify that **anonymous** SEARCH cannot leak items.  Authenticated\n",
        "   SEARCH returns features via either dispatch path.\n",
        "\n",
        "Private ES uses a separate index prefix governed by the\n",
        "`items_elasticsearch_private_driver` class — its routing entries are\n",
        "rejected by the privacy cascade if a public driver is added in the\n",
        "same collection (Cycle F.6 guard).\n",
    ))
    cells.append(_bootstrap_cell("uc3"))
    cells.append(_create_catalog_collection_cell())
    cells.append(_show_delta_helper_cell())
    cells.append(_put_helper_cell())

    cells.append(md(
        "## Step 1 — Mark the collection private\n",
        "\n",
        "`collection_privacy.is_private=true` triggers the Cycle F.6 privacy\n",
        "cascade — only private-class drivers may be referenced in routing.\n",
    ))
    cells.append(code("""
        put_config("collection_privacy", {"is_private": True})
        show_config_delta("collection_privacy")
        """))

    cells.append(md("## Step 2 — PATCH PG driver (sidecars: geometries + attributes)"))
    cells.append(code("""
        items_pg_driver = {
            "engine_ref": "postgresql_engine_config",
            "sidecars": [
                {"sidecar_type": "geometries"},
                {
                    "sidecar_type": "attributes",
                    "enable_external_id": True,
                    "external_id_field": "properties.code",
                    "enable_asset_id": True,
                },
            ],
        }
        put_config("items_postgresql_driver_config", items_pg_driver)
        """))

    cells.append(md("## Step 3 — PATCH routing for PG + private ES (no public ES)"))
    cells.append(code("""
        routing = {
            "operations": {
                "WRITE": [
                    {"driver_ref": "items_postgresql_driver",         "on_failure": "fatal"},
                    {"driver_ref": "items_elasticsearch_private_driver", "write_mode": "async", "on_failure": "outbox"},
                ],
                "READ": [
                    {"driver_ref": "items_elasticsearch_private_driver", "hints": ["geometry_simplified"], "on_failure": "warn"},
                    {"driver_ref": "items_postgresql_driver",         "hints": ["geometry_exact"],     "on_failure": "fatal"},
                ],
                "SEARCH": [
                    {"driver_ref": "items_elasticsearch_private_driver", "hints": ["geometry_simplified"], "on_failure": "warn"},
                    {"driver_ref": "items_postgresql_driver",         "hints": ["geometry_exact"],     "on_failure": "fatal"},
                ],
            },
        }
        put_config("items_routing_config", routing)
        show_config_delta("items_routing_config")
        """))

    cells.append(md(
        "## Step 4 — Confirm privacy cascade rejects a public-driver write\n",
        "\n",
        "Attempting to add `items_elasticsearch_driver` (public) to routing must\n",
        "fail with 422 because the collection is marked private.  This is the\n",
        "Cycle F.6 guard.\n",
    ))
    cells.append(code("""
        bad_routing = json.loads(json.dumps(routing))  # deep copy
        bad_routing["operations"]["WRITE"].append({
            "driver_ref": "items_elasticsearch_driver",  # public — should be rejected
            "write_mode": "async", "on_failure": "outbox",
        })
        url = f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/plugins/items_routing_config"
        r = client.put(url, content=json.dumps(bad_routing))
        print(f"PUT routing with public ES on private collection: {r.status_code}")
        print(f"  body: {r.text[:300]}")
        assert r.status_code in (422, 409, 400), \\
            f"Expected 422/409/400 from privacy cascade, got {r.status_code}"
        """))

    cells.append(md("## Step 5 — Ingest 3 features"))
    cells.append(code("""
        ingest_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        for i, code_val in enumerate(["P-001", "P-002", "P-003"]):
            feat = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": f"priv-{i}-{RUN_ID}",
                "collection": COLLECTION_ID,
                "geometry": {"type": "Point", "coordinates": [12.5 + i*0.05, 41.9]},
                "bbox": [12.5 + i*0.05, 41.9, 12.5 + i*0.05, 41.9],
                "properties": {"datetime": "2024-01-10T00:00:00Z", "code": code_val},
                "assets": {},
                "links": [],
            }
            r = client.post(ingest_url, content=json.dumps(feat))
            print(f"  ingest {code_val}: {r.status_code}")
        """))

    cells.append(md(
        "## Step 6 — Anonymous probe must NOT leak items\n",
        "\n",
        "Open a fresh client without the Bearer.  SEARCH should return 401/403\n",
        "(or an empty list) — never the actual rows.\n",
    ))
    cells.append(code("""
        anon = httpx.Client(base_url=BASE_URL, timeout=60.0)  # no auth
        search_url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
        r = anon.get(search_url, params={"limit": 5})
        print(f"anonymous SEARCH: {r.status_code}")
        print(f"  body: {r.text[:200]}")
        assert r.status_code in (401, 403, 404) or len(r.json().get("features", [])) == 0, \\
            f"PRIVACY LEAK: anonymous returned items! {r.text[:200]}"
        anon.close()
        """))

    cells.append(md("## Step 7 — Authenticated SEARCH returns via both paths"))
    cells.append(code("""
        if not TOKEN:
            print("(skipped: no token)")
        else:
            for hint in ["geometry_simplified", "geometry_exact"]:
                r = client.get(search_url, params={"limit": 5, "hint": hint})
                feats = r.json().get("features", []) if r.status_code == 200 else []
                print(f"  authenticated hint={hint}: HTTP {r.status_code} count={len(feats)}")
        """))

    cells.append(md("## Teardown"))
    cells.append(_teardown_cell())
    return cells


# ---------------------------------------------------------------------------
# UC4 — Asset duplicate refusal config round-trip
# ---------------------------------------------------------------------------


def nb_uc4() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []
    cells.append(md(
        "# UC4 — Asset duplicate refusal: config round-trip\n",
        "\n",
        "**Persona:** ingestion-pipeline operator who needs idempotent uploads\n",
        "by default but switches to fail-on-duplicate when a strict batch is\n",
        "running.\n",
        "\n",
        "**Goal:** demonstrate that `assets_write_policy.on_conflict` is a real\n",
        "config-API surface — not a baked-in default.  We toggle it through two\n",
        "PATCH cycles and observe both behaviors:\n",
        "\n",
        "1. PATCH `on_conflict=refuse_return` → re-uploading the same asset_id\n",
        "   returns 200 with the existing row (idempotent).\n",
        "2. PATCH back to `on_conflict=refuse_fail` (the default) → re-upload\n",
        "   returns **409 Conflict**.\n",
        "\n",
        "Both modes use `[asset_id, filename]` as the identity matcher chain.\n",
        "\n",
        "**Critical:** `assets_write_policy` is a **catalog-scope** config (not\n",
        "collection-scope) — it controls assets across every collection in the\n",
        "catalog.\n",
    ))
    cells.append(_bootstrap_cell("uc4"))
    cells.append(_create_catalog_collection_cell())
    cells.append(_show_delta_helper_cell())
    cells.append(_put_helper_cell())

    cells.append(md(
        "## Step 1 — PATCH catalog-scope `assets_write_policy` to `refuse_return`\n",
    ))
    cells.append(code("""
        relaxed_policy = {
            "on_conflict": "refuse_return",
            "identity_matchers": ["asset_id", "filename"],
        }
        put_config("assets_write_policy", relaxed_policy, level="catalog")
        show_config_delta("assets_write_policy", level="catalog")
        """))

    cells.append(md(
        "## Step 2 — Upload + re-upload under `refuse_return`\n",
        "\n",
        "First upload: 201 Created.  Second upload of the same id: 200 OK with\n",
        "the existing row's metadata (idempotent — no error).\n",
    ))
    cells.append(code("""
        ASSET_ID = "feature-pack-A"
        url = f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/assets/{ASSET_ID}"
        blob = {"type": "FeatureCollection", "features": []}
        files_form = lambda: {"file": (f"{ASSET_ID}.geojson", json.dumps(blob), "application/geo+json")}

        r1 = client.put(url, files=files_form(),
                        headers={k: v for k, v in client.headers.items() if k.lower() != "content-type"})
        print(f"first upload : {r1.status_code} — {r1.text[:200]}")
        assert r1.status_code in (200, 201), f"first upload failed: {r1.status_code}"

        r2 = client.put(url, files=files_form(),
                        headers={k: v for k, v in client.headers.items() if k.lower() != "content-type"})
        print(f"second upload: {r2.status_code} — {r2.text[:200]}")
        assert r2.status_code in (200, 201), f"refuse_return should be idempotent (200), got {r2.status_code}"
        print("  idempotent: refuse_return returned existing row, no error.")
        """))

    cells.append(md(
        "## Step 3 — PATCH back to `refuse_fail` (the default)\n",
    ))
    cells.append(code("""
        strict_policy = {
            "on_conflict": "refuse_fail",
            "identity_matchers": ["asset_id", "filename"],
        }
        put_config("assets_write_policy", strict_policy, level="catalog")
        show_config_delta("assets_write_policy", level="catalog")
        """))

    cells.append(md(
        "## Step 4 — Re-upload under `refuse_fail` → 409 Conflict\n",
    ))
    cells.append(code("""
        r3 = client.put(url, files=files_form(),
                        headers={k: v for k, v in client.headers.items() if k.lower() != "content-type"})
        print(f"third upload : {r3.status_code} — {r3.text[:200]}")
        assert r3.status_code == 409, f"refuse_fail should return 409 on duplicate, got {r3.status_code}"
        print("  refuse_fail confirmed: duplicate asset_id returned 409 Conflict.")
        """))

    cells.append(md("## Teardown"))
    cells.append(_teardown_cell())
    return cells


# ---------------------------------------------------------------------------
# Main — emit all four notebooks
# ---------------------------------------------------------------------------


def main() -> None:
    write_nb(os.path.join(HERE, "01_uc1_pg_full_sidecars_routing.ipynb"), nb_uc1())
    write_nb(os.path.join(HERE, "02_uc2_schema_patch_multiversion.ipynb"), nb_uc2())
    write_nb(os.path.join(HERE, "03_uc3_private_es.ipynb"), nb_uc3())
    write_nb(os.path.join(HERE, "04_uc4_asset_refusal.ipynb"), nb_uc4())


if __name__ == "__main__":
    main()
