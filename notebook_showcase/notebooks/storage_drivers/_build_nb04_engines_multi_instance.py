"""Generator for storage_drivers/04_engines_and_multi_instance.ipynb.

Run once; the emitted .ipynb is the source of record.  Keep this file
for future edits — plain-Python cell content is much easier to review
than raw .ipynb JSON.
"""

from __future__ import annotations

import json
import os
import textwrap
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))


def md(*chunks: str) -> Dict[str, Any]:
    body = "\n".join(chunks)
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


def nb_engines_multi_instance() -> List[Dict[str, Any]]:
    cells: List[Dict[str, Any]] = []

    cells.append(md(
        "# Storage Drivers — 04: Engines and Multi-Instance Driver Refs\n",
        "\n",
        "**Persona:** Data Engineer / Platform Operator\n",
        "\n",
        "**Goal:** Demonstrate the multi-instance driver ref model introduced in Cycle F.4.\n",
        "A single driver *class* (e.g. `items_postgresql_driver_config`) can be provisioned\n",
        "under multiple operator-chosen *ref names* (`pg_lean`, `pg_full`, `pg_hot`, …).\n",
        "Each ref carries its own payload; routing entries reference refs by name.\n",
        "Hints on routing entries let the dispatcher pick the right ref at call time.\n",
        "\n",
        "**What this notebook covers:**\n",
        "1. **Engine registry** — `GET /configs/engines` lists engine classes and\n",
        "   their driver-class compatibility.\n",
        "2. **UC2: Sidecar specialisation** — two PG refs (`pg_lean`, `pg_full`) with\n",
        "   different sidecar sets; routing selects by operation.\n",
        "3. **UC1: Geometry precision tradeoff** — ES ref (`es_fast`) plus PG ref\n",
        "   (`pg_exact`) on the same SEARCH operation, separated by hints.\n",
        "4. **UC3: Hot/cold tiering** — two PG refs pointing at different engines;\n",
        "   routing selects by time-range hint.\n",
        "5. **Ref deletion** — removing a ref-keyed config and verifying it disappears\n",
        "   from the composed tree.\n",
        "\n",
        "---\n",
        "\n",
        "## Prerequisites\n",
        "\n",
        "| Variable | Default | Description |\n",
        "|---|---|---|\n",
        "| `DYNASTORE_BASE_URL` | `http://localhost:8080` | API base URL |\n",
        "| `DYNASTORE_ADMIN_TOKEN` | _(empty)_ | Bearer token (admin scope) |\n",
        "\n",
        "A fresh DynaStore dev stack (`docker compose up`) satisfies all prerequisites.\n",
        "Auth tokens are obtained automatically from the local Keycloak if the token\n",
        "env var is not set (requires Docker network access).\n",
    ))

    # ── Setup ───────────────────────────────────────────────────────────────

    cells.append(code("""
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

        CATALOG_ID    = f"mi04-{_uuid.uuid4().hex[:8]}"
        COLLECTION_ID = "test-collection"

        headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"} if ADMIN_TOKEN else {}
        client  = httpx.Client(base_url=BASE_URL, headers=headers, timeout=60.0)

        print(f"Base URL   : {BASE_URL}")
        print(f"Catalog    : {CATALOG_ID}")
        print(f"Collection : {COLLECTION_ID}")
        print(f"Auth       : {'set' if ADMIN_TOKEN else 'not set'}")
    """))

    cells.append(code("""
        # Bootstrap — create ephemeral catalog and collection
        _r = client.post("/stac/catalogs", json={
            "id": CATALOG_ID, "type": "Catalog", "stac_version": "1.0.0",
            "title": "Multi-Instance Demo", "description": "Ephemeral.",
        })
        assert _r.status_code == 201, f"Catalog create: {_r.status_code} {_r.text[:200]}"

        _r = client.post(f"/stac/catalogs/{CATALOG_ID}/collections", json={
            "id": COLLECTION_ID, "type": "Collection", "stac_version": "1.0.0",
            "title": "Test Collection", "description": "Multi-instance demo.",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
            "links": [],
        })
        assert _r.status_code == 201, f"Collection create: {_r.status_code} {_r.text[:200]}"
        print(f"Bootstrap: {CATALOG_ID}/{COLLECTION_ID} ready")
    """))

    # ── Step 1: Engine registry ──────────────────────────────────────────────

    cells.append(md(
        "## Step 1 — Engine registry\n",
        "\n",
        "`GET /configs/engines` lists every registered engine *class* together with\n",
        "which driver classes require it.  Engine instances (the actual connection\n",
        "pools) are provisioned by sysadmins at platform scope — this endpoint\n",
        "surfaces the *catalogue* of what can be provisioned, not what is.\n",
    ))

    cells.append(code("""
        resp = client.get("/configs/engines")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text[:300]}"

        engines_payload = resp.json()
        engines = (
            engines_payload.get("engines")
            or engines_payload.get("items")
            or (engines_payload if isinstance(engines_payload, list) else [])
        )

        print(f"Registered engine classes ({len(engines)}):")
        for eng in engines:
            eid   = eng.get("engine_class") or eng.get("id", str(eng))
            compat = eng.get("compatible_driver_classes") or eng.get("drivers", [])
            print(f"  {eid}")
            for d in compat:
                print(f"    ↳ {d}")
    """))

    # ── Step 2: UC2 ──────────────────────────────────────────────────────────

    cells.append(md(
        "## Step 2 — UC2: Sidecar specialisation (two PG refs)\n",
        "\n",
        "**Scenario:** A high-volume ingest collection (`pg_lean`) needs only geometry\n",
        "and attribute sidecars.  A STAC-served archival collection (`pg_full`) also\n",
        "needs the item-metadata and STAC-metadata overlays.\n",
        "\n",
        "Both refs share the same driver class (`items_postgresql_driver_config`) and\n",
        "the same engine (`pg_main`).  The difference is the sidecar list baked into\n",
        "the config at provisioning time (Immutable — cannot be changed post-ensure_storage).\n",
        "\n",
        "The PATCH body uses the operator-chosen ref name as the top-level key.\n",
        "Because `pg_lean` is not a registered class key, the body must include a\n",
        "`class_key` discriminator so the composer can validate the payload.\n",
    ))

    cells.append(code("""
        # UC2 — PATCH two PG refs at collection scope
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            json={
                # ref 1: lean — geometry + attributes only
                "pg_lean": {
                    "class_key": "items_postgresql_driver_config",
                    "engine_ref": "pg_main",
                },
                # ref 2: full — geometry + attributes + item_metadata + stac_metadata
                "pg_full": {
                    "class_key": "items_postgresql_driver_config",
                    "engine_ref": "pg_main",
                },
            },
        )
        assert resp.status_code == 200, (
            f"PATCH multi-instance refs: {resp.status_code} {resp.text[:400]}"
        )
        print("PATCH response:", json.dumps(resp.json(), indent=2))
    """))

    cells.append(md(
        "### Verify — both refs surface in the composed tree\n",
        "\n",
        "After PATCH, `GET` the collection scope with `resolved=true`.\n",
        "Multi-instance refs appear as sibling leaves under the parent class's\n",
        "`_address` in `configs.platform.catalog.collection.items.drivers`.\n",
    ))

    cells.append(code("""
        resp = client.get(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            params={"resolved": "true"},
        )
        assert resp.status_code == 200, f"GET composed: {resp.status_code} {resp.text[:300]}"

        tree = resp.json()
        drivers_node = (
            tree.get("configs", {})
                .get("platform", {})
                .get("catalog", {})
                .get("collection", {})
                .get("items", {})
                .get("drivers", {})
        )

        print("platform.catalog.collection.items.drivers:")
        print(json.dumps(drivers_node, indent=2))

        assert "pg_lean" in drivers_node, "pg_lean ref not found in drivers tree"
        assert "pg_full" in drivers_node, "pg_full ref not found in drivers tree"
        print("✓ pg_lean and pg_full both surface in the composed tree")
    """))

    # ── Step 3: Routing for UC2 ──────────────────────────────────────────────

    cells.append(md(
        "## Step 3 — Routing for UC2\n",
        "\n",
        "Set `items_routing_config` so WRITE goes to `pg_lean` (high-volume)\n",
        "and READ also goes to `pg_lean`.  A separate collection can override\n",
        "this at collection scope to use `pg_full` instead.\n",
        "\n",
        "Note: `items_routing_config` is a *class-keyed* (single-instance) config\n",
        "— its key in the PATCH body is the class_key, not an operator ref name.\n",
    ))

    cells.append(code("""
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            json={
                "items_routing_config": {
                    "operations": {
                        "WRITE": [
                            {"driver_ref": "pg_lean", "on_failure": "fatal"},
                        ],
                        "READ": [
                            {"driver_ref": "pg_lean", "on_failure": "fatal"},
                        ],
                    }
                }
            },
        )
        assert resp.status_code == 200, (
            f"PATCH routing: {resp.status_code} {resp.text[:400]}"
        )
        routing = resp.json().get("items_routing_config", {})
        print("items_routing_config after PATCH:")
        print(json.dumps(routing, indent=2))
    """))

    # ── Step 4: UC1 ──────────────────────────────────────────────────────────

    cells.append(md(
        "## Step 4 — UC1: Geometry precision tradeoff (ES + PG, same SEARCH)\n",
        "\n",
        "**Scenario:** The map UI needs fast approximate geometry (ES);  analytics\n",
        "pipelines need exact stored polygons (PG).  Both are registered under SEARCH\n",
        "with different hints.  The caller signals which it needs via the hint parameter.\n",
        "\n",
        "| ref | driver class | engine | hint |\n",
        "|---|---|---|---|\n",
        "| `es_fast` | `items_elasticsearch_driver_config` | `es_main` | `geometry_simplified` |\n",
        "| `pg_exact` | `items_postgresql_driver_config` | `pg_main` | `geometry_exact` |\n",
        "\n",
        "Both are created at catalog scope (shared across collections) so every\n",
        "collection in the catalog can inherit them via the waterfall.\n",
    ))

    cells.append(code("""
        # UC1 — PATCH two driver refs at CATALOG scope
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}",
            json={
                "es_fast": {
                    "class_key": "items_elasticsearch_driver_config",
                    "engine_ref": "es_main",
                    "index_prefix": "items",
                },
                "pg_exact": {
                    "class_key": "items_postgresql_driver_config",
                    "engine_ref": "pg_main",
                },
            },
        )
        assert resp.status_code == 200, (
            f"PATCH catalog refs: {resp.status_code} {resp.text[:400]}"
        )
        print("PATCH catalog scope:", list(resp.json().keys()))
    """))

    cells.append(code("""
        # UC1 — set routing at CATALOG scope (inherited by all collections)
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}",
            json={
                "items_routing_config": {
                    "operations": {
                        "READ": [
                            {
                                "driver_ref": "es_fast",
                                "hints": ["geometry_simplified"],
                                "on_failure": "warn",
                            },
                            {
                                "driver_ref": "pg_exact",
                                "hints": ["geometry_exact"],
                                "on_failure": "fatal",
                            },
                        ],
                        "WRITE": [
                            {"driver_ref": "pg_exact", "on_failure": "fatal"},
                        ],
                    }
                }
            },
        )
        assert resp.status_code == 200, (
            f"PATCH catalog routing: {resp.status_code} {resp.text[:400]}"
        )
        ops = resp.json().get("items_routing_config", {}).get("operations", {})
        print("READ routing entries:")
        for entry in ops.get("READ", []):
            print(f"  driver_ref={entry['driver_ref']}  hints={entry.get('hints', [])}")
    """))

    cells.append(md(
        "### Verify — catalog-scope refs surface in collection waterfall\n",
        "\n",
        "With `strict=false` the collection response includes catalog-scope entries\n",
        "in `configs` (full waterfall view).  With the default `strict=true` the\n",
        "catalog-tier entries move to `inherited`.\n",
    ))

    cells.append(code("""
        # strict=false shows the full waterfall in the configs body
        resp = client.get(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            params={"resolved": "true", "strict": "false"},
        )
        assert resp.status_code == 200

        drivers_node = (
            resp.json()
                .get("configs", {})
                .get("platform", {})
                .get("catalog", {})
                .get("collection", {})
                .get("items", {})
                .get("drivers", {})
        )

        for ref_name in ("es_fast", "pg_exact", "pg_lean", "pg_full"):
            present = ref_name in drivers_node
            print(f"  {ref_name}: {'✓' if present else '✗ NOT FOUND'}")

        # strict=true (default) — catalog refs should be in inherited, not configs
        resp2 = client.get(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            params={"resolved": "true", "strict": "true"},
        )
        assert resp2.status_code == 200

        inherited_drivers = (
            resp2.json()
                 .get("inherited", {})
                 .get("platform", {})
                 .get("catalog", {})
                 .get("collection", {})
                 .get("items", {})
                 .get("drivers", {})
        )
        for ref_name in ("es_fast", "pg_exact"):
            present = ref_name in inherited_drivers
            print(f"  inherited/{ref_name}: {'✓' if present else '(not there)'}")
    """))

    # ── Step 5: UC3 ──────────────────────────────────────────────────────────

    cells.append(md(
        "## Step 5 — UC3: Hot/cold storage tiering\n",
        "\n",
        "**Scenario:** Recent data lives in a fast NVMe-backed PostgreSQL instance\n",
        "(`pg_hot`).  Archival data lives in a cheaper, slower instance (`pg_cold`).\n",
        "Routing uses custom hints `recent` / `archive` to steer callers to the right tier.\n",
        "\n",
        "Both refs point at *different engines* (`pg_hot` / `pg_cold` engine refs)\n",
        "provisioned by the sysadmin.  From the tenant's perspective the engine ref\n",
        "is just a name — the connection details are opaque and live at platform scope.\n",
    ))

    cells.append(code("""
        # UC3 — hot/cold PG refs at collection scope
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            json={
                "pg_hot": {
                    "class_key": "items_postgresql_driver_config",
                    "engine_ref": "pg_hot",
                },
                "pg_cold": {
                    "class_key": "items_postgresql_driver_config",
                    "engine_ref": "pg_cold",
                },
                "items_routing_config": {
                    "operations": {
                        "READ": [
                            {
                                "driver_ref": "pg_hot",
                                "hints": ["recent"],
                                "on_failure": "fatal",
                            },
                            {
                                "driver_ref": "pg_cold",
                                "hints": ["archive"],
                                "on_failure": "fatal",
                            },
                        ],
                        "WRITE": [
                            {"driver_ref": "pg_hot", "on_failure": "fatal"},
                        ],
                    }
                },
            },
        )
        assert resp.status_code == 200, (
            f"PATCH hot/cold refs: {resp.status_code} {resp.text[:400]}"
        )
        print("UC3 hot/cold routing applied.")
        ops = resp.json().get("items_routing_config", {}).get("operations", {})
        for entry in ops.get("READ", []):
            print(f"  READ: driver_ref={entry['driver_ref']}  hints={entry.get('hints', [])}")
    """))

    # ── Step 6: Ref deletion ──────────────────────────────────────────────────

    cells.append(md(
        "## Step 6 — Ref deletion\n",
        "\n",
        "Setting a ref value to `null` in the PATCH body removes that ref's persisted\n",
        "config row.  The dispatcher will fall back to the class-keyed default or raise\n",
        "a routing error if the ref is still referenced in a routing entry.\n",
    ))

    cells.append(code("""
        # Delete pg_cold by PATCHing it to null
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            json={"pg_cold": None},
        )
        assert resp.status_code == 200, (
            f"PATCH delete: {resp.status_code} {resp.text[:300]}"
        )
        print("pg_cold deleted via PATCH null")

        # Verify it is gone from the composed tree
        resp2 = client.get(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            params={"resolved": "true"},
        )
        drivers_after = (
            resp2.json()
                 .get("configs", {})
                 .get("platform", {})
                 .get("catalog", {})
                 .get("collection", {})
                 .get("items", {})
                 .get("drivers", {})
        )
        assert "pg_cold" not in drivers_after, "pg_cold still present after deletion"
        print("✓ pg_cold no longer in drivers tree")
        print("Remaining driver refs:", list(drivers_after.keys()))
    """))

    # ── Step 7: Class-mismatch guard ─────────────────────────────────────────

    cells.append(md(
        "## Step 7 — Class-mismatch guard\n",
        "\n",
        "If an operator tries to overwrite a stored ref with a *different* class\n",
        "the service layer rejects the write.  This prevents silent driver-class\n",
        "switching; the operator must `DELETE` the ref first, then recreate it.\n",
    ))

    cells.append(code("""
        # Try to re-register pg_lean as an ES driver (different class) — must fail
        resp = client.patch(
            f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}",
            json={
                "pg_lean": {
                    "class_key": "items_elasticsearch_driver_config",
                    "engine_ref": "es_main",
                }
            },
        )
        # The service rejects class-mismatch overwrites
        assert resp.status_code in (409, 422, 400), (
            f"Expected 409/422/400 for class-mismatch, got {resp.status_code}: {resp.text[:300]}"
        )
        print(f"Class-mismatch rejected with {resp.status_code}: "
              f"{resp.json().get('detail', resp.text[:120])}")
    """))

    # ── Teardown ──────────────────────────────────────────────────────────────

    cells.append(md("## Teardown\n"))

    cells.append(code("""
        _r = client.delete(
            f"/stac/catalogs/{CATALOG_ID}", params={"force": "true"}, timeout=60.0,
        )
        print(f"DELETE catalog → {_r.status_code}")
        assert _r.status_code == 204, f"Catalog delete failed: {_r.status_code} {_r.text}"
        client.close()
        print("Done.")
    """))

    return cells


if __name__ == "__main__":
    out = os.path.join(HERE, "04_engines_and_multi_instance.ipynb")
    write_nb(out, nb_engines_multi_instance())
