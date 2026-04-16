"""
Session-scoped fixtures for notebook integration tests.

Creates the standard demo infrastructure that most notebooks depend on:
- demo-catalog
- sentinel2-l2a collection with PostgreSQL driver + routing configs

Notebooks that *create* infrastructure (catalog/01, catalog/02, storage_drivers/*)
use their own unique IDs and are unaffected.
"""
import os
import json
import pytest
import httpx

BASE_URL = os.environ.get("DYNASTORE_BASE_URL", "http://localhost:8080")
SYSADMIN_TOKEN = os.environ.get("DYNASTORE_SYSADMIN_TOKEN", "")
CATALOG_ID = os.environ.get("CATALOG_ID", "demo-catalog")
COLLECTION_ID = os.environ.get("COLLECTION_ID", "sentinel2-l2a")


def _admin_headers():
    return {
        "Authorization": f"Bearer {SYSADMIN_TOKEN}",
        "Content-Type": "application/json",
    }


def _ensure_catalog(client: httpx.Client) -> bool:
    r = client.get(f"/stac/catalogs/{CATALOG_ID}")
    if r.status_code == 200:
        return False
    r = client.post("/stac/catalogs", json={
        "id": CATALOG_ID,
        "type": "Catalog",
        "title": "Demo Catalog",
        "description": "Standard demo catalog for integration tests.",
        "stac_version": "1.0.0",
    })
    if r.status_code not in (200, 201, 409):
        raise RuntimeError(f"Failed to create {CATALOG_ID}: {r.status_code} {r.text[:300]}")
    return r.status_code == 201


def _ensure_collection(client: httpx.Client) -> bool:
    r = client.get(f"/stac/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}")
    if r.status_code == 200:
        return False
    r = client.post(f"/stac/catalogs/{CATALOG_ID}/collections", json={
        "id": COLLECTION_ID,
        "type": "Collection",
        "stac_version": "1.0.0",
        "title": "Sentinel-2 L2A",
        "description": "Demo Sentinel-2 collection for integration tests.",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
    })
    if r.status_code not in (200, 201, 409):
        raise RuntimeError(f"Failed to create {COLLECTION_ID}: {r.status_code} {r.text[:300]}")
    return r.status_code == 201


def _apply_driver_configs(client: httpx.Client):
    base = f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}"
    client.put(f"{base}/configs/driver:postgresql", json={
        "enabled": True,
        "partition_type": "LIST",
        "sidecars": ["item_metadata", "stac_metadata"],
    })
    client.put(f"{base}/configs/routing", json={
        "enabled": True,
        "operations": {
            "WRITE": [{"driver": "driver:postgresql", "on_failure": "fatal"}],
            "READ": [{"driver": "driver:postgresql", "on_failure": "fatal"}],
        },
    })


def _delete_catalog(client: httpx.Client):
    client.delete(f"/stac/catalogs/{CATALOG_ID}", params={"force": "true"})


@pytest.fixture(scope="session", autouse=True)
def demo_infrastructure():
    """Set up and tear down the standard demo catalog + collection for the session."""
    if not SYSADMIN_TOKEN:
        yield
        return

    with httpx.Client(
        base_url=BASE_URL, headers=_admin_headers(), timeout=30.0, follow_redirects=True
    ) as client:
        catalog_created = _ensure_catalog(client)
        collection_created = _ensure_collection(client)
        if collection_created:
            _apply_driver_configs(client)

        yield

        if catalog_created:
            _delete_catalog(client)
