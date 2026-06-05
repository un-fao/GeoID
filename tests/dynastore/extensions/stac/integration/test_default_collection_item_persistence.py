"""Live-DB integration coverage for item persistence on a DEFAULT collection.

Regression guard for the silent-data-loss path: a *default-body* STAC collection
(created via ``POST /stac/catalogs/{cat}/collections`` with no declared
``attribute_schema`` — the JSONB attributes path) must actually PERSIST items
ingested over ``/items``. The existing ingest+read-back integration coverage
(``test_attribute_filter_items.py``) only exercises a COLUMNAR collection; this
fills the gap where items were accepted (``201 {"ids":[...]}``) but never
retrievable (``numberMatched == 0``).

Requires a reachable PostgreSQL service (Docker/CI test path), like every test
in this directory.
"""

import pytest


MARKER = pytest.mark.enable_extensions("stac", "assets", "features")

_ITEMS = [
    ("item_one", [10.0, 10.0]),
    ("item_two", [11.0, 11.0]),
    ("item_three", [20.0, 20.0]),
]


def _item(item_id: str, coords: list, collection: str) -> dict:
    return {
        "id": item_id,
        "stac_version": "1.0.0",
        "type": "Feature",
        "collection": collection,
        "geometry": {"type": "Point", "coordinates": coords},
        "bbox": [coords[0], coords[1], coords[0], coords[1]],
        "properties": {"datetime": "2024-01-01T10:00:00Z"},
        "links": [],
        "assets": {},
    }


async def _create_default_collection(client, catalog: str, collection: str) -> None:
    """Create catalog + a bare default collection over the STAC route.

    No ``layer_config`` / ``attribute_schema`` — this is the default JSONB
    attributes path, the exact shape a generic STAC migration produces.
    Best-effort delete-first keeps the test idempotent across reruns.
    """
    await client.delete(f"/stac/catalogs/{catalog}?force=true")
    r = await client.post(
        "/stac/catalogs",
        json={"id": catalog, "type": "Catalog", "description": "default-persist"},
    )
    assert r.status_code in (200, 201), f"catalog: {r.status_code} {r.text}"

    r = await client.post(
        f"/stac/catalogs/{catalog}/collections",
        json={
            "id": collection,
            "type": "Collection",
            "description": "default JSONB collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )
    assert r.status_code in (200, 201), f"collection: {r.status_code} {r.text}"


@MARKER
async def test_default_collection_items_persist(sysadmin_in_process_client):
    """Items ingested into a default JSONB collection must be readable back.

    Fails on the silent-loss bug: writes return 201 but GET /items reports
    numberMatched == 0 because the per-collection physical table was never
    provisioned (is_active trusted a pinned-but-nonexistent physical_table).
    """
    catalog, collection = "def_persist_cat", "def_persist_col"
    client = sysadmin_in_process_client
    await _create_default_collection(client, catalog, collection)

    for item_id, coords in _ITEMS:
        r = await client.post(
            f"/stac/catalogs/{catalog}/collections/{collection}/items",
            json=_item(item_id, coords, collection),
        )
        assert r.status_code in (200, 201), f"ingest {item_id}: {r.status_code} {r.text}"

    r = await client.get(
        f"/stac/catalogs/{catalog}/collections/{collection}/items?limit=50"
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("numberMatched") == len(_ITEMS), (
        f"expected {len(_ITEMS)} persisted, got numberMatched="
        f"{body.get('numberMatched')} / {len(body.get('features', []))} returned"
    )
    assert len(body["features"]) == len(_ITEMS)


@MARKER
async def test_orphan_physical_table_pin_no_silent_loss(sysadmin_in_process_client):
    """Reproduce the review-env state: ``physical_table`` pinned, table absent.

    ``is_active()`` keys only on ``physical_table is not None``, so when a name
    is pinned without the table having been created (a partial-provisioning
    divergence), activation (``ensure_storage``) is permanently skipped. The
    write path must NOT silently succeed-without-persisting: it must either fail
    loud (>=400) or self-heal by re-provisioning. A ``2xx`` with the item not
    readable back is the silent-data-loss bug this guards against.
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalog, collection = "orphan_pin_cat", "orphan_pin_col"
    client = sysadmin_in_process_client
    await _create_default_collection(client, catalog, collection)  # PENDING

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    # Pin a physical_table that was never created → the exact divergence.
    await catalogs.set_physical_table(catalog, collection, "t_orphan_never_created")  # type: ignore[attr-defined]
    assert await catalogs.is_active(catalog, collection) is True

    w = await client.post(
        f"/stac/catalogs/{catalog}/collections/{collection}/items",
        json=_item("orphan_probe", [1.0, 2.0], collection),
    )
    g = await client.get(
        f"/stac/catalogs/{catalog}/collections/{collection}/items?limit=50"
    )
    matched = g.json().get("numberMatched") if g.status_code == 200 else None

    if w.status_code in (200, 201):
        assert matched, (
            f"SILENT DATA LOSS: write returned {w.status_code} but the item is "
            f"not readable back (numberMatched={matched})."
        )
    else:
        assert w.status_code >= 400, (
            f"expected fail-loud (>=400) or self-heal, got {w.status_code}: {w.text}"
        )
