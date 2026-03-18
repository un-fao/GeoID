import pytest
import json
from httpx import AsyncClient


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features", "config")
async def test_stac_search_filters(in_process_client, setup_catalog, setup_collection):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Create items with distinct properties and geometries
    item1 = {
        "id": "item1",
        "stac_version": "1.0.0",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [10.0, 10.0]},
        "bbox": [10.0, 10.0, 10.0, 10.0],
        "properties": {"datetime": "2024-01-01T10:00:00Z", "prop_a": "value1"},
        "links": [{"rel": "self", "href": "http://example.com/item1"}],
        "assets": {},
    }

    item2 = {
        "id": "item2",
        "stac_version": "1.0.0",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [20.0, 20.0]},
        "bbox": [20.0, 20.0, 20.0, 20.0],
        "properties": {"datetime": "2024-01-02T10:00:00Z", "prop_a": "value2"},
        "links": [{"rel": "self", "href": "http://example.com/item2"}],
        "assets": {},
    }

    # Insert items
    r1 = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items", json=item1
    )
    print(f"Item1 Insert: {r1.status_code} {r1.text}")
    assert r1.status_code in [200, 201]

    r2 = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items", json=item2
    )
    print(f"Item2 Insert: {r2.status_code} {r2.text}")
    assert r2.status_code in [200, 201]

    # 1. Test BBOX Filter (Spatial Only Optimization Candidate)
    # item1 is in [9,9,11,11], item2 is not
    search_bbox = {
        "catalog_id": catalog_id,
        "bbox": [9.0, 9.0, 11.0, 11.0],
        "collections": [collection_id],
    }
    r = await in_process_client.post("/stac/search", json=search_bbox)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == "item1"

    # 3. Test IDs Filter (Attributes Sidecar)
    search_ids = {
        "catalog_id": catalog_id,
        "ids": ["item1", "item2"],
        "collections": [collection_id],
    }
    r = await in_process_client.post("/stac/search", json=search_ids)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 2

    search_ids_single = {
        "catalog_id": catalog_id,
        "ids": ["item2"],
        "collections": [collection_id],
    }
    r = await in_process_client.post("/stac/search", json=search_ids_single)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == "item2"

    # 4. Test Datetime Filter (Attributes Sidecar)
    search_dt = {
        "catalog_id": catalog_id,
        "datetime": "2024-01-02T00:00:00Z/..",  # From Jan 2nd onwards
        "collections": [collection_id],
    }
    r = await in_process_client.post("/stac/search", json=search_dt)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    # Both items are valid (infinite validity), so both match the open interval start
    assert len(features) == 2
    # Sorted by time DESC, so item2 (Jan 2) first
    assert features[0]["id"] == "item2"

    # 5. Combined Filters
    search_combined = {
        "catalog_id": catalog_id,
        "bbox": [0.0, 0.0, 30.0, 30.0],  # Both
        "ids": ["item1"],  # Only item1
        "collections": [collection_id],
    }
    r = await in_process_client.post("/stac/search", json=search_combined)
    if r.status_code != 200:
        print(f"\nResponse: {r.json()}")
    assert r.status_code == 200
    features = r.json()["features"]
    assert len(features) == 1
    assert features[0]["id"] == "item1"

    # Cleanup handled by fixtures
