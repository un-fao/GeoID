import pytest

from httpx import AsyncClient
from uuid import uuid4

# Increase timeout for the endpoint specifically if needed, 
# or use a custom client with longer timeout.
@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac", "config")
async def test_bulk_item_creation_success(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{uuid4().hex[:8]}"
    collection_id = "test_bulk_collection"
    
    # 1. Create Catalog
    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    catalog_response = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_response.status_code == 201
    
    # 2. Create Collection
    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    collection_response = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_response.status_code == 201
    
    # 3. Bulk Create Items
    bulk_payload = test_data_loader("bulk_items.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=bulk_payload, timeout=60.0)
    assert response.status_code == 201
    data = response.json()
    assert "ids" in data
    assert len(data["ids"]) == 2
    
    # 4. Verify items exist
    items_response = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items")
    assert items_response.status_code == 200
    items_data = items_response.json()
    assert len(items_data["features"]) == 2

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac", "config")
async def test_bulk_item_creation_atomicity(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{uuid4().hex[:8]}"
    collection_id = "test_atomic_collection"
    
    # Create Catalog and Collection
    catalog_data = test_data_loader("catalog_atomic.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
    collection_data = test_data_loader("collection_atomic.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    # Payload with one invalid item (invalid geometry type)
    bulk_payload = test_data_loader("bulk_items_invalid.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=bulk_payload, timeout=60.0)
    # Validation should fail
    assert response.status_code >= 400
    
    # Verify NO items exist (atomicity)
    items_response = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items")
    assert items_response.status_code == 200
    items_data = items_response.json()
    # It might return an empty FeatureCollection
    assert len(items_data.get("features", [])) == 0

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac", "config")
async def test_single_item_creation_with_extra_fields(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{uuid4().hex[:8]}"
    collection_id = "test_fix_collection"
    
    catalog_data = test_data_loader("catalog_fix.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
 
    collection_data = test_data_loader("collection_fix.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    single_payload = test_data_loader("single_item_extra.json")
    
    response = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", json=single_payload, timeout=60.0)
    assert response.status_code == 201
    assert "Location" in response.headers

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac", "config")
async def test_filter_with_unknown_property(sysadmin_in_process_client: AsyncClient, test_data_loader):
    catalog_id = f"c_{uuid4().hex[:8]}"
    collection_id = "test_filter_collection"
    
    catalog_data = test_data_loader("catalog_filter.json")
    catalog_data["id"] = catalog_id
    catalog_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs", 
        json=catalog_data,
        timeout=60.0
    )
    assert catalog_resp.status_code == 201, f"Failed to create catalog: {catalog_resp.text}"
    collection_data = test_data_loader("collection_filter.json")
    collection_data["id"] = collection_id
    collection_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json=collection_data,
        timeout=60.0
    )
    assert collection_resp.status_code == 201, f"Failed to create collection: {collection_resp.text}"
    
    # Add a feature first so the collection table exists
    item_payload = test_data_loader("item_test.json")
    item_resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_payload,
        timeout=30.0
    )
    assert item_resp.status_code == 201, f"Failed to create item: {item_resp.text}"
    
    # Request with unknown property in CQL2 filter
    # Should return 400 Bad Request with error about unknown property
    response = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"filter": "unknown_prop='value'"}
    )
    
    assert response.status_code == 400
    error_detail = response.json()["detail"]
    assert "Unknown property" in error_detail or "unknown" in error_detail.lower()
