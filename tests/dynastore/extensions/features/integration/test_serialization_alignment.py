import pytest
from httpx import AsyncClient
from dynastore.tools.identifiers import generate_id_hex

@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "config")
async def test_item_properties_serialization_alignment(sysadmin_in_process_client: AsyncClient, test_data_loader):
    """
    Verifies that item properties are correctly serialized and NOT null when queried via OGC Features.
    This tests the alignment with ItemsProtocol.
    """
    catalog_id = f"c_{generate_id_hex()[:8]}"
    collection_id = "serialization_test_coll"
    
    # 1. Create Catalog
    catalog_data = test_data_loader("catalog.json")
    catalog_data["id"] = catalog_id
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data)
    assert r.status_code == 201
    
    # 2. Create Collection
    collection_data = test_data_loader("collection.json")
    collection_data["id"] = collection_id
    r = await sysadmin_in_process_client.post(f"/features/catalogs/{catalog_id}/collections", json=collection_data)
    assert r.status_code == 201
    
    # 3. Create Item with specific properties
    item_payload = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [12.5, 41.9]
        },
        "properties": {
            "test_key": "test_value",
            "number_val": 42,
            "nested": {"a": 1}
        }
    }
    
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", 
        json=item_payload
    )
    assert r.status_code == 201
    
    # 4. Get Items and verify properties
    r = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items")
    assert r.status_code == 200
    data = r.json()
    
    assert "features" in data
    assert len(data["features"]) == 1
    feature = data["features"][0]
    
    # Verify properties are present and NOT null
    props = feature["properties"]
    assert props["test_key"] == "test_value"
    assert props["number_val"] == 42
    assert props["nested"] == {"a": 1}
    
    # 5. Get Single Item and verify properties
    item_id = feature["id"]
    r = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}")
    assert r.status_code == 200
    single_feature = r.json()
    
    assert single_feature["id"] == item_id
    assert single_feature["properties"]["test_key"] == "test_value"
    assert single_feature["properties"]["number_val"] == 42
    assert single_feature["properties"]["nested"] == {"a": 1}
