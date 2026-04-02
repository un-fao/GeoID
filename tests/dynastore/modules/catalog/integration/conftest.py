import pytest
import os
from dynastore.tools.identifiers import generate_id_hex

@pytest.fixture
def dynastore_extensions():
    # Both features and web extensions are needed for test_search_api.py
    return ["features", "web"]

@pytest.fixture
def catalog_id():
    return f"cat_{generate_id_hex()}"

@pytest.fixture
def collection_id():
    return f"coll_{generate_id_hex()}"

@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "title": "Test Catalog",
        "description": "Test Catalog for Query Transform",
    }

@pytest.fixture
def collection_data(collection_id):
    return {
        "id": collection_id,
        "description": "Test Collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }

@pytest.fixture
async def setup_catalog(in_process_client, catalog_data, catalog_id):
    # Ensure cleanup first
    await in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")
    
    response = await in_process_client.post("/features/catalogs", json=catalog_data)
    assert response.status_code in (201, 409)
    
    yield catalog_id
    
    await in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")

@pytest.fixture
async def setup_collection(in_process_client, setup_catalog, collection_data, collection_id):
    catalog_id = setup_catalog
    # Ensure cleanup first
    await in_process_client.delete(f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true")
    
    response = await in_process_client.post(f"/features/catalogs/{catalog_id}/collections", json=collection_data)
    assert response.status_code in (201, 409)
    
    yield collection_id
    
    await in_process_client.delete(f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true")

@pytest.fixture
async def setup_catalog_with_collection(setup_catalog, setup_collection):
    yield setup_catalog, setup_collection
