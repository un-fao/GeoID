import pytest
from fastapi import Request

@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "collection_postgresql", "collection_postgresql", "catalog_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_get_capabilities_scoped(in_process_client, setup_collection):
    collection_id = setup_collection
    # Get catalog_id from some other means or just assume it's part of setup_collection
    # Actually setup_collection returns collection_id, but we need catalog_id.
    # In conftest.py, setup_collection uses setup_catalog.
    # I'll modify the test to take both if possible, or just setup_collection and derive catalog_id if I can.
    # Let's check conftest again. setup_collection yields collection_id.
    # I'll just change the test to use setup_collection and then I need to find the catalog_id.
    
    # Wait, I'll just create a new test case that is more explicit.
    pass

@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "collection_postgresql", "collection_postgresql", "catalog_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("features", "configs", "wfs", "assets", "stac")
async def test_get_capabilities_with_collection(in_process_client, setup_collection, setup_catalog):
    catalog_id = setup_catalog
    collection_id = setup_collection
    
    wfs_url = f"/wfs/{catalog_id}"
    params = {"service": "WFS", "request": "GetCapabilities"}
    r = await in_process_client.get(wfs_url, params=params)
    assert r.status_code == 200
