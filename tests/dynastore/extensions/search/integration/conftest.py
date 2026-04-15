import pytest


@pytest.fixture
async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
    await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")
    await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data)
    yield catalog_id
    await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.fixture
async def setup_collection(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    catalog_id = setup_catalog
    await sysadmin_in_process_client.delete(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true"
    )
    await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    yield collection_id
