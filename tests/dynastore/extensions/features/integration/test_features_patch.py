import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "config")
async def test_features_patch_operations(
    in_process_client, catalog_data, catalog_id, collection_data, collection_id
):
    # 1. Create Catalog
    r = await in_process_client.post("/features/catalogs", json=catalog_data)
    assert r.status_code == 201

    # 2. PATCH Catalog (update title)
    new_title = "Patched Features Catalog Title"
    patch_data = {"title": new_title}
    # We use PATCH method
    r = await in_process_client.patch(
        f"/features/catalogs/{catalog_id}", json=patch_data
    )
    assert r.status_code == 200
    assert r.json()["title"] == new_title

    # Verify persistence
    r = await in_process_client.get(f"/features/catalogs/{catalog_id}")
    assert r.status_code == 200
    assert r.json()["title"] == new_title

    # 3. Create Collection
    r = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    # 4. PATCH Collection (update description)
    new_desc = "Patched Features Collection Description"
    patch_col_data = {"description": new_desc}
    r = await in_process_client.patch(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}",
        json=patch_col_data,
    )
    assert r.status_code == 200
    assert r.json()["description"] == new_desc

    # Verify persistence
    r = await in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.status_code == 200
    assert r.json()["description"] == new_desc
