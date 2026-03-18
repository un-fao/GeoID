import pytest


@pytest.mark.asyncio
async def test_catalog_lifecycle(sysadmin_in_process_client, catalog_data, catalog_id):
    # Pre-clean with hard delete to avoid duplicate key constraints
    # await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")

    try:
        # Create
        r = await sysadmin_in_process_client.post(
            "/features/catalogs", json=catalog_data
        )
        if r.status_code != 201:
            print(f"\n[DEBUG] Create Catalog Response ({r.status_code}): {r.text}")
        assert r.status_code == 201

        # Get
        r = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}")
        assert r.status_code == 200
    finally:
        # Cleanup with hard delete handled by session fixture
        pass
        # await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.mark.asyncio
async def test_collection_lifecycle(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    catalog_id = setup_catalog
    # Create
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code != 201:
        print(f"\n[DEBUG] Create Collection Response ({r.status_code}): {r.text}")
    assert r.status_code == 201

    # Get
    r = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_catalog_conflict_409(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    """Test that creating a duplicate catalog returns 409 Conflict."""
    # Pre-clean with hard delete
    # await sysadmin_in_process_client.delete(
    #     f"/features/catalogs/{catalog_id}?force=true"
    # )

    try:
        # Create catalog
        r = await sysadmin_in_process_client.post(
            "/features/catalogs", json=catalog_data
        )
        assert r.status_code == 201

        # Try to create the same catalog again
        r = await sysadmin_in_process_client.post(
            "/features/catalogs", json=catalog_data
        )
        assert r.status_code == 409
        assert "already exists" in r.json()["detail"].lower()
    finally:
        # Cleanup with hard delete handled by session fixture
        pass
        # await sysadmin_in_process_client.delete(
        #     f"/features/catalogs/{catalog_id}?force=true"
        # )


@pytest.mark.asyncio
async def test_collection_conflict_409(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    """Test that creating a duplicate collection returns 409 Conflict."""
    catalog_id = setup_catalog
    # Create collection
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code != 201:
        print(f"\n[DEBUG] Create Collection 1 Response ({r.status_code}): {r.text}")
    assert r.status_code == 201

    # Try to create the same collection again
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code != 409:
        print(f"\n[DEBUG] Create Collection 2 Response ({r.status_code}): {r.text}")
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"].lower()
