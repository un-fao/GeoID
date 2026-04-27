import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_catalog_lifecycle(sysadmin_in_process_client, in_process_client, catalog_data, catalog_id):

    # Ensure cleanup first
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}")

    # Create via STAC
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201
    assert r.json()["id"] == catalog_id

    # Get via STAC
    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}")
    assert r.status_code == 200
    assert r.json()["id"] == catalog_id

    # Cleanup
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}")


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_collection_lifecycle(
    in_process_client, setup_catalog, collection_data, collection_id
):
    catalog_id = setup_catalog
    # Create via STAC
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201
    assert r.json()["id"] == collection_id

    # Get via STAC
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.status_code == 200
    assert r.json()["id"] == collection_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_item_lifecycle(
    in_process_client, setup_catalog, setup_collection, item_raw_data, item_id
):
    catalog_id = setup_catalog
    collection_id = setup_collection

    # 1. Create Item via STAC
    # item_raw_data already has id=item_id (from fixture)
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_raw_data,
    )
    assert r.status_code == 201

    # Check that we can get the item using the ID returned in the Location header or body
    # location_header = r.headers.get("Location")

    # Try multiple keys for ID as it might be 'id', 'geoid', or 'external_id' depending on serialization
    returned_body = r.json()
    returned_id = returned_body.get("id")

    # # The returned_id might be different from the requested item_id if the server
    # # enforces a different ID generation strategy (e.g. UUIDv7).
    # # We should use the returned_id for subsequent operations.
    # # Note: If returned_id is None, something is wrong with the response model
    # if returned_id is None and "properties" in returned_body:
    #     # Some STAC implementations might put ID in properties for some reason, or we might be looking at wrong level
    #     returned_id = returned_body["properties"].get("id") or returned_body[
    #         "properties"
    #     ].get("geoid")

    # Check if ID is in the feature root (standard STAC)
    if returned_id is None and "features" in returned_body:
        # If it returned a FeatureCollection (bulk add?)
        returned_id = returned_body["features"][0].get("id")

    assert returned_id is not None, f"Response ID is missing. Body: {returned_body}"
    final_item_id = returned_id

    # 2. Get Item via STAC using the logical ID (item_id)
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{final_item_id}"
    )
    assert r.status_code == 200, (
        f"Failed to get item {final_item_id}. Response: {r.text}"
    )
    assert r.json()["id"] == final_item_id

    # 3. Update Item via STAC
    updated_data = item_raw_data.copy()
    updated_data["id"] = final_item_id  # Ensure ID matches for update
    # Note: 'properties' might be flattened or structured differently in the input item_raw_data vs STAC output
    if "properties" not in updated_data:
        updated_data["properties"] = {}
    updated_data["properties"]["custom_updated"] = True

    r = await in_process_client.put(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{final_item_id}",
        json=updated_data,
    )
    assert r.status_code == 200, (
        f"Failed to update item {final_item_id}. Response: {r.text}"
    )

    # Verify the property update in the response
    props = r.json().get("properties", {})
    # 'custom_updated' might be a string "true" or boolean True depending on serialization
    assert str(props.get("custom_updated")).lower() == "true", (
        f"Property 'custom_updated' not found or incorrect in {props}"
    )

    # 4. Delete Item via STAC
    url = (
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{final_item_id}"
    )
    print(f"\nDEBUG: DELETE URL: {url}")
    r = await in_process_client.delete(url)
    assert r.status_code == 204, (
        f"Failed to delete item {final_item_id}. Response: {r.text}"
    )

    # 5. Verify deletion
    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{final_item_id}"
    )
    assert r.status_code == 404, (
        f"Item {final_item_id} still exists after deletion. Response: {r.text}"
    )

    # Explicit cleanup of parent resources if not handled by fixtures (though fixtures should handle it)
    # But for safety in this specific test flow:
    pass


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_catalog_conflict_409(sysadmin_in_process_client, in_process_client, catalog_data, catalog_id):
    """Test that creating a duplicate catalog returns 409 Conflict."""
    # Pre-clean
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}")

    # Create catalog
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    # Try to create the same catalog again
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"].lower()

    # Cleanup
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}")


@pytest.mark.asyncio
@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_collection_conflict_409(
    in_process_client, setup_catalog, collection_data, collection_id
):
    """Test that creating a duplicate collection returns 409 Conflict."""
    catalog_id = setup_catalog
    # Create collection
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    # Try to create the same collection again
    r = await in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"].lower()


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac")
async def test_stac_root(in_process_client):
    r = await in_process_client.get("/stac/")
    assert r.status_code == 200
    assert r.json()["id"] == "dynastore-stac-root"
