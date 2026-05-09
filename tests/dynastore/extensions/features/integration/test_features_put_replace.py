"""PUT-replace semantics for OGC API Features Catalog and Collection.

Issue #450 — OGC API Features Part 4 requires PUT to replace the whole
resource (idempotent, full body) and PATCH to partially update. These
tests pin the new ``replace_catalog`` / ``replace_collection`` handlers
introduced to honour that contract.
"""

import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_put_replaces_catalog(
    sysadmin_in_process_client, in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post(
        "/features/catalogs", json=catalog_data
    )
    assert r.status_code == 201

    replacement = {**catalog_data, "title": "Replaced Features Catalog Title"}
    r = await sysadmin_in_process_client.put(
        f"/features/catalogs/{catalog_id}", json=replacement
    )
    assert r.status_code == 200, r.text
    assert r.json()["title"] == "Replaced Features Catalog Title"

    r = await in_process_client.get(f"/features/catalogs/{catalog_id}")
    assert r.json()["title"] == "Replaced Features Catalog Title"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_put_rejects_catalog_id_mismatch(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post(
        "/features/catalogs", json=catalog_data
    )
    assert r.status_code == 201

    bad = {**catalog_data, "id": "different-id"}
    r = await sysadmin_in_process_client.put(
        f"/features/catalogs/{catalog_id}", json=bad
    )
    assert r.status_code == 400


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_put_rejects_partial_catalog_body(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post(
        "/features/catalogs", json=catalog_data
    )
    assert r.status_code == 201

    # Missing required ``id`` — ``CatalogReplaceRequest`` enforces it, so the
    # framework rejects with 422 before the handler runs. This is the key
    # PUT-vs-PATCH distinction (PATCH would accept the same body).
    partial = {"title": "no-id-here"}
    r = await sysadmin_in_process_client.put(
        f"/features/catalogs/{catalog_id}", json=partial
    )
    assert r.status_code == 422


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_put_replaces_collection(
    sysadmin_in_process_client,
    in_process_client,
    catalog_data,
    catalog_id,
    collection_data,
    collection_id,
):
    r = await sysadmin_in_process_client.post(
        "/features/catalogs", json=catalog_data
    )
    assert r.status_code == 201
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    replacement = {
        **collection_data,
        "description": "Replaced Features Collection Description",
    }
    r = await sysadmin_in_process_client.put(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}",
        json=replacement,
    )
    assert r.status_code == 200, r.text
    assert r.json()["description"] == "Replaced Features Collection Description"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_put_rejects_collection_id_mismatch(
    sysadmin_in_process_client,
    catalog_data,
    catalog_id,
    collection_data,
    collection_id,
):
    r = await sysadmin_in_process_client.post(
        "/features/catalogs", json=catalog_data
    )
    assert r.status_code == 201
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    bad = {**collection_data, "id": "different-id"}
    r = await sysadmin_in_process_client.put(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}",
        json=bad,
    )
    assert r.status_code == 400
