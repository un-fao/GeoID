import pytest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.catalog.asset_service import (
    AssetBase,
    AssetTypeEnum,
)
from tests.dynastore.test_utils import generate_test_id

@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_assets_api_crud(
    sysadmin_in_process_client,
    catalog_obj,
    catalog_id,
    collection_obj,
    collection_id,
):
    """
    Integration test for Assets API.
    Verifies CRUD operations and Partitioning logic (implicitly).
    """
    # Sysadmin client is required for asset writes after PR #149 IAM tightening.
    client = sysadmin_in_process_client

    catalogs = get_protocol(CatalogsProtocol)

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    try:
        # --- Catalog Asset ---
        cat_asset_id = f"CAT_ASSET_{generate_test_id()}"
        cat_asset_payload = {
            "asset_id": cat_asset_id,
            "filename": "cat_asset.tif",
            "uri": "gs://bucket/cat_asset.tif",
            "asset_type": "ASSET",
            "metadata": {"type": "report"}
        }

        # Create
        resp = await client.post(f"/assets/catalogs/{catalog_id}", json=cat_asset_payload)
        assert resp.status_code == 201
        cat_asset = resp.json()
        assert cat_asset["asset_id"] == cat_asset_id
        assert cat_asset["collection_id"] is None

        # Get
        resp = await client.get(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}")
        assert resp.status_code == 200
        assert resp.json()["asset_id"] == cat_asset_id

        # List
        resp = await client.get(f"/assets/catalogs/{catalog_id}")
        assert resp.status_code == 200
        assets = resp.json()
        assert any(a["asset_id"] == cat_asset_id for a in assets)

        # Update (Metadata only)
        update_payload = {"metadata": {"type": "report", "updated": True}}
        resp = await client.put(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}", json=update_payload)
        assert resp.status_code == 200
        assert resp.json()["metadata"]["updated"] is True

        # --- Collection Asset ---
        coll_asset_id = f"COLL_ASSET_{generate_test_id()}"
        coll_asset_payload = {
            "asset_id": coll_asset_id,
            "filename": "coll_asset.tif",
            "uri": "gs://bucket/coll_asset.tif",
            "asset_type": "RASTER",
            "metadata": {"sensor": "S2"}
        }

        # Create (triggers partition creation)
        resp = await client.post(f"/assets/catalogs/{catalog_id}/collections/{collection_id}", json=coll_asset_payload)
        assert resp.status_code == 201
        coll_asset = resp.json()
        assert coll_asset["asset_id"] == coll_asset_id
        assert coll_asset["collection_id"] == collection_id

        # Get
        resp = await client.get(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 200
        assert resp.json()["asset_id"] == coll_asset_id

        # Search — collection-scoped assets live behind the collection-scoped
        # search route; the catalog-scoped route only returns catalog-tier
        # (collection-unbound) assets, and the body ``collection_id`` was
        # dropped when the scoped search endpoints were split out.
        search_payload = {
            "filters": [
                {"field": "asset_id", "op": "eq", "value": coll_asset_id}
            ],
            "limit": 10
        }
        resp = await client.post(
            f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets-search",
            json=search_payload,
        )
        assert resp.status_code == 200
        results = resp.json()
        assert len(results) == 1
        assert results[0]["asset_id"] == coll_asset_id

        # Helper for search error logic
        search_payload_err = {
            "filters": [
                {"field": "non_existent_field", "op": "eq", "value": "check"}
            ],
            "limit": 10
        }
        resp = await client.post(f"/assets/catalogs/{catalog_id}/assets-search", json=search_payload_err)
        # Depending on implementation, might 400 or 500. SQL injection protection throws value error?
        # Basic validation passes, but SQL execution might fail if column doesn't exist?
        # actually validate_sql_identifier catches it if it has weird chars, but "non_existent_field" is valid identifier.
        # Postgres will throw generic error. AssetService catches Exception and returns 400.
        assert resp.status_code == 400

        # Delete Collection Asset
        resp = await client.delete(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 204

        # Verify Deletion
        resp = await client.get(f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{coll_asset_id}")
        assert resp.status_code == 404

        # Delete Catalog Asset
        resp = await client.delete(f"/assets/catalogs/{catalog_id}/assets/{cat_asset_id}")
        assert resp.status_code == 204

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_assets_patch_merge_preserves_siblings(
    sysadmin_in_process_client,
    catalog_obj,
    catalog_id,
    collection_obj,
    collection_id,
):
    """Regression for the gdalinfo-clobber bug.

    PATCH must deep-merge ``metadata`` (RFC 7396): a user editing
    ``metadata.title`` MUST NOT wipe sibling keys (e.g. ``gdalinfo``)
    injected by other writers. Setting a key to ``null`` removes it.
    """
    client = sysadmin_in_process_client
    catalogs = get_protocol(CatalogsProtocol)

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    try:
        asset_id = f"PATCH_MERGE_{generate_test_id()}"
        create_payload = {
            "asset_id": asset_id,
            "filename": "raster.tif",
            "uri": "gs://bucket/raster.tif",
            "asset_type": "RASTER",
            "metadata": {
                "title": "original",
                "description": "",
                "gdalinfo": {
                    "size": [4096, 4096],
                    "bands": [{"type": "Byte", "noDataValue": 0}],
                },
            },
        }
        resp = await client.post(
            f"/assets/catalogs/{catalog_id}/collections/{collection_id}",
            json=create_payload,
        )
        assert resp.status_code == 201

        base = f"/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}"

        # 1. PATCH only metadata.title — gdalinfo must survive.
        resp = await client.patch(base, json={"metadata": {"title": "new title"}})
        assert resp.status_code == 200, resp.text
        meta = resp.json()["metadata"]
        assert meta["title"] == "new title"
        assert meta["description"] == ""
        assert meta["gdalinfo"]["size"] == [4096, 4096]
        assert meta["gdalinfo"]["bands"] == [{"type": "Byte", "noDataValue": 0}]

        # 2. Null on a key removes it.
        resp = await client.patch(base, json={"metadata": {"description": None}})
        assert resp.status_code == 200
        meta = resp.json()["metadata"]
        assert "description" not in meta
        assert "gdalinfo" in meta

        # 3. Nested PATCH merges inside gdalinfo, doesn't replace it.
        resp = await client.patch(base, json={"metadata": {"gdalinfo": {"crs": "EPSG:4326"}}})
        assert resp.status_code == 200
        gi = resp.json()["metadata"]["gdalinfo"]
        assert gi["size"] == [4096, 4096]  # untouched
        assert gi["crs"] == "EPSG:4326"     # added

        # 4. Empty PATCH body is a no-op (idempotent).
        resp = await client.patch(base, json={})
        assert resp.status_code == 200
        assert resp.json()["metadata"]["title"] == "new title"

        # 5. PUT still replaces metadata wholesale (existing contract).
        resp = await client.put(base, json={"metadata": {"only": "this"}})
        assert resp.status_code == 200
        assert resp.json()["metadata"] == {"only": "this"}

        # 6. Unknown top-level key in PATCH is rejected (422).
        resp = await client.patch(base, json={"asset_id": "hacked"})
        assert resp.status_code == 422

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
