#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import pytest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_immutability(sysadmin_in_process_client, app_lifespan, catalog_obj, catalog_id):
    """
    Verifies that only metadata can be updated on an asset using the new strict PUT endpoints.
    """
    from dynastore.modules.concurrency import await_all_background_tasks

    # Sysadmin client is required for asset writes after PR #149 IAM tightening.
    local_api_client = sysadmin_in_process_client

    # Create Catalog
    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.delete_catalog(catalog_id, force=True)
    await await_all_background_tasks() # Wait reliably

    cat = await catalogs.create_catalog(catalog_obj)

    # Verify existence
    check = await catalogs.get_catalog(catalog_id)
    assert check is not None, "Catalog was not created!"

    asset_id_val = "test-asset-immutability"
    asset_payload = {
        "asset_id": asset_id_val,
        "filename": "test.tif",
        "uri": "gs://bucket/test.tif",
        "asset_type": "RASTER",
        "metadata": {"initial": True}
    }

    url = f"/assets/catalogs/{catalog_id}"
    resp = await local_api_client.post(url, json=asset_payload)

    if resp.status_code != 201:
        print(f"CREATE FAILED: {resp.text}")
    assert resp.status_code == 201
    created_asset = resp.json()
    asset_id = created_asset["asset_id"]

    # 2. Try to update immutable fields (should be ignored/no effect)
    update_payload = {
        "asset_id": "new-code", # Attempt to change asset_id
        "catalog_id": "wrong_cat", # Attempt to change catalog_id
        "metadata": {"initial": False, "updated": True}
    }
    resp = await local_api_client.put(f"/assets/catalogs/{catalog_id}/assets/{asset_id_val}", json=update_payload)
    assert resp.status_code == 200
    updated_asset = resp.json()

    assert updated_asset["asset_id"] == asset_id_val # ID remained the same
    assert updated_asset["catalog_id"] == catalog_id # Catalog ID remained the same
    assert updated_asset["metadata"]["updated"] is True # Metadata was updated


@pytest.mark.asyncio
@pytest.mark.local_only
@pytest.mark.enable_extensions("processes", "assets")
@pytest.mark.enable_tasks("gdal")
async def test_gdal_runs_as_ogc_asset_process(sysadmin_in_process_client, app_lifespan, catalog_obj, catalog_id):
    """
    `gdal` is discoverable and executable as a standard OGC process at the
    catalog mount ``/processes/catalogs/{cat}/processes/gdal[/execution]``, with
    the target ``asset_id`` supplied in the request body ``inputs`` — replacing
    the retired asset-task SPI surface (`/assets/.../tasks/...`).
    """
    from unittest.mock import patch, MagicMock
    from dynastore.modules.concurrency import await_all_background_tasks

    local_api_client = sysadmin_in_process_client

    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.delete_catalog(catalog_id, force=True)
    await await_all_background_tasks()
    await catalogs.create_catalog(catalog_obj)

    # Force-import gdalinfo_task so unittest.mock.patch can resolve its
    # 'gdal_module' attribute via the dotted path. The gdal package's
    # __init__.py is intentionally empty (osgeo is optional in some
    # SCOPEs) so the submodule isn't bound until first import.
    try:
        import dynastore.tasks.gdal.gdalinfo_task  # noqa: F401
    except ImportError as _e:
        pytest.skip(f"GDAL python bindings not installed: {_e}")
    with patch("dynastore.tasks.gdal.gdalinfo_task.gdal_module") as mock_gdal_module:
        mock_gdal_module.GDAL_AVAILABLE = True
        mock_gdal_module.get_raster_info = MagicMock(
            return_value={"driver": "GTiff", "width": 100, "height": 100}
        )

        # 1. Create RASTER asset.
        asset_id = "test-asset-ogc-gdal"
        asset_payload = {
            "asset_id": asset_id,
            "filename": "test.tif",
            "uri": "gs://bucket/test.tif",
            "asset_type": "RASTER",
            "metadata": {},
        }
        resp = await local_api_client.post(f"/assets/catalogs/{catalog_id}", json=asset_payload)
        assert resp.status_code == 201

        # 2. Discovery: gdal listed at the catalog mount (CATALOG-scoped).
        resp = await local_api_client.get(
            f"/processes/catalogs/{catalog_id}/processes"
        )
        assert resp.status_code == 200, resp.text
        listed = resp.json()["processes"]
        assert any(p["id"] == "gdal" for p in listed), f"Expected 'gdal'. Got: {listed}"

        # 3. Execution via the standard OGC route; asset_id supplied in the body
        #    inputs, sync selected via the Prefer header.
        resp = await local_api_client.post(
            f"/processes/catalogs/{catalog_id}/processes/gdal/execution",
            json={"inputs": {"asset_id": asset_id}},
            headers={"Prefer": "respond-sync"},
        )
        # 404/422 would mean the route or scope wiring is wrong — hard failures.
        assert resp.status_code not in (404, 422), resp.text
        if resp.status_code == 400:
            # Tolerated only for backend file-access errors (no real bucket in CI).
            assert any(
                x in resp.text
                for x in ["No such file", "does not exist", "Could not open", "403"]
            ), f"Unexpected 400: {resp.text}"
        else:
            assert resp.status_code in (200, 201), resp.text
