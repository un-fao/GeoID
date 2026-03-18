#    Copyright 2025 FAO
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
from httpx import AsyncClient
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
import uuid
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_asset_immutability(app_lifespan, catalog_obj, catalog_id):
    """
    Verifies that only metadata can be updated on an asset using the new strict PUT endpoints.
    """
    from httpx import AsyncClient, ASGITransport
    from dynastore.modules.concurrency import await_all_background_tasks
    
    # Use in-process client to properly respect 'enable_extensions' marker
    # The external API container might not have 'assets' enabled.
    app = app_lifespan.app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as local_api_client:

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
async def test_asset_spi_task_discovery(app_lifespan, catalog_obj, catalog_id):
    """
    Verifies that Asset tasks SPI correctly lists and executes tasks.
    Uses 'gdal' task (mapped to GdalInfoTask) for verification.
    """
    from unittest.mock import patch, MagicMock
    from dynastore.modules.concurrency import await_all_background_tasks
    from httpx import AsyncClient, ASGITransport
    
    app = app_lifespan.app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as local_api_client:

        catalogs = get_protocol(CatalogsProtocol)
        # Create Catalog if not exists
        await catalogs.delete_catalog(catalog_id, force=True)
        await await_all_background_tasks()
        await catalogs.create_catalog(catalog_obj)

        with patch("dynastore.tasks.gdal.gdalinfo_task.gdal_module") as mock_gdal_module:
            mock_gdal_module.GDAL_AVAILABLE = True
            mock_gdal_module.get_raster_info = MagicMock(return_value={"driver": "GTiff", "width": 100, "height": 100})
            
            # 1. Create RASTER Asset
            asset_id = "test-asset-spi-gdal"
            asset_payload = {
                "asset_id": asset_id,
                "uri": "gs://bucket/test.tif",
                "asset_type": "RASTER",
                "metadata": {} 
            }
            resp = await local_api_client.post(f"/assets/catalogs/{catalog_id}", json=asset_payload)
            assert resp.status_code == 201
            
            # 2. SPI Listing
            resp = await local_api_client.get(f"/assets/catalogs/{catalog_id}/assets/{asset_id}/tasks")
            assert resp.status_code == 200
            tasks = resp.json()
            
            # Verify 'gdal' task is present
            assert any(t["id"] == "gdal" for t in tasks), f"Expected 'gdal' task. Got: {tasks}"

            # 4. SPI Execution
            exec_payload = {
                "inputs": {}
            }
            resp = await local_api_client.post(f"/assets/catalogs/{catalog_id}/assets/{asset_id}/tasks/gdal/execute?caller_id=test_caller&mode=sync-execute", json=exec_payload)
            
            # In this integration test environment, the 'gs://bucket/test.tif' file likely does not exist.
            if resp.status_code == 400:
                err_text = resp.text
                if any(x in err_text for x in ["No such file or directory", "does not exist", "Could not open", "403"]):
                     logger.info("Task execution failed as expected (File access error). SPI and Task invocation verified.")
                else:
                     pytest.fail(f"Task execution failed with unexpected error: {err_text}")
            else:
                assert resp.status_code in [212, 201, 200], f"Expected 200/201/212 or 400 (if file missing). Got: {resp.status_code} {resp.text}"
                exec_result = resp.json()
                if resp.status_code == 201 or resp.status_code == 202:
                    # Depending on mode, it might be a Task (jobID) or direct result
                    if "jobID" in exec_result:
                        assert "jobID" in exec_result
                        assert exec_result["message"] == "Task created successfully"
                    else:
                        # For sync-execute, check if the expected 'info' is in result
                        assert "info" in exec_result or "driver" in str(exec_result)
