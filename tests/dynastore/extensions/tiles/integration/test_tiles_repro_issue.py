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
from httpx import AsyncClient
from tests.dynastore.test_utils import generate_test_id

@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tiles")
@pytest.mark.enable_extensions("tiles", "assets", "features")
async def test_tiles_with_non_existent_collection(in_process_client: AsyncClient):
    """
    Reproduces the TypeError: object Response can't be used in 'await' expression
    by requesting a tile for a non-existent collection.
    """
    catalog_id = f"c_{generate_test_id()}"
    
    # Create the catalog first
    await in_process_client.post(
        f"/features/catalogs", 
        json={"id": catalog_id, "title": "Test Catalog"}
    )
    
    # Request a tile for a collection that doesn't exist
    z, x, y = 0, 0, 0
    non_existent_collection = "i_do_not_exist"
    tile_url = f"/tiles/{catalog_id}/tiles/{z}/{x}/{y}.mvt?collections={non_existent_collection}"
    
    # This should hit line 364 in tiles_service.py:
    # return await TilesService._finalize_response(request, b"")
    # And raise TypeError
    response = await in_process_client.get(tile_url)
    
    # If it doesn't crash, we expect a 404 or an empty response (204 or 200 with empty body)
    # The current code intended to return an empty response via _finalize_response(request, b"")
    # which actually might return 200 with empty content or 304 if ETag matches.
    assert response.status_code in [200, 204, 404]
