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

@pytest.mark.asyncio
async def test_search_catalogs_and_collections(sysadmin_in_process_client: AsyncClient, setup_catalog_with_collection):
    catalog_id, collection_id = setup_catalog_with_collection
    
    # 1. Test Catalog Search (match) — use a 12-char prefix for uniqueness
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs?q={catalog_id[:12]}")
    assert res.status_code == 200
    data = res.json()
    assert any(c['id'] == catalog_id for c in data)

    # 2. Test Catalog Search (no match)
    res = await sysadmin_in_process_client.get("/web/dashboard/catalogs?q=this_should_not_match_anything")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 0

    # 3. Test Collection Search (match)
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs/{catalog_id}/collections?q={collection_id[:12]}")
    assert res.status_code == 200
    data = res.json()
    assert any(c['id'] == collection_id for c in data)

    # 4. Test Collection Search (no match)
    res = await sysadmin_in_process_client.get(f"/web/dashboard/catalogs/{catalog_id}/collections?q=this_should_not_match")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 0

