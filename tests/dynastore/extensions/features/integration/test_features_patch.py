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


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets")
async def test_features_patch_operations(
    sysadmin_in_process_client, catalog_data, catalog_id, collection_data, collection_id
):
    # 1. Create Catalog
    r = await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data)
    assert r.status_code == 201

    # 2. PATCH Catalog (update title)
    new_title = "Patched Features Catalog Title"
    patch_data = {"title": new_title}
    # We use PATCH method
    r = await sysadmin_in_process_client.patch(
        f"/features/catalogs/{catalog_id}", json=patch_data
    )
    assert r.status_code == 200
    assert r.json()["title"] == new_title

    # Verify persistence
    r = await sysadmin_in_process_client.get(f"/features/catalogs/{catalog_id}")
    assert r.status_code == 200
    assert r.json()["title"] == new_title

    # 3. Create Collection
    r = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    # 4. PATCH Collection (update description)
    new_desc = "Patched Features Collection Description"
    patch_col_data = {"description": new_desc}
    r = await sysadmin_in_process_client.patch(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}",
        json=patch_col_data,
    )
    assert r.status_code == 200
    assert r.json()["description"] == new_desc

    # Verify persistence
    r = await sysadmin_in_process_client.get(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.status_code == 200
    assert r.json()["description"] == new_desc
