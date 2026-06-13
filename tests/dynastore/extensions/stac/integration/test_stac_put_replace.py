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

"""PUT-replace semantics for STAC catalog and collection.

Issue #282 — OGC API Features Part 4 / STAC Transaction Extension require
PUT to replace the whole resource (idempotent, full body) and PATCH to
partially update. These tests pin the new ``replace_stac_catalog`` /
``replace_stac_collection`` handlers introduced to honour that contract.
"""

import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_replaces_catalog(
    sysadmin_in_process_client, in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    replacement = {**catalog_data, "title": "Replaced Title"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json=replacement
    )
    assert r.status_code == 200, r.text
    assert r.json()["title"] == "Replaced Title"

    r = await in_process_client.get(f"/stac/catalogs/{catalog_id}")
    assert r.json()["title"] == "Replaced Title"


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_rejects_id_mismatch(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    bad = {**catalog_data, "id": "different-id"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json=bad
    )
    assert r.status_code == 400


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_rejects_partial_body(
    sysadmin_in_process_client, catalog_data, catalog_id
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201

    # Missing required ``id`` — ``STACCatalogRequest`` enforces the field, so
    # the framework rejects with 422 before the handler runs. This is the key
    # PUT-vs-PATCH distinction (PATCH would accept the same body).
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}", json={"title": "no-id"}
    )
    assert r.status_code == 422


@pytest.mark.asyncio
@pytest.mark.enable_extensions("stac", "assets", "features")
async def test_stac_put_replaces_collection(
    sysadmin_in_process_client,
    in_process_client,
    catalog_data,
    catalog_id,
    collection_data,
    collection_id,
):
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    assert r.status_code == 201
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    assert r.status_code == 201

    replacement = {**collection_data, "description": "Replaced Description"}
    r = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}",
        json=replacement,
    )
    assert r.status_code == 200, r.text
    assert r.json()["description"] == "Replaced Description"

    r = await in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert r.json()["description"] == "Replaced Description"
