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

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql"
    ),
    pytest.mark.enable_extensions("features", "wfs", "assets", "stac"),
]


async def test_get_capabilities_scoped(in_process_client_module, setup_collection):
    pass


async def test_get_capabilities_with_collection(
    in_process_client_module, setup_collection, setup_catalog
):
    catalog_id = setup_catalog
    wfs_url = f"/wfs/{catalog_id}"
    params = {"service": "WFS", "request": "GetCapabilities"}
    r = await in_process_client_module.get(wfs_url, params=params)
    assert r.status_code == 200
