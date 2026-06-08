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

"""Verify the assets extension registers PUT (replace) and PATCH (update)
as distinct routes per OGC API Features Part 4 conformance — Issue #282.

Asset metadata has only one mutable field (``metadata``, free-form dict)
so the partial-vs-full distinction collapses at the data layer; both
methods bind to the same handler. The split lives at the route layer so
the OpenAPI spec exposes both operations correctly.
"""

from __future__ import annotations

import pytest


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_assets_routes_register_put_and_patch(in_process_client):
    """OpenAPI sanity: ``PUT`` and ``PATCH`` both registered for both
    catalog-asset and collection-asset paths."""
    response = await in_process_client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]

    catalog_asset_path = "/assets/catalogs/{catalog_id}/assets/{asset_id}"
    coll_asset_path = (
        "/assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}"
    )

    for path in (catalog_asset_path, coll_asset_path):
        assert path in paths, f"missing path {path}"
        ops = paths[path]
        assert "put" in ops, f"PUT missing on {path}"
        assert "patch" in ops, f"PATCH missing on {path}"
