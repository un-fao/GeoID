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

"""Live POST /features/.../items coverage for two ingestion fixes:

* ``required: false`` ⇒ the field accepts ``null`` (its sidecar column is
  nullable, so a present-but-null value must validate) — previously a null on
  a non-required field was rejected by the write validator.
* a single row that fails value validation no longer aborts the whole batch —
  it is reported as a per-row rejection in a 207 ``IngestionReport`` while the
  valid siblings persist.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from tests.dynastore.test_utils import generate_test_id


async def _make_collection(client: AsyncClient, schema_fields: dict) -> tuple[str, str]:
    catalog_id = f"c_{generate_test_id()}"
    collection_id = f"col_{generate_test_id()}"

    cat_resp = await client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "nullable/partial test"},
        timeout=60.0,
    )
    assert cat_resp.status_code == 201, cat_resp.text

    col_resp = await client.post(
        f"/features/catalogs/{catalog_id}/collections",
        json={
            "id": collection_id,
            "description": "nullable/partial test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
        timeout=60.0,
    )
    assert col_resp.status_code == 201, col_resp.text

    schema_resp = await client.put(
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
        "/plugins/items_schema",
        json={"fields": schema_fields},
        timeout=60.0,
    )
    assert schema_resp.status_code in (200, 201), schema_resp.text
    return catalog_id, collection_id


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_non_required_field_accepts_null(
    sysadmin_in_process_client: AsyncClient,
) -> None:
    catalog_id, collection_id = await _make_collection(
        sysadmin_in_process_client,
        {
            "code": {"name": "code", "data_type": "string", "required": True},
            "start_date": {"name": "start_date", "data_type": "string", "required": False},
            "end_date": {"name": "end_date", "data_type": "string", "required": False},
        },
    )

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"code": "r1", "start_date": None, "end_date": None},
    }
    resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=feature,
        timeout=60.0,
    )
    assert resp.status_code == 201, (
        f"null on a non-required field must be accepted, got "
        f"{resp.status_code}: {resp.text}"
    )


@pytest.mark.asyncio
@pytest.mark.enable_extensions("features", "assets", "stac")
async def test_one_bad_row_is_rejected_not_fatal(
    sysadmin_in_process_client: AsyncClient,
) -> None:
    catalog_id, collection_id = await _make_collection(
        sysadmin_in_process_client,
        {
            "code": {"name": "code", "data_type": "string", "required": True},
            "lanes": {"name": "lanes", "data_type": "integer", "required": False},
        },
    )

    collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {"code": "good", "lanes": 2},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1, 1]},
                # wrong type for an integer field — not relaxed by nullability
                "properties": {"code": "bad", "lanes": "not-a-number"},
            },
        ],
    }
    resp = await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=collection,
        timeout=60.0,
    )

    # 207 Multi-Status: the bad row is reported, the good row persists — the
    # whole batch is NOT aborted (pre-fix this was a single fatal 4xx/5xx).
    assert resp.status_code == 207, (
        f"expected 207 partial, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert len(body["accepted_ids"]) == 1, body
    assert len(body["rejections"]) == 1, body
    rej = body["rejections"][0]
    assert rej["reason"] == "validation_error"
    assert "is not of type" in rej["message"]
    assert body["total"] == 2
