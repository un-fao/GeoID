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

"""Graceful STAC ingestion into a COLUMNAR collection with no temporal sink (#1253).

A COLUMNAR attribute collection persists only its declared columns. When the
collection has no ``validity`` sink, an item's ``properties.datetime`` has
nowhere to land and is dropped on write, so the stored row echoes back with no
temporal value. STAC requires every item to carry a ``datetime`` — previously
this echo 500'd (``pystac`` rejects an item whose ``datetime`` is ``None``).

The canonical fix is to enable validity so the item's own datetime round-trips
(covered by ``test_attribute_filter_items.py``). This test pins the *fallback*
contract for a collection that is nonetheless configured without validity: the
create must succeed (not 500) and the echoed item must carry a valid
``datetime`` — the ingestion timestamp, stamped because the item's own value
could not be preserved.

Live-DB integration test: requires a reachable PostgreSQL service (Docker/CI).
"""

import pytest
from httpx import AsyncClient

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol


MARKER = pytest.mark.enable_extensions("stac", "assets", "features")

ATTR = "adm2_pcode"


async def _create_columnar_collection_without_validity(
    catalog_id: str, collection_id: str
) -> None:
    """Create a PG-backed COLUMNAR collection with NO validity sink."""
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
        AttributeStorageMode,
        AttributeSchemaEntry,
        AttributeIndexType,
        PostgresType,
    )

    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.ensure_catalog_exists(catalog_id)

    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(
                name=ATTR,
                type=PostgresType.TEXT,
                index=AttributeIndexType.BTREE,
            ),
        ],
    )
    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(), attr_sidecar]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "COLUMNAR no-validity collection (#1253)"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )


@MARKER
@pytest.mark.asyncio
async def test_stac_create_without_validity_sink_stamps_ingestion_datetime(
    sysadmin_in_process_client: AsyncClient, catalog_id, collection_id
):
    """An item carrying a datetime, posted to a COLUMNAR collection with no
    validity sink, is accepted (not 500) and echoed with a valid datetime."""
    await _create_columnar_collection_without_validity(catalog_id, collection_id)

    item = {
        "id": "item_no_validity",
        "stac_version": "1.0.0",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [10.0, 10.0]},
        "bbox": [10.0, 10.0, 10.0, 10.0],
        "properties": {"datetime": "2024-01-01T10:00:00Z", ATTR: "PK001"},
        "links": [],
        "assets": {},
    }

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item,
    )

    # The #1253 regression guard: previously this echo 500'd because the dropped
    # datetime left the rendered item with datetime=None.
    assert r.status_code in (200, 201), f"{r.status_code} {r.text}"
    body = r.json()
    assert body["properties"].get("datetime"), (
        "STAC item must carry a datetime; expected an ingestion-timestamp "
        f"fallback when no validity sink stores the item's own value: {body}"
    )
