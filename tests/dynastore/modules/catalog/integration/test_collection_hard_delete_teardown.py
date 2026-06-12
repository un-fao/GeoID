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

"""Hard delete must tear down the hub AND every sidecar table (#1994).

Regression repro: ``delete_collection(force=True)`` only dropped the hub table;
sidecar tables (_attributes, _geometries, _item_metadata) were left behind
because ``ItemsPostgresqlDriver.drop_storage`` circularly delegated to
``CatalogsProtocol.delete_collection`` instead of owning physical teardown.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.tools.discovery import get_protocol


async def _tables_with_prefix(conn, schema: str, prefix: str) -> list[str]:
    """Return all table names in *schema* whose name starts with *prefix*."""
    result = await conn.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name LIKE :prefix"
        ),
        {"schema": schema, "prefix": f"{prefix}%"},
    )
    return [row[0] for row in result.fetchall()]


@pytest.mark.asyncio
async def test_hard_delete_drops_hub_and_all_sidecars(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """Hard-deleting a collection must remove the hub table AND all sidecar tables.

    Steps:
    1. Create catalog + collection.
    2. Write one item — triggers lazy provisioning of hub + sidecars.
    3. Assert hub and at least one sidecar table exist (precondition).
    4. Hard-delete the collection (force=True).
    5. Assert no tables with the physical table prefix remain (main assertion).
    6. Teardown: hard-delete the catalog.
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"

    # --- 1. Setup ---
    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Teardown Test Catalog"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Teardown test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        # --- 2. Write one item to trigger lazy provisioning ---
        await catalogs.upsert(
            catalog_id,
            collection_id,
            {
                "id": "teardown-item-1",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                "properties": {"name": "Teardown Test Item"},
            },
        )

        # --- 3. Resolve physical coordinates and assert precondition ---
        phys_schema = await catalogs.resolve_physical_schema(catalog_id)
        assert phys_schema, f"Could not resolve physical schema for catalog {catalog_id!r}"

        phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id)
        assert phys_table, (
            f"Could not resolve physical table for {catalog_id!r}/{collection_id!r}"
        )

        async with managed_transaction(app_lifespan.engine) as conn:
            before = await _tables_with_prefix(conn, phys_schema, phys_table)

        assert phys_table in before, (
            f"Hub table {phys_table!r} not found before delete; got: {before}"
        )
        # At least one sidecar must exist after an item write
        sidecars_before = [t for t in before if t != phys_table]
        assert sidecars_before, (
            f"No sidecar tables found before delete; hub exists but sidecars missing: {before}"
        )

        # --- 4. Hard-delete the collection ---
        await catalogs.delete_collection(catalog_id, collection_id, force=True)

        # --- 5. Assert: no tables with the prefix remain ---
        async with managed_transaction(app_lifespan.engine) as conn:
            after = await _tables_with_prefix(conn, phys_schema, phys_table)

        assert after == [], (
            f"Orphaned tables left behind after hard delete of "
            f"{catalog_id!r}/{collection_id!r}: {after}"
        )

    finally:
        # --- 6. Teardown ---
        await catalogs.delete_catalog(catalog_id, force=True)
