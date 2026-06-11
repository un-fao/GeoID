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

"""Unit-tests for ``ItemsPostgresqlDriver.drop_storage`` (#1994).

Exercises the driver method directly (not through the catalog service) so
that teardown ownership — hub + every sidecar in a single call — is verified
independently from the ``_purge_collection_storage`` path covered by the
catalog integration test.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_id_hex


@pytest.fixture
def catalog_id():
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    return f"coll_{generate_id_hex()}"


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
async def test_drop_storage_removes_hub_and_sidecars(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """drop_storage removes hub and all sidecar tables; second call is a no-op.

    Steps:
    1. Create catalog + collection.
    2. Write one item — triggers lazy provisioning of hub + sidecars.
    3. Resolve physical coordinates and assert hub + at least one sidecar exist.
    4. Call the PG driver's drop_storage directly.
    5. Assert zero ``{phys_table}%`` tables remain.
    6. Call drop_storage again — must return silently (idempotent).
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"

    # Resolve the PG driver the same way CollectionService does.
    from dynastore.tools.discovery import get_protocols
    from dynastore.models.protocols.storage_driver import Capability, CollectionItemsStore

    pg_driver = None
    for driver in get_protocols(CollectionItemsStore):
        if Capability.QUERY_FALLBACK_SOURCE in driver.capabilities:
            pg_driver = driver
            break
    assert pg_driver is not None, "ItemsPostgresqlDriver not registered"

    # --- 1. Setup ---
    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Drop Storage Test Catalog"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Drop storage test collection",
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
                "id": "drop-storage-item-1",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                "properties": {"name": "Drop Storage Test Item"},
            },
        )

        # --- 3. Resolve physical coordinates and assert precondition ---
        phys_schema = await catalogs.resolve_physical_schema(catalog_id)
        assert phys_schema, f"Could not resolve physical schema for {catalog_id!r}"

        phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id)
        assert phys_table, (
            f"Could not resolve physical table for {catalog_id!r}/{collection_id!r}"
        )

        async with managed_transaction(app_lifespan.engine) as conn:
            before = await _tables_with_prefix(conn, phys_schema, phys_table)

        assert phys_table in before, (
            f"Hub table {phys_table!r} not found before drop_storage; got: {before}"
        )
        sidecars_before = [t for t in before if t != phys_table]
        assert sidecars_before, (
            f"No sidecar tables found before drop_storage; got: {before}"
        )

        # --- 4. Call driver drop_storage directly ---
        await pg_driver.drop_storage(catalog_id, collection_id)

        # --- 5. Assert: no tables with the prefix remain ---
        async with managed_transaction(app_lifespan.engine) as conn:
            after = await _tables_with_prefix(conn, phys_schema, phys_table)

        assert after == [], (
            f"Orphaned tables after drop_storage({catalog_id!r}, {collection_id!r}): {after}"
        )

        # --- 6. Second call must be a silent no-op ---
        await pg_driver.drop_storage(catalog_id, collection_id)

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_drop_storage_without_pin_is_noop(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """drop_storage on a collection with no physical table pin returns without error.

    A collection created but never activated (no item written) has no
    ``physical_table`` in the driver config.  ``drop_storage`` must return
    silently rather than raising.
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"

    from dynastore.tools.discovery import get_protocols
    from dynastore.models.protocols.storage_driver import Capability, CollectionItemsStore

    pg_driver = None
    for driver in get_protocols(CollectionItemsStore):
        if Capability.QUERY_FALLBACK_SOURCE in driver.capabilities:
            pg_driver = driver
            break
    assert pg_driver is not None, "ItemsPostgresqlDriver not registered"

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "No-pin Test Catalog"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "No-pin test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        # No item written — physical table is never provisioned.
        await pg_driver.drop_storage(catalog_id, collection_id)
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
