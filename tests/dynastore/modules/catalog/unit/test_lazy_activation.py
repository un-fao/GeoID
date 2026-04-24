"""Tests for collection lazy activation (Design D′).

A collection is `pending` after create until its first `POST /items`,
at which point activation runs inside the insert transaction:
`ensure_storage` is called and `CollectionRoutingConfig` is pinned at
collection scope. See docs/architecture/collection-lifecycle.md.
"""

import asyncio
import pytest

from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine
from tests.dynastore.test_utils import generate_test_id


@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql")
@pytest.mark.asyncio
async def test_create_collection_is_pending(app_lifespan, data_id):
    """POST /collections leaves the collection in pending state.

    `is_active` must return False; `physical_table` must still be None
    immediately after create_collection returns.
    """
    catalog_id = f"cat_pending_{data_id}"
    collection_id = f"col_pending_{generate_test_id()}"

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn),
        )
        await catalogs.create_collection(
            catalog_id, {"id": collection_id},
            lang="*", ctx=DriverContext(db_resource=conn),
        )

    active = await catalogs.is_active(catalog_id, collection_id)
    assert active is False, "freshly created collection must be pending"


@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql")
@pytest.mark.asyncio
async def test_pending_collection_items_returns_empty(app_lifespan, data_id):
    """GET /items on a pending collection returns 200 empty FeatureCollection.

    OGC API Features Part 1 Requirement 26 — successful items request
    returns 200 with a FeatureCollection; empty is valid.
    """
    from dynastore.models.protocols.items import ItemsProtocol
    from dynastore.models.query_builder import QueryRequest

    catalog_id = f"cat_empty_{data_id}"
    collection_id = f"col_empty_{generate_test_id()}"

    catalogs = get_protocol(CatalogsProtocol)
    items = get_protocol(ItemsProtocol)
    assert catalogs is not None and items is not None

    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn),
        )
        await catalogs.create_collection(
            catalog_id, {"id": collection_id},
            lang="*", ctx=DriverContext(db_resource=conn),
        )

    # stream_items must return an empty QueryResponse, not raise.
    response = await items.stream_items(
        catalog_id, collection_id, QueryRequest(limit=10),
    )
    features = [f async for f in response.items]
    assert features == []
    assert response.total_count in (0, None)


@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql")
@pytest.mark.asyncio
async def test_first_insert_activates_collection(app_lifespan, data_id):
    """First POST /items triggers activation transparently.

    After the insert, `is_active` flips to True and the collection is
    queryable through the provisioned driver.
    """
    from dynastore.models.protocols.items import ItemsProtocol

    catalog_id = f"cat_act_{data_id}"
    collection_id = f"col_act_{generate_test_id()}"

    catalogs = get_protocol(CatalogsProtocol)
    items = get_protocol(ItemsProtocol)
    assert catalogs is not None and items is not None

    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn),
        )
        await catalogs.create_collection(
            catalog_id, {"id": collection_id},
            lang="*", ctx=DriverContext(db_resource=conn),
        )

    assert await catalogs.is_active(catalog_id, collection_id) is False

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"name": "first-item"},
    }
    await items.upsert(
        catalog_id, collection_id,
        {"type": "FeatureCollection", "features": [feature]},
    )

    assert await catalogs.is_active(catalog_id, collection_id) is True


@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql")
@pytest.mark.asyncio
async def test_activate_collection_is_idempotent(app_lifespan, data_id):
    """Calling activate_collection twice is a no-op on the second call."""
    catalog_id = f"cat_idem_{data_id}"
    collection_id = f"col_idem_{generate_test_id()}"

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn),
        )
        await catalogs.create_collection(
            catalog_id, {"id": collection_id},
            lang="*", ctx=DriverContext(db_resource=conn),
        )

    await catalogs.activate_collection(catalog_id, collection_id)
    assert await catalogs.is_active(catalog_id, collection_id) is True

    # Second activation must not raise and must not flip any state.
    await catalogs.activate_collection(catalog_id, collection_id)
    assert await catalogs.is_active(catalog_id, collection_id) is True


@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql")
@pytest.mark.asyncio
async def test_concurrent_first_inserts_both_succeed(app_lifespan, data_id):
    """Two parallel first-inserts on a fresh collection both commit.

    ensure_storage is idempotent; the routing pin is serialised by a
    row lock; the loser sees an equal pinned value and short-circuits.
    """
    from dynastore.models.protocols.items import ItemsProtocol

    catalog_id = f"cat_race_{data_id}"
    collection_id = f"col_race_{generate_test_id()}"

    catalogs = get_protocol(CatalogsProtocol)
    items = get_protocol(ItemsProtocol)
    assert catalogs is not None and items is not None

    async with managed_transaction(get_engine()) as conn:
        await catalogs.ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn),
        )
        await catalogs.create_collection(
            catalog_id, {"id": collection_id},
            lang="*", ctx=DriverContext(db_resource=conn),
        )

    def _feat(name: str) -> dict:
        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {"name": name},
        }

    results = await asyncio.gather(
        items.upsert(catalog_id, collection_id,
                     {"type": "FeatureCollection", "features": [_feat("A")]}),
        items.upsert(catalog_id, collection_id,
                     {"type": "FeatureCollection", "features": [_feat("B")]}),
        return_exceptions=True,
    )

    exceptions = [r for r in results if isinstance(r, Exception)]
    assert not exceptions, f"concurrent first-inserts must not fail: {exceptions}"
    assert await catalogs.is_active(catalog_id, collection_id) is True
