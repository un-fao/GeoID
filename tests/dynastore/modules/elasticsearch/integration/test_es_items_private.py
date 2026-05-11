"""Integration tests — ItemsElasticsearchPrivateDriver: index isolation.

Verifies that items indexed via the private driver:
  - land in the per-tenant private index ({prefix}-{cat}-private-items)
  - do NOT appear in the public items index ({prefix}-{cat}-items)
  - are readable via the private driver's read_entities()
  - are NOT readable via the public driver's read_entities()

Tests call the driver directly (not via the routing API) to avoid the
complexity of patching a collection's ItemsRoutingConfig at test time.

All tests require a live ES instance and are skipped otherwise.
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from tests.dynastore.modules.elasticsearch.integration.conftest import (
    doc_exists,
    make_item,
    refresh_private_items_index,
)

pytestmark = [
    pytest.mark.enable_modules("elasticsearch"),
    pytest.mark.elasticsearch,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx(catalog_id: str, collection_id: str):
    from dynastore.models.protocols.indexer import IndexContext
    return IndexContext(catalog=catalog_id, collection=collection_id)


def _make_upsert_op(item_id: str, item: dict):
    from dynastore.models.protocols.indexer import IndexOp
    return IndexOp(op_type="upsert", entity_type="item", entity_id=item_id, payload=item)


def _make_delete_op(item_id: str):
    from dynastore.models.protocols.indexer import IndexOp
    return IndexOp(op_type="delete", entity_type="item", entity_id=item_id, payload=None)


def _private_driver():
    from dynastore.modules.storage.drivers.elasticsearch_private import (
        ItemsElasticsearchPrivateDriver,
    )
    return ItemsElasticsearchPrivateDriver()


def _public_driver():
    from dynastore.modules.storage.drivers.elasticsearch import ItemsElasticsearchDriver
    return ItemsElasticsearchDriver()


async def _private_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        get_private_index_name,
    )
    return get_private_index_name(get_index_prefix(), catalog_id)


async def _public_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
    return get_tenant_items_index(get_index_prefix(), catalog_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_private_index_receives_item(app_lifespan):
    """index() sends item to the per-catalog private index."""
    cat = f"priv-test-{uuid4().hex[:8]}"
    col = "col-a"
    item_id = "priv-item-1"
    item = make_item(item_id)

    driver = _private_driver()
    ctx = _make_ctx(cat, col)
    op = _make_upsert_op(item_id, item)

    await driver.ensure_storage(cat)
    await driver.index(ctx, op)
    await asyncio.sleep(0)
    await refresh_private_items_index(cat)

    priv_idx = await _private_index(cat)
    assert await doc_exists(priv_idx, item_id), "item not found in private index"

    # Cleanup
    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_public_index_does_not_receive_private_item(app_lifespan):
    """Items indexed via the private driver must NOT appear in the public index."""
    cat = f"priv-test-{uuid4().hex[:8]}"
    col = "col-b"
    item_id = "priv-isolation-1"
    item = make_item(item_id)

    driver = _private_driver()
    ctx = _make_ctx(cat, col)
    op = _make_upsert_op(item_id, item)

    await driver.ensure_storage(cat)
    await driver.index(ctx, op)
    await asyncio.sleep(0)
    await refresh_private_items_index(cat)

    pub_idx = await _public_index(cat)
    assert not await doc_exists(pub_idx, item_id), "private item leaked into public index"

    # Cleanup
    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_private_read_entities_returns_item(app_lifespan):
    """read_entities() on the private driver returns the indexed item."""
    cat = f"priv-test-{uuid4().hex[:8]}"
    col = "col-c"
    item_id = "priv-read-1"
    item = make_item(item_id, lon=5.0, lat=45.0)

    driver = _private_driver()
    ctx = _make_ctx(cat, col)
    op = _make_upsert_op(item_id, item)

    await driver.ensure_storage(cat)
    await driver.index(ctx, op)
    await asyncio.sleep(0)
    await refresh_private_items_index(cat)

    features = [f async for f in driver.read_entities(cat, col, entity_ids=[item_id])]
    assert len(features) == 1, f"expected 1 feature, got {len(features)}"
    assert features[0].id == item_id

    # Cleanup
    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_public_driver_cannot_see_private_item(app_lifespan):
    """Public driver's read_entities() must not return a private-indexed item."""
    cat = f"priv-test-{uuid4().hex[:8]}"
    col = "col-d"
    item_id = "priv-no-leak-1"
    item = make_item(item_id)

    priv = _private_driver()
    pub = _public_driver()
    ctx = _make_ctx(cat, col)
    op = _make_upsert_op(item_id, item)

    await priv.ensure_storage(cat)
    await priv.index(ctx, op)
    await asyncio.sleep(0)
    await refresh_private_items_index(cat)

    pub_features = [f async for f in pub.read_entities(cat, col, entity_ids=[item_id])]
    assert len(pub_features) == 0, "private item leaked to public read_entities"

    # Cleanup
    await priv.drop_storage(cat)


@pytest.mark.asyncio
async def test_private_index_bulk_roundtrip(app_lifespan):
    """index_bulk() indexes multiple items; all land in the private index."""
    cat = f"priv-test-{uuid4().hex[:8]}"
    col = "col-e"
    items = [make_item(f"bulk-{i}", lon=float(i), lat=float(i)) for i in range(3)]

    driver = _private_driver()
    ctx = _make_ctx(cat, col)
    ops = [_make_upsert_op(it["id"], it) for it in items]

    await driver.ensure_storage(cat)
    result = await driver.index_bulk(ctx, ops)
    await asyncio.sleep(0)
    await refresh_private_items_index(cat)

    assert result.failed == 0, f"bulk had failures: {result.failures}"
    priv_idx = await _private_index(cat)
    for it in items:
        assert await doc_exists(priv_idx, it["id"]), f"bulk item {it['id']} not in private index"

    # Cleanup
    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_private_drop_removes_index(app_lifespan):
    """drop_storage() deletes the per-tenant private index."""
    from dynastore.modules.elasticsearch.client import get_client

    cat = f"priv-test-{uuid4().hex[:8]}"
    driver = _private_driver()

    await driver.ensure_storage(cat)
    priv_idx = await _private_index(cat)
    es = get_client()
    assert await es.indices.exists(index=priv_idx), "index not created by ensure_storage"

    await driver.drop_storage(cat)
    assert not await es.indices.exists(index=priv_idx), "index still exists after drop_storage"
