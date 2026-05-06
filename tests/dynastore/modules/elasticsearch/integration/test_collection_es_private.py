"""Integration tests — CollectionElasticsearchPrivateDriver: index isolation.

Verifies that collection envelopes stored via the private driver:
  - land in the per-tenant private index ({prefix}-{cat}-collections-private)
  - are readable via get_metadata() returning a STAC-shaped dict
  - do NOT appear in the shared public collections index ({prefix}-collections)
  - are NOT readable via the public CollectionElasticsearchDriver.get_metadata()
  - are searchable by q= and by bbox via search_metadata()
  - are removed by delete_metadata()
  - have their index deleted by drop_storage()

All tests require a live ES instance and are skipped otherwise.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from tests.dynastore.modules.elasticsearch.integration.conftest import (
    doc_exists,
    refresh_private_collection_index,
)

pytestmark = [
    pytest.mark.enable_modules("elasticsearch"),
    pytest.mark.elasticsearch,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_collection(collection_id: str, title: str = "Test Collection") -> dict:
    return {
        "id": collection_id,
        "type": "Collection",
        "title": {"en": title},
        "description": {"en": f"Integration test collection {collection_id}"},
        "extent": {
            "spatial": {"bbox": [[-10.0, 35.0, 20.0, 55.0]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
        "stac_version": "1.0.0",
    }


def _private_driver():
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )
    return CollectionElasticsearchPrivateDriver()


def _public_driver():
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriver,
    )
    return CollectionElasticsearchDriver()


async def _private_collection_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import (
        get_tenant_collections_private_index,
    )
    return get_tenant_collections_private_index(get_index_prefix(), catalog_id)


async def _public_collection_index() -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_index_name
    return get_index_name(get_index_prefix(), "collection")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ensure_creates_per_tenant_index(app_lifespan):
    """ensure_storage() creates the per-tenant private collections index."""
    from dynastore.modules.elasticsearch.client import get_client

    cat = f"col-priv-{uuid4().hex[:8]}"
    driver = _private_driver()

    await driver.ensure_storage(cat)
    priv_idx = await _private_collection_index(cat)
    es = get_client()
    assert await es.indices.exists(index=priv_idx), "private collection index not created"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_upsert_metadata_lands_in_private_index(app_lifespan):
    """upsert_metadata() stores the document in the per-tenant private index."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col)

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)
    await refresh_private_collection_index(cat)

    priv_idx = await _private_collection_index(cat)
    assert await doc_exists(priv_idx, col), "collection not found in private index"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_get_metadata_round_trips_stac_shape(app_lifespan):
    """get_metadata() returns a STAC-shaped dict after upsert."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col, title="RoundTrip Test")

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)

    result = await driver.get_metadata(cat, col)
    assert result is not None, "get_metadata returned None"
    assert result.get("id") == col
    assert result.get("type") == "Collection"
    # temporal interval must be STAC shape [[start, end]], not ES date_range
    interval = result.get("extent", {}).get("temporal", {}).get("interval")
    assert isinstance(interval, list), "temporal interval not a list"
    assert isinstance(interval[0], list), "interval[0] is not a list (expected STAC [[s,e]] shape)"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_private_not_in_shared_index(app_lifespan):
    """Documents upserted to the private driver must NOT appear in the shared public index."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col)

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)
    await refresh_private_collection_index(cat)

    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_index_name
    shared_idx = get_index_name(get_index_prefix(), "collection")
    es = get_client()
    try:
        shared_exists = bool(await es.exists(index=shared_idx, id=col))
    except Exception:
        shared_exists = False

    assert not shared_exists, "private collection leaked into shared public index"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_public_driver_does_not_see_private_collection(app_lifespan):
    """Public CollectionElasticsearchDriver.get_metadata() must return None for private docs."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col)

    priv = _private_driver()
    pub = _public_driver()

    await priv.ensure_storage(cat)
    await priv.upsert_metadata(cat, col, metadata)
    await refresh_private_collection_index(cat)

    result = await pub.get_metadata(cat, col)
    assert result is None, "private collection leaked to public get_metadata"

    await priv.drop_storage(cat)


@pytest.mark.asyncio
async def test_search_metadata_returns_by_q(app_lifespan):
    """search_metadata(q=...) returns the collection when the title matches."""
    unique_token = f"UNIQ{uuid4().hex[:10].upper()}"
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col, title=f"Collection {unique_token} Test")

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)
    # upsert uses refresh=wait_for so the doc is immediately searchable

    results, total = await driver.search_metadata(cat, q=unique_token)
    assert total >= 1, f"expected at least 1 result for q={unique_token!r}, got {total}"
    ids = [r.get("id") for r in results]
    assert col in ids, f"collection {col!r} not returned by search_metadata q={unique_token!r}"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_search_metadata_spatial_filter(app_lifespan):
    """search_metadata(bbox=...) returns collections whose extent intersects the bbox."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col)  # extent bbox [-10,35,20,55]

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)
    # upsert uses refresh=wait_for

    # Overlapping bbox — should return the collection
    results_in, _ = await driver.search_metadata(cat, bbox=[0.0, 40.0, 10.0, 50.0])
    ids_in = [r.get("id") for r in results_in]
    assert col in ids_in, "collection not returned for intersecting bbox"

    # Disjoint bbox — should NOT return the collection
    results_out, _ = await driver.search_metadata(cat, bbox=[50.0, 50.0, 60.0, 60.0])
    ids_out = [r.get("id") for r in results_out]
    assert col not in ids_out, "collection incorrectly returned for disjoint bbox"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_delete_metadata_removes_doc(app_lifespan):
    """delete_metadata() removes the document; subsequent get_metadata returns None."""
    cat = f"col-priv-{uuid4().hex[:8]}"
    col = f"col-{uuid4().hex[:6]}"
    metadata = _make_collection(col)

    driver = _private_driver()
    await driver.ensure_storage(cat)
    await driver.upsert_metadata(cat, col, metadata)

    # Confirm it exists
    assert await driver.get_metadata(cat, col) is not None, "doc not found before delete"

    await driver.delete_metadata(cat, col)

    result = await driver.get_metadata(cat, col)
    assert result is None, "get_metadata returned doc after delete_metadata"

    await driver.drop_storage(cat)


@pytest.mark.asyncio
async def test_drop_storage_removes_index(app_lifespan):
    """drop_storage() deletes the per-tenant private collection index."""
    from dynastore.modules.elasticsearch.client import get_client

    cat = f"col-priv-{uuid4().hex[:8]}"
    driver = _private_driver()

    await driver.ensure_storage(cat)
    priv_idx = await _private_collection_index(cat)
    es = get_client()
    assert await es.indices.exists(index=priv_idx), "index not created by ensure_storage"

    await driver.drop_storage(cat)
    assert not await es.indices.exists(index=priv_idx), "index still exists after drop_storage"
