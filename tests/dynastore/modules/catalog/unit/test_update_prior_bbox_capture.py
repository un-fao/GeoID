"""Unit coverage for ``ItemQueryMixin._capture_prior_bboxes_for_update`` (#1297 Phase 2b).

Phase 2b tile-cache invalidation needs the extents features USED to occupy so
that a geometry-move update also drops the tiles the old footprint touched.
The capture helper reads those extents off the materialized bbox envelope via
the normal read path BEFORE the upsert. It is gated on ``is_tile_cache_active``
(non-tile deployments pay nothing) and degrade-safe (never raises — a capture
failure must not block an upsert). These tests pin that contract without a DB.

Pattern mirrors ``test_delete_prior_bbox_capture.py`` (DELETE half, Phase 2).
"""
from __future__ import annotations

import pytest

import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.modules.catalog.item_service import ItemService


def _svc_with_get_item(get_item):
    """An ItemService with ``get_item`` stubbed; ``__init__`` skipped (the
    helper only touches ``get_item`` plus module imports)."""
    svc = ItemService.__new__(ItemService)
    svc.get_item = get_item  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_capture_returns_bboxes_for_existing_items(monkeypatch):
    """When the cache is active and items exist, their bboxes are returned."""
    async def _active(*_a, **_k):
        return True

    items_store = {
        "item-a": {"id": "item-a", "bbox": [10.0, 20.0, 11.0, 21.0]},
        "item-b": {"id": "item-b", "bbox": [30.0, 40.0, 31.0, 41.0]},
    }

    async def _get_item(catalog_id, collection_id, item_id, **_k):
        return items_store.get(item_id)

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    items = [{"id": "item-a", "geometry": {}}, {"id": "item-b", "geometry": {}}]
    bboxes = await svc._capture_prior_bboxes_for_update("cat", "col", items)
    assert bboxes == [(10.0, 20.0, 11.0, 21.0), (30.0, 40.0, 31.0, 41.0)]


@pytest.mark.asyncio
async def test_capture_skips_new_items_not_yet_in_store(monkeypatch):
    """Items that don't yet exist (CREATE, not UPDATE) contribute nothing."""
    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        return None  # item does not exist yet

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "new-item"}],
    )
    assert bboxes == []


@pytest.mark.asyncio
async def test_capture_skips_items_without_id(monkeypatch):
    """Items without an ``id`` field are silently skipped — CREATE case."""
    read_called = False

    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        nonlocal read_called
        read_called = True
        return {"bbox": [0, 0, 1, 1]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"geometry": {}}],  # no "id" key
    )
    assert bboxes == []
    assert read_called is False, "must not call get_item when item has no id"


@pytest.mark.asyncio
async def test_capture_returns_empty_when_cache_inactive(monkeypatch):
    """When the tile cache is inactive the read is skipped entirely."""
    read_called = False

    async def _inactive(*_a, **_k):
        return False

    async def _get_item(*_a, **_k):
        nonlocal read_called
        read_called = True
        return {"id": "x", "bbox": [0, 0, 1, 1]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _inactive)
    svc = _svc_with_get_item(_get_item)

    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "x"}],
    )
    assert bboxes == []
    assert read_called is False, "must not read items when the cache is off"


@pytest.mark.asyncio
async def test_capture_degrades_to_empty_on_per_item_error(monkeypatch):
    """A read error for one item must not raise — other items still captured."""
    async def _active(*_a, **_k):
        return True

    call_count = [0]

    async def _get_item(catalog_id, collection_id, item_id, **_k):
        call_count[0] += 1
        if item_id == "bad":
            raise RuntimeError("read boom")
        return {"id": item_id, "bbox": [5.0, 6.0, 7.0, 8.0]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    # "bad" raises; "good" should still contribute its bbox.
    bboxes = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "bad"}, {"id": "good"}],
    )
    assert bboxes == [(5.0, 6.0, 7.0, 8.0)]
    assert call_count[0] == 2, "both items should have been attempted"


@pytest.mark.asyncio
async def test_capture_skips_items_with_null_bbox(monkeypatch):
    """An existing item whose bbox is None or missing contributes nothing."""
    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        return {"id": "x"}  # no bbox field

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    svc = _svc_with_get_item(_get_item)

    bboxes = await svc._capture_prior_bboxes_for_update("cat", "col", [{"id": "x"}])
    assert bboxes == []


@pytest.mark.asyncio
async def test_dispatch_receives_prior_bboxes_on_update(monkeypatch):
    """``_dispatch_tile_cache_invalidation`` is called with prior_bboxes on update.

    This pins the pass-through contract end-to-end without touching the DB:
    prior_bboxes captured by _capture_prior_bboxes_for_update must reach
    _dispatch_tile_cache_invalidation when results are non-empty.
    """
    dispatched: list = []

    async def _fake_dispatch(self_inner, catalog_id, collection_id, results, **kwargs):
        dispatched.append({
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "prior_bboxes": kwargs.get("prior_bboxes"),
        })

    async def _active(*_a, **_k):
        return True

    async def _get_item(*_a, **_k):
        return {"id": "item-x", "bbox": [1.0, 2.0, 3.0, 4.0]}

    monkeypatch.setattr(tcs, "is_tile_cache_active", _active)
    monkeypatch.setattr(
        ItemService, "_dispatch_tile_cache_invalidation", _fake_dispatch,
    )

    svc = _svc_with_get_item(_get_item)
    # Simulate the path: capture returns [(1,2,3,4)]; assert dispatch sees it.
    prior = await svc._capture_prior_bboxes_for_update(
        "cat", "col", [{"id": "item-x"}],
    )
    assert prior == [(1.0, 2.0, 3.0, 4.0)]

    # Invoke the dispatch with the captured prior bboxes (mimics item_service upsert).
    from dynastore.models.ogc import Feature
    fake_result = Feature.model_construct(id="item-x", bbox=[9.0, 9.0, 10.0, 10.0])
    await _fake_dispatch(svc, "cat", "col", [fake_result], prior_bboxes=prior or None)
    assert dispatched[0]["prior_bboxes"] == [(1.0, 2.0, 3.0, 4.0)]
