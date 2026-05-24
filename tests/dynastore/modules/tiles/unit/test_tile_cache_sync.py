"""Unit tests for write-reactive tile-cache invalidation (#1292, Phase 1).

DB-free: exercises the pure coverage/coalesce/cap math, the capability
gate + degrade, the OUTBOX enqueue (with a fake OutboxStore), the drain-side
:class:`TileCacheBulkIndexer` (with a fake provider), and config-reactivity
reconcile (with fakes), without touching Postgres or a bucket.
"""
from __future__ import annotations

from typing import Any, List, Tuple

import pytest

from dynastore.modules.tiles import tile_cache_sync as tcs
from dynastore.modules.tiles.tile_cache_sync import (
    DEFAULT_MAX_TILES_PER_BATCH,
    TILE_CACHE_DRIVER_ID,
    affected_tiles_for_batch,
    clamp_zoom_range_to_cap,
    coverage_for_features,
    decode_tile_payload,
    feature_bbox,
)


# ---------------------------------------------------------------------------
# bbox extraction (no geometry loading)
# ---------------------------------------------------------------------------


def test_feature_bbox_prefers_explicit_bbox():
    f = {"bbox": [12.0, 41.0, 13.0, 42.0], "geometry": None}
    assert feature_bbox(f) == (12.0, 41.0, 13.0, 42.0)


def test_feature_bbox_orders_and_clamps_corners():
    # Inverted corners + out-of-range latitude get normalized/clamped.
    f = {"bbox": [13.0, 90.0, 12.0, -90.0]}
    w, s, e, n = feature_bbox(f)  # type: ignore[misc]
    assert w == 12.0 and e == 13.0
    assert s >= -85.06 and n <= 85.06


def test_feature_bbox_falls_back_to_geometry_bounds():
    f = {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [2, 0], [2, 3], [0, 3], [0, 0]]],
        },
    }
    assert feature_bbox(f) == (0.0, 0.0, 2.0, 3.0)


def test_feature_bbox_none_when_no_geometry_or_bbox():
    assert feature_bbox({"properties": {}}) is None


def test_feature_bbox_reads_object_attributes():
    class _F:
        bbox = [1.0, 2.0, 3.0, 4.0]
        geometry = None

    assert feature_bbox(_F()) == (1.0, 2.0, 3.0, 4.0)


# ---------------------------------------------------------------------------
# coverage math across a zoom range
# ---------------------------------------------------------------------------


def test_coverage_grows_with_zoom():
    bbox = (12.0, 41.0, 13.0, 42.0)
    z2 = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 2, 2)
    z6 = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 6, 6)
    assert len(z6) >= len(z2)
    # Tile coords carry the tms id and zoom.
    for (tms_id, z, _x, _y) in z6:
        assert tms_id == "WebMercatorQuad"
        assert z == 6


def test_coverage_spans_full_zoom_range():
    bbox = (12.0, 41.0, 13.0, 42.0)
    tiles = affected_tiles_for_batch([bbox], ["WebMercatorQuad"], 0, 4)
    zooms = {z for (_t, z, _x, _y) in tiles}
    assert zooms == {0, 1, 2, 3, 4}


def test_empty_inputs_yield_empty_coverage():
    assert affected_tiles_for_batch([], ["WebMercatorQuad"], 0, 6) == set()
    assert affected_tiles_for_batch([(1, 2, 3, 4)], [], 0, 6) == set()


# ---------------------------------------------------------------------------
# per-batch coalescing + dedup
# ---------------------------------------------------------------------------


def test_overlapping_bboxes_coalesce_and_dedup():
    # Two nearly-identical bboxes at low zoom land on the same tile(s).
    a = (12.0, 41.0, 12.1, 41.1)
    b = (12.05, 41.05, 12.15, 41.15)
    one = affected_tiles_for_batch([a], ["WebMercatorQuad"], 3, 3)
    both = affected_tiles_for_batch([a, b], ["WebMercatorQuad"], 3, 3)
    # Coalesced set is a SET — no duplicate tiles even though two bboxes
    # contributed; at z3 these collapse to the same tile.
    assert both == one


def test_disjoint_bboxes_union():
    a = (12.0, 41.0, 13.0, 42.0)
    b = (-100.0, 30.0, -99.0, 31.0)
    union = affected_tiles_for_batch([a, b], ["WebMercatorQuad"], 5, 5)
    only_a = affected_tiles_for_batch([a], ["WebMercatorQuad"], 5, 5)
    only_b = affected_tiles_for_batch([b], ["WebMercatorQuad"], 5, 5)
    assert union == (only_a | only_b)
    assert only_a.isdisjoint(only_b)


def test_coverage_for_features_skips_features_without_bbox():
    feats = [
        {"bbox": [12.0, 41.0, 13.0, 42.0]},
        {"properties": {}},  # no bbox/geometry — skipped, not an error
    ]
    cov = coverage_for_features(feats, ["WebMercatorQuad"], 4, 4)
    assert cov.bbox_count == 1
    assert len(cov.tiles) >= 1


# ---------------------------------------------------------------------------
# fan-out cap for large bboxes
# ---------------------------------------------------------------------------


def test_cap_clamps_world_bbox():
    world = {"bbox": [-180.0, -85.0, 180.0, 85.0]}
    cov = coverage_for_features(
        [world], ["WebMercatorQuad"], 0, 12, max_tiles=100,
    )
    assert cov.clamped is True
    assert len(cov.tiles) <= 100


def test_cap_keeps_small_bbox_full_range():
    small = {"bbox": [12.0, 41.0, 12.01, 41.01]}
    cov = coverage_for_features(
        [small], ["WebMercatorQuad"], 0, 8, max_tiles=DEFAULT_MAX_TILES_PER_BATCH,
    )
    assert cov.clamped is False
    zooms = {z for (_t, z, _x, _y) in cov.tiles}
    assert max(zooms) == 8


def test_clamp_returns_at_least_min_zoom():
    world = (-180.0, -85.0, 180.0, 85.0)
    # Even an absurdly small cap keeps the coarsest zoom.
    z = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=1)
    assert z == 0


def test_clamp_monotonic_in_cap():
    world = (-180.0, -85.0, 180.0, 85.0)
    low = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=50)
    high = clamp_zoom_range_to_cap([world], ["WebMercatorQuad"], 0, 10, max_tiles=5000)
    assert high >= low


# ---------------------------------------------------------------------------
# payload encode/decode round-trip
# ---------------------------------------------------------------------------


def test_payload_round_trip():
    tiles = {("WebMercatorQuad", 3, 4, 5), ("WebMercatorQuad", 6, 7, 8)}
    payload = tcs._encode_tile_payload(tiles)
    decoded = set(decode_tile_payload(payload))
    assert decoded == tiles


def test_decode_skips_malformed_entries():
    payload = {"tiles": [{"tms_id": "X", "z": 1, "x": 2, "y": 3}, {"bad": True}]}
    assert decode_tile_payload(payload) == [("X", 1, 2, 3)]


# ===========================================================================
# Capability gate + degrade
# ===========================================================================


class _FakeProvider:
    def __init__(self, *, exists_raises: bool = False) -> None:
        self.exists_raises = exists_raises
        self.deleted: List[Tuple] = []

    async def check_tile_exists(self, *args, **kwargs) -> bool:
        if self.exists_raises:
            raise RuntimeError("bucket unreachable")
        return False

    async def delete_tile(self, catalog_id, collection_id, tms_id, z, x, y, fmt) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, fmt))
        return True

    async def delete_tiles_for_collection(self, catalog_id, collection_id) -> int:
        return 0


@pytest.mark.asyncio
async def test_gate_off_when_no_provider(monkeypatch):
    monkeypatch.setattr(tcs, "get_protocol", lambda *_a, **_k: None, raising=False)
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    assert await tcs.is_tile_cache_active("cat", "col") is False


@pytest.mark.asyncio
async def test_gate_on_when_provider_usable(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    assert await tcs.is_tile_cache_active("cat", "col") is True


@pytest.mark.asyncio
async def test_gate_degrades_when_store_unusable(monkeypatch):
    provider = _FakeProvider(exists_raises=True)
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    # Store probe raises → degrade to disabled (warn + False), never raise.
    assert await tcs.is_tile_cache_active("cat", "col") is False


# ===========================================================================
# Enqueue side (OUTBOX, atomic) — with a fake OutboxStore
# ===========================================================================


class _FakeOutbox:
    def __init__(self) -> None:
        self.rows: List[Any] = []
        self.conns: List[Any] = []

    async def enqueue_bulk(self, conn=None, *, catalog_id: str, rows) -> None:
        self.conns.append(conn)
        self.rows.extend(rows)


@pytest.mark.asyncio
async def test_enqueue_builds_single_coalesced_row(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _extent(_cat, _col):
        return ["WebMercatorQuad"], 0, 4

    monkeypatch.setattr(tcs, "_resolve_tile_extent", _extent)

    outbox = _FakeOutbox()
    sentinel_conn = object()
    feats = [
        {"id": "a", "bbox": [12.0, 41.0, 13.0, 42.0]},
        {"id": "b", "bbox": [12.5, 41.5, 13.5, 42.5]},
    ]
    n = await tcs.enqueue_tile_invalidations(
        sentinel_conn, "cat", "col", feats, outbox=outbox,
    )
    assert n > 0
    # ONE coalesced outbox row for the whole batch — not one per feature/tile.
    assert len(outbox.rows) == 1
    rec = outbox.rows[0]
    assert rec.driver_id == TILE_CACHE_DRIVER_ID
    assert rec.collection_id == "col"
    # Enqueued on the caller's connection (atomic with the write txn).
    assert outbox.conns == [sentinel_conn]
    # Payload carries the coalesced tile set.
    decoded = decode_tile_payload(rec.payload)
    assert len(decoded) == n


@pytest.mark.asyncio
async def test_enqueue_noop_when_inactive(monkeypatch):
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    outbox = _FakeOutbox()
    n = await tcs.enqueue_tile_invalidations(
        object(), "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}], outbox=outbox,
    )
    assert n == 0
    assert outbox.rows == []


@pytest.mark.asyncio
async def test_enqueue_never_raises_on_outbox_error(monkeypatch):
    provider = _FakeProvider()
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)

    async def _extent(_cat, _col):
        return ["WebMercatorQuad"], 0, 2

    monkeypatch.setattr(tcs, "_resolve_tile_extent", _extent)

    class _BoomOutbox:
        async def enqueue_bulk(self, *a, **k):
            raise RuntimeError("outbox down")

    # The enqueue swallows the error and returns 0 (write must not break).
    n = await tcs.enqueue_tile_invalidations(
        object(), "cat", "col", [{"id": "a", "bbox": [12, 41, 13, 42]}],
        outbox=_BoomOutbox(),
    )
    assert n == 0


# ===========================================================================
# Drain-side TileCacheBulkIndexer — with a fake provider
# ===========================================================================


def _make_op(tiles, *, op_id=None, catalog="cat", collection="col"):
    from uuid import uuid4

    from dynastore.models.protocols.indexing import IndexableOp

    return IndexableOp(
        op_id=op_id or uuid4(),
        op="upsert",
        catalog_id=catalog,
        collection_id=collection,
        driver_instance_id="inst",
        item_id=None,
        payload=tcs._encode_tile_payload(set(tiles)),
        idempotency_key="k",
    )


@pytest.mark.asyncio
async def test_indexer_deletes_tiles_and_passes():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    provider = _FakeProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    tiles = {("WebMercatorQuad", 3, 4, 5), ("WebMercatorQuad", 4, 8, 10)}
    op = _make_op(tiles)
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    assert result.transient == [] and result.poison == []
    assert len(provider.deleted) == 2


@pytest.mark.asyncio
async def test_indexer_idempotent_on_absent_tiles():
    # delete_tile returning True for an absent tile → still passes (mark-stale
    # is idempotent / order-tolerant).
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    class _AbsentProvider(_FakeProvider):
        async def delete_tile(self, *a, **k) -> bool:
            return True  # absent tile delete = no-op success

    indexer = TileCacheBulkIndexer(provider=_AbsentProvider())
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]


@pytest.mark.asyncio
async def test_indexer_retries_when_no_provider(monkeypatch):
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    indexer = TileCacheBulkIndexer(provider=None)
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    # Whole op → transient (retry); the store may come back.
    assert [oid for oid, _r in result.transient] == [op.op_id]
    assert result.passed == []


@pytest.mark.asyncio
async def test_indexer_retries_on_store_error():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    class _BoomProvider(_FakeProvider):
        async def delete_tile(self, *a, **k) -> bool:
            raise RuntimeError("store hiccup")

    indexer = TileCacheBulkIndexer(provider=_BoomProvider())
    op = _make_op({("WebMercatorQuad", 3, 4, 5)})
    result = await indexer.index_bulk([op])
    assert [oid for oid, _r in result.transient] == [op.op_id]


@pytest.mark.asyncio
async def test_indexer_passes_empty_payload():
    from dynastore.tasks.outbox_drain.tile_cache_indexer_adapter import (
        TileCacheBulkIndexer,
    )

    from dynastore.models.protocols.indexing import IndexableOp
    from uuid import uuid4

    provider = _FakeProvider()
    indexer = TileCacheBulkIndexer(provider=provider)
    op = IndexableOp(
        op_id=uuid4(), op="upsert", catalog_id="cat", collection_id="col",
        driver_instance_id="inst", item_id=None, payload={"tiles": []},
        idempotency_key="k",
    )
    result = await indexer.index_bulk([op])
    assert result.passed == [op.op_id]
    assert provider.deleted == []


# ===========================================================================
# Config reactivity — reconcile
# ===========================================================================


@pytest.mark.asyncio
async def test_reconcile_purges_when_cache_disabled(monkeypatch):
    provider = _FakeProvider()
    purged: List[Tuple[str, str]] = []

    async def _invalidate(cat, col):
        purged.append((cat, col))

    async def _load_cfg():
        class _Cfg:
            cache_enabled = False
        return _Cfg()

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    import dynastore.modules.tiles.tiles_module as _tm
    monkeypatch.setattr(_tm, "invalidate_collection_tiles", _invalidate)
    import dynastore.modules.gcp.tiles_storage as _gcs
    monkeypatch.setattr(_gcs, "_load_caching_config", _load_cfg)

    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "purged_cache_disabled"
    assert purged == [("cat", "col")]


@pytest.mark.asyncio
async def test_reconcile_reconciles_when_enabled(monkeypatch):
    provider = _FakeProvider()
    purged: List[Tuple[str, str]] = []

    async def _invalidate(cat, col):
        purged.append((cat, col))

    async def _load_cfg():
        class _Cfg:
            cache_enabled = True
        return _Cfg()

    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: provider)
    import dynastore.modules.tiles.tiles_module as _tm
    monkeypatch.setattr(_tm, "invalidate_collection_tiles", _invalidate)
    import dynastore.modules.gcp.tiles_storage as _gcs
    monkeypatch.setattr(_gcs, "_load_caching_config", _load_cfg)

    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "reconciled"
    assert purged == [("cat", "col")]


@pytest.mark.asyncio
async def test_reconcile_noop_without_provider(monkeypatch):
    import dynastore.modules as _mods
    monkeypatch.setattr(_mods, "get_protocol", lambda *_a, **_k: None)
    report = await tcs.reconcile_tile_cache("cat", "col")
    assert report["action"] == "no_provider"
