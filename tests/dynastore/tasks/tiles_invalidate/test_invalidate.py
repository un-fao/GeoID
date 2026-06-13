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

"""``TileInvalidateTask`` delete-only path tests.

DB-free: drives ``TileInvalidateTask.run`` with a fake config manager and a
fake ``TileStorageProtocol`` that records deletes. The task reads inputs from
``payload.inputs.inputs`` (OGC Process dispatch pattern), so each test builds
the payload with the request dict nested under ExecuteRequest.inputs — the
same envelope shape used in production.
"""
from __future__ import annotations

from typing import List, Sequence, Tuple

import pytest

pytest.importorskip("morecantile")  # optional dep — skip when SCOPE excludes it

import dynastore.tasks.tiles_invalidate.task as task_mod
from dynastore.tasks.tiles_invalidate.task import TileInvalidateTask
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.tiles.tiles_config import TilesConfig, TilesPreseedConfig
from dynastore.modules.tiles.tile_cache_sync import (
    SERVED_TILE_FORMATS,
    affected_tiles_for_batch,
)


class _FakeStore:
    """Fake TileStorageProtocol recording coordinate-variant deletes."""

    def __init__(self) -> None:
        self.variant_calls: List[Tuple] = []
        self.save_tile_calls: List[Tuple] = []

    async def delete_tile_variants(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        formats: Sequence[str],
    ) -> bool:
        self.variant_calls.append(
            (catalog_id, collection_id, tms_id, z, x, y, tuple(formats))
        )
        return True

    async def save_tile(self, *args, **kwargs) -> None:
        self.save_tile_calls.append((args, kwargs))


class _LegacyStore:
    """Fake provider WITHOUT the variant primitive — exercises per-format fallback."""

    def __init__(self) -> None:
        self.deleted: List[Tuple] = []

    async def delete_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
    ) -> bool:
        self.deleted.append((catalog_id, collection_id, tms_id, z, x, y, format))
        return True


class _FakeConfigManager:
    def __init__(self, preseed: TilesPreseedConfig, runtime: TilesConfig) -> None:
        self._preseed = preseed
        self._runtime = runtime

    async def get_config(self, model, catalog_id, collection_id=None):
        if model is TilesPreseedConfig:
            return self._preseed
        if model is TilesConfig:
            return self._runtime
        return None


class _FakeCatalogs:
    async def resolve_physical_schema(self, catalog_id, ctx=None):
        return "s_" + catalog_id


def _make_task() -> TileInvalidateTask:
    # Bypass __init__ (which calls get_engine()) and inject the engine directly.
    # run() wraps the engine in DriverContext, whose db_resource is validated as a
    # real SQLAlchemy (Async)Engine — so a bare object() is rejected. This engine
    # is NEVER connected: schema resolution is faked (_FakeCatalogs), the deletes
    # hit the fake store, and task_id is None so update_task is skipped.
    from sqlalchemy.ext.asyncio import create_async_engine

    task = TileInvalidateTask.__new__(TileInvalidateTask)
    task.engine = create_async_engine(
        "postgresql+asyncpg://localhost/_tiles_invalidate_test"
    )
    return task


def _make_payload(request_dict: dict) -> TaskPayload:
    """Build a TaskPayload embedding request_dict as the OGC ExecuteRequest inner inputs.

    task_id is set to None (after construction, since the model requires a UUID)
    so the run() path skips the DB update_task calls — we only test the delete
    fan-out.
    """
    from uuid import uuid4

    payload = TaskPayload(
        task_id=uuid4(),
        caller_id="test",
        inputs=ExecuteRequest(inputs=request_dict),
    )
    payload.task_id = None  # type: ignore[assignment]
    return payload


def _patch_protocols(monkeypatch, store, config_manager, catalogs=None):
    """Patch get_protocol to return appropriate fakes."""
    from dynastore.modules.tiles.tiles_module import TileStorageProtocol
    from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol

    def _get_protocol(proto):
        if proto is TileStorageProtocol:
            return store
        if proto is ConfigsProtocol:
            return config_manager
        if proto is CatalogsProtocol:
            return catalogs or _FakeCatalogs()
        return None

    monkeypatch.setattr(task_mod, "get_protocol", _get_protocol)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalidate_deletes_covered_tiles(monkeypatch):
    """TileInvalidateTask.run deletes all tiles covering the request bbox."""
    store = _FakeStore()
    runtime = TilesConfig(
        min_zoom=0, max_zoom=2, supported_tms_ids=["WebMercatorQuad"]
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, store, cfgmgr)

    bbox = [12.0, 41.0, 13.0, 42.0]
    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [tuple(bbox)],
    })

    task = _make_task()
    result = await task.run(payload)
    assert result is None

    expected = affected_tiles_for_batch(
        [tuple(bbox)], ["WebMercatorQuad"], 0, 2,
    )
    assert len(store.variant_calls) == len(expected)

    deleted_coords = set()
    for (cat, col, tms_id, z, x, y, formats) in store.variant_calls:
        assert (cat, col) == ("cat", "col")
        assert set(formats) == set(SERVED_TILE_FORMATS)
        deleted_coords.add((tms_id, z, x, y))
    assert deleted_coords == expected


@pytest.mark.asyncio
async def test_invalidate_legacy_fallback_deletes_each_format(monkeypatch):
    """Falls back to per-format delete_tile when delete_tile_variants is absent."""
    store = _LegacyStore()
    runtime = TilesConfig(
        min_zoom=5, max_zoom=5, supported_tms_ids=["WebMercatorQuad"]
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, store, cfgmgr)

    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [(12.0, 41.0, 12.01, 41.01)],
    })

    task = _make_task()
    await task.run(payload)

    assert len(store.deleted) >= len(SERVED_TILE_FORMATS)
    deleted_formats = {d[6] for d in store.deleted}
    assert deleted_formats == set(SERVED_TILE_FORMATS)


@pytest.mark.asyncio
async def test_invalidate_degrades_when_no_store(monkeypatch):
    """Missing TileStorageProtocol logs a warning and completes without error."""
    runtime = TilesConfig(
        min_zoom=0, max_zoom=2, supported_tms_ids=["WebMercatorQuad"]
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, None, cfgmgr)

    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [(12.0, 41.0, 13.0, 42.0)],
    })

    task = _make_task()
    result = await task.run(payload)
    assert result is None


@pytest.mark.asyncio
async def test_invalidate_uses_request_update_bbox(monkeypatch):
    """update_bbox from the request drives coverage (not the config bbox)."""
    store = _FakeStore()
    runtime = TilesConfig(
        min_zoom=4,
        max_zoom=4,
        supported_tms_ids=["WebMercatorQuad"],
        bbox=[(-180.0, -85.0, 180.0, 85.0)],
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, store, cfgmgr)

    small = (12.0, 41.0, 12.01, 41.01)
    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [small],
    })

    task = _make_task()
    await task.run(payload)

    expected = affected_tiles_for_batch([small], ["WebMercatorQuad"], 4, 4)
    assert len(store.variant_calls) == len(expected)
    world = affected_tiles_for_batch(
        [(-180.0, -85.0, 180.0, 85.0)], ["WebMercatorQuad"], 4, 4
    )
    assert len(expected) < len(world)


@pytest.mark.asyncio
async def test_invalidate_no_render_no_save(monkeypatch):
    """TileInvalidateTask never calls save_tile — it is a delete-only task."""
    store = _FakeStore()
    runtime = TilesConfig(
        min_zoom=0, max_zoom=1, supported_tms_ids=["WebMercatorQuad"]
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, store, cfgmgr)

    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [(10.0, 40.0, 11.0, 41.0)],
    })

    task = _make_task()
    await task.run(payload)

    assert store.save_tile_calls == [], "TileInvalidateTask must never call save_tile"
    assert len(store.variant_calls) > 0, "deletes should have occurred"


@pytest.mark.asyncio
async def test_invalidate_respects_bbox_cap(monkeypatch):
    """Coverage is bounded — world bbox at high zoom is capped, not unbounded."""
    store = _FakeStore()
    runtime = TilesConfig(
        min_zoom=0, max_zoom=10, supported_tms_ids=["WebMercatorQuad"]
    )
    preseed = TilesPreseedConfig(
        preseed_enabled=True, target_tms_ids=["WebMercatorQuad"]
    )
    cfgmgr = _FakeConfigManager(preseed, runtime)
    _patch_protocols(monkeypatch, store, cfgmgr)

    payload = _make_payload({
        "catalog_id": "cat",
        "collection_id": "col",
        "update_bbox": [(-180.0, -85.0, 180.0, 85.0)],
    })

    task = _make_task()
    await task.run(payload)

    from dynastore.modules.tiles.tile_cache_sync import DEFAULT_MAX_TILES_PER_BATCH

    assert len(store.variant_calls) <= DEFAULT_MAX_TILES_PER_BATCH, (
        "tile invalidation must respect the cap — world bbox at high zoom "
        f"produced {len(store.variant_calls)} deletes, cap is {DEFAULT_MAX_TILES_PER_BATCH}"
    )
