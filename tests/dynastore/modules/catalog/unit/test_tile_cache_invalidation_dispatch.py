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

"""``ItemService._dispatch_tile_cache_invalidation`` (#1292).

Verifies the write-path hook:
* enqueues invalidations on a wrapping TX (atomic with the obligation),
* is a no-op for an empty batch,
* NEVER raises out — a cache failure must not break the write.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, List

import pytest

import dynastore.modules.catalog.item_service as item_service_mod
import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.modules.catalog.item_service import ItemService


@asynccontextmanager
async def _fake_tx(_engine):
    yield "fake-conn"


@pytest.mark.asyncio
async def test_dispatch_enqueues_on_wrapping_tx(monkeypatch):
    calls: List[Any] = []

    async def _fake_enqueue(conn, catalog_id, collection_id, features, **kw):
        calls.append((conn, catalog_id, collection_id, list(features)))
        return len(features)

    monkeypatch.setattr(tcs, "enqueue_tile_invalidations", _fake_enqueue)
    monkeypatch.setattr(item_service_mod, "managed_transaction", _fake_tx)

    svc = ItemService(engine=object())  # type: ignore[arg-type]
    feats = [{"id": "a", "bbox": [0, 0, 1, 1]}]
    await svc._dispatch_tile_cache_invalidation("cat", "col", feats)  # type: ignore[arg-type]

    assert len(calls) == 1
    conn, cat, col, passed = calls[0]
    assert conn == "fake-conn"  # enqueued on the wrapping TX conn
    assert (cat, col) == ("cat", "col")
    assert passed == feats


@pytest.mark.asyncio
async def test_dispatch_noop_on_empty(monkeypatch):
    called = False

    async def _fake_enqueue(*a, **k):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(tcs, "enqueue_tile_invalidations", _fake_enqueue)
    svc = ItemService(engine=object())  # type: ignore[arg-type]
    await svc._dispatch_tile_cache_invalidation("cat", "col", [])  # type: ignore[arg-type]
    assert called is False


@pytest.mark.asyncio
async def test_dispatch_never_raises(monkeypatch):
    async def _boom_enqueue(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(tcs, "enqueue_tile_invalidations", _boom_enqueue)
    monkeypatch.setattr(item_service_mod, "managed_transaction", _fake_tx)

    svc = ItemService(engine=object())  # type: ignore[arg-type]
    # Must not raise — invalidation failure cannot break the write.
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_dispatch_uses_pool_conn_without_engine(monkeypatch):
    seen_conn: List[Any] = []

    async def _fake_enqueue(conn, catalog_id, collection_id, features, **kw):
        seen_conn.append(conn)
        return 0

    monkeypatch.setattr(tcs, "enqueue_tile_invalidations", _fake_enqueue)
    svc = ItemService(engine=None)  # type: ignore[arg-type]
    await svc._dispatch_tile_cache_invalidation(
        "cat", "col", [{"id": "a", "bbox": [0, 0, 1, 1]}],  # type: ignore[arg-type]
    )
    # No engine → enqueue with conn=None (own-pool, still durable).
    assert seen_conn == [None]
