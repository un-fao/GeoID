"""Routing-honouring fallback when the ES items index is absent.

Items READ/SEARCH route to the routing config's ordered driver list. The
default lists the ES items driver first and the PostgreSQL read driver second.
ES-first is an optimisation; PG is the WRITE primary / system of record, so it
always holds every item.

Previously, when the ES driver was selected but its per-tenant index did not
exist (a PG-only catalog, or indexing had not yet run), the dispatch returned
an empty ES result and the PG-resident rows were invisible (the #914
silent-empty consequence: ``/items`` returned 0 and single-item GET 404 even
though the row was in PG).

Both dispatch entry points now ask the ES driver whether its index is
available; when it is not, they return ``None`` so the configured PG fallback
(``stream_items``) serves the listing. Drivers without an ``index_available``
method keep their prior behaviour (the guard is a no-op for them).
"""

from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.extensions.tools.query import maybe_dispatch_items_to_search_driver
from dynastore.models.ogc import Feature
from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.catalog import item_query
from dynastore.modules.storage.routing_config import Operation


def _feat(fid: str) -> Feature:
    return Feature.model_validate({
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    })


class _FakeEsItemsDriver:
    """ES items driver reporting per-tenant index availability."""

    is_es_items_driver = True
    capabilities = frozenset({Capability.READ})

    def __init__(self, features: List[Any], total: int, index_available: bool):
        self._features = features
        self._total = total
        self._index_available = index_available
        self.read_calls: list = []
        self.count_calls: list = []

    async def index_available(self, catalog_id: str) -> bool:
        return self._index_available

    async def read_entities(self, catalog_id, collection_id, *, entity_ids=None,
                            request=None, context=None, limit=100, offset=0,
                            db_resource=None):
        self.read_calls.append(collection_id)
        for f in self._features:
            yield f

    async def count_entities(self, catalog_id, collection_id, *, request=None,
                             db_resource=None):
        self.count_calls.append(collection_id)
        return self._total


# --------------------------------------------------------------------------
# SEARCH / list path: maybe_dispatch_items_to_search_driver
# --------------------------------------------------------------------------

def _patch_search_resolver(monkeypatch, driver):
    import dynastore.modules.storage.router as _router

    async def _fake(cat, cid=None, **_kw):
        from types import SimpleNamespace
        return SimpleNamespace(driver=driver)

    monkeypatch.setattr(_router, "get_items_search_driver", _fake)


@pytest.mark.asyncio
async def test_search_dispatch_degrades_to_pg_when_index_absent(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("x")], total=1, index_available=False)
    _patch_search_resolver(monkeypatch, drv)
    resp = await maybe_dispatch_items_to_search_driver(
        catalog_id="cat-x", collection_id="col-a", bbox=None, intersects=None,
        datetime=None, ids=None, limit=10, offset=0, has_complex_filter=False,
    )
    assert resp is None                # defers to the PG stream_items path
    assert not drv.read_calls          # the index-less ES driver was never queried
    assert not drv.count_calls


@pytest.mark.asyncio
async def test_search_dispatch_uses_es_when_index_present(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("y")], total=1, index_available=True)
    _patch_search_resolver(monkeypatch, drv)
    resp = await maybe_dispatch_items_to_search_driver(
        catalog_id="cat-x", collection_id="col-a", bbox=None, intersects=None,
        datetime=None, ids=None, limit=10, offset=0, has_complex_filter=False,
    )
    assert resp is not None
    features = [f async for f in resp]
    assert [f.id for f in features] == ["y"]


# --------------------------------------------------------------------------
# READ / browse path: item_query._try_driver_dispatch (also feeds get_item)
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_browse_dispatch_degrades_to_pg_when_index_absent():
    drv = _FakeEsItemsDriver(features=[_feat("z")], total=1, index_available=False)
    with patch(
        "dynastore.modules.storage.router.get_driver",
        AsyncMock(return_value=drv),
    ):
        resp = await item_query._try_driver_dispatch(
            "cat-x", "col-a", Operation.READ, None, 10, 0,
        )
    assert resp is None                # PG path serves it instead
    assert not drv.read_calls


@pytest.mark.asyncio
async def test_browse_dispatch_uses_es_when_index_present():
    drv = _FakeEsItemsDriver(features=[_feat("w")], total=1, index_available=True)
    with patch(
        "dynastore.modules.storage.router.get_driver",
        AsyncMock(return_value=drv),
    ):
        resp = await item_query._try_driver_dispatch(
            "cat-x", "col-a", Operation.READ, None, 10, 0,
        )
    assert resp is not None
