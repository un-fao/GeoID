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

"""Routing-driven fallback in ``CollectionService.list_collections``.

``list_collections`` no longer carries a hand-rolled
``collection_core``⋈``collection_stac`` fallback query.  Instead it asks the
collection-metadata router for the SEARCH-routed driver first and, when that
slice is empty (e.g. the public_catalog preset routes collection SEARCH to ES
only and the ES collection index has no rows for the catalog yet), re-runs the
same query against the READ-routed driver — PG under every preset, the system
of record.  The composition PG driver hydrates its STAC slice, so the rows
that come back are COMPLETE collections regardless of which slice served them.
"""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock

import pytest

from dynastore.modules.catalog import collection_router, collection_service
from dynastore.modules.storage.routing_config import Operation

_COMPLETE_ROW = {
    "id": "coll-a",
    "title": {"en": "A complete collection"},
    "description": {"en": "carries STAC fields"},
    "license": "CC-BY-4.0",
    "extent": {
        "spatial": {"bbox": [[-10.0, 35.0, 30.0, 60.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    },
    "providers": [{"name": "FAO", "roles": ["producer"]}],
}


@pytest.mark.asyncio
async def test_list_collections_serves_complete_rows_from_search_without_fallback(
    monkeypatch,
):
    """When the SEARCH slice already returns rows (e.g. private_catalog routes
    collection SEARCH PG-first, and the composition driver hydrated STAC),
    list_collections returns them directly and does NOT hit the READ fallback.
    """
    ops: List[str] = []

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        return [_COMPLETE_ROW], 1

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat")

    assert [c.id for c in out] == ["coll-a"]
    assert out[0].extent is not None            # STAC field survived
    assert ops == [Operation.SEARCH]            # READ fallback NOT reached


@pytest.mark.asyncio
async def test_list_collections_falls_back_to_read_routing_when_search_empty(
    monkeypatch,
):
    """When the SEARCH slice is empty (ES-only routing, index not populated),
    list_collections re-runs against the READ-routed driver (PG) and returns
    the complete rows it yields — the replacement for the old hand-rolled SQL.
    """
    ops: List[str] = []

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        if operation == Operation.SEARCH:
            return [], 0
        return [_COMPLETE_ROW], 1

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat")

    assert [c.id for c in out] == ["coll-a"]
    assert out[0].extent is not None            # complete via READ routing
    assert ops == [Operation.SEARCH, Operation.READ]


@pytest.mark.asyncio
async def test_list_collections_empty_when_no_slice_has_rows(monkeypatch):
    """Genuinely empty catalog: both SEARCH and READ return nothing → []."""
    ops: List[str] = []

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        return [], 0

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat")

    assert out == []
    assert ops == [Operation.SEARCH, Operation.READ]


@pytest.mark.asyncio
async def test_list_collections_read_fallback_survives_search_exception(
    monkeypatch,
):
    """A raised SEARCH (transient driver error) must not abort the listing —
    the READ-routed driver still answers from the system of record.
    """
    ops: List[str] = []

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        if operation == Operation.SEARCH:
            raise RuntimeError("transient ES outage")
        return [_COMPLETE_ROW], 1

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat")

    assert [c.id for c in out] == ["coll-a"]
    assert ops == [Operation.SEARCH, Operation.READ]
