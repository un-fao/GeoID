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

"""Unit tests pinning collection expansion for unscoped STAC ``/search``.

A catalog-scoped ``/search`` with no ``collections`` parameter must still be
servable by the routing-resolved search driver. Before expansion, the
search-driver dispatch (``_maybe_dispatch_to_es_search``) declined unscoped
requests, and the PostgreSQL fallback then dropped every collection whose READ
driver carries no PG layer config — so a catalog whose items routing pins an
ES search driver answered ``numberMatched: 0`` to ``GET /search`` and to
``ids``-only lookups while the same query scoped with ``collections=`` matched.

``_expand_collections_for_search`` closes that hole: an unscoped request is
rewritten to explicitly scope ALL collections of the catalog (single
``list_collections`` round-trip, reused downstream), after which the dispatch
decision matrix applies unchanged. An already-scoped request and an empty
catalog pass through untouched.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pytest

from dynastore.extensions.stac.search import (
    ItemSearchRequest,
    _expand_collections_for_search,
)


@dataclass
class _Coll:
    id: str


class _FakeCatalogs:
    """CatalogsProtocol stand-in — records ``list_collections`` calls."""

    def __init__(self, collection_ids: List[str]):
        self._ids = collection_ids
        self.calls: list = []

    async def list_collections(
        self, catalog_id: str, *args: Any, limit: int = 1000, ctx: Any = None, **kw: Any
    ) -> List[_Coll]:
        self.calls.append({"catalog_id": catalog_id, "limit": limit})
        return [_Coll(cid) for cid in self._ids]


def _request(collections: Optional[List[str]] = None) -> ItemSearchRequest:
    return ItemSearchRequest(catalog_id="cat-1", collections=collections, limit=5)


@pytest.mark.asyncio
async def test_unscoped_request_expands_to_all_catalog_collections():
    catalogs = _FakeCatalogs(["c-alpha", "c-beta"])
    req = _request(collections=None)

    out = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out.collections == ["c-alpha", "c-beta"]
    assert catalogs.calls and catalogs.calls[0]["catalog_id"] == "cat-1"
    # The original request object is not mutated in place.
    assert req.collections is None


@pytest.mark.asyncio
async def test_scoped_request_passes_through_without_enumeration():
    catalogs = _FakeCatalogs(["c-alpha", "c-beta"])
    req = _request(collections=["c-alpha"])

    out = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out is req
    assert catalogs.calls == []


@pytest.mark.asyncio
async def test_empty_catalog_passes_through_unscoped():
    catalogs = _FakeCatalogs([])
    req = _request(collections=None)

    out = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out is req
    assert out.collections is None


@pytest.mark.asyncio
async def test_expansion_preserves_other_request_fields():
    catalogs = _FakeCatalogs(["c-alpha"])
    req = ItemSearchRequest(
        catalog_id="cat-1",
        collections=None,
        ids=["item-1"],
        datetime="2024-01-01T00:00:00Z/..",
        limit=7,
        offset=3,
    )

    out = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out.collections == ["c-alpha"]
    assert out.ids == ["item-1"]
    assert out.datetime == "2024-01-01T00:00:00Z/.."
    assert out.limit == 7 and out.offset == 3
