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

"""Unit tests for the cross-catalog ``/search`` STAC routes.

Pins the thin route layer over ``modules/elasticsearch/global_search``:
GET parameter parsing, POST body validation (no ``catalog_id``, no CQL2
``filter`` at this surface), the no-backend → HTTP 501 mapping, and the
``_catalog_id`` / ``_collection_id`` serializer markers injected per hit.
"""

from __future__ import annotations

from typing import Any, Dict

import pytest
from fastapi import HTTPException

from dynastore.extensions.stac.search import ItemSearchRequest
from dynastore.extensions.stac.stac_service import STACService
from dynastore.modules.elasticsearch.global_search import (
    GlobalSearchHit,
    GlobalSearchPage,
)


class _Req:
    """Minimal Request stand-in — the serializer is monkeypatched away."""


def _service() -> STACService:
    return object.__new__(STACService)


def _item(iid: str, collection: str) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "id": iid,
        "collection": collection,
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }


@pytest.fixture
def capture(monkeypatch):
    """Patch the engine + serializer; capture what the route threads through."""
    calls: Dict[str, Any] = {"engine": None, "serializer": None}
    page = GlobalSearchPage(
        hits=[
            GlobalSearchHit(catalog_id="cat-a", item=_item("i1", "c1")),
            GlobalSearchHit(catalog_id=None, item=_item("i2", "c2")),
        ],
        total=7,
    )

    async def fake_engine(**kw):
        calls["engine"] = kw
        return calls.get("page_override", page)

    async def fake_serializer(request, features, total, limit, offset, cfg, lang="en"):
        calls["serializer"] = {
            "features": features, "total": total,
            "limit": limit, "offset": offset,
        }
        return {"type": "FeatureCollection", "numberMatched": total}

    import dynastore.modules.elasticsearch.global_search as gs
    import dynastore.extensions.stac.stac_generator as gen

    monkeypatch.setattr(gs, "search_public_items", fake_engine)
    monkeypatch.setattr(gen, "create_search_results_collection", fake_serializer)
    return calls


@pytest.mark.asyncio
async def test_get_parses_params_and_threads_to_engine(capture):
    svc = _service()
    await svc.search_items_global_get(
        _Req(),
        language="en",
        bbox="1,2,3,4",
        datetime="2024-01-01T00:00:00Z/..",
        ids="i1, i2",
        collections="c1,c2",
        limit=5,
        offset=10,
        sortby="-datetime,+id",
    )
    kw = capture["engine"]
    assert kw["ids"] == ["i1", "i2"]
    assert kw["collections"] == ["c1", "c2"]
    assert kw["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert kw["datetime"] == "2024-01-01T00:00:00Z/.."
    assert kw["sortby"] == ["-datetime", "+id"]
    assert kw["limit"] == 5 and kw["offset"] == 10


@pytest.mark.asyncio
async def test_get_rejects_malformed_bbox(capture):
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        kw = _get_defaults(); kw["bbox"] = "1,2,3"
        await svc.search_items_global_get(_Req(), language="en", **kw)
    assert exc.value.status_code == 400


def _get_defaults() -> Dict[str, Any]:
    """Explicit None defaults — a direct call bypasses FastAPI's Query()
    resolution, so omitted params would otherwise arrive as Query sentinels."""
    return dict(
        bbox=None, datetime=None, ids=None, collections=None,
        limit=10, offset=0, sortby=None,
    )


@pytest.mark.asyncio
async def test_hits_carry_catalog_markers_for_the_serializer(capture):
    svc = _service()
    await svc.search_items_global_get(_Req(), language="en", **_get_defaults())
    feats = capture["serializer"]["features"]
    assert [f.properties["_catalog_id"] for f in feats] == ["cat-a", ""]
    assert [f.properties["_collection_id"] for f in feats] == ["c1", "c2"]
    assert capture["serializer"]["total"] == 7


@pytest.mark.asyncio
async def test_no_backend_maps_to_501(capture):
    capture["page_override"] = None
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_items_global_get(_Req(), language="en", **_get_defaults())
    assert exc.value.status_code == 501


@pytest.mark.asyncio
async def test_post_rejects_catalog_id(capture):
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_items_global_post(
            _Req(), ItemSearchRequest(catalog_id="cat-1"), language="en"
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_post_rejects_cql2_filter(capture):
    body = ItemSearchRequest.model_construct(filter=object())
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_items_global_post(_Req(), body, language="en")
    assert exc.value.status_code == 501


@pytest.mark.asyncio
async def test_post_threads_structural_fields(capture):
    body = ItemSearchRequest(
        collections=["c9"], ids=["i9"], bbox=(0.0, 0.0, 1.0, 1.0),
        datetime="2024-02-02T00:00:00Z", limit=3, offset=6,
    )
    svc = _service()
    await svc.search_items_global_post(_Req(), body, language="en")
    kw = capture["engine"]
    assert kw["collections"] == ["c9"] and kw["ids"] == ["i9"]
    assert kw["bbox"] == [0.0, 0.0, 1.0, 1.0]
    assert kw["limit"] == 3 and kw["offset"] == 6
