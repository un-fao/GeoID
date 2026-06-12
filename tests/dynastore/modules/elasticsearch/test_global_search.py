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

"""Unit tests for cross-catalog search over the platform public items alias.

Pins the ``search_public_items`` contract: alias-scoped query construction,
catalog attribution from the concrete per-hit index name
(``{prefix}-{catalog_id}-items``), exact totals via ``track_total_hits``,
the no-client → ``None`` (HTTP 501 upstream) and absent-alias → empty-page
mappings, and pagination/sort threading.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from dynastore.modules.elasticsearch.global_search import (
    GlobalSearchPage,
    catalog_id_from_index,
    search_public_items,
)


class _FakeEs:
    """Records the search call and returns a canned ES response."""

    def __init__(self, response: Dict[str, Any]):
        self._response = response
        self.calls: List[Dict[str, Any]] = []

    async def search(self, *, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append({"index": index, "body": body})
        return self._response


class _NotFoundEs:
    status_code = 404

    async def search(self, *, index: str, body: Dict[str, Any]):
        exc = Exception("index_not_found_exception")
        exc.status_code = 404  # type: ignore[attr-defined]
        raise exc


def _hit(index: str, source: Dict[str, Any]) -> Dict[str, Any]:
    return {"_index": index, "_source": source}


def _response(hits: List[Dict[str, Any]], total: int) -> Dict[str, Any]:
    return {"hits": {"total": {"value": total}, "hits": hits}}


@pytest.fixture(autouse=True)
def _fixed_prefix(monkeypatch):
    from dynastore.modules.elasticsearch import client as es_client

    monkeypatch.setattr(es_client, "get_index_prefix", lambda: "dynastore-test")


# ── catalog attribution from index name ─────────────────────────────────


def test_catalog_id_from_index_simple():
    assert catalog_id_from_index("p-cat1-items", "p") == "cat1"


def test_catalog_id_from_index_with_dashes_in_catalog_and_prefix():
    assert (
        catalog_id_from_index("dynastore-dev-es-drain-probe-items", "dynastore-dev")
        == "es-drain-probe"
    )


def test_catalog_id_from_index_rejects_foreign_index():
    assert catalog_id_from_index("other-index", "dynastore-dev") is None
    assert catalog_id_from_index("dynastore-dev--items", "dynastore-dev") is None


# ── engine contract ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_no_client_returns_none(monkeypatch):
    from dynastore.modules.elasticsearch import client as es_client

    monkeypatch.setattr(es_client, "get_client", lambda: None)
    assert await search_public_items() is None


@pytest.mark.asyncio
async def test_absent_alias_maps_to_empty_page():
    page = await search_public_items(es_client=_NotFoundEs())
    assert isinstance(page, GlobalSearchPage)
    assert page.hits == [] and page.total == 0


@pytest.mark.asyncio
async def test_queries_platform_alias_with_pagination_and_total():
    es = _FakeEs(
        _response(
            [
                _hit(
                    "dynastore-test-cat-a-items",
                    {"type": "Feature", "id": "i1", "collection": "c1",
                     "geometry": None, "properties": {}},
                ),
                _hit(
                    "dynastore-test-cat-b-items",
                    {"type": "Feature", "id": "i2", "collection": "c2",
                     "geometry": None, "properties": {}},
                ),
            ],
            total=42,
        )
    )

    page = await search_public_items(limit=2, offset=10, es_client=es)

    assert page is not None
    call = es.calls[0]
    assert call["index"] == "dynastore-test-items"
    assert call["body"]["from"] == 10 and call["body"]["size"] == 2
    assert call["body"]["track_total_hits"] is True
    assert page.total == 42
    assert [h.catalog_id for h in page.hits] == ["cat-a", "cat-b"]
    assert [h.item["id"] for h in page.hits] == ["i1", "i2"]


@pytest.mark.asyncio
async def test_structural_filters_thread_into_query():
    es = _FakeEs(_response([], total=0))

    await search_public_items(
        ids=["i1"],
        collections=["c1", "c2"],
        bbox=[0.0, 0.0, 10.0, 10.0],
        datetime="2024-01-01T00:00:00Z/..",
        es_client=es,
    )

    q = es.calls[0]["body"]["query"]
    filters = q["bool"]["filter"]
    assert {"terms": {"id": ["i1"]}} in filters
    assert {"terms": {"collection": ["c1", "c2"]}} in filters
    assert any("geo_shape" in f for f in filters)


@pytest.mark.asyncio
async def test_sortby_threads_with_single_score_tiebreaker():
    es = _FakeEs(_response([], total=0))

    await search_public_items(sortby=["-datetime", "+id"], es_client=es)

    sort = es.calls[0]["body"]["sort"]
    assert sort[-1] == {"_score": {"order": "desc"}}
    assert sum(1 for c in sort if "_score" in c) == 1
    assert len(sort) == 3
