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

"""Unit tests for the listing-visibility integration in
``CollectionElasticsearchDriver.search_metadata``.

Exercises:
- Non-empty visible set → ``{"terms": {"id": [...]}}`` injected into filter clauses.
- Empty visible set → ([], 0) returned without consulting the ES client.

No live Elasticsearch required.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
)


_RESOLVE_COL = "dynastore.models.protocols.visibility.resolve_collection_listing_ids"
_GET_CLIENT = (
    "dynastore.modules.elasticsearch.collection_es_driver"
    ".CollectionElasticsearchDriver._get_client"
)
_INDEX_NAME = (
    "dynastore.modules.elasticsearch.collection_es_driver"
    ".CollectionElasticsearchDriver._index_name"
)

_EMPTY_ES_RESPONSE = {
    "hits": {
        "hits": [],
        "total": {"value": 0},
    }
}


def _make_driver() -> CollectionElasticsearchDriver:
    return CollectionElasticsearchDriver()


# ---------------------------------------------------------------------------
# Visible set present → terms filter injected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visible_ids_injected_as_terms_filter(monkeypatch):
    """When resolve returns {c1}, a ``{"terms": {"id": ["c1"]}}`` filter clause
    must appear in the ES query body."""
    monkeypatch.setattr(
        _RESOLVE_COL, AsyncMock(return_value=frozenset({"c1"}))
    )

    captured_body: Dict[str, Any] = {}

    fake_client = MagicMock()

    async def _fake_search(**kwargs):
        captured_body.update(kwargs)
        return _EMPTY_ES_RESPONSE

    fake_client.search = _fake_search

    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=fake_client), \
         patch(_INDEX_NAME, return_value="test-collections"):
        result = await driver.search_metadata("cat-x", limit=10)

    assert result == ([], 0)
    body = captured_body.get("body", {})
    filter_clauses: List[Dict[str, Any]] = (
        body.get("query", {})
        .get("bool", {})
        .get("filter", [])
    )
    terms_clauses = [c for c in filter_clauses if "terms" in c]
    id_terms = [c["terms"] for c in terms_clauses if "id" in c.get("terms", {})]
    assert id_terms, (
        f"Expected a {{\"terms\": {{\"id\": [...]}}}} filter clause; "
        f"got filter_clauses={filter_clauses}"
    )
    assert id_terms[0]["id"] == ["c1"], (
        f"terms id list must match the visible set; got {id_terms[0]['id']}"
    )


# ---------------------------------------------------------------------------
# Empty visible set → short-circuit without client call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_visible_set_returns_empty_without_client(monkeypatch):
    """resolve returns frozenset() → ([], 0) must be returned before calling
    the ES client."""
    monkeypatch.setattr(
        _RESOLVE_COL, AsyncMock(return_value=frozenset())
    )

    fake_client = MagicMock()
    fake_client.search = AsyncMock()

    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=fake_client), \
         patch(_INDEX_NAME, return_value="test-collections"):
        result = await driver.search_metadata("cat-x", limit=10)

    assert result == ([], 0)
    fake_client.search.assert_not_called()
