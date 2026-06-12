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

"""Unit tests for the transparent listing-visibility integration in
``CatalogService.list_catalogs``.

Covers the three branching cases:
1. Empty visible set → short-circuit to [] without touching the DB.
2. Non-empty visible set ∩ explicit ids → only the intersection is forwarded
   to SQL (id = ANY clause).
3. No visibility context (background work) → ``ids`` passes through unchanged.

DB interactions are mocked; no PostgreSQL required.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_service import CatalogService


_VIS_MODULE = "dynastore.models.protocols.visibility"
_RESOLVE_FN = f"{_VIS_MODULE}.resolve_catalog_listing_ids"
_MANAGED_TX = "dynastore.modules.catalog.catalog_service.managed_transaction"
_GET_ENGINE = "dynastore.modules.catalog.catalog_service.get_catalog_engine"


@asynccontextmanager
async def _null_tx(_engine):
    """Async context-manager stub that yields a fake connection."""
    conn = MagicMock()
    yield conn


def _make_service() -> CatalogService:
    """Construct a bare CatalogService with no real engine."""
    return CatalogService(engine=None)


# ---------------------------------------------------------------------------
# (1) Empty visible set → short-circuit, no DB call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_visible_set_returns_empty_without_db(monkeypatch):
    """resolve_catalog_listing_ids returns frozenset() → list_catalogs is []
    immediately, never touching managed_transaction."""
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=frozenset()))

    conn_entered = False

    @asynccontextmanager
    async def _spy_tx(_engine):
        nonlocal conn_entered
        conn_entered = True
        yield MagicMock()

    with patch(_MANAGED_TX, side_effect=_spy_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()):
        svc = _make_service()
        result = await svc.list_catalogs(limit=100)

    assert result == []
    assert not conn_entered, "DB must not be touched when visible set is empty"


# ---------------------------------------------------------------------------
# (2) Visible set ∩ explicit ids → intersection forwarded to SQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_intersection_forwarded_to_sql(monkeypatch):
    """When visible={a,b} and ids={b,c}, only {b} must reach the SQL query."""
    monkeypatch.setattr(
        _RESOLVE_FN, AsyncMock(return_value=frozenset({"a", "b"}))
    )

    captured_ids = None

    @asynccontextmanager
    async def _capture_tx(_engine):
        conn = MagicMock()

        async def _execute(conn_inner, **kwargs):
            nonlocal captured_ids
            captured_ids = kwargs.get("ids")
            return []

        # The DQLQuery.execute call will use the connection; we capture via
        # patching the inner execute on the DQLQuery instance instead.
        yield conn

    with patch(_MANAGED_TX, side_effect=_capture_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=[]),
         ) as mock_execute:
        svc = _make_service()
        result = await svc.list_catalogs(limit=100, ids={"b", "c"})

    assert result == []
    mock_execute.assert_awaited_once()
    _, kwargs = mock_execute.call_args
    forwarded = set(kwargs.get("ids", []))
    assert forwarded == {"b"}, (
        f"Only the intersection {{b}} must be forwarded to SQL; got {forwarded}"
    )


# ---------------------------------------------------------------------------
# (3) No context → ids passes through unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_context_ids_unchanged(monkeypatch):
    """When resolve_catalog_listing_ids returns None (no visibility context),
    the original ids kwarg reaches the SQL query unmodified."""
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    with patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=[]),
         ) as mock_execute:
        svc = _make_service()
        await svc.list_catalogs(limit=10, ids={"x", "y"})

    mock_execute.assert_awaited_once()
    _, kwargs = mock_execute.call_args
    forwarded = set(kwargs.get("ids", []))
    assert forwarded == {"x", "y"}, (
        "No-context path must forward the caller's ids unchanged"
    )


@pytest.mark.asyncio
async def test_no_context_none_ids_stays_none(monkeypatch):
    """When there is no visibility context and no explicit ids, the unfiltered
    query branch (no id=ANY clause) must be taken."""
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    with patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=[]),
         ) as mock_execute:
        svc = _make_service()
        await svc.list_catalogs(limit=10)

    mock_execute.assert_awaited_once()
    _, kwargs = mock_execute.call_args
    # The unfiltered path uses the module-level _list_catalogs_query, which
    # does not accept an 'ids' kwarg.
    assert "ids" not in kwargs, (
        "Unfiltered path must not inject ids into the query"
    )
