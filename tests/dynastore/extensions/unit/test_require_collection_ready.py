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

"""Unit tests for ``require_collection_ready`` (issue #2065).

The collection lifecycle gate ensures mutating endpoints reject writes
against deleted or missing collections before they reach the driver
layer.  The globally-registered :class:`CollectionNotAliveExceptionHandler`
maps the resulting exception to the appropriate HTTP status:

* ``reason='missing'``    → 404
* ``reason='tombstoned'`` → 410
* ``reason='lookup-error'``  → 503 (fail-closed)

When :class:`CatalogsProtocol` is not registered the helper itself raises
``HTTPException(503)`` rather than silently allowing the write through.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from dynastore.extensions.tools.catalog_readiness import require_collection_ready
from dynastore.modules.catalog.collection_service import CollectionNotAliveError


# ---------------------------------------------------------------------------
# Helper-level unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_propagates_missing_unchanged():
    """``ensure_alive`` raising CollectionNotAliveError(reason='missing')
    propagates out of the helper untouched so the registered handler
    maps it to 404."""
    svc = MagicMock()
    svc.ensure_alive = AsyncMock(
        side_effect=CollectionNotAliveError("cat1", "col1", "missing")
    )
    with pytest.raises(CollectionNotAliveError) as excinfo:
        await require_collection_ready("cat1", "col1", catalogs_svc=svc)

    assert excinfo.value.reason == "missing"
    assert excinfo.value.catalog_id == "cat1"
    assert excinfo.value.collection_id == "col1"


@pytest.mark.asyncio
async def test_propagates_tombstoned_unchanged():
    """``ensure_alive`` raising CollectionNotAliveError(reason='tombstoned')
    propagates out of the helper untouched so the registered handler
    maps it to 410."""
    svc = MagicMock()
    svc.ensure_alive = AsyncMock(
        side_effect=CollectionNotAliveError("cat1", "col1", "tombstoned")
    )
    with pytest.raises(CollectionNotAliveError) as excinfo:
        await require_collection_ready("cat1", "col1", catalogs_svc=svc)

    assert excinfo.value.reason == "tombstoned"


@pytest.mark.asyncio
async def test_propagates_lookup_error_unchanged():
    """A lookup-error reason (gate failed closed) propagates too."""
    svc = MagicMock()
    svc.ensure_alive = AsyncMock(
        side_effect=CollectionNotAliveError("cat1", "col1", "lookup-error")
    )
    with pytest.raises(CollectionNotAliveError) as excinfo:
        await require_collection_ready("cat1", "col1", catalogs_svc=svc)

    assert excinfo.value.reason == "lookup-error"


@pytest.mark.asyncio
async def test_alive_collection_returns_none():
    """A live collection results in a normal return (``None``) — no exception."""
    svc = MagicMock()
    svc.ensure_alive = AsyncMock(return_value=None)

    result = await require_collection_ready("cat1", "col1", catalogs_svc=svc)

    assert result is None
    svc.ensure_alive.assert_awaited_once_with("cat1", "col1")


@pytest.mark.asyncio
async def test_no_protocol_raises_503(monkeypatch):
    """When ``get_protocol(CatalogsProtocol)`` returns ``None``, the helper
    raises ``HTTPException(503)`` to refuse the write loudly rather than
    allowing it to flow through to an unknown state."""
    import dynastore.extensions.tools.catalog_readiness as mod

    monkeypatch.setattr(mod, "get_protocol", lambda _cls: None)

    with pytest.raises(HTTPException) as excinfo:
        await require_collection_ready("cat1", "col1")

    assert excinfo.value.status_code == 503
    assert "CatalogsProtocol not available" in excinfo.value.detail
    assert "cat1/col1" in excinfo.value.detail
