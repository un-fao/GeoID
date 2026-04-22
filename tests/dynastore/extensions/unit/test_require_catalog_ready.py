"""Unit tests for ``OGCServiceMixin._require_catalog_ready``.

The fail-fast guard rejects API operations against a catalog whose
``provisioning_status`` isn't ``'ready'``.  Without the guard,
endpoints like ``POST /stac/catalogs/{catalog_id}/collections`` would
either 500 deep inside a driver (bucket doesn't exist) or silently
half-succeed and corrupt state.

Expected behaviour:

- ``provisioning_status='ready'`` → returns the catalog model
- ``provisioning_status='provisioning'`` → 409 with retry hint
- ``provisioning_status='failed'`` → 409 with delete/recreate hint
- Catalog absent → 404
- Unknown status value → 409 (loud, not silent)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from dynastore.extensions.ogc_base import OGCServiceMixin


class _Host(OGCServiceMixin):
    """Minimal host class for exercising the mixin without a full
    FastAPI / protocol-discovery stack."""


def _make_catalog(status: str) -> Any:
    m = MagicMock()
    m.provisioning_status = status
    return m


@pytest.mark.asyncio
async def test_ready_returns_catalog():
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=_make_catalog("ready"))

    result = await host._require_catalog_ready("cat1", catalogs_svc=svc)

    assert result is not None
    assert result.provisioning_status == "ready"


@pytest.mark.asyncio
async def test_provisioning_returns_409_with_retry_hint():
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=_make_catalog("provisioning"))

    with pytest.raises(HTTPException) as excinfo:
        await host._require_catalog_ready("cat_pending", catalogs_svc=svc)

    assert excinfo.value.status_code == 409
    assert "still provisioning" in excinfo.value.detail.lower()
    # Hint tells the client HOW to recover
    assert "cat_pending" in excinfo.value.detail


@pytest.mark.asyncio
async def test_failed_returns_409_with_delete_recreate_hint():
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=_make_catalog("failed"))

    with pytest.raises(HTTPException) as excinfo:
        await host._require_catalog_ready("cat_broken", catalogs_svc=svc)

    assert excinfo.value.status_code == 409
    assert "failed" in excinfo.value.detail.lower()
    # Hint explicitly mentions the DELETE path — this is the only
    # actionable recovery on 'failed' state.
    assert "DELETE" in excinfo.value.detail
    assert "recreate" in excinfo.value.detail.lower()


@pytest.mark.asyncio
async def test_absent_catalog_returns_404():
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=None)

    with pytest.raises(HTTPException) as excinfo:
        await host._require_catalog_ready("ghost", catalogs_svc=svc)

    assert excinfo.value.status_code == 404
    assert "ghost" in excinfo.value.detail


@pytest.mark.asyncio
async def test_unknown_status_fails_loud():
    """An unexpected status value must surface as an error rather than
    silently proceed — ``provisioning_status`` is a discrete state
    machine; drift is a bug to be investigated, not swallowed."""
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=_make_catalog("xyzzy"))

    with pytest.raises(HTTPException) as excinfo:
        await host._require_catalog_ready("cat_weird", catalogs_svc=svc)

    assert excinfo.value.status_code == 409
    assert "xyzzy" in excinfo.value.detail


@pytest.mark.asyncio
async def test_missing_status_attribute_defaults_to_ready():
    """Defensive: catalog models that predate the provisioning_status
    column (or were built from cached dicts) must not cause spurious
    409s.  Treat the absence of the field as legacy-ready."""
    host = _Host()
    svc = MagicMock()
    legacy_catalog = MagicMock(spec=[])  # no provisioning_status attr
    svc.get_catalog_model = AsyncMock(return_value=legacy_catalog)

    result = await host._require_catalog_ready("cat_legacy", catalogs_svc=svc)
    assert result is legacy_catalog


@pytest.mark.asyncio
async def test_none_status_defaults_to_ready():
    """If the column is NULL (shouldn't happen given NOT NULL DEFAULT,
    but belt-and-braces) treat it as ready rather than flagging
    spurious 409s on migrated data."""
    host = _Host()
    svc = MagicMock()
    svc.get_catalog_model = AsyncMock(return_value=_make_catalog(None))

    result = await host._require_catalog_ready("cat_null", catalogs_svc=svc)
    assert result is not None
