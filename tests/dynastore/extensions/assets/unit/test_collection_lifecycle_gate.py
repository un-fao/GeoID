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

"""Regression tests for the collection lifecycle write-gate on the assets surface.

These tests pin that collection-scoped mutating handlers refuse writes when
the collection is dead — before any driver is contacted.  The gate is
``require_collection_ready``; its unit tests live in
``tests/dynastore/extensions/unit/test_require_collection_ready.py``.

Coverage:
* ``delete_collection_asset_by_id`` — blocked when collection is tombstoned;
  the underlying assets driver's ``delete_assets`` is never called.
* ``delete_collection_asset_by_id`` — allowed when the collection is alive;
  the driver proceeds normally.
* ``create_collection_asset`` — blocked when collection is missing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.assets import assets_service as svc_module
from dynastore.extensions.assets.assets_service import ASSETS_TAG, AssetService
from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetKind,
    AssetStatus,
    AssetTypeEnum,
)
from dynastore.modules.catalog.collection_service import CollectionNotAliveError


# ---------------------------------------------------------------------------
# Scaffolding
# ---------------------------------------------------------------------------


def _build_service() -> AssetService:
    app = FastAPI()
    svc = AssetService.__new__(AssetService)
    svc.app = app
    svc.router = APIRouter(prefix="/assets", tags=[ASSETS_TAG])
    svc._setup_routes()
    app.include_router(svc.router)
    return svc


def _make_asset(
    *,
    asset_id: str = "a1",
    catalog_id: str = "cat1",
    collection_id: Optional[str] = "col1",
    kind: AssetKind = AssetKind.PHYSICAL,
    status: AssetStatus = AssetStatus.ACTIVE,
) -> Asset:
    now = datetime.now(timezone.utc)
    return Asset(
        asset_id=asset_id,
        asset_type=AssetTypeEnum.RASTER,
        kind=kind,
        status=status,
        catalog_id=catalog_id,
        collection_id=collection_id,
        filename=f"{asset_id}.tif" if kind == AssetKind.PHYSICAL else None,
        href=None if kind == AssetKind.PHYSICAL else f"https://x/{asset_id}",
        uri=f"gs://b/{asset_id}.tif" if kind == AssetKind.PHYSICAL else None,
        metadata={},
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------------------
# delete_collection_asset_by_id — gate blocks write on dead collection
# ---------------------------------------------------------------------------


def test_delete_collection_asset_blocked_when_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate propagates ``CollectionNotAliveError`` before the driver is called.

    The :class:`CollectionNotAliveExceptionHandler` (registered globally) maps
    ``reason='tombstoned'`` to HTTP 410.  In a TestClient without that handler
    registered the exception propagates as a 500 — so we assert the driver was
    never reached, which is the load-bearing property under test.
    """
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )
    dead_error = CollectionNotAliveError("cat1", "col1", "tombstoned")
    monkeypatch.setattr(
        svc_module,
        "require_collection_ready",
        AsyncMock(side_effect=dead_error),
    )

    assets_proto = AsyncMock()
    assets_proto.delete_assets = AsyncMock(return_value=1)

    def _fake_get_protocol(proto_cls: Any) -> Any:
        return None

    monkeypatch.setattr(svc_module, "get_protocol", _fake_get_protocol)

    svc = _build_service()
    # Inject the assets protocol directly onto the service property.
    # The ``assets`` property resolves via get_protocol which we've
    # monkeypatched to return None — that would 503 the handler.
    # Patch the property on the instance via __class__ so the mock is used.
    type(svc).assets = property(lambda self: assets_proto)  # type: ignore[method-assign]

    client = TestClient(svc.app, raise_server_exceptions=False)
    resp = client.delete("/assets/catalogs/cat1/collections/col1/assets/a1")

    # The gate fired (collection dead) — the driver was never reached.
    assets_proto.delete_assets.assert_not_awaited()
    # Without the global exception handler registered, a propagated
    # CollectionNotAliveError surfaces as 500 from TestClient.
    assert resp.status_code == 500


def test_delete_collection_asset_allowed_when_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the collection is alive the handler reaches the driver normally."""
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        svc_module, "require_collection_ready", AsyncMock(return_value=None)
    )

    assets_proto = AsyncMock()
    assets_proto.delete_assets = AsyncMock(return_value=1)

    monkeypatch.setattr(svc_module, "get_protocol", lambda _cls: None)

    svc = _build_service()
    type(svc).assets = property(lambda self: assets_proto)  # type: ignore[method-assign]

    client = TestClient(svc.app)
    resp = client.delete("/assets/catalogs/cat1/collections/col1/assets/a1")

    assert resp.status_code == 204
    assets_proto.delete_assets.assert_awaited_once()


# ---------------------------------------------------------------------------
# create_collection_asset — gate blocks write when collection missing
# ---------------------------------------------------------------------------


def test_create_collection_asset_blocked_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``require_collection_ready`` fires before the asset create driver call."""
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )
    dead_error = CollectionNotAliveError("cat1", "col1", "missing")
    monkeypatch.setattr(
        svc_module,
        "require_collection_ready",
        AsyncMock(side_effect=dead_error),
    )

    assets_proto = AsyncMock()
    assets_proto.create_asset = AsyncMock(return_value=_make_asset())

    monkeypatch.setattr(svc_module, "get_protocol", lambda _cls: None)

    svc = _build_service()
    type(svc).assets = property(lambda self: assets_proto)  # type: ignore[method-assign]

    payload = {
        "asset_id": "new_asset",
        "asset_type": "RASTER",
        "filename": "scene.tif",
        "uri": "gs://bucket/scene.tif",
        "owned_by": "gcs",
        "metadata": {},
    }

    client = TestClient(svc.app, raise_server_exceptions=False)
    resp = client.post(
        "/assets/catalogs/cat1/collections/col1", json=payload
    )

    # The gate fired — the driver was never reached.
    assets_proto.create_asset.assert_not_awaited()
    assert resp.status_code == 500
