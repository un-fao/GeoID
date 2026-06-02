#    Copyright 2025 FAO
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

"""Unit tests for the shared catalog + collection CRUD bodies on ``OGCServiceMixin``.

Covers M-2 (catalog CRUD) and M-3 (collection CRUD) extracted in issue #1510.

Tests prove:

* (a) Default hooks fire the Features-style behaviour (no pre-create
  validation, no readiness guard, standard ``.localize()`` path,
  ``DriverContext`` forwarded when a ``db_resource`` is given).
* (b) A STAC-like subclass's overrides are invoked: validation called,
  ``stac_localize`` used, readiness guard called, ``stac_context=True``
  passed to ``create_collection``.
* (c) Delegation passes through arguments and catalogs-service calls correctly.

All collaborators are mocked — no database is touched.
"""

from typing import Any, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.ogc_base import OGCServiceMixin


# ---------------------------------------------------------------------------
# Shared helpers / stub models
# ---------------------------------------------------------------------------


def _make_fake_conn() -> MagicMock:
    """Return a MagicMock that passes ``isinstance(x, AsyncConnection)`` checks.

    ``DriverContext.db_resource`` is typed as ``Optional[DbResource]`` and
    Pydantic validates the type at runtime.  Using ``spec=AsyncConnection``
    makes the mock pass the discriminated-union isinstance check without
    requiring a live database session.
    """
    return MagicMock(spec=AsyncConnection)


class _LocalizableCatalog:
    """Minimal stub that mimics ``LocalizableModelMixin.localize``."""

    def localize(self, language: str) -> Tuple[Dict[str, Any], Any]:
        return {"id": "cat1", "lang": language}, {"en"}


def _make_catalogs_svc(**overrides) -> AsyncMock:
    """Return a mock CatalogsProtocol with standard return values."""
    svc = AsyncMock()
    svc.create_catalog = AsyncMock(return_value=_LocalizableCatalog())
    svc.update_catalog = AsyncMock(return_value=_LocalizableCatalog())
    svc.delete_catalog = AsyncMock(return_value=True)
    svc.create_collection = AsyncMock(return_value=_LocalizableCatalog())
    svc.update_collection = AsyncMock(return_value=_LocalizableCatalog())
    svc.delete_collection = AsyncMock(return_value=True)
    for k, v in overrides.items():
        setattr(svc, k, v)
    return svc


# ---------------------------------------------------------------------------
# Features-style subclass (default hook behaviour)
# ---------------------------------------------------------------------------


class _FeaturesSvc(OGCServiceMixin):
    """Minimal concrete subclass exercising the default (Features) hooks."""


# ---------------------------------------------------------------------------
# STAC-style subclass — records calls to each override
# ---------------------------------------------------------------------------


class _STACSvc(OGCServiceMixin):
    """Subclass with STAC-like hook overrides for verifying the seams."""

    def __init__(self):
        self._validate_catalog_create_called = False
        self._require_catalog_write_ready_calls: list = []
        self._pre_update_collection_validate_calls: list = []

    def _validate_catalog_create(self) -> None:
        self._validate_catalog_create_called = True

    async def _require_catalog_write_ready(
        self, catalog_id: str, catalogs_svc=None
    ) -> None:
        self._require_catalog_write_ready_calls.append(catalog_id)

    def _make_collection_create_kwargs(self) -> Dict[str, Any]:
        return {"stac_context": True}

    def _localize_resource(self, model: Any, language: str) -> Tuple[Dict[str, Any], Any]:
        # Simulate stac_localize — wraps the standard output in a STAC key.
        data, langs = model.localize(language)
        return {"stac": True, **data}, langs

    async def _pre_update_collection_validate(
        self,
        catalog_id: str,
        collection_id: str,
        input_data: Dict[str, Any],
        request=None,
    ) -> None:
        self._pre_update_collection_validate_calls.append(
            (catalog_id, collection_id, input_data, request)
        )


# ---------------------------------------------------------------------------
# M-2: catalog CRUD — Features-style defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_no_validation():
    """Default _validate_catalog_create is a no-op — no exception raised."""
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())

    resp = await svc._ogc_create_catalog(
        {"id": "cat1"}, {"id": "cat1"}, "en", _make_fake_conn()
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_passes_ctx():
    """Features passes a DriverContext when db_resource is not None."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", db_conn)

    create_call = catalogs_svc.create_catalog.call_args
    assert "ctx" in create_call.kwargs
    from dynastore.models.driver_context import DriverContext
    assert isinstance(create_call.kwargs["ctx"], DriverContext)
    assert create_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_no_ctx_when_db_resource_none():
    """When db_resource is None no ctx kwarg is forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)

    create_call = catalogs_svc.create_catalog.call_args
    assert "ctx" not in create_call.kwargs


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_standard_localize():
    """Default _localize_resource uses model.localize()."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "fr", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data["lang"] == "fr"
    assert "stac" not in data


@pytest.mark.asyncio
async def test_ogc_replace_catalog_features_no_readiness_guard():
    """Default _require_catalog_write_ready is a no-op."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_replace_catalog("cat1", {"id": "cat1"}, "en", _make_fake_conn())
    assert resp.status_code == 200
    catalogs_svc.update_catalog.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_replace_catalog_features_404_on_missing():
    """404 is raised when update_catalog returns None."""
    catalogs_svc = _make_catalogs_svc(update_catalog=AsyncMock(return_value=None))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_catalog("cat1", {"id": "cat1"}, "en", None)
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_ogc_update_catalog_features():
    """PATCH: detect_use_lang applied; update_catalog called with correct lang."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_catalog("cat1", {"title": {"en": "Test"}}, "en", None)
    catalogs_svc.update_catalog.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_delete_catalog_features_passes_ctx():
    """Features delete passes DriverContext; returns 204 on success."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    resp = await svc._ogc_delete_catalog("cat1", False, db_conn)
    assert resp.status_code == 204

    delete_call = catalogs_svc.delete_catalog.call_args
    from dynastore.models.driver_context import DriverContext
    assert isinstance(delete_call.kwargs.get("ctx"), DriverContext)
    assert delete_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_delete_catalog_features_404():
    """404 raised when delete_catalog returns False."""
    catalogs_svc = _make_catalogs_svc(delete_catalog=AsyncMock(return_value=False))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_delete_catalog("cat1", False, None)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# M-2: catalog CRUD — STAC override hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_stac_calls_validate_hook():
    """_validate_catalog_create is invoked on STAC create."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)
    assert svc._validate_catalog_create_called


@pytest.mark.asyncio
async def test_ogc_create_catalog_stac_uses_stac_localize():
    """STAC _localize_resource override is called; returns STAC-keyed dict."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data.get("stac") is True


@pytest.mark.asyncio
async def test_ogc_replace_catalog_stac_calls_readiness_guard():
    """_require_catalog_write_ready is invoked for STAC replace."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_replace_catalog("mycat", {"id": "mycat"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_update_catalog_stac_calls_readiness_guard():
    """_require_catalog_write_ready is invoked for STAC update."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_catalog("mycat", {"title": "x"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_delete_catalog_stac_no_ctx():
    """STAC delete passes None db_resource → no ctx kwarg forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_delete_catalog("cat1", False, None)
    assert resp.status_code == 204
    delete_call = catalogs_svc.delete_catalog.call_args
    assert "ctx" not in delete_call.kwargs


# ---------------------------------------------------------------------------
# M-3: collection CRUD — Features-style defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_collection_features_no_stac_context():
    """Default _make_collection_create_kwargs returns empty dict."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", _make_fake_conn())
    create_call = catalogs_svc.create_collection.call_args
    assert "stac_context" not in create_call.kwargs


@pytest.mark.asyncio
async def test_ogc_create_collection_features_passes_ctx():
    """Features create_collection includes DriverContext."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", db_conn)
    create_call = catalogs_svc.create_collection.call_args
    from dynastore.models.driver_context import DriverContext
    assert isinstance(create_call.kwargs.get("ctx"), DriverContext)
    assert create_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_replace_collection_features_no_readiness():
    """Default replace does not call readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_replace_collection("cat1", "col1", {"id": "col1"}, "en")
    assert resp.status_code == 200
    catalogs_svc.update_collection.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_replace_collection_features_404():
    """404 raised when update_collection returns None."""
    catalogs_svc = _make_catalogs_svc(update_collection=AsyncMock(return_value=None))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_collection("cat1", "col1", {"id": "col1"}, "en")
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_ogc_update_collection_features_no_validate_hook():
    """Default _pre_update_collection_validate is a no-op (no exception)."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_update_collection("cat1", "col1", {"title": "x"}, "en")
    assert resp.status_code == 200
    catalogs_svc.update_collection.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_delete_collection_features_passes_ctx():
    """Features delete_collection includes DriverContext; returns 204."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    resp = await svc._ogc_delete_collection("cat1", "col1", False, db_conn)
    assert resp.status_code == 204

    delete_call = catalogs_svc.delete_collection.call_args
    from dynastore.models.driver_context import DriverContext
    ctx_val = delete_call.kwargs.get("ctx")
    assert isinstance(ctx_val, DriverContext)
    assert ctx_val.db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_delete_collection_features_404():
    """404 raised when delete_collection returns False."""
    catalogs_svc = _make_catalogs_svc(delete_collection=AsyncMock(return_value=False))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_delete_collection("cat1", "col1", False, None)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# M-3: collection CRUD — STAC override hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_passes_stac_context():
    """STAC _make_collection_create_kwargs injects stac_context=True."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    create_call = catalogs_svc.create_collection.call_args
    assert create_call.kwargs.get("stac_context") is True


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_calls_readiness():
    """STAC create calls _require_catalog_write_ready."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("mycat", {"id": "col1"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_uses_stac_localize():
    """STAC _localize_resource wraps the returned model."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data.get("stac") is True


@pytest.mark.asyncio
async def test_ogc_replace_collection_stac_calls_readiness():
    """STAC replace calls readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_replace_collection("mycat", "col1", {"id": "col1"}, "en")
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_update_collection_stac_calls_pre_validate_hook():
    """STAC _pre_update_collection_validate is called with correct args."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    sentinel_request: Any = object()
    patch_data = {"title": "new"}
    await svc._ogc_update_collection("cat1", "col1", patch_data, "en", sentinel_request)

    assert len(svc._pre_update_collection_validate_calls) == 1
    cat_id, col_id, data, req = svc._pre_update_collection_validate_calls[0]
    assert cat_id == "cat1"
    assert col_id == "col1"
    assert data is patch_data
    assert req is sentinel_request


@pytest.mark.asyncio
async def test_ogc_update_collection_stac_calls_readiness():
    """STAC update calls readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_collection("mycat", "col1", {"title": "x"}, "en")
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_delete_collection_stac_no_ctx():
    """STAC delete passes None db_resource → no ctx forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_delete_collection("cat1", "col1", False, None)
    assert resp.status_code == 204
    delete_call = catalogs_svc.delete_collection.call_args
    assert "ctx" not in delete_call.kwargs


# ---------------------------------------------------------------------------
# Validate hook raises propagate to the caller
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_validation_failure_propagates():
    """When _validate_catalog_create raises, the error propagates out."""
    class _FailValidateSvc(OGCServiceMixin):
        def _validate_catalog_create(self) -> None:
            raise HTTPException(status_code=422, detail="No STAC driver.")

    catalogs_svc = _make_catalogs_svc()
    svc = _FailValidateSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_create_catalog({"id": "x"}, {"id": "x"}, "en", None)
    assert exc_info.value.status_code == 422
    catalogs_svc.create_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_ogc_create_collection_readiness_failure_propagates():
    """When _require_catalog_write_ready raises, the write is aborted."""
    class _FailReadySvc(OGCServiceMixin):
        async def _require_catalog_write_ready(self, catalog_id, catalogs_svc=None):
            raise HTTPException(status_code=409, detail="Not provisioned.")

    catalogs_svc = _make_catalogs_svc()
    svc = _FailReadySvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    assert exc_info.value.status_code == 409
    catalogs_svc.create_collection.assert_not_awaited()
