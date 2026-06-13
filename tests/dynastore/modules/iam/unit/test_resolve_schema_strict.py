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

"""Unit tests for ``IamService.resolve_schema(strict=...)`` (#1698).

Catalog-scoped *write* operations (granting/revoking a catalog role) must
never silently retarget to the platform ``iam`` schema when the tenant
schema cannot be resolved — a silent fallback turns a failed write into a
204 with the grant written nowhere the caller expects (the #1698 symptom).

``strict=True`` makes resolution raise instead of falling back, so the
write surfaces a real error. The default (``strict=False``) keeps the
historical, deliberately-defensive fallback used by the read/middleware
path.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine

from dynastore.modules.iam import iam_service as iam_service_mod
from dynastore.modules.iam.iam_service import IamService


def _svc() -> IamService:
    # resolve_schema does not touch self.storage; inject a stub so the
    # constructor never reaches for a live engine.
    return IamService(storage=MagicMock())


def _fake_catalogs(
    *, returns: Optional[str] = None, raises: Optional[Exception] = None
) -> MagicMock:
    catalogs = MagicMock()
    # Use a real SQLAlchemy Engine: DriverContext.db_resource is typed as
    # DbResource (a Union of SQLAlchemy types) so a plain object() is no
    # longer accepted after #1555 moved the field type from Any.
    catalogs.engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    if raises is not None:
        catalogs.resolve_physical_schema = AsyncMock(side_effect=raises)
    else:
        catalogs.resolve_physical_schema = AsyncMock(return_value=returns)
    return catalogs


async def test_strict_raises_when_catalogs_protocol_unavailable(monkeypatch):
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: None)
    svc = _svc()

    with pytest.raises(ValueError):
        await svc.resolve_schema("cat_a", strict=True)


async def test_nonstrict_falls_back_to_iam_when_protocol_unavailable(monkeypatch):
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: None)
    svc = _svc()

    assert await svc.resolve_schema("cat_a") == "iam"


async def test_strict_raises_when_physical_schema_missing(monkeypatch):
    catalogs = _fake_catalogs(returns=None)
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: catalogs)
    svc = _svc()

    with pytest.raises(ValueError):
        await svc.resolve_schema("cat_a", strict=True)


async def test_nonstrict_falls_back_when_physical_schema_missing(monkeypatch):
    catalogs = _fake_catalogs(returns=None)
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: catalogs)
    svc = _svc()

    assert await svc.resolve_schema("cat_a") == "iam"


async def test_strict_propagates_resolution_error(monkeypatch):
    catalogs = _fake_catalogs(raises=ValueError("Catalog 'cat_a' not found."))
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: catalogs)
    svc = _svc()

    with pytest.raises(ValueError):
        await svc.resolve_schema("cat_a", strict=True)


async def test_nonstrict_swallows_resolution_error(monkeypatch):
    catalogs = _fake_catalogs(raises=ValueError("Catalog 'cat_a' not found."))
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: catalogs)
    svc = _svc()

    assert await svc.resolve_schema("cat_a") == "iam"


async def test_strict_returns_resolved_tenant_schema(monkeypatch):
    catalogs = _fake_catalogs(returns="s_tenant1")
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: catalogs)
    svc = _svc()

    assert await svc.resolve_schema("cat_a", strict=True) == "s_tenant1"


async def test_system_catalog_returns_iam_even_when_strict(monkeypatch):
    # ``_system_`` is legitimately the platform scope — strict must not
    # turn the platform write path into an error.
    monkeypatch.setattr(iam_service_mod, "get_protocol", lambda _proto: None)
    svc = _svc()

    assert await svc.resolve_schema("_system_", strict=True) == "iam"
    assert await svc.resolve_schema(None, strict=True) == "iam"
