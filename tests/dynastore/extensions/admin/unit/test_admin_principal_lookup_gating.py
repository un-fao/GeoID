#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for `_is_catalog_only_admin` + `GET /admin/principals` gating (#723).

The route is opened to the ``catalog_admin`` sentinel via
``admin_principal_lookup`` so the catalog-grant flow can resolve a target
principal by subject_id, but catalog-only admins must NOT be able to
enumerate the platform principal directory.

Behaviour exercised here:
- ``_is_catalog_only_admin`` is False for anonymous / sysadmin /
  platform-admin / platform-grant callers (the existing surface that
  reaches this route via ``admin_access``) — they keep enumerating.
- ``_is_catalog_only_admin`` is True for principals whose only authority
  is a catalog-tier admin grant — the route must require ``q``.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.admin.admin_service import (
    _is_catalog_only_admin,
)


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"
_MEMBERSHIP_CACHE = "dynastore.extensions.iam.membership_cache.get_membership_cached"


def _make_request(principal: Optional[Any]) -> Any:
    state = SimpleNamespace(principal=principal)
    return SimpleNamespace(state=state)


def _make_principal(
    *,
    provider: str = "local",
    subject_id: str = "alice",
    roles: Optional[list[str]] = None,
) -> Any:
    return SimpleNamespace(
        provider=provider, subject_id=subject_id, roles=roles or []
    )


@contextmanager
def _patched(
    membership: Optional[Dict[str, Any]] = None,
    iam_query: Any = MagicMock(),
):
    def _get_proto(cls):
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        if cls is IamQueryProtocol:
            return iam_query
        return None

    cache_mock = AsyncMock(return_value=membership or {})
    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MEMBERSHIP_CACHE, new=cache_mock):
        yield cache_mock


@pytest.mark.asyncio
async def test_anonymous_caller_is_not_catalog_only_admin():
    """Anonymous callers reached this route via an authoritative ALLOW;
    the helper returns False so the route does not refuse them on the
    enumeration-narrowing branch.
    """
    req = _make_request(principal=None)
    assert await _is_catalog_only_admin(req) is False


@pytest.mark.asyncio
async def test_sysadmin_role_is_not_catalog_only_admin():
    req = _make_request(principal=_make_principal(roles=["sysadmin"]))
    assert await _is_catalog_only_admin(req) is False


@pytest.mark.asyncio
async def test_admin_role_is_not_catalog_only_admin():
    req = _make_request(principal=_make_principal(roles=["admin"]))
    assert await _is_catalog_only_admin(req) is False


@pytest.mark.asyncio
async def test_platform_grant_holder_is_not_catalog_only_admin():
    """A principal without sysadmin/admin role but with a platform-scope
    grant (`membership.platform=True`) is treated as a platform caller.
    """
    req = _make_request(principal=_make_principal(roles=["user"]))
    with _patched(membership={"platform": True, "catalog_roles": {}}):
        assert await _is_catalog_only_admin(req) is False


@pytest.mark.asyncio
async def test_catalog_only_admin_is_catalog_only_admin():
    """The actual catalog-only admin path: principal carries the sentinel
    role and only catalog-tier grants.
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))
    with _patched(membership={
        "platform": False,
        "catalog_roles": {"cat-a": ["admin"]},
    }):
        assert await _is_catalog_only_admin(req) is True


@pytest.mark.asyncio
async def test_principal_without_provider_or_subject_is_not_catalog_only_admin():
    """Service principals (no provider/subject) can't be membership-checked;
    refuse the narrowing branch so the route falls through to its existing
    sysadmin-only behaviour.
    """
    req = _make_request(principal=SimpleNamespace(
        provider=None, subject_id=None, roles=["custom"],
    ))
    assert await _is_catalog_only_admin(req) is False


@pytest.mark.asyncio
async def test_no_iam_query_protocol_is_not_catalog_only_admin():
    """Slim deployments without IamQueryProtocol can't resolve memberships;
    the narrowing branch is skipped so existing platform-admin behaviour
    is preserved.
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    def _get_proto(_cls):
        return None  # IamQueryProtocol → None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        assert await _is_catalog_only_admin(req) is False


# ---------------------------------------------------------------------------
# Handler-level integration: GET /admin/principals narrowing.
# ---------------------------------------------------------------------------

from dynastore.extensions.admin.admin_service import AdminService  # noqa: E402
from fastapi import HTTPException  # noqa: E402

_handler = AdminService.list_principals


def _make_iam_mgr() -> MagicMock:
    mgr = MagicMock()
    mgr.list_principals = AsyncMock(return_value=[])
    mgr.search_principals = AsyncMock(return_value=[])
    mgr.storage = MagicMock()
    mgr.storage.list_platform_roles = AsyncMock(return_value=[])
    return mgr


@pytest.mark.asyncio
async def test_handler_rejects_catalog_admin_without_q():
    """Catalog-only admin reaches /admin/principals with no q → 400.

    Enumeration of the platform principal directory is restricted to
    callers who reach this route via ``admin_access`` (sysadmin/admin).
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))
    mgr = _make_iam_mgr()

    def _get_proto(cls):
        from dynastore.modules.iam.iam_service import IamService
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        if cls is IamService:
            return mgr
        if cls is IamQueryProtocol:
            return MagicMock()
        return None

    cache_mock = AsyncMock(return_value={
        "platform": False, "catalog_roles": {"cat-a": ["admin"]},
    })
    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MEMBERSHIP_CACHE, new=cache_mock):
        with pytest.raises(HTTPException) as exc:
            await _handler(req, limit=50, offset=0, provider=None, q=None,
                           role=None, catalog_id=None)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_handler_rejects_catalog_admin_with_blank_q():
    """``q=""`` (whitespace) is treated as missing — no enumeration loophole."""
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))
    mgr = _make_iam_mgr()

    def _get_proto(cls):
        from dynastore.modules.iam.iam_service import IamService
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        if cls is IamService:
            return mgr
        if cls is IamQueryProtocol:
            return MagicMock()
        return None

    cache_mock = AsyncMock(return_value={
        "platform": False, "catalog_roles": {"cat-a": ["admin"]},
    })
    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MEMBERSHIP_CACHE, new=cache_mock):
        with pytest.raises(HTTPException) as exc:
            await _handler(req, limit=50, offset=0, provider=None, q="   ",
                           role=None, catalog_id=None)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_handler_accepts_catalog_admin_with_q():
    """Catalog admin with an actual search term reaches search_principals.

    This is the FE catalog-grant flow: ``q={target_subject_id}&limit=1``.
    """
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))
    mgr = _make_iam_mgr()

    def _get_proto(cls):
        from dynastore.modules.iam.iam_service import IamService
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        if cls is IamService:
            return mgr
        if cls is IamQueryProtocol:
            return MagicMock()
        return None

    cache_mock = AsyncMock(return_value={
        "platform": False, "catalog_roles": {"cat-a": ["admin"]},
    })
    with patch(_GET_PROTOCOL, side_effect=_get_proto), \
         patch(_MEMBERSHIP_CACHE, new=cache_mock):
        result = await _handler(req, limit=1, offset=0, provider=None,
                                q="target-subject", role=None, catalog_id=None)
    assert result == []
    mgr.search_principals.assert_awaited_once()


@pytest.mark.asyncio
async def test_handler_allows_sysadmin_without_q():
    """Sysadmin reaches via admin_access and may enumerate the directory."""
    req = _make_request(principal=_make_principal(roles=["sysadmin"]))
    mgr = _make_iam_mgr()

    def _get_proto(cls):
        from dynastore.modules.iam.iam_service import IamService
        if cls is IamService:
            return mgr
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        result = await _handler(req, limit=50, offset=0, provider=None, q=None,
                                role=None, catalog_id=None)
    assert result == []
    mgr.list_principals.assert_awaited_once()
