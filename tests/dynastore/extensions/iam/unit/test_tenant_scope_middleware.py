"""Unit contract for ``TenantScopeMiddleware``.

The middleware runs after ``IamMiddleware`` (which populates
``request.state.principal`` / ``principal_role``). For URLs matching a
tenant-scope registry rule it:
  - resolves the principal's catalog memberships via ``IamQueryProtocol``
    (cached) and writes ``request.state.authorized_catalogs``;
  - extracts ``catalog_id`` from the configured source (default: query param);
  - short-circuits with 401/403 + structured ``dashboard_authz.denied`` log
    when the caller can't see that catalog.

Routes themselves carry zero authz code — the gate is entirely in middleware.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock

import pytest


# Ergonomic stub for Starlette Request — only the surface the middleware uses.
class _FakeRequest:
    def __init__(
        self,
        path: str,
        *,
        principal: Optional[Any] = None,
        principal_role: Any = None,
        query: Optional[Dict[str, str]] = None,
    ) -> None:
        self.url = SimpleNamespace(path=path)
        self.query_params = query or {}
        self.state = SimpleNamespace()
        if principal is not None:
            self.state.principal = principal
        if principal_role is not None:
            self.state.principal_role = principal_role


class _Principal:
    """Each test uses a unique subject_id to avoid stale @cached membership."""
    def __init__(self, provider: str, subject_id: str) -> None:
        self.provider = provider
        self.subject_id = subject_id


@pytest.fixture
def fake_iam_query():
    iam = AsyncMock()
    iam.list_catalog_memberships = AsyncMock(
        return_value={"platform": False, "catalogs": ["acme"], "total": 1}
    )
    return iam


@pytest.fixture
def fake_iam_query_platform():
    iam = AsyncMock()
    iam.list_catalog_memberships = AsyncMock(
        return_value={"platform": True, "catalogs": [], "total": 0}
    )
    return iam


async def _make_middleware(monkeypatch, iam_query):
    from dynastore.extensions.iam import tenant_scope_middleware as tsm
    from dynastore.models.protocols.iam_query import IamQueryProtocol

    def _stub_get_protocol(proto):
        if proto is IamQueryProtocol:
            return iam_query
        return None

    monkeypatch.setattr(tsm, "get_protocol", _stub_get_protocol)
    return tsm.TenantScopeMiddleware(app=None)


async def _call(mw, request) -> tuple[bool, Any]:
    """Invoke ``dispatch`` with a sentinel ``call_next``; return
    (call_next_invoked, response_or_none)."""
    captured = {"called": False}

    async def call_next(req):
        captured["called"] = True
        return SimpleNamespace(status_code=200, body=b"")

    response = await mw.dispatch(request, call_next)
    return captured["called"], response


# ---------------------------------------------------------------------------
# Tests — pass-through when URL doesn't match any registry rule
# ---------------------------------------------------------------------------

async def test_passthrough_when_no_rule_matches(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest("/something/unrelated", principal=_Principal("local", "u1"))

    called, _ = await _call(mw, request)

    assert called is True
    fake_iam_query.list_catalog_memberships.assert_not_called()


async def test_dashboard_route_matches_registry(monkeypatch, fake_iam_query):
    """Sanity: registry must include /web/dashboard/{stats,logs,events,tasks,
    processes,ogc-compliance}."""
    from dynastore.extensions.iam.tenant_scope_registry import match_tenant_scope_rule

    for ep in ("stats", "logs", "events", "tasks", "processes", "ogc-compliance"):
        rule = match_tenant_scope_rule(f"/web/dashboard/{ep}")
        assert rule is not None, f"registry should match /web/dashboard/{ep}"


# ---------------------------------------------------------------------------
# Tests — anonymous handling
# ---------------------------------------------------------------------------

async def test_anonymous_blocked_when_rule_requires_identity(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest("/web/dashboard/stats", query={"catalog_id": "acme"})

    called, response = await _call(mw, request)

    assert called is False, "middleware must short-circuit anonymous requests"
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Tests — sysadmin / platform grant bypasses per-catalog check
# ---------------------------------------------------------------------------

async def test_sysadmin_role_bypass(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "admin"),
        principal_role=["sysadmin"],
        query={"catalog_id": "_system_"},
    )

    called, _ = await _call(mw, request)
    assert called is True


async def test_platform_grant_bypass_via_membership(monkeypatch, fake_iam_query_platform):
    """Identity carries platform=True even without sysadmin role label."""
    mw = await _make_middleware(monkeypatch, fake_iam_query_platform)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "platform_role_holder"),
        principal_role=["custom_platform_role"],
        query={"catalog_id": "_system_"},
    )

    called, _ = await _call(mw, request)
    assert called is True


# ---------------------------------------------------------------------------
# Tests — per-catalog admin gating
# ---------------------------------------------------------------------------

async def test_catalog_admin_owned_catalog_passes(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "cadmin"),
        principal_role=["catalog_admin"],
        query={"catalog_id": "acme"},
    )

    called, _ = await _call(mw, request)
    assert called is True
    # And the middleware writes the resolved scope into request.state for
    # downstream handlers / data-layer Protocols to read.
    assert getattr(request.state, "authorized_catalogs", None) == {
        "platform": False, "catalogs": ["acme"], "total": 1,
    }


async def test_catalog_admin_unowned_catalog_403(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "cadmin"),
        principal_role=["catalog_admin"],
        query={"catalog_id": "other_catalog"},
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


async def test_catalog_admin_system_scope_403(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "cadmin"),
        principal_role=["catalog_admin"],
        query={"catalog_id": "_system_"},
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


async def test_default_catalog_id_when_absent_is_system(monkeypatch, fake_iam_query):
    """When ?catalog_id= is missing, the registry default ('_system_') applies."""
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "cadmin"),
        principal_role=["catalog_admin"],
        # no query params
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Tests — no IAM available
# ---------------------------------------------------------------------------

async def test_iam_unavailable_returns_503(monkeypatch):
    mw = await _make_middleware(monkeypatch, iam_query=None)
    request = _FakeRequest(
        "/web/dashboard/stats",
        principal=_Principal("local", "cadmin"),
        principal_role=["catalog_admin"],
        query={"catalog_id": "acme"},
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 503
