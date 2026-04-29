"""Unit contract for ``TenantScopeMiddleware``.

The middleware runs after ``IamMiddleware`` (which populates
``request.state.principal`` / ``principal_role``). For URLs matching a
tenant-scope registry rule it:
  - resolves the principal's catalog memberships via ``IamQueryProtocol``
    (cached) and writes ``request.state.authorized_catalogs``;
  - extracts ``catalog_id`` from the configured source (path-based via
    a named regex capture group on the rule's ``pattern``);
  - pins ``request.state.tenant_scope`` (catalog_id, collection_id) for
    downstream Protocol consumers;
  - short-circuits with 401/403 + structured ``dashboard_authz.denied`` log
    when the caller can't see that catalog.

Routes themselves carry zero authz code — the gate is entirely in middleware.
URL convention: ``/web/dashboard/catalogs/{catalog_id}[/collections/{collection_id}]/...``
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock

import pytest


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
    captured = {"called": False}

    async def call_next(_req):
        captured["called"] = True
        return SimpleNamespace(status_code=200, body=b"")

    response = await mw.dispatch(request, call_next)
    return captured["called"], response


# ---------------------------------------------------------------------------
# Registry — pattern shape
# ---------------------------------------------------------------------------

async def test_passthrough_when_no_rule_matches(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest("/something/unrelated", principal=_Principal("local", "u1"))

    called, _ = await _call(mw, request)

    assert called is True
    fake_iam_query.list_catalog_memberships.assert_not_called()


async def test_dashboard_root_does_not_match(monkeypatch, fake_iam_query):
    """``/web/dashboard/`` (root catalog picker) is NOT in the registry —
    anonymous callers can load the picker page."""
    from dynastore.extensions.iam.tenant_scope_registry import match_tenant_scope_rule

    for path in ("/web/dashboard", "/web/dashboard/", "/web/dashboard/catalogs"):
        assert match_tenant_scope_rule(path) is None, (
            f"{path} should NOT be tenant-scoped — it's the catalog picker root"
        )


async def test_per_catalog_data_endpoints_match_registry():
    """Every per-catalog data endpoint matches the path-based per-catalog rule."""
    from dynastore.extensions.iam.tenant_scope_registry import match_tenant_scope_rule

    for ep in ("stats", "logs", "events", "tasks", "ogc-compliance", "processes/", "collections", ""):
        path = f"/web/dashboard/catalogs/acme/{ep}".rstrip("/")
        rule = match_tenant_scope_rule(path)
        assert rule is not None, f"registry should match {path}"
        assert rule.id == "dashboard_per_catalog"


async def test_per_collection_data_endpoints_match_registry():
    """Per-collection routes match the more-specific collection rule."""
    from dynastore.extensions.iam.tenant_scope_registry import match_tenant_scope_rule

    for ep in ("stats", "logs", "events"):
        path = f"/web/dashboard/catalogs/acme/collections/foo/{ep}"
        rule = match_tenant_scope_rule(path)
        assert rule is not None, f"registry should match {path}"
        assert rule.id == "dashboard_per_collection", (
            f"{path} should match per-collection rule first (more specific)"
        )


# ---------------------------------------------------------------------------
# Anonymous handling
# ---------------------------------------------------------------------------

async def test_anonymous_blocked_on_per_catalog_route(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest("/web/dashboard/catalogs/acme/stats")

    called, response = await _call(mw, request)

    assert called is False
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# Sysadmin / platform grant bypass
# ---------------------------------------------------------------------------

async def test_sysadmin_role_bypass(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/_system_/stats",
        principal=_Principal("local", "sys_admin_user"),
        principal_role=["sysadmin"],
    )

    called, _ = await _call(mw, request)
    assert called is True


async def test_platform_grant_bypass_via_membership(monkeypatch, fake_iam_query_platform):
    mw = await _make_middleware(monkeypatch, fake_iam_query_platform)
    request = _FakeRequest(
        "/web/dashboard/catalogs/_system_/stats",
        principal=_Principal("local", "platform_role_holder"),
        principal_role=["custom_platform_role"],
    )

    called, _ = await _call(mw, request)
    assert called is True


# ---------------------------------------------------------------------------
# Per-catalog admin gating — path extracts catalog_id
# ---------------------------------------------------------------------------

async def test_catalog_admin_owned_catalog_passes(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/acme/stats",
        principal=_Principal("local", "cadmin_owned"),
        principal_role=["catalog_admin"],
    )

    called, _ = await _call(mw, request)

    assert called is True
    # Scope is pinned to request.state for downstream Protocol consumers.
    assert request.state.tenant_scope.catalog_id == "acme"
    assert request.state.tenant_scope.collection_id is None
    assert getattr(request.state, "authorized_catalogs", None) == {
        "platform": False, "catalogs": ["acme"], "total": 1,
    }


async def test_catalog_admin_unowned_catalog_403(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/other_catalog/stats",
        principal=_Principal("local", "cadmin_unowned"),
        principal_role=["catalog_admin"],
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


async def test_catalog_admin_system_scope_403(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/_system_/stats",
        principal=_Principal("local", "cadmin_system"),
        principal_role=["catalog_admin"],
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Per-collection scope — collection_id pinned to state
# ---------------------------------------------------------------------------

async def test_collection_scope_pins_collection_id(monkeypatch, fake_iam_query):
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/acme/collections/myset/stats",
        principal=_Principal("local", "cadmin_collection"),
        principal_role=["catalog_admin"],
    )

    called, _ = await _call(mw, request)

    assert called is True
    assert request.state.tenant_scope.catalog_id == "acme"
    assert request.state.tenant_scope.collection_id == "myset"


async def test_collection_scope_inherits_catalog_gate(monkeypatch, fake_iam_query):
    """Collection-scoped requests gate on catalog_id (caller must own catalog)."""
    mw = await _make_middleware(monkeypatch, fake_iam_query)
    request = _FakeRequest(
        "/web/dashboard/catalogs/other_catalog/collections/myset/stats",
        principal=_Principal("local", "cadmin_other_coll"),
        principal_role=["catalog_admin"],
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# IAM unavailable
# ---------------------------------------------------------------------------

async def test_iam_unavailable_returns_503(monkeypatch):
    mw = await _make_middleware(monkeypatch, iam_query=None)
    request = _FakeRequest(
        "/web/dashboard/catalogs/acme/stats",
        principal=_Principal("local", "cadmin_iam_unavail"),
        principal_role=["catalog_admin"],
    )

    called, response = await _call(mw, request)
    assert called is False
    assert response.status_code == 503
