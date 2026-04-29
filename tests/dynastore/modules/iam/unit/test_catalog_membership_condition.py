"""Unit tests for ``CatalogMembershipHandler``.

Pure async unit tests — no app lifespan, no DB. The IamQueryProtocol is
swapped via ``register_plugin`` / ``unregister_plugin`` so the handler
exercises its real Protocol-resolution path through ``get_protocol``.

Each test case constructs an ``EvaluationContext`` directly (the same
shape ``IamMiddleware`` builds during request dispatch) and verifies the
handler's allow / deny verdict matches the IAM model — sysadmin and
platform grants pass; others must hold a grant in the URL's catalog.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from dynastore.models.auth import Principal
from dynastore.models.protocols.iam_query import IamQueryProtocol
from dynastore.modules.iam.conditions import (
    CatalogMembershipHandler,
    EvaluationContext,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin


pytestmark = pytest.mark.asyncio


class _StubIamQuery:
    """Minimal IamQueryProtocol implementor returning a canned membership."""

    def __init__(self, catalogs: Optional[List[str]] = None, platform: bool = False):
        self._catalogs = catalogs or []
        self._platform = platform
        self.calls: List[Dict[str, Any]] = []

    async def list_catalog_memberships(
        self, provider: str, subject_id: str,
    ) -> Dict[str, Any]:
        self.calls.append({"provider": provider, "subject_id": subject_id})
        return {
            "platform": self._platform,
            "catalogs": list(self._catalogs),
            "total": len(self._catalogs),
        }


@pytest.fixture
def handler() -> CatalogMembershipHandler:
    return CatalogMembershipHandler()


@pytest.fixture
def iam_query():
    """Register a stub IamQueryProtocol implementor for the duration of the test."""
    stub = _StubIamQuery(catalogs=["acme"], platform=False)
    register_plugin(stub)
    try:
        yield stub
    finally:
        unregister_plugin(stub)


def _make_ctx(
    catalog_id: Optional[str],
    principal: Optional[Principal],
) -> EvaluationContext:
    extras: Dict[str, Any] = {}
    if principal is not None:
        extras["principal_obj"] = principal
    return EvaluationContext(
        request=None,
        storage=None,  # type: ignore[arg-type]
        catalog_id=catalog_id,
        path="/web/dashboard/catalogs/acme/stats",
        method="GET",
        extras=extras,
    )


_SUBJECT_COUNTER = 0


def _principal(roles: Optional[List[str]] = None) -> Principal:
    """Create a unique-subject Principal so the per-pod membership cache
    can't bleed across tests in the same pytest session."""
    global _SUBJECT_COUNTER
    _SUBJECT_COUNTER += 1
    sub = f"alice-{_SUBJECT_COUNTER}"
    return Principal(
        provider="internal",
        subject_id=sub,
        display_name=sub,
        roles=roles or ["admin"],
        is_active=True,
    )


async def test_anonymous_principal_denied(handler):
    """No principal → fail closed (per-catalog policy doesn't fit anonymous)."""
    ctx = _make_ctx(catalog_id="acme", principal=None)
    assert await handler.evaluate({}, ctx) is False


async def test_missing_catalog_id_denied(handler):
    """Path declared per-catalog but extractor failed → fail closed."""
    ctx = _make_ctx(catalog_id=None, principal=_principal())
    assert await handler.evaluate({}, ctx) is False


async def test_sysadmin_passes_without_iam_call(handler):
    """SYSADMIN role short-circuits — no IAM round trip."""
    ctx = _make_ctx(catalog_id="acme", principal=_principal(roles=["sysadmin"]))
    # Intentionally NO iam_query fixture — handler must not need it.
    assert await handler.evaluate({}, ctx) is True


async def test_sysadmin_bypass_disabled_by_config(handler, iam_query):
    """allow_sysadmin=False forces the IAM membership check even for sysadmin."""
    # Stub returns memberships for "acme" only — caller asks for "other".
    iam_query._catalogs = ["acme"]
    ctx = _make_ctx(catalog_id="other", principal=_principal(roles=["sysadmin"]))
    assert await handler.evaluate({"allow_sysadmin": False}, ctx) is False


async def test_platform_membership_passes(handler):
    """A platform-grant membership passes regardless of catalog_id."""
    stub = _StubIamQuery(catalogs=[], platform=True)
    register_plugin(stub)
    try:
        ctx = _make_ctx(catalog_id="any-catalog", principal=_principal())
        assert await handler.evaluate({}, ctx) is True
    finally:
        unregister_plugin(stub)


async def test_platform_bypass_disabled_by_config(handler):
    """allow_platform=False ignores the platform-grant shortcut."""
    stub = _StubIamQuery(catalogs=["acme"], platform=True)
    register_plugin(stub)
    try:
        ctx = _make_ctx(catalog_id="other", principal=_principal())
        assert await handler.evaluate({"allow_platform": False}, ctx) is False
    finally:
        unregister_plugin(stub)


async def test_membership_match_passes(handler, iam_query):
    """Catalog admin asking for an owned catalog → allow."""
    iam_query._catalogs = ["acme"]
    ctx = _make_ctx(catalog_id="acme", principal=_principal())
    assert await handler.evaluate({}, ctx) is True


async def test_membership_miss_denied(handler, iam_query):
    """Catalog admin asking for a NON-owned catalog → deny."""
    iam_query._catalogs = ["acme"]
    ctx = _make_ctx(catalog_id="other", principal=_principal())
    assert await handler.evaluate({}, ctx) is False


async def test_iam_unavailable_denied(handler):
    """No IamQueryProtocol registered → fail closed."""
    ctx = _make_ctx(catalog_id="acme", principal=_principal())
    # No fixture → no stub registered.
    assert await handler.evaluate({}, ctx) is False


async def test_principal_without_provider_denied(handler, iam_query):
    """Principal with no provider/subject_id is unidentifiable → fail closed."""
    p = Principal(
        provider="",  # empty
        subject_id="",
        display_name="ghost",
        roles=["admin"],
        is_active=True,
    )
    ctx = _make_ctx(catalog_id="acme", principal=p)
    assert await handler.evaluate({}, ctx) is False
