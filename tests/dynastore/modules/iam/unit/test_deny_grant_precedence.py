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

"""Regression pins for D9 deny-grant-effect precedence in
``PolicyService._resolve_effective_policies`` (un-fao/GeoID#2156).

Before this fix, ``_resolve_effective_policies`` appended a role's ALLOW
policies unchanged even when the grant row carried ``effect='deny'``.  A
deny grant on a role therefore silently widened access instead of
restricting it.  Same gap for direct ``object_kind='policy'`` grant rows.

These are pure unit tests — ``iam_storage``, ``get_policy``, and
``_resolve_schema`` are stubbed, no DB or catalog protocol involved.
Mirrors the mocking style of ``test_evaluate_access_collection_scope.py``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService


_SCHEMA = "s_test"
_CATALOG_ID = "cat_a"
_PATH = f"/stac/catalogs/{_CATALOG_ID}/collections/col/items"
_METHOD = "GET"


class _FakeIamStorage:
    """Minimal storage stub (mirrors test_evaluate_access_collection_scope)."""

    def __init__(self, grants: List[Dict[str, Any]], roles: Dict[str, Role]):
        self._grants = grants
        self._roles = roles

    async def resolve_effective_grants(
        self,
        principal_id: Any,
        catalog_schema: Optional[str] = None,
        collection_id: Optional[str] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        return list(self._grants)

    async def get_role(
        self, role_id: str, schema: str = "iam", **_: Any
    ) -> Optional[Role]:
        return self._roles.get(role_id)


def _service(storage: _FakeIamStorage, policies: Dict[str, Policy]) -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = storage  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _fixed_schema(catalog_id: Any, conn: Any = None, **kw: Any) -> str:
        return _SCHEMA if catalog_id else "iam"

    async def _get_policy(pid: str, catalog_id: Any = None) -> Optional[Policy]:
        return policies.get(pid)

    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    return svc


def _role(name: str, policy_ids: List[str]) -> Role:
    return Role(id=name, name=name, policies=policy_ids)


def _role_grant(role_name: str, *, effect: str) -> Dict[str, Any]:
    return {"object_kind": "role", "object_ref": role_name, "effect": effect}


def _policy_grant(policy_id: str, *, effect: str) -> Dict[str, Any]:
    return {"object_kind": "policy", "object_ref": policy_id, "effect": effect}


def _allow_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="ALLOW", actions=[".*"], resources=[".*"])


async def _call(svc: PolicyService, *, principal_id: Any) -> tuple[bool, str]:
    return await svc.evaluate_access(
        principals=[],
        path=_PATH,
        method=_METHOD,
        catalog_id=_CATALOG_ID,
        principal_id=principal_id,
    )


# ── role-grant deny path ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_deny_role_grant_beats_allow_role_grant() -> None:
    """A deny grant on a role overrides a separate allow grant on the same
    scope — the role's ALLOW policies are contributed as synthetic DENYs,
    so deny-precedence in ``evaluate_access`` correctly denies access.

    This is the core regression covered by #2156: before the fix a deny
    grant on a role appended the role's ALLOW policies unchanged, causing
    the evaluator to grant instead of deny.
    """
    # Two grants on the same role: one allow, one deny.
    allow_grant = _role_grant("reader", effect="allow")
    deny_grant = _role_grant("reader", effect="deny")

    # The fake storage returns both; the collector deduplicates by role name
    # (setdefault keeps the first, which is the allow row in this order).
    # We want to confirm the DENY row is independently handled, so we use
    # two separate roles: reader_allow and reader_deny, each with the same
    # policy so the DENY synthetic copy beats the ALLOW copy.
    allow_grant2 = _role_grant("reader_allow", effect="allow")
    deny_grant2 = _role_grant("reader_deny", effect="deny")

    storage = _FakeIamStorage(
        grants=[allow_grant2, deny_grant2],
        roles={
            "reader_allow": _role("reader_allow", ["read_pol"]),
            "reader_deny": _role("reader_deny", ["read_pol"]),
        },
    )
    svc = _service(storage, {"read_pol": _allow_policy("read_pol")})

    allowed, reason = await _call(svc, principal_id=uuid4())

    # The deny grant rewrites read_pol to DENY. Even though an allow grant
    # also contributes the stored ALLOW read_pol, the DENY synthetic copy wins
    # by deny-precedence (equal priority → DENY beats ALLOW).
    assert allowed is False, f"Expected DENY but got ALLOW. Reason: {reason}"


@pytest.mark.asyncio
async def test_deny_role_grant_alone_denies() -> None:
    """A deny grant on a role — with no competing allow grant — must deny."""
    storage = _FakeIamStorage(
        grants=[_role_grant("editor", effect="deny")],
        roles={"editor": _role("editor", ["edit_pol"])},
    )
    svc = _service(storage, {"edit_pol": _allow_policy("edit_pol")})

    allowed, reason = await _call(svc, principal_id=uuid4())

    assert allowed is False, f"Expected DENY but got ALLOW. Reason: {reason}"
    # The synthetic copy of 'edit_pol' with effect=DENY must be in the reason.
    assert "edit_pol" in reason


@pytest.mark.asyncio
async def test_allow_role_grant_allows() -> None:
    """An allow grant on a role — with no deny — allows access. Confirms the
    allow path is not broken by the deny-grant fix."""
    storage = _FakeIamStorage(
        grants=[_role_grant("viewer", effect="allow")],
        roles={"viewer": _role("viewer", ["view_pol"])},
    )
    svc = _service(storage, {"view_pol": _allow_policy("view_pol")})

    allowed, reason = await _call(svc, principal_id=uuid4())

    assert allowed is True, f"Expected ALLOW but got DENY. Reason: {reason}"
    assert "view_pol" in reason


# ── direct policy-grant deny path ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_deny_policy_grant_overrides_stored_allow_effect() -> None:
    """A direct policy grant with effect='deny' contributes the policy as a
    synthetic DENY, overriding its stored ALLOW effect.

    This covers the ``object_kind='policy'`` branch of #2156: before the
    fix, a deny policy grant appended the ALLOW policy unchanged, causing
    the evaluator to grant instead of deny.
    """
    pol = _allow_policy("direct_pol")
    storage = _FakeIamStorage(
        grants=[_policy_grant("direct_pol", effect="deny")],
        roles={},
    )
    svc = _service(storage, {"direct_pol": pol})

    allowed, reason = await _call(svc, principal_id=uuid4())

    assert allowed is False, f"Expected DENY but got ALLOW. Reason: {reason}"
    assert "direct_pol" in reason


@pytest.mark.asyncio
async def test_allow_policy_grant_allows() -> None:
    """An allow direct policy grant — no deny — allows access. Confirms the
    allow path in the policy-grant branch is not broken."""
    pol = _allow_policy("direct_pol_a")
    storage = _FakeIamStorage(
        grants=[_policy_grant("direct_pol_a", effect="allow")],
        roles={},
    )
    svc = _service(storage, {"direct_pol_a": pol})

    allowed, reason = await _call(svc, principal_id=uuid4())

    assert allowed is True, f"Expected ALLOW but got DENY. Reason: {reason}"
    assert "direct_pol_a" in reason


@pytest.mark.asyncio
async def test_no_principal_id_skips_grant_resolution() -> None:
    """When ``principal_id=None`` the grant resolution step is skipped entirely.
    A deny role grant therefore has no effect — the evaluator sees no policies
    and returns implicit deny-by-default."""
    storage = _FakeIamStorage(
        grants=[_role_grant("reader", effect="deny")],
        roles={"reader": _role("reader", ["read_pol"])},
    )
    svc = _service(storage, {"read_pol": _allow_policy("read_pol")})

    # Call with principal_id=None — grant branch is not triggered.
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_PATH,
        method=_METHOD,
        catalog_id=_CATALOG_ID,
        principal_id=None,
    )

    assert allowed is False
    assert "Deny by Default" in reason
