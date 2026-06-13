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

"""Unit tests for the per-binding live counter view (#1342 / #1346).

The route enumerates a principal's grants at a given scope and attaches the
live ``UsageCounterProtocol`` reading for each binding's quota condition.
Mirrors :mod:`test_iam_effective_permission_route` style — direct call to
the route handler with the IamService and counter protocol mocked, so no
DB or Valkey is needed.

Pins:

  * Happy path: two grants with rate-limit / max-count quotas both return
    populated counter cells (count / limit / remaining / window_seconds).
  * Empty: a principal with zero grants returns ``entries=[]``.
  * Valkey down (PG-fallback acceptable): counter.get raises → response
    still returns 200, ``valkey_available=False``, the offending counter
    cell falls back to ``count=0``.
  * Sysadmin guard: non-sysadmin → 403.
  * 404 on unknown principal, 422 on malformed UUID.
  * window_seconds round-trips correctly from grant.quota → response cell.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"

# Bind the class-body handler once. The route is declared via
# ``@router.get`` at class-body scope, so it's an attribute we can call
# directly with keyword arguments — same pattern as
# test_iam_effective_permission_route.
_usage = AdminService.list_grant_usage


def _req(roles):
    return SimpleNamespace(state=SimpleNamespace(
        principal=SimpleNamespace(
            id=uuid4(), provider="local", subject_id="alice", roles=roles,
        ),
        principal_role=list(roles),
        policy_allowed=True,
    ))


def _mgr(principal=None, grants=None, scope_schema="iam"):
    mgr = MagicMock()
    principal = principal if principal is not None else SimpleNamespace(
        id=uuid4(), subject_id="alice", roles=["editor"],
    )
    mgr.get_principal = AsyncMock(return_value=principal)
    mgr.resolve_schema = AsyncMock(return_value=scope_schema)
    mgr.storage = MagicMock()
    mgr.storage.list_grants_for_subject = AsyncMock(return_value=grants or [])
    return mgr


def _patch_iam(mgr, counter=None):
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.usage_counter import UsageCounterProtocol

    def _get_proto(cls):
        if cls is IamService:
            return mgr
        if cls is UsageCounterProtocol:
            return counter
        return None

    return patch(_GET_PROTOCOL, side_effect=_get_proto)


# ---- Happy path ---------------------------------------------------------


@pytest.mark.asyncio
async def test_two_grants_with_quotas_carry_counters():
    pid = uuid4()
    grant_a_id = uuid4()
    grant_b_id = uuid4()
    grants = [
        {
            "id": grant_a_id,
            "subject_kind": "principal",
            "subject_ref": str(pid),
            "object_kind": "role",
            "object_ref": "editor",
            "effect": "allow",
            "resource_kind": None,
            "resource_ref": None,
            "quota": {
                "rate_limit": {"limit": 100, "window_seconds": 60},
            },
            "valid_from": None,
            "valid_until": None,
        },
        {
            "id": grant_b_id,
            "subject_kind": "principal",
            "subject_ref": str(pid),
            "object_kind": "policy",
            "object_ref": "read_only",
            "effect": "allow",
            "resource_kind": "collection",
            "resource_ref": "collA",
            "quota": {
                "max_count": {"limit": 10},
            },
            "valid_from": None,
            "valid_until": None,
        },
    ]

    counter = MagicMock()
    # rate_limit row for grant A → 17/100; max_count row for grant B → 3/10.
    async def _get(namespace, principal_key, *, window_seconds=None):
        if window_seconds == 60:
            return 17
        if window_seconds is None:
            return 3
        return 0
    counter.get = _get

    mgr = _mgr(grants=grants)
    req = _req(roles=["sysadmin"])
    with _patch_iam(mgr, counter=counter):
        out = await _usage(req, principal_id=str(pid), catalog_id=None)

    assert out.principal_id == str(pid)
    assert out.valkey_available is True
    assert len(out.entries) == 2

    by_id = {e.grant_id: e for e in out.entries}
    a = by_id[str(grant_a_id)]
    assert a.counters.rate_limit is not None
    assert a.counters.rate_limit.count == 17
    assert a.counters.rate_limit.limit == 100
    assert a.counters.rate_limit.window_seconds == 60
    assert a.counters.rate_limit.remaining == 83
    # window_start is the floored bucket — a non-empty ISO string.
    assert a.counters.rate_limit.window_start
    assert "T" in a.counters.rate_limit.window_start
    assert a.counters.max_count is None

    b = by_id[str(grant_b_id)]
    assert b.counters.max_count is not None
    assert b.counters.max_count.count == 3
    assert b.counters.max_count.limit == 10
    assert b.counters.max_count.remaining == 7
    assert b.counters.rate_limit is None
    # Resource scope is echoed back from the grant row.
    assert b.resource_kind == "collection"
    assert b.resource_ref == "collA"


# ---- Empty principal ---------------------------------------------------


@pytest.mark.asyncio
async def test_empty_grants_returns_empty_entries():
    pid = uuid4()
    counter = MagicMock()
    counter.get = AsyncMock(return_value=0)
    mgr = _mgr(grants=[])
    req = _req(roles=["sysadmin"])
    with _patch_iam(mgr, counter=counter):
        out = await _usage(req, principal_id=str(pid), catalog_id=None)
    assert out.entries == []
    # counter was registered → valkey_available stays True even with no
    # rows to probe.
    assert out.valkey_available is True


# ---- Valkey down → PG fallback, valkey_available=False ------------------


@pytest.mark.asyncio
async def test_valkey_unavailable_marks_response_and_falls_back():
    pid = uuid4()
    grant_id = uuid4()
    grants = [{
        "id": grant_id,
        "subject_kind": "principal",
        "subject_ref": str(pid),
        "object_kind": "role",
        "object_ref": "editor",
        "effect": "allow",
        "quota": {"rate_limit": {"limit": 100, "window_seconds": 60}},
        "valid_from": None,
        "valid_until": None,
    }]

    counter = MagicMock()
    async def _raise(*args, **kwargs):
        raise RuntimeError("Valkey unreachable and PG row missing")
    counter.get = _raise

    mgr = _mgr(grants=grants)
    req = _req(roles=["sysadmin"])
    with _patch_iam(mgr, counter=counter):
        out = await _usage(req, principal_id=str(pid), catalog_id=None)

    # Response is served (200 / model returned) — the route doesn't 503.
    # The wire flag flips to False so the UI shows the stale banner.
    assert out.valkey_available is False
    # The counter cell still renders with count=0 so the UI has something
    # to draw.
    assert len(out.entries) == 1
    rl = out.entries[0].counters.rate_limit
    assert rl is not None
    assert rl.count == 0
    assert rl.limit == 100
    assert rl.window_seconds == 60
    assert rl.remaining == 100


# ---- Sysadmin guard ----------------------------------------------------


@pytest.mark.asyncio
async def test_non_sysadmin_rejected_403():
    pid = uuid4()
    req = _req(roles=["admin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _usage(req, principal_id=str(pid), catalog_id=None)
    assert exc.value.status_code == 403
    # Guard runs BEFORE we hit the IamService — we never get to a principal
    # lookup, so the storage call is never awaited.
    mgr.get_principal.assert_not_awaited()
    mgr.storage.list_grants_for_subject.assert_not_awaited()


# ---- 404 / 422 ---------------------------------------------------------


@pytest.mark.asyncio
async def test_404_unknown_principal():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.get_principal = AsyncMock(return_value=None)
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _usage(req, principal_id=str(pid), catalog_id=None)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_422_invalid_principal_uuid():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _usage(req, principal_id="not-a-uuid", catalog_id=None)
    assert exc.value.status_code == 422
    mgr.get_principal.assert_not_awaited()


# ---- Counter shape: window_seconds round-trips --------------------------


@pytest.mark.asyncio
async def test_window_seconds_and_count_round_trip():
    pid = uuid4()
    grant_id = uuid4()
    # Pick a non-default window width so a stray "60" can't fake a pass.
    grants = [{
        "id": grant_id,
        "subject_kind": "principal",
        "subject_ref": str(pid),
        "object_kind": "role",
        "object_ref": "editor",
        "effect": "allow",
        "quota": {"rate_limit": {"limit": 200, "window_seconds": 300}},
        "valid_from": None,
        "valid_until": None,
    }]
    counter = MagicMock()
    async def _get(namespace, principal_key, *, window_seconds=None):
        # The route must pass the window_seconds from grant.quota through.
        assert window_seconds == 300
        # And the namespace must match quota_namespace(grant_id).
        assert namespace == f"grant:{grant_id}"
        # principal_key is scope=principal → the principal id from the URL.
        assert principal_key == str(pid)
        return 42
    counter.get = _get
    mgr = _mgr(grants=grants)
    req = _req(roles=["sysadmin"])
    with _patch_iam(mgr, counter=counter):
        out = await _usage(req, principal_id=str(pid), catalog_id=None)

    rl = out.entries[0].counters.rate_limit
    assert rl is not None
    assert rl.window_seconds == 300
    assert rl.count == 42
    assert rl.limit == 200
    assert rl.remaining == 158


# ---- Catalog-scope schema lookup ---------------------------------------


@pytest.mark.asyncio
async def test_catalog_scope_resolves_schema():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    mgr = _mgr(grants=[], scope_schema="catalog_cat1")
    with _patch_iam(mgr):
        out = await _usage(req, principal_id=str(pid), catalog_id="cat1")
    assert out.catalog_id == "cat1"
    mgr.resolve_schema.assert_awaited_once_with("cat1")
    # storage was called with the resolved schema, not the literal "iam".
    call = mgr.storage.list_grants_for_subject.await_args
    assert call.kwargs["scope_schema"] == "catalog_cat1"


# ---- No counter backend registered --------------------------------------


@pytest.mark.asyncio
async def test_no_counter_backend_marks_response_unavailable():
    """When no UsageCounterProtocol is registered at all, the route still
    returns the binding list (counters empty) and sets valkey_available
    to False so the UI shows the same stale banner."""
    pid = uuid4()
    grant_id = uuid4()
    grants = [{
        "id": grant_id,
        "subject_kind": "principal",
        "subject_ref": str(pid),
        "object_kind": "role",
        "object_ref": "editor",
        "effect": "allow",
        "quota": {"rate_limit": {"limit": 100, "window_seconds": 60}},
        "valid_from": None,
        "valid_until": None,
    }]
    mgr = _mgr(grants=grants)
    req = _req(roles=["sysadmin"])
    # No counter passed — _patch_iam returns None for UsageCounterProtocol.
    with _patch_iam(mgr, counter=None):
        out = await _usage(req, principal_id=str(pid), catalog_id=None)
    assert out.valkey_available is False
    # Binding is still listed; the counter cell is the empty default.
    assert len(out.entries) == 1
    assert out.entries[0].counters.rate_limit is None
    assert out.entries[0].counters.max_count is None
