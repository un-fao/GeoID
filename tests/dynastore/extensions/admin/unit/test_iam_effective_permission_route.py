"""Unit tests for the IAM effective-permissions explainer route (#1346).

Mirrors :mod:`test_iam_denylist_routes` style. Pins:

  * Allow / deny / deny-precedence cases produce the right top-level
    decision, decision_reason, deny_precedence_applied, and grants_considered
    shape.
  * Validity-window data carries through from the resolver into the trace.
  * The rate_limit condition trace lights up under-budget vs over-budget.
  * Sysadmin guard rejects non-sysadmin callers with 403.
  * 404 on unknown principal.
  * 422 on invalid action vocabulary / invalid UUID.

All tests mock the IamService (and through it the policy service) so no DB
or catalog protocol is needed.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.modules.iam.policies import _GrantTraceRecord, _TraceCollector


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"

_explain = AdminService.get_effective_permissions_explained


def _req(roles):
    return SimpleNamespace(state=SimpleNamespace(
        principal=SimpleNamespace(
            id=uuid4(), provider="local", subject_id="alice", roles=roles,
        ),
        principal_role=list(roles),
        policy_allowed=True,
    ))


def _mgr(decision=(True, "Allowed by policy allow_pol"), records=None,
         deny_precedence=False, decision_reason=None, principal=None):
    mgr = MagicMock()
    principal = principal or SimpleNamespace(
        id=uuid4(),
        subject_id="alice",
        roles=["editor"],
        custom_policies=None,
    )
    mgr.get_principal = AsyncMock(return_value=principal)

    perm = MagicMock()

    async def _exec_eval(**kwargs):
        collector = kwargs.get("trace_collector")
        if collector is not None and records is not None:
            collector.records.extend(records)
            collector.deny_precedence_applied = deny_precedence
            collector.decision_reason = decision_reason or decision[1]
        return decision

    perm.evaluate_access = _exec_eval
    mgr.get_policy_service = MagicMock(return_value=perm)
    return mgr


def _patch_iam(mgr):
    from dynastore.modules.iam.iam_service import IamService

    def _get_proto(cls):
        if cls is IamService:
            return mgr
        return None

    return patch(_GET_PROTOCOL, side_effect=_get_proto)


# ---- Happy paths --------------------------------------------------------


@pytest.mark.asyncio
async def test_allow_from_catalog_wide_grant_no_deny():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    rec = _GrantTraceRecord(
        policy_id="allow_pol", grant_id="g-1", subject_kind="role",
        subject_ref="editor", object_kind="role", object_ref="editor",
        effect="allow", matched=True,
    )
    mgr = _mgr(
        decision=(True, "Allowed by policy allow_pol"),
        records=[rec], deny_precedence=False,
        decision_reason="Allowed by policy allow_pol",
    )
    with _patch_iam(mgr):
        out = await _explain(
            req, principal_id=str(pid), action="GET",
            catalog_id="cat1", collection_id=None,
            resource_kind=None, resource_ref=None,
        )
    assert out.decision == "allow"
    assert out.decision_reason == "Allowed by policy allow_pol"
    assert out.deny_precedence_applied is False
    assert len(out.grants_considered) == 1
    g = out.grants_considered[0]
    assert g.grant_id == "g-1"
    assert g.effect == "allow"
    assert g.matched is True


@pytest.mark.asyncio
async def test_deny_wins_response_shape():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    allow_rec = _GrantTraceRecord(
        policy_id="allow_pol", grant_id="g-allow", subject_kind="role",
        subject_ref="editor", object_kind="role", object_ref="editor",
        effect="allow", matched=True,
    )
    deny_rec = _GrantTraceRecord(
        policy_id="deny_pol", grant_id="g-deny", subject_kind="role",
        subject_ref="blocked", object_kind="role", object_ref="blocked",
        effect="deny", matched=True, resource_kind="collection",
        resource_ref="collA",
    )
    mgr = _mgr(
        decision=(False, "Explicit DENY by policy deny_pol"),
        records=[allow_rec, deny_rec], deny_precedence=True,
        decision_reason="Explicit DENY by policy deny_pol",
    )
    with _patch_iam(mgr):
        out = await _explain(
            req, principal_id=str(pid), action="GET",
            catalog_id="cat1", collection_id="collA",
            resource_kind="collection", resource_ref="collA",
        )
    assert out.decision == "deny"
    assert "deny_pol" in out.decision_reason
    assert out.deny_precedence_applied is True
    by_grant = {g.grant_id: g for g in out.grants_considered}
    assert by_grant["g-allow"].matched is True
    assert by_grant["g-deny"].matched is True
    assert by_grant["g-deny"].effect == "deny"


@pytest.mark.asyncio
async def test_validity_window_carried_through():
    from datetime import datetime, timedelta, timezone
    vf = datetime.now(tz=timezone.utc) - timedelta(days=1)
    vu = datetime.now(tz=timezone.utc) + timedelta(days=1)
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    rec = _GrantTraceRecord(
        policy_id="allow_pol", grant_id="g-w", subject_kind="role",
        subject_ref="editor", object_kind="role", object_ref="editor",
        effect="allow", matched=True,
        valid_from=vf, valid_until=vu, in_validity_window=True,
    )
    mgr = _mgr(
        decision=(True, "Allowed by policy allow_pol"),
        records=[rec], deny_precedence=False,
        decision_reason="Allowed by policy allow_pol",
    )
    with _patch_iam(mgr):
        out = await _explain(
            req, principal_id=str(pid), action="GET",
            catalog_id="cat1", collection_id=None,
            resource_kind=None, resource_ref=None,
        )
    assert out.grants_considered[0].valid_from == vf
    assert out.grants_considered[0].valid_until == vu
    assert out.grants_considered[0].in_validity_window is True


@pytest.mark.asyncio
async def test_condition_trace_under_budget():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    rec = _GrantTraceRecord(
        policy_id="rl_pol", grant_id="g-rl", subject_kind="role",
        subject_ref="editor", object_kind="role", object_ref="editor",
        effect="allow", matched=True,
        conditions_evaluated=[{
            "type": "rate_limit",
            "config": {"limit": 100, "window_seconds": 60},
            "passed": True,
            "detail": "rate 73/100 within window",
        }],
    )
    mgr = _mgr(
        decision=(True, "Allowed by policy rl_pol"),
        records=[rec], deny_precedence=False,
        decision_reason="Allowed by policy rl_pol",
    )
    with _patch_iam(mgr):
        out = await _explain(
            req, principal_id=str(pid), action="GET",
            catalog_id="cat1", collection_id=None,
            resource_kind=None, resource_ref=None,
        )
    g = out.grants_considered[0]
    assert len(g.conditions_evaluated) == 1
    assert g.conditions_evaluated[0].type == "rate_limit"
    assert g.conditions_evaluated[0].passed is True
    assert g.conditions_evaluated[0].detail == "rate 73/100 within window"


@pytest.mark.asyncio
async def test_condition_trace_over_budget():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    rec = _GrantTraceRecord(
        policy_id="rl_pol", grant_id="g-rl", subject_kind="role",
        subject_ref="editor", object_kind="role", object_ref="editor",
        effect="allow", matched=False, why_not="condition 'rate_limit' did not pass",
        conditions_evaluated=[{
            "type": "rate_limit",
            "config": {"limit": 100, "window_seconds": 60},
            "passed": False,
            "detail": "rate 105/100 — over budget",
        }],
    )
    mgr = _mgr(
        decision=(False, "Deny by Default (No matching ALLOW policy found)"),
        records=[rec], deny_precedence=False,
        decision_reason="Deny by Default (No matching ALLOW policy found)",
    )
    with _patch_iam(mgr):
        out = await _explain(
            req, principal_id=str(pid), action="GET",
            catalog_id="cat1", collection_id=None,
            resource_kind=None, resource_ref=None,
        )
    assert out.decision == "deny"
    g = out.grants_considered[0]
    assert g.matched is False
    assert g.conditions_evaluated[0].passed is False
    assert "over budget" in g.conditions_evaluated[0].detail


# ---- Sysadmin guard -----------------------------------------------------


@pytest.mark.asyncio
async def test_non_sysadmin_rejected_403():
    pid = uuid4()
    req = _req(roles=["admin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _explain(
                req, principal_id=str(pid), action="GET",
                catalog_id=None, collection_id=None,
                resource_kind=None, resource_ref=None,
            )
    assert exc.value.status_code == 403
    mgr.get_principal.assert_not_awaited()


# ---- 404 / 422 ----------------------------------------------------------


@pytest.mark.asyncio
async def test_404_unknown_principal():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    mgr.get_principal = AsyncMock(return_value=None)
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _explain(
                req, principal_id=str(pid), action="GET",
                catalog_id=None, collection_id=None,
                resource_kind=None, resource_ref=None,
            )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_422_invalid_action_vocabulary():
    pid = uuid4()
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _explain(
                req, principal_id=str(pid), action="FLY",
                catalog_id=None, collection_id=None,
                resource_kind=None, resource_ref=None,
            )
    assert exc.value.status_code == 422
    mgr.get_principal.assert_not_awaited()


@pytest.mark.asyncio
async def test_422_invalid_principal_uuid():
    req = _req(roles=["sysadmin"])
    mgr = _mgr()
    with _patch_iam(mgr):
        with pytest.raises(HTTPException) as exc:
            await _explain(
                req, principal_id="not-a-uuid", action="GET",
                catalog_id=None, collection_id=None,
                resource_kind=None, resource_ref=None,
            )
    assert exc.value.status_code == 422
    mgr.get_principal.assert_not_awaited()
