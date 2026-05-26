"""Pure-unit pins for the evaluate_access trace byproduct (#1346).

The trace is computed from the SAME walk the hot path uses — a collector
threaded through evaluate_access records each step. Tests here pin:

  * The trace flag does NOT change the verdict (drift property).
  * Allow / deny / deny-precedence cases produce correctly-shaped traces.
  * Per-condition outcomes are recorded.
  * Grant identity (id / scope / validity window) flows from the
    resolver into the trace records.
  * Records for non-matching policies carry a why_not reason.

Mirrors the mocking style of test_evaluate_access_collection_scope.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from dynastore.models.auth import Policy, Condition
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService, _TraceCollector


_SCHEMA = "s_test_catalog"
_CATALOG_ID = "test_catalog"
_COLL_A = "collA"


class _FakeIamStorage:
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
        out: List[Dict[str, Any]] = []
        for row in self._grants:
            rk = row.get("resource_kind")
            rr = row.get("resource_ref")
            if rk is None:
                out.append(row)
            elif collection_id is not None and rk == "collection" and rr == collection_id:
                out.append(row)
        return out

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

    async def _fixed_schema(catalog_id, conn=None):  # noqa: ANN001
        return _SCHEMA if catalog_id else "iam"

    async def _get_policy(pid, catalog_id=None):  # noqa: ANN001
        return policies.get(pid)

    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    return svc


def _role(name: str, policy_ids: List[str]) -> Role:
    return Role(id=name, name=name, policies=policy_ids)


def _grant(
    role_name: str,
    *,
    grant_id: str = "g-1",
    effect: str = "allow",
    resource_kind: Optional[str] = None,
    resource_ref: Optional[str] = None,
    valid_from: Any = None,
    valid_until: Any = None,
) -> Dict[str, Any]:
    return {
        "id": grant_id,
        "object_kind": "role",
        "object_ref": role_name,
        "effect": effect,
        "resource_kind": resource_kind,
        "resource_ref": resource_ref,
        "valid_from": valid_from,
        "valid_until": valid_until,
    }


def _allow_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="ALLOW", actions=[".*"], resources=[".*"])


def _deny_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="DENY", actions=[".*"], resources=[".*"])


async def _call(
    svc: PolicyService,
    *,
    collection_id: Optional[str],
    principal_id: Any,
    trace: bool,
):
    collector = _TraceCollector() if trace else None
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=f"/catalogs/{_CATALOG_ID}/collections/{collection_id}/items",
        method="GET",
        catalog_id=_CATALOG_ID,
        principal_id=principal_id,
        collection_id=collection_id,
        trace_collector=collector,
    )
    return allowed, reason, collector


# ---- Drift property: trace flag never changes the verdict --------------


@pytest.mark.asyncio
async def test_trace_flag_does_not_change_verdict_allow() -> None:
    storage = _FakeIamStorage(
        grants=[_grant("editor", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    pid = uuid4()
    a1, r1, _ = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=False)
    a2, r2, col = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=True)
    assert a1 == a2 is True
    assert r1 == r2
    assert col is not None
    assert col.decision_reason == r2


@pytest.mark.asyncio
async def test_trace_flag_does_not_change_verdict_deny() -> None:
    storage = _FakeIamStorage(
        grants=[
            _grant("editor", resource_kind="collection", resource_ref=_COLL_A),
            _grant("blocked", grant_id="g-deny", effect="allow"),
        ],
        roles={
            "editor": _role("editor", ["allow_pol"]),
            "blocked": _role("blocked", ["deny_pol"]),
        },
    )
    svc = _service(
        storage,
        {"allow_pol": _allow_policy("allow_pol"), "deny_pol": _deny_policy("deny_pol")},
    )
    pid = uuid4()
    a1, r1, _ = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=False)
    a2, r2, col = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=True)
    assert a1 == a2 is False
    assert r1 == r2
    assert col is not None
    assert col.deny_precedence_applied is True


@pytest.mark.asyncio
async def test_trace_flag_does_not_change_verdict_deny_by_default() -> None:
    storage = _FakeIamStorage(grants=[], roles={})
    svc = _service(storage, {})
    pid = uuid4()
    a1, r1, _ = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=False)
    a2, r2, col = await _call(svc, collection_id=_COLL_A, principal_id=pid, trace=True)
    assert a1 == a2 is False
    assert r1 == r2
    assert col is not None
    assert col.deny_precedence_applied is False
    assert "Deny by Default" in col.decision_reason


# ---- Allow case: grant identity flows into trace records ---------------


@pytest.mark.asyncio
async def test_trace_records_grant_identity_for_allow() -> None:
    storage = _FakeIamStorage(
        grants=[
            _grant(
                "editor",
                grant_id="g-allow-1",
                resource_kind="collection",
                resource_ref=_COLL_A,
            )
        ],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    _, _, col = await _call(svc, collection_id=_COLL_A, principal_id=uuid4(), trace=True)
    assert col is not None
    matched = [r for r in col.records if r.matched]
    assert len(matched) == 1
    rec = matched[0]
    assert rec.grant_id == "g-allow-1"
    assert rec.subject_kind == "role"
    assert rec.subject_ref == "editor"
    assert rec.object_kind == "role"
    assert rec.object_ref == "editor"
    assert rec.resource_kind == "collection"
    assert rec.resource_ref == _COLL_A
    assert rec.effect == "allow"


# ---- Deny-precedence: both grants visible in trace ---------------------


@pytest.mark.asyncio
async def test_trace_marks_deny_precedence_and_records_both() -> None:
    storage = _FakeIamStorage(
        grants=[
            _grant(
                "editor",
                grant_id="g-allow",
                resource_kind="collection",
                resource_ref=_COLL_A,
            ),
            _grant("blocked", grant_id="g-deny", effect="allow"),
        ],
        roles={
            "editor": _role("editor", ["allow_pol"]),
            "blocked": _role("blocked", ["deny_pol"]),
        },
    )
    svc = _service(
        storage,
        {"allow_pol": _allow_policy("allow_pol"), "deny_pol": _deny_policy("deny_pol")},
    )
    allowed, reason, col = await _call(
        svc, collection_id=_COLL_A, principal_id=uuid4(), trace=True
    )
    assert allowed is False
    assert col is not None
    assert col.deny_precedence_applied is True
    assert "deny_pol" in col.decision_reason
    by_grant = {r.grant_id: r for r in col.records}
    assert "g-allow" in by_grant and by_grant["g-allow"].matched is True
    assert "g-deny" in by_grant and by_grant["g-deny"].matched is True


# ---- Non-match: why_not populated for method mismatch ------------------


@pytest.mark.asyncio
async def test_trace_records_why_not_for_method_mismatch() -> None:
    pol = Policy(id="post_only", effect="ALLOW", actions=["POST"], resources=[".*"])
    storage = _FakeIamStorage(
        grants=[_grant("editor", grant_id="g-x", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["post_only"])},
    )
    svc = _service(storage, {"post_only": pol})
    _, _, col = await _call(svc, collection_id=_COLL_A, principal_id=uuid4(), trace=True)
    assert col is not None
    rec = next(r for r in col.records if r.policy_id == "post_only")
    assert rec.matched is False
    assert rec.why_not is not None
    assert "method" in rec.why_not.lower()


# ---- Per-condition outcomes recorded -----------------------------------


@pytest.mark.asyncio
async def test_trace_records_condition_outcomes(monkeypatch) -> None:
    cond_true = Condition(type="cond_true", config={"k": 1})
    cond_false = Condition(type="cond_false", config={"k": 2})

    pol_a = Policy(
        id="needs_true", effect="ALLOW", actions=[".*"], resources=[".*"],
        conditions=[cond_true],
    )
    pol_b = Policy(
        id="needs_false", effect="ALLOW", actions=[".*"], resources=[".*"],
        conditions=[cond_false],
    )

    storage = _FakeIamStorage(
        grants=[
            _grant("r_a", grant_id="g-a", resource_kind="collection", resource_ref=_COLL_A),
            _grant("r_b", grant_id="g-b", resource_kind="collection", resource_ref=_COLL_A),
        ],
        roles={
            "r_a": _role("r_a", ["needs_true"]),
            "r_b": _role("r_b", ["needs_false"]),
        },
    )
    svc = _service(storage, {"needs_true": pol_a, "needs_false": pol_b})

    async def _fake_check(cond, ctx):  # noqa: ANN001
        return cond.type == "cond_true"

    monkeypatch.setattr(svc, "_evaluate_condition", _fake_check)

    _, _, col = await _call(svc, collection_id=_COLL_A, principal_id=uuid4(), trace=True)
    assert col is not None
    by_pol = {r.policy_id: r for r in col.records}
    a = by_pol["needs_true"]
    assert a.matched is True
    assert len(a.conditions_evaluated) == 1
    assert a.conditions_evaluated[0]["type"] == "cond_true"
    assert a.conditions_evaluated[0]["passed"] is True

    b = by_pol["needs_false"]
    assert b.matched is False
    assert len(b.conditions_evaluated) == 1
    assert b.conditions_evaluated[0]["type"] == "cond_false"
    assert b.conditions_evaluated[0]["passed"] is False
    assert b.why_not is not None and "cond_false" in b.why_not


# ---- Validity window carries through into trace records ---------------


@pytest.mark.asyncio
async def test_trace_carries_validity_window_from_grant_row() -> None:
    from datetime import datetime, timedelta, timezone

    vf = datetime.now(tz=timezone.utc) - timedelta(days=1)
    vu = datetime.now(tz=timezone.utc) + timedelta(days=1)
    storage = _FakeIamStorage(
        grants=[
            _grant(
                "editor",
                grant_id="g-window",
                resource_kind="collection",
                resource_ref=_COLL_A,
                valid_from=vf,
                valid_until=vu,
            )
        ],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    _, _, col = await _call(svc, collection_id=_COLL_A, principal_id=uuid4(), trace=True)
    assert col is not None
    rec = next(r for r in col.records if r.grant_id == "g-window")
    assert rec.valid_from == vf
    assert rec.valid_until == vu
    assert rec.in_validity_window is True


# ---- Backward compat: omitting trace_collector kwarg works -----------


@pytest.mark.asyncio
async def test_evaluate_access_without_trace_kwarg_unchanged() -> None:
    storage = _FakeIamStorage(
        grants=[_grant("editor", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=f"/catalogs/{_CATALOG_ID}/collections/{_COLL_A}/items",
        method="GET",
        catalog_id=_CATALOG_ID,
        principal_id=uuid4(),
        collection_id=_COLL_A,
    )
    assert allowed is True
    assert "allow_pol" in reason
