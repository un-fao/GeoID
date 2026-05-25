"""Pure-unit pins for per-binding quota / rate-limit wiring (un-fao/GeoID#1344).

A grant row may carry a ``quota`` JSONB spec. During ``evaluate_access`` the
resolver turns each in-scope ALLOW grant's quota (or the configured
``IamScaleConfig`` default) into ``rate_limit`` / ``max_count`` conditions and
stashes them on the request context, namespaced by the grant id so two grants
that differ only by ``resource_ref`` never share a counter bucket. The
middleware then enforces them in its condition step.

Covered here without a DB:
  * ``IamScaleConfig`` defaults + ``quota_to_conditions`` / ``quota_namespace``
    / ``usage_counter_hash_partitions`` / ``build_usage_counters_steps``.
  * ``evaluate_access`` stashes a grant's quota conditions on the request
    context with the grant-id namespace.
  * A per-binding quota overrides the platform default.
  * DENY grants impose no quota.
  * Distinct grants get distinct counter namespaces (per-scope counting).
  * No request context (the compile_read_filter style call) → no stash.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService
from dynastore.modules.iam.scale_config import (
    IamScaleConfig,
    build_usage_counters_steps,
    quota_namespace,
    quota_to_conditions,
    usage_counter_hash_partitions,
    valkey_required_at_startup,
)


_SCHEMA = "s_test_catalog"
_CATALOG_ID = "test_catalog"
_COLL_A = "collA"


class _Ctx:
    """Minimal request-context stand-in carrying the ``extras`` dict the
    resolver writes the synthesized quota conditions into."""

    def __init__(self) -> None:
        self.extras: Dict[str, Any] = {}


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

    async def get_role(self, role_id: str, schema: str = "iam", **_: Any) -> Optional[Role]:
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


def _allow_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="ALLOW", actions=[".*"], resources=[".*"])


def _grant(
    grant_id: str,
    role_name: str,
    *,
    effect: str = "allow",
    resource_kind: Optional[str] = None,
    resource_ref: Optional[str] = None,
    quota: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "id": grant_id,
        "object_kind": "role",
        "object_ref": role_name,
        "effect": effect,
        "resource_kind": resource_kind,
        "resource_ref": resource_ref,
        "quota": quota,
    }


async def _call(svc: PolicyService, ctx: Any, *, collection_id: Optional[str], principal_id: Any):
    return await svc.evaluate_access(
        principals=[],
        path=f"/catalogs/{_CATALOG_ID}/collections/{collection_id}/items",
        method="GET",
        request_context=ctx,
        catalog_id=_CATALOG_ID,
        principal_id=principal_id,
        collection_id=collection_id,
    )


# --- helper-level pins --------------------------------------------------------


def test_scale_config_defaults() -> None:
    cfg = IamScaleConfig()
    assert cfg.valkey_required is False
    assert cfg.usage_counter_hash_partitions == 1
    assert cfg.default_rate_limit is None
    assert cfg.default_quota is None
    assert IamScaleConfig.class_key() == "iam_scale_config"


def test_quota_to_conditions_explicit() -> None:
    conds, mapping = quota_to_conditions(
        {"rate_limit": {"limit": 5, "window_seconds": 60}, "max_count": {"limit": 100}},
        "grant:abc",
    )
    by_type = {c.type: c.config for c in conds}
    assert by_type["rate_limit"] == {"limit": 5, "window_seconds": 60, "scope": "principal"}
    assert by_type["max_count"] == {"limit": 100, "scope": "principal"}
    assert set(mapping.values()) == {"grant:abc"}
    # the mapping keys are the identities of the returned configs
    assert {id(c.config) for c in conds} == set(mapping.keys())


def test_quota_default_fallback_applies_when_grant_has_none() -> None:
    conds, _ = quota_to_conditions(
        None, "grant:x", default_rate_limit={"limit": 3, "window_seconds": 10}
    )
    assert [c.type for c in conds] == ["rate_limit"]
    assert conds[0].config["limit"] == 3


def test_per_binding_quota_overrides_default() -> None:
    conds, _ = quota_to_conditions(
        {"rate_limit": {"limit": 7, "window_seconds": 1}},
        "grant:x",
        default_rate_limit={"limit": 3, "window_seconds": 10},
    )
    assert conds[0].config["limit"] == 7  # per-binding wins


def test_quota_empty_no_default_yields_nothing() -> None:
    conds, mapping = quota_to_conditions(None, "grant:x")
    assert conds == [] and mapping == {}


def test_quota_namespace_distinct_per_grant() -> None:
    assert quota_namespace("a") != quota_namespace("b")
    assert quota_namespace("a") == "grant:a"


def test_usage_counter_hash_partitions_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IAM_USAGE_COUNTER_HASH_PARTITIONS", raising=False)
    assert usage_counter_hash_partitions() == 1
    monkeypatch.setenv("IAM_USAGE_COUNTER_HASH_PARTITIONS", "8")
    assert usage_counter_hash_partitions() == 8
    monkeypatch.setenv("IAM_USAGE_COUNTER_HASH_PARTITIONS", "garbage")
    assert usage_counter_hash_partitions() == 1
    monkeypatch.setenv("IAM_USAGE_COUNTER_HASH_PARTITIONS", "0")
    assert usage_counter_hash_partitions() == 1


def test_build_usage_counters_steps_shapes() -> None:
    assert len(build_usage_counters_steps(1)) == 1  # flat table only
    assert len(build_usage_counters_steps(4)) == 5  # parent + 4 partitions


@pytest.mark.asyncio
async def test_valkey_required_env_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    """The startup guard reads IAM_VALKEY_REQUIRED first so a cold boot (no
    platform_configs yet) can still enforce the requirement in prod."""
    monkeypatch.setenv("IAM_VALKEY_REQUIRED", "true")
    assert await valkey_required_at_startup() is True
    monkeypatch.setenv("IAM_VALKEY_REQUIRED", "0")
    assert await valkey_required_at_startup() is False
    # Unset → falls back to the (default False) persisted config; no
    # PlatformConfigsProtocol registered in this unit context → defaults.
    monkeypatch.delenv("IAM_VALKEY_REQUIRED", raising=False)
    assert await valkey_required_at_startup() is False


# --- resolver wiring pins -----------------------------------------------------


@pytest.mark.asyncio
async def test_evaluate_access_stashes_grant_quota_conditions() -> None:
    gid = "g-123"
    storage = _FakeIamStorage(
        grants=[
            _grant(
                gid,
                "editor",
                resource_kind="collection",
                resource_ref=_COLL_A,
                quota={"rate_limit": {"limit": 5, "window_seconds": 60}},
            )
        ],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    ctx = _Ctx()
    allowed, reason = await _call(svc, ctx, collection_id=_COLL_A, principal_id=uuid4())
    assert allowed is True, reason

    conds = ctx.extras.get("_grant_quota_conditions")
    assert conds and [c.type for c in conds] == ["rate_limit"]
    ns_map = ctx.extras.get("_policy_id_by_config_id")
    assert ns_map and set(ns_map.values()) == {f"grant:{gid}"}


@pytest.mark.asyncio
async def test_deny_grant_imposes_no_quota() -> None:
    storage = _FakeIamStorage(
        grants=[
            _grant(
                "g-deny",
                "blocked",
                effect="deny",
                quota={"rate_limit": {"limit": 1, "window_seconds": 1}},
            )
        ],
        roles={"blocked": _role("blocked", [])},
    )
    svc = _service(storage, {})
    ctx = _Ctx()
    await _call(svc, ctx, collection_id=_COLL_A, principal_id=uuid4())
    assert ctx.extras.get("_grant_quota_conditions") in (None, [])


@pytest.mark.asyncio
async def test_distinct_grants_get_distinct_namespaces() -> None:
    storage = _FakeIamStorage(
        grants=[
            _grant("g-1", "editor", quota={"rate_limit": {"limit": 5, "window_seconds": 60}}),
            _grant("g-2", "viewer", quota={"max_count": {"limit": 9}}),
        ],
        roles={
            "editor": _role("editor", ["allow_pol"]),
            "viewer": _role("viewer", ["allow_pol"]),
        },
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    ctx = _Ctx()
    await _call(svc, ctx, collection_id=_COLL_A, principal_id=uuid4())
    ns_map = ctx.extras.get("_policy_id_by_config_id") or {}
    assert set(ns_map.values()) == {"grant:g-1", "grant:g-2"}


@pytest.mark.asyncio
async def test_repeat_resolution_does_not_duplicate_conditions() -> None:
    """A second resolution on the same request context must not append a
    grant's quota conditions twice (would double-increment the counter)."""
    storage = _FakeIamStorage(
        grants=[_grant("g-1", "editor", quota={"rate_limit": {"limit": 5, "window_seconds": 60}})],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    ctx = _Ctx()
    pid = uuid4()
    await _call(svc, ctx, collection_id=_COLL_A, principal_id=pid)
    await _call(svc, ctx, collection_id=_COLL_A, principal_id=pid)
    conds = ctx.extras.get("_grant_quota_conditions") or []
    assert len(conds) == 1, "grant quota conditions must be deduped per grant id"


@pytest.mark.asyncio
async def test_no_request_context_skips_quota_stash() -> None:
    """compile_read_filter-style resolution (no request_context) must not
    attempt to stash quota conditions — and must not raise."""
    storage = _FakeIamStorage(
        grants=[_grant("g-x", "editor", quota={"rate_limit": {"limit": 5, "window_seconds": 60}})],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    policies = await svc._resolve_effective_policies(
        principals=[],
        schema=_SCHEMA,
        catalog_id=_CATALOG_ID,
        principal_id=uuid4(),
        collection_id=_COLL_A,
        request_context=None,
    )
    assert any(p.id == "allow_pol" for p in policies)
