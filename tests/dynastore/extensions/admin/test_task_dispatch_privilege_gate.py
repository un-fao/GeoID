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

"""End-to-end policy resolution tests for the per-action privilege gate (#1785).

Verifies the full ALLOW/DENY resolution using PolicyService.evaluate_access
with the real admin_policies() + admin_role_bindings() declarations.  No DB
required: custom_policies is used to inject all relevant policies, and
principals=[] + custom_policies mirrors the pattern in test_evaluate_access_deny_precedence.

Assertions:
  - admin + backfill_envelope_attrs on catalog route → DENIED
  - admin + backfill_envelope_attrs on collection route → DENIED
  - admin + reindex on catalog route → ALLOWED
  - admin + reindex on collection route → ALLOWED
  - sysadmin + backfill_envelope_attrs on catalog/collection routes → ALLOWED
    (sysadmin is exempt via allow_sysadmin bypass in the handler)
"""
from __future__ import annotations

import json
from typing import Any, List
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.conditions import EvaluationContext
from dynastore.modules.iam.policies import PolicyService
from dynastore.extensions.admin.policies import admin_policies, admin_role_bindings
from dynastore.models.protocols.authorization import IamRolesConfig


_cfg = IamRolesConfig()
_SYSADMIN = _cfg.sysadmin_role_name
_ADMIN = _cfg.admin_role_name


def _service() -> PolicyService:
    """Build a PolicyService with no DB dependencies (same pattern as
    test_evaluate_access_deny_precedence)."""
    svc = PolicyService.__new__(PolicyService)
    svc._state = None   # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]
    return svc


def _make_request(action: str) -> Any:
    """Build a minimal Starlette-like request mock whose .json() returns action."""
    request = MagicMock()
    request.method = "POST"

    async def _json():
        return {"action": action}

    request.json = _json
    return request


def _make_ctx(roles: list, action: str, path: str) -> EvaluationContext:
    """Build an EvaluationContext carrying the principal and request body."""
    principal = SimpleNamespace(roles=roles)
    return EvaluationContext(
        request=_make_request(action),
        storage=MagicMock(),
        method="POST",
        path=path,
        extras={"principal_obj": principal},
    )


def _all_policies_for_role(role_name: str) -> List[Policy]:
    """Return the subset of admin_policies() that are bound to role_name."""
    # Build a mapping role_name -> set of policy ids from the bindings.
    role_policy_ids: dict = {}
    for rb in admin_role_bindings():
        role_policy_ids.setdefault(rb.name, set()).update(rb.policies or [])

    policy_ids = role_policy_ids.get(role_name, set())
    policies_by_id = {p.id: p for p in admin_policies()}
    return [policies_by_id[pid] for pid in policy_ids if pid in policies_by_id]


_CATALOG_PATH = "/admin/catalogs/acme/tasks"
_COLLECTION_PATH = "/admin/catalogs/acme/collections/s2/tasks"


# ---------------------------------------------------------------------------
# Admin role — backfill_envelope_attrs must be DENIED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_backfill_catalog_route_denied():
    """admin + backfill_envelope_attrs on catalog route → DENY by privileged_deny policy."""
    svc = _service()
    ctx = _make_ctx(roles=[_ADMIN], action="backfill_envelope_attrs", path=_CATALOG_PATH)
    policies = _all_policies_for_role(_ADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_CATALOG_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is False
    assert "admin_task_dispatch_privileged_deny" in reason


@pytest.mark.asyncio
async def test_admin_backfill_collection_route_denied():
    """admin + backfill_envelope_attrs on collection route → DENIED."""
    svc = _service()
    ctx = _make_ctx(roles=[_ADMIN], action="backfill_envelope_attrs", path=_COLLECTION_PATH)
    policies = _all_policies_for_role(_ADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_COLLECTION_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is False
    assert "admin_task_dispatch_privileged_deny" in reason


# ---------------------------------------------------------------------------
# Admin role — reindex must be ALLOWED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_reindex_catalog_route_allowed():
    """admin + reindex on catalog route → ALLOWED (not gated)."""
    svc = _service()
    ctx = _make_ctx(roles=[_ADMIN], action="reindex", path=_CATALOG_PATH)
    policies = _all_policies_for_role(_ADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_CATALOG_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is True


@pytest.mark.asyncio
async def test_admin_reindex_collection_route_allowed():
    """admin + reindex on collection route → ALLOWED."""
    svc = _service()
    ctx = _make_ctx(roles=[_ADMIN], action="reindex", path=_COLLECTION_PATH)
    policies = _all_policies_for_role(_ADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_COLLECTION_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is True


# ---------------------------------------------------------------------------
# Sysadmin role — all actions must be ALLOWED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sysadmin_backfill_catalog_route_allowed():
    """sysadmin + backfill_envelope_attrs on catalog route → ALLOWED (exempt)."""
    svc = _service()
    ctx = _make_ctx(roles=[_SYSADMIN], action="backfill_envelope_attrs", path=_CATALOG_PATH)
    policies = _all_policies_for_role(_SYSADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_CATALOG_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is True


@pytest.mark.asyncio
async def test_sysadmin_backfill_collection_route_allowed():
    """sysadmin + backfill_envelope_attrs on collection route → ALLOWED (exempt)."""
    svc = _service()
    ctx = _make_ctx(roles=[_SYSADMIN], action="backfill_envelope_attrs", path=_COLLECTION_PATH)
    policies = _all_policies_for_role(_SYSADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_COLLECTION_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is True


@pytest.mark.asyncio
async def test_sysadmin_reindex_catalog_route_allowed():
    """sysadmin + reindex on catalog route → ALLOWED."""
    svc = _service()
    ctx = _make_ctx(roles=[_SYSADMIN], action="reindex", path=_CATALOG_PATH)
    policies = _all_policies_for_role(_SYSADMIN)
    allowed, reason = await svc.evaluate_access(
        principals=[],
        path=_CATALOG_PATH,
        method="POST",
        request_context=ctx,
        custom_policies=policies,
    )
    assert allowed is True


# ---------------------------------------------------------------------------
# Policy shape assertions (lock in the new DENY policy structure)
# ---------------------------------------------------------------------------


def test_privileged_deny_policy_in_admin_policies():
    """admin_task_dispatch_privileged_deny must be present with DENY effect."""
    policies = {p.id: p for p in admin_policies()}
    assert "admin_task_dispatch_privileged_deny" in policies
    p = policies["admin_task_dispatch_privileged_deny"]
    assert p.effect == "DENY"
    assert "POST" in p.actions
    # Must cover both catalog and collection task-dispatch paths.
    assert len(p.resources) == 2
    import re
    assert re.match(p.resources[0], _CATALOG_PATH)
    assert re.match(p.resources[1], _COLLECTION_PATH)


def test_privileged_deny_condition_type_and_config():
    """The DENY policy uses the request_action_privilege condition handler."""
    policies = {p.id: p for p in admin_policies()}
    p = policies["admin_task_dispatch_privileged_deny"]
    assert len(p.conditions) == 1
    cond = p.conditions[0]
    assert cond.type == "request_action_privilege"
    assert "backfill_envelope_attrs" in cond.config.get("gated_actions", [])
    required_role = cond.config.get("required_role")
    cfg = IamRolesConfig()
    assert required_role == cfg.sysadmin_role_name


def test_privileged_deny_bound_to_admin_and_sysadmin():
    """DENY policy must appear in both sysadmin and admin bindings."""
    from collections import defaultdict
    policy_sets: dict = defaultdict(set)
    for rb in admin_role_bindings():
        for pol in (rb.policies or []):
            policy_sets[rb.name].add(pol)

    cfg = IamRolesConfig()
    assert "admin_task_dispatch_privileged_deny" in policy_sets.get(cfg.sysadmin_role_name, set())
    assert "admin_task_dispatch_privileged_deny" in policy_sets.get(cfg.admin_role_name, set())


def test_admin_policies_ids_include_new_deny():
    """Policy ID list now includes admin_task_dispatch_privileged_deny."""
    ids = [p.id for p in admin_policies()]
    assert "admin_task_dispatch_privileged_deny" in ids
    # Verify ordering — DENY comes after the two ALLOW task-dispatch policies.
    idx_collection = ids.index("admin_task_dispatch_collection")
    idx_deny = ids.index("admin_task_dispatch_privileged_deny")
    assert idx_deny > idx_collection
