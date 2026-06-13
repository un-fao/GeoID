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

"""Unit tests for RequestActionPrivilegeHandler (conditions.py).

The handler is designed for use on a DENY policy.  It returns:
  - True  → DENY applies (gated action + principal lacks required role)
  - False → DENY does not apply (action not gated, or principal holds required
             role, or sysadmin bypass, or fail-open cases)

All permutations are tested below.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dynastore.modules.iam.conditions import (
    EvaluationContext,
    RequestActionPrivilegeHandler,
)
from dynastore.models.protocols.authorization import IamRolesConfig

_cfg = IamRolesConfig()
_SYSADMIN = _cfg.sysadmin_role_name  # e.g. "sysadmin"
_ADMIN = _cfg.admin_role_name         # e.g. "admin"

_GATED_ACTIONS = ["backfill_envelope_attrs"]
_CONFIG: Dict[str, Any] = {
    "gated_actions": _GATED_ACTIONS,
    "required_role": _SYSADMIN,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request(body: Optional[Dict[str, Any]], method: str = "POST") -> Any:
    """Build a minimal Starlette-like request mock with a cached JSON body."""
    request = MagicMock()
    request.method = method

    async def _json():
        if body is None:
            raise ValueError("no body")
        return body

    request.json = _json
    return request


def _make_ctx(
    *,
    roles: list,
    body: Optional[Dict[str, Any]] = None,
    method: str = "POST",
) -> EvaluationContext:
    principal = SimpleNamespace(roles=roles)
    return EvaluationContext(
        request=_make_request(body, method=method),
        storage=MagicMock(),
        method=method,
        path="/admin/catalogs/acme/collections/s2/tasks",
        extras={"principal_obj": principal},
    )


_HANDLER = RequestActionPrivilegeHandler()


# ---------------------------------------------------------------------------
# Core semantics: gated action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gated_action_admin_role_deny_applies():
    """backfill_envelope_attrs + admin role → handler returns True (DENY applies)."""
    ctx = _make_ctx(roles=[_ADMIN], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is True


@pytest.mark.asyncio
async def test_gated_action_sysadmin_role_deny_does_not_apply():
    """backfill_envelope_attrs + sysadmin role → handler returns False (exempt)."""
    ctx = _make_ctx(roles=[_SYSADMIN], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_gated_action_required_role_held_deny_does_not_apply():
    """When the principal holds the required_role explicitly, DENY does not apply."""
    config = {**_CONFIG, "required_role": _SYSADMIN}
    ctx = _make_ctx(roles=[_SYSADMIN], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(config, ctx)
    assert result is False


# ---------------------------------------------------------------------------
# Non-gated action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_gated_action_reindex_admin_deny_does_not_apply():
    """reindex is not in gated_actions → handler returns False even for admin."""
    ctx = _make_ctx(roles=[_ADMIN], body={"action": "reindex"})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_non_gated_action_reindex_sysadmin_deny_does_not_apply():
    """reindex + sysadmin → still False (action not in gated set)."""
    ctx = _make_ctx(roles=[_SYSADMIN], body={"action": "reindex"})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_non_gated_action_unknown_action_deny_does_not_apply():
    """An action not in gated_actions never triggers the DENY."""
    ctx = _make_ctx(roles=[_ADMIN], body={"action": "some_other_action"})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


# ---------------------------------------------------------------------------
# Fail-open cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_request_deny_does_not_apply():
    """GET requests are never affected by this DENY handler."""
    ctx = _make_ctx(roles=[_ADMIN], body={"action": "backfill_envelope_attrs"}, method="GET")
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_unreadable_body_fail_open():
    """Body that raises on .json() → fail open (False), not fail closed."""
    ctx = _make_ctx(roles=[_ADMIN], body=None)  # body=None raises ValueError in mock
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_missing_action_field_fail_open():
    """Body without 'action' key → fail open."""
    ctx = _make_ctx(roles=[_ADMIN], body={"params": {}})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_empty_action_field_fail_open():
    """Body with empty string 'action' → treated as absent → fail open."""
    ctx = _make_ctx(roles=[_ADMIN], body={"action": ""})
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_non_dict_body_fail_open():
    """JSON body that is a list not a dict → fail open."""
    request = MagicMock()
    request.method = "POST"

    async def _json():
        return ["backfill_envelope_attrs"]

    request.json = _json
    principal = SimpleNamespace(roles=[_ADMIN])
    ctx = EvaluationContext(
        request=request,
        storage=MagicMock(),
        method="POST",
        path="/admin/catalogs/acme/tasks",
        extras={"principal_obj": principal},
    )
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_no_request_object_fail_open():
    """ctx.request is None → fail open."""
    principal = SimpleNamespace(roles=[_ADMIN])
    ctx = EvaluationContext(
        request=None,
        storage=MagicMock(),
        method="POST",
        path="/admin/catalogs/acme/tasks",
        extras={"principal_obj": principal},
    )
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_no_principal_object_fail_open():
    """ctx.extras has no principal_obj → fail open."""
    ctx = EvaluationContext(
        request=_make_request({"action": "backfill_envelope_attrs"}),
        storage=MagicMock(),
        method="POST",
        path="/admin/catalogs/acme/tasks",
        extras={},
    )
    result = await _HANDLER.evaluate(_CONFIG, ctx)
    assert result is False


@pytest.mark.asyncio
async def test_empty_gated_actions_config_fail_open():
    """Mis-configured policy with empty gated_actions → never applies (False)."""
    config = {**_CONFIG, "gated_actions": []}
    ctx = _make_ctx(roles=[_ADMIN], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(config, ctx)
    assert result is False


# ---------------------------------------------------------------------------
# Sysadmin bypass configuration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allow_sysadmin_false_denies_sysadmin_bypass_only():
    """When allow_sysadmin=False, the sysadmin-BYPASS path is disabled.

    However, if ``required_role`` is set to a DIFFERENT role and the principal
    only holds the sysadmin name (not the required_role), the DENY still
    applies — neither the bypass nor the required_role check saves them.
    """
    # required_role is a role the principal does NOT hold.
    config = {
        "gated_actions": _GATED_ACTIONS,
        "required_role": "some_other_privileged_role",
        "allow_sysadmin": False,
    }
    # Principal only holds sysadmin — not required_role, and bypass is disabled.
    ctx = _make_ctx(roles=[_SYSADMIN], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(config, ctx)
    assert result is True  # DENY applies: bypass disabled AND required_role not held


@pytest.mark.asyncio
async def test_custom_sysadmin_role_name_is_honoured():
    """allow_sysadmin bypass uses the configured sysadmin_role name."""
    config = {**_CONFIG, "sysadmin_role": "platform_superuser"}
    # Principal holds the custom sysadmin role but NOT the default sysadmin role.
    ctx = _make_ctx(roles=["platform_superuser"], body={"action": "backfill_envelope_attrs"})
    result = await _HANDLER.evaluate(config, ctx)
    assert result is False  # Custom sysadmin role exempts the principal


# ---------------------------------------------------------------------------
# handler.type
# ---------------------------------------------------------------------------


def test_handler_type_is_request_action_privilege():
    assert RequestActionPrivilegeHandler().type == "request_action_privilege"
