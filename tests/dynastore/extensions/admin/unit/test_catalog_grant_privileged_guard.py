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

"""Unit tests for catalog-scope grant/revoke privilege-escalation narrowing (#723).

A catalog admin must be able to grant the catalog-tier admin role to a
colleague within their own catalog. Before this fix the
``ensure_privileged_role_assignment`` guard used the platform-spanning
``admin_role_set`` (``{"sysadmin","admin"}``) so even sentinel-only
catalog admins were rejected with 403 on
``POST /admin/catalogs/{cid}/principals/{pid}/roles`` when granting
``"admin"``. The fix narrows the catalog-scope guards to
``IamRolesConfig.platform_admin_tier_role_set`` (``{"sysadmin"}`` by
default) so only platform-tier privilege escalation is blocked.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.policies import AssignRoleRequest


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"
_grant = AdminService.grant_catalog_role
_revoke = AdminService.revoke_catalog_role


def _req(roles):
    return SimpleNamespace(state=SimpleNamespace(
        principal=SimpleNamespace(
            provider="local", subject_id="alice", roles=roles,
        ),
        principal_role=list(roles),
        policy_allowed=True,
    ))


def _mgr_with_admin_registered():
    mgr = MagicMock()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="admin")])
    mgr.get_principal = AsyncMock(return_value=SimpleNamespace(subject_id="bob"))
    mgr.resolve_schema = AsyncMock(return_value="cat_a_schema")
    mgr.storage = MagicMock()
    mgr.storage.grant_catalog_role = AsyncMock(return_value=None)
    mgr.storage.revoke_catalog_role = AsyncMock(return_value=None)
    return mgr


def _patch_protocols(mgr):
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs = MagicMock()
    catalogs.get_catalog_model = AsyncMock(return_value=SimpleNamespace(id="cat-a"))

    def _get_proto(cls):
        if cls is IamService:
            return mgr
        if cls is CatalogsProtocol:
            return catalogs
        return None
    return patch(_GET_PROTOCOL, side_effect=_get_proto)


def test_platform_admin_tier_role_set_default_is_sysadmin_only():
    """Default config: catalog-tier "admin" must NOT be in the platform-tier
    privileged set, so catalog-scope grants of "admin" are admitted."""
    cfg = IamRolesConfig()
    assert "sysadmin" in cfg.platform_admin_tier_role_set
    assert "admin" not in cfg.platform_admin_tier_role_set
    # Sanity: the broader admin_role_set still spans both tiers.
    assert {"sysadmin", "admin"}.issubset(cfg.admin_role_set)


@pytest.mark.asyncio
async def test_catalog_admin_can_grant_admin_to_colleague():
    """The #723 fix: a sentinel-only catalog admin POSTs admin role grant
    within their catalog → 204, no platform-tier guard fires."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr_with_admin_registered()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _grant(req, catalog_id="cat-a", principal_id=pid,
                     body=AssignRoleRequest(role="admin"))
    mgr.storage.grant_catalog_role.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_admin_cannot_grant_sysadmin_at_catalog_scope():
    """The narrowed guard still blocks platform-tier sysadmin grants."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr_with_admin_registered()
    mgr.list_roles = AsyncMock(return_value=[SimpleNamespace(name="sysadmin")])
    pid = uuid4()
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _grant(req, catalog_id="cat-a", principal_id=pid,
                         body=AssignRoleRequest(role="sysadmin"))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_catalog_admin_can_revoke_admin_from_colleague():
    """Symmetric to the grant path — revoking catalog-tier admin is admitted."""
    req = _req(roles=["catalog_admin"])
    mgr = _mgr_with_admin_registered()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _revoke(req, catalog_id="cat-a", principal_id=pid,
                      role_name="admin")
    mgr.storage.revoke_catalog_role.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_admin_cannot_revoke_sysadmin_at_catalog_scope():
    req = _req(roles=["catalog_admin"])
    mgr = _mgr_with_admin_registered()
    pid = uuid4()
    with _patch_protocols(mgr):
        with pytest.raises(HTTPException) as exc:
            await _revoke(req, catalog_id="cat-a", principal_id=pid,
                          role_name="sysadmin")
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_sysadmin_grant_admin_at_catalog_scope_still_works():
    """Sysadmin reaches the route via admin_access; the narrowed guard
    is a no-op for them on catalog-tier role names."""
    req = _req(roles=["sysadmin"])
    mgr = _mgr_with_admin_registered()
    pid = uuid4()
    with _patch_protocols(mgr):
        await _grant(req, catalog_id="cat-a", principal_id=pid,
                     body=AssignRoleRequest(role="admin"))
    mgr.storage.grant_catalog_role.assert_awaited_once()
