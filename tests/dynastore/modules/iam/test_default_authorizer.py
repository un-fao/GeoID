#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Regression tests for `DefaultAuthorizer`.

Guards against reintroducing the `policy_allowed` blanket-bypass that
let any authenticated user satisfy `Permission.SYSADMIN` / `ADMIN` so
long as a broad path-level ALLOW policy matched their request
(e.g. a baseline `user` role with `resource=".*"` hitting
`GET /logs/system`, which is wired with `Depends(require_sysadmin)`).
"""

import pytest

from dynastore.models.protocols.authorization import Permission
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.modules.iam.authorization.default import DefaultAuthorizer


@pytest.mark.asyncio
async def test_sysadmin_denied_for_user_even_with_policy_allowed():
    ctx = SecurityContext(
        principal_id="testuser",
        roles=frozenset({"user"}),
        policy_allowed=True,
    )
    with pytest.raises(PermissionError):
        await DefaultAuthorizer().check(ctx, Permission.SYSADMIN)


@pytest.mark.asyncio
async def test_admin_denied_for_user_even_with_policy_allowed():
    ctx = SecurityContext(
        principal_id="testuser",
        roles=frozenset({"user"}),
        policy_allowed=True,
    )
    with pytest.raises(PermissionError):
        await DefaultAuthorizer().check(ctx, Permission.ADMIN)


@pytest.mark.asyncio
async def test_sysadmin_granted_only_by_role():
    ctx = SecurityContext(
        principal_id="root",
        roles=frozenset({"sysadmin"}),
        policy_allowed=False,
    )
    await DefaultAuthorizer().check(ctx, Permission.SYSADMIN)


@pytest.mark.asyncio
async def test_admin_granted_by_admin_role():
    ctx = SecurityContext(
        principal_id="ops",
        roles=frozenset({"admin"}),
        policy_allowed=False,
    )
    await DefaultAuthorizer().check(ctx, Permission.ADMIN)


@pytest.mark.asyncio
async def test_admin_granted_by_sysadmin_role():
    ctx = SecurityContext(
        principal_id="root",
        roles=frozenset({"sysadmin"}),
        policy_allowed=False,
    )
    await DefaultAuthorizer().check(ctx, Permission.ADMIN)


@pytest.mark.asyncio
async def test_authenticated_granted_for_principal_with_no_roles():
    ctx = SecurityContext(principal_id="anyone", roles=frozenset())
    await DefaultAuthorizer().check(ctx, Permission.AUTHENTICATED)


@pytest.mark.asyncio
async def test_authenticated_denied_when_neither_principal_nor_roles():
    ctx = SecurityContext()
    with pytest.raises(PermissionError):
        await DefaultAuthorizer().check(ctx, Permission.AUTHENTICATED)


@pytest.mark.asyncio
async def test_sysadmin_denied_for_anonymous_with_policy_allowed():
    ctx = SecurityContext(principal_id=None, roles=frozenset(), policy_allowed=True)
    with pytest.raises(PermissionError):
        await DefaultAuthorizer().check(ctx, Permission.SYSADMIN)
