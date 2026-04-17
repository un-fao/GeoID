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

"""
`DefaultAuthorizer` — fail-closed sentinel used when no `IamAuthorizer`
is registered (e.g. running with `scope_catalog`, no IAM module loaded).

Grants privileged access only if the middleware already attached the
matching role to the `SecurityContext`. Otherwise raises `PermissionError`.
Public endpoints never reach an authorizer because guards are opt-in.

`SecurityContext.policy_allowed` is intentionally NOT honoured here: it
signals only that the global path-level policy filter in
`IamMiddleware` did not explicitly deny the request. That is necessary
but not sufficient for role-gated endpoints — otherwise any principal
carrying a broad ALLOW policy (e.g. a baseline `user` role with
`resource=".*"`) would bypass `require_sysadmin` / `require_admin`.
Role enforcement must be independent of path-level policy decisions.
"""

from dynastore.models.protocols.authorization import Permission
from dynastore.models.protocols.authorization_context import SecurityContext


_ADMIN_ROLES = {"admin", "sysadmin"}


class DefaultAuthorizer:
    """Minimal role-based authorizer. Reads only `SecurityContext.roles`."""

    async def check(self, ctx: SecurityContext, permission: Permission) -> None:
        if permission is Permission.AUTHENTICATED:
            if ctx.principal_id or ctx.roles:
                return
            raise PermissionError("Authentication required.")
        if permission is Permission.ADMIN:
            if ctx.roles & _ADMIN_ROLES:
                return
            raise PermissionError("Administrative privileges required.")
        if permission is Permission.SYSADMIN:
            if "sysadmin" in ctx.roles:
                return
            raise PermissionError("System administrator privileges required.")
        raise PermissionError(f"Unknown permission: {permission!r}")
