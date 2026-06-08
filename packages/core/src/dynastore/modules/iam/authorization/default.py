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
`resource=".*"`) would bypass the role-level checks for sysadmin and admin.
Role enforcement must be independent of path-level policy decisions.

Role *names* are read from a constructor-supplied ``IamRolesConfig`` so
deployments that rename the platform super-user role wire it through
without code changes.
"""

from typing import Optional

from dynastore.models.protocols.authorization import (
    IamRolesConfig,
    Permission,
)
from dynastore.models.protocols.authorization_context import SecurityContext


class DefaultAuthorizer:
    """Minimal role-based authorizer. Reads only `SecurityContext.roles`."""

    def __init__(self, role_config: Optional[IamRolesConfig] = None) -> None:
        self._role_config = role_config or IamRolesConfig()

    async def check(self, ctx: SecurityContext, permission: Permission) -> None:
        cfg = self._role_config
        if permission is Permission.AUTHENTICATED:
            if ctx.principal_id or ctx.roles:
                return
            raise PermissionError("Authentication required.")
        if permission is Permission.ADMIN:
            if ctx.roles & cfg.admin_role_set:
                return
            raise PermissionError("Administrative privileges required.")
        if permission is Permission.SYSADMIN:
            if cfg.sysadmin_role_name in ctx.roles:
                return
            raise PermissionError("System administrator privileges required.")
        raise PermissionError(f"Unknown permission: {permission!r}")
