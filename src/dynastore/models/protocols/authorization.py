#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Authorization protocol — narrow, framework-free permission façade.

Used by both extensions (via FastAPI `Depends` wrappers in
`dynastore.extensions.iam.guards`) and tasks (via
`dynastore.modules.iam.authorization.checks.require_permission`).

When `IamModule` is loaded, an `IamAuthorizer` is registered via
`register_plugin` and returned by `get_protocols(AuthorizerProtocol)`.
When it is not loaded, the `DefaultAuthorizer` sentinel takes over and
fails closed on privileged checks.
"""

from enum import Enum
from typing import Protocol, runtime_checkable

from dynastore.models.protocols.authorization_context import SecurityContext


class Permission(str, Enum):
    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    AUTHENTICATED = "authenticated"


class DefaultRole(str, Enum):
    """Canonical identifiers for the platform-seeded default roles.

    These are the names used when seeding the `roles` table on first boot.
    Consumers MUST reference these constants instead of hard-coding the
    strings, so the seed set can evolve centrally. Operators can still
    add/rename/remove roles at runtime through the admin API.
    """

    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    USER = "user"
    ANONYMOUS = "anonymous"


@runtime_checkable
class AuthorizerProtocol(Protocol):
    """Narrow façade used by guards and task-side `require_permission`.

    `check(ctx, perm)` must raise an appropriate error (HTTPException in
    FastAPI surfaces, PermissionError in task surfaces) when the
    permission is denied; returns None on success.
    """

    async def check(self, ctx: SecurityContext, permission: Permission) -> None: ...
