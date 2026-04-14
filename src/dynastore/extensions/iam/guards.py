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
FastAPI dependency wrappers for the IAM authorization façade.

These dependencies are always importable — their only non-stdlib imports
are `fastapi` (a base dep) and the framework-free
`dynastore.modules.iam.authorization` submodule. They therefore work in
scopes where `module_iam` extras (``pydantic[email]``, ``PyJWT``) are not
installed; in that case `DefaultAuthorizer` fail-closes privileged routes.

Tasks (no `Request`) should call
`dynastore.modules.iam.authorization.require_permission` directly instead.
"""

from typing import Optional
from uuid import UUID

from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from dynastore.models.auth import Principal
from dynastore.models.protocols.authorization import Permission
from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.modules.iam.authorization import require_permission


http_bearer = HTTPBearer(auto_error=False, scheme_name="HTTPBearer")


_SYNTHETIC_SYSADMIN_ID = UUID("00000000-0000-0000-0000-000000000000")


def _build_context(request: Request) -> SecurityContext:
    """Assemble a `SecurityContext` from middleware-populated request state."""
    principal = getattr(request.state, "principal", None)
    principal_role = getattr(request.state, "principal_role", None)

    roles: set[str] = set()
    if principal is not None:
        roles.update(getattr(principal, "roles", []) or [])
    if principal_role:
        if isinstance(principal_role, str):
            roles.add(principal_role)
        elif isinstance(principal_role, (list, tuple, set, frozenset)):
            for r in principal_role:
                roles.add(r if isinstance(r, str) else getattr(r, "value", str(r)))
        else:
            roles.add(getattr(principal_role, "value", str(principal_role)))

    principal_id: Optional[str] = None
    if principal is not None:
        principal_id = (
            getattr(principal, "subject_id", None)
            or getattr(principal, "display_name", None)
            or getattr(request.state, "principal_id", None)
        )

    return SecurityContext(
        principal_id=principal_id,
        roles=frozenset(roles),
        policy_allowed=bool(getattr(request.state, "policy_allowed", False)),
    )


async def _enforce(request: Request, permission: Permission, detail: str) -> None:
    ctx = _build_context(request)
    try:
        await require_permission(ctx, permission)
    except PermissionError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


async def require_sysadmin(
    request: Request,
    _bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> None:
    """Require a sysadmin principal. Raises 403 otherwise."""
    await _enforce(
        request,
        Permission.SYSADMIN,
        "System Administrator privileges required.",
    )


async def require_admin(
    request: Request,
    _bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> None:
    """Require an admin (or sysadmin) principal. Raises 403 otherwise."""
    await _enforce(
        request,
        Permission.ADMIN,
        "Administrative privileges required (Admin Role).",
    )


async def require_authenticated(
    request: Request,
    _bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> None:
    """Require any authenticated principal. Raises 401 otherwise."""
    ctx = _build_context(request)
    try:
        await require_permission(ctx, Permission.AUTHENTICATED)
    except PermissionError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )


def _synthetic_sysadmin() -> Principal:
    return Principal(
        id=_SYNTHETIC_SYSADMIN_ID,
        display_name="sysadmin",
        subject_id="sysadmin",
        attributes={
            "role": "sysadmin",
            "description": "System Administrator (Synthetic)",
        },
    )


async def get_principal(
    request: Request,
    _bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Principal:
    """Return the authenticated `Principal`, or raise 401."""
    principal = getattr(request.state, "principal", None)
    if principal is not None:
        return principal

    principal_role = getattr(request.state, "principal_role", None)
    if principal_role == "sysadmin" or (
        isinstance(principal_role, (list, tuple, set, frozenset))
        and "sysadmin" in principal_role
    ):
        return _synthetic_sysadmin()

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing credentials."
    )


async def get_principal_optional(
    request: Request,
    _bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Optional[Principal]:
    """Return the authenticated `Principal`, or None when anonymous."""
    principal = getattr(request.state, "principal", None)
    if principal is not None:
        return principal

    principal_role = getattr(request.state, "principal_role", None)
    if principal_role == "sysadmin" or (
        isinstance(principal_role, (list, tuple, set, frozenset))
        and "sysadmin" in principal_role
    ):
        return _synthetic_sysadmin()

    return None


def ensure_sysadmin_if_targeting_admin(request: Request, target_role: str) -> None:
    """Business rule: only sysadmins may manage admin/sysadmin principals."""
    if target_role not in ("sysadmin", "admin"):
        return

    caller_role = getattr(request.state, "principal_role", None) or []
    if isinstance(caller_role, str):
        caller_role = [caller_role]

    if "sysadmin" in caller_role:
        return

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Insufficient privileges: Only System Administrators can manage Admin accounts.",
    )
