#    Copyright 2025 FAO
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

"""Tenant-scope authorization for dashboard data endpoints.

The route-level web policy already gates `/web/dashboard/.*` for read access,
but at the platform layer that policy permits anonymous GETs. The dashboard
data endpoints (stats / logs / events) need a second, value-level check
because the response payload reveals per-catalog operational data.

Identity is read from ``request.state.principal`` and
``request.state.principal_role``, populated by ``IamMiddleware`` —
re-decoding the Authorization header inside the route can fail for
locally-minted JWTs even when the request is correctly authenticated.
The centralised ``AuthenticatorProtocol.authenticate_and_get_role`` is
only used as a fallback when middleware state is unavailable.

Two-tier policy:

- platform admins (effective role includes ``sysadmin``) may read any
  catalog including the synthetic ``_system_`` view;
- catalog admins (any other authenticated caller) must hold at least one
  grant in the requested catalog's tenant schema. ``_system_`` is denied.

Anonymous callers receive 401 so the frontend can prompt for login rather
than silently rendering an empty dashboard.

The catalog-membership lookup is the hot path here: every authenticated,
non-sysadmin dashboard request would otherwise trigger an O(N catalogs)
DB probe (one query per active catalog). It is wrapped with the project's
``@cached`` decorator (60s TTL, 10s jitter, per-pod) so the same caller's
membership is computed at most once per cache window.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request

from dynastore.models.auth import Principal
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.iam_query import IamQueryProtocol
from dynastore.tools.cache import cached
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

PLATFORM_CATALOG_ID = "_system_"


@dataclass
class DashboardCaller:
    """Resolved caller identity for dashboard endpoints."""

    principal: Optional[Principal] = None
    effective_roles: List[str] = field(default_factory=list)

    @property
    def is_anonymous(self) -> bool:
        return self.principal is None

    @property
    def is_platform_admin(self) -> bool:
        # ``effective_roles`` is the post-hierarchy expansion produced by
        # IamService.authenticate_and_get_role: a custom role that inherits
        # from sysadmin will surface ``sysadmin`` here too, so this remains
        # data-driven against the role hierarchy rather than a hard-coded
        # username check.
        return DefaultRole.SYSADMIN.value in self.effective_roles


async def resolve_dashboard_caller(request: Request) -> DashboardCaller:
    """Resolve the calling identity for a dashboard request.

    Prefers the middleware-populated request.state (canonical, signature-
    validated source) and only re-runs the AuthenticatorProtocol chain
    when state is empty (e.g. tests or callers that bypass IamMiddleware).
    Never raises on auth failure — callers decide how to respond.
    """
    state_principal = getattr(request.state, "principal", None)
    state_roles = getattr(request.state, "principal_role", None)
    if state_principal is not None or state_roles:
        if isinstance(state_roles, (list, tuple, set)):
            roles_list = [str(r) for r in state_roles]
        elif state_roles:
            roles_list = [str(state_roles)]
        else:
            roles_list = []
        return DashboardCaller(principal=state_principal, effective_roles=roles_list)

    # Fallback: middleware did not populate state. Re-run centralised
    # auth so test harnesses and any caller that bypasses IamMiddleware
    # still get a consistent answer.
    auth = get_protocol(AuthenticatorProtocol)
    if auth is None:
        return DashboardCaller()
    try:
        roles, principal = await auth.authenticate_and_get_role(request)
    except Exception as exc:
        logger.debug("Dashboard auth resolution failed: %s", exc)
        return DashboardCaller()
    return DashboardCaller(principal=principal, effective_roles=list(roles or []))


@cached(
    maxsize=512,
    ttl=60,
    jitter=10,
    namespace="dashboard_authz_membership",
    distributed=False,
    ignore=["iam_query"],
)
async def _get_membership(
    iam_query: IamQueryProtocol, provider: str, subject_id: str
) -> Dict[str, Any]:
    """Cached wrapper around ``IamQueryProtocol.list_catalog_memberships``.

    Cache key is ``(provider, subject_id)`` — the iam_query singleton is
    intentionally excluded so cache hits survive across the fixture
    teardown/setup boundaries that can occur between tests. 60s TTL is a
    safe upper bound on grant-change propagation; jitter prevents the
    classic thundering-herd pattern when many sessions log in at once.
    """
    return await iam_query.list_catalog_memberships(
        provider=provider, subject_id=subject_id
    )


def _log_denial(
    reason: str, *, caller: DashboardCaller, catalog_id: Optional[str]
) -> None:
    """Emit a structured WARNING for a denied dashboard request.

    The format ``dashboard_authz.denied caller=<provider>:<sub> ...`` is
    string-indexable by Cloud Logging without any new dependency. Token
    material is never logged — only the resolved provider/subject_id and
    the denial reason.
    """
    if caller.principal is not None:
        caller_id = (
            f"{caller.principal.provider or '?'}:{caller.principal.subject_id or '?'}"
        )
    else:
        caller_id = "anonymous"
    logger.warning(
        "dashboard_authz.denied caller=%s catalog=%s reason=%s",
        caller_id,
        catalog_id or "-",
        reason,
    )


async def authorize_dashboard_catalog(
    caller: DashboardCaller, catalog_id: str
) -> None:
    """Raise HTTPException unless ``caller`` may read dashboard data for ``catalog_id``.

    - anonymous → 401
    - sysadmin (or role inheriting sysadmin) → allow any catalog including ``_system_``
    - other authenticated callers requesting ``_system_`` → 403
    - other authenticated callers without any grant in ``catalog_id`` → 403
    """
    if caller.is_anonymous:
        _log_denial("anonymous", caller=caller, catalog_id=catalog_id)
        raise HTTPException(status_code=401, detail="Authentication required")

    if caller.is_platform_admin:
        return

    if catalog_id == PLATFORM_CATALOG_ID:
        _log_denial("system_requires_sysadmin", caller=caller, catalog_id=catalog_id)
        raise HTTPException(
            status_code=403,
            detail="Platform-scope dashboard requires sysadmin",
        )

    iam_query = get_protocol(IamQueryProtocol)
    if iam_query is None:
        _log_denial("iam_unavailable", caller=caller, catalog_id=catalog_id)
        raise HTTPException(status_code=503, detail="IAM service unavailable")

    # Defensive narrowing: is_anonymous already guarantees this, but using
    # an explicit check (instead of `assert`) keeps the contract intact
    # under `python -O` and survives any future change to is_anonymous.
    if caller.principal is None:
        _log_denial("no_principal", caller=caller, catalog_id=catalog_id)
        raise HTTPException(status_code=401, detail="Authentication required")

    provider = caller.principal.provider
    subject_id = caller.principal.subject_id
    if not provider or not subject_id:
        _log_denial("identity_not_resolvable", caller=caller, catalog_id=catalog_id)
        raise HTTPException(status_code=403, detail="Identity not resolvable")

    membership = await _get_membership(iam_query, provider, subject_id)
    if membership.get("platform"):
        # Identity carries a platform-scope grant even though the role label
        # didn't expand to ``sysadmin`` (e.g. a custom platform role) — treat
        # as platform admin for dashboard read purposes.
        return

    if catalog_id not in membership.get("catalogs", []):
        _log_denial("not_authorized", caller=caller, catalog_id=catalog_id)
        raise HTTPException(
            status_code=403, detail="Not authorized for this catalog"
        )
