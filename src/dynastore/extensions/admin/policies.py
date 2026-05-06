#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.auth import Condition
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_admin_policies():
    """Register admin-access *policies* via ``PermissionProtocol``.

    Policies are stable code-level contracts that say "this URL set, with
    these conditions, ALLOWs". *Roles* — and which roles are bound to
    which policies — are operator-managed in the DB
    (``iam.roles`` / ``iam.policies`` storages, REST surface
    ``/admin/roles`` and ``/admin/policies``). Treat the catalog-admin
    role binding the same way you'd treat any custom role: it lives in
    the DB, the operator manages it, the code just registers the policy.

    Two policies are registered here:

    - ``admin_access`` — broad ``/admin/.*`` ALLOW. Default role
      bindings are seeded for back-compat with existing deployments
      (sysadmin and admin); operators may add or remove bindings via
      REST.

    - ``admin_catalog_access`` — per-catalog mutation surface gated by
      ``catalog_admin_required``. **No default role bindings**:
      operators who want a particular role to gain catalog-admin
      authority do it via ``POST /admin/roles`` (with ``policies:
      ["admin_catalog_access"]``) or programmatic ``register_role``,
      and reference the same role name in this policy's condition
      config. Behaves like a custom role end-to-end.
    """
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; admin policies not registered.")
        return

    admin_policy = Policy(
        id="admin_access",
        description="Grants full access to admin management endpoints.",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[
            "/admin",
            "/admin/",
            "/admin/.*",
            "/web/pages/admin_panel",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    pm.register_policy(admin_policy)
    # Back-compat seeding: existing deployments expect SYSADMIN+ADMIN to
    # carry admin_access. Operators may rebind via REST.
    for role_name in (DefaultRole.SYSADMIN.value, DefaultRole.ADMIN.value):
        pm.register_role(Role(name=role_name, policies=["admin_access"]))

    # Catalog-scoped admin policy. Resource regex matches every URL under
    # /admin/catalogs/{cat}/...; the catalog_admin_required condition
    # gates by per-catalog role grants. The condition's config carries
    # an empty required_roles list by default — until an operator
    # populates it (or replaces this Policy with their own variant), the
    # condition admits only sysadmin/platform-grant principals via the
    # bypass paths. This is intentional: no role gets catalog-admin
    # authority unless explicitly granted via storage.
    admin_catalog_policy = Policy(
        id="admin_catalog_access",
        description=(
            "Per-catalog admin access; admits roles declared in this "
            "policy's catalog_admin_required condition. Sysadmin and "
            "platform-grant principals bypass."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
        effect="ALLOW",
        conditions=[
            Condition(
                type="catalog_admin_required",
                config={"required_roles": []},
            )
        ],
    )
    pm.register_policy(admin_catalog_policy)
    # No default role bindings — operators bind via /admin/roles or by
    # editing this policy's required_roles condition list. Mirrors how
    # custom roles join the system.

    logger.debug("Admin policies registered via PermissionProtocol.")
