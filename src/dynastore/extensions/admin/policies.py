#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_ADMIN_ROLES = ["sysadmin", "admin"]


def register_admin_policies():
    """Register admin-access policy (sysadmin/admin only) via PermissionProtocol."""
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
            "/web/pages/migrations_panel",  # migrations dashboard
        ],
        effect="ALLOW",
    )
    pm.register_policy(admin_policy)

    for role_name in _ADMIN_ROLES:
        pm.register_role(Role(name=role_name, policies=["admin_access"]))

    logger.debug("Admin policies registered via PermissionProtocol.")
