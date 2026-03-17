#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_ADMIN_ROLES = ["sysadmin", "admin"]


def register_admin_policies():
    """Register admin-access policy (sysadmin/admin only).

    Always registers into the module-level in-memory registry so that
    provision_default_policies() picks it up at lifespan startup,
    regardless of whether PermissionProtocol is already available.
    """
    from dynastore.modules.apikey.policies import (
        register_policy as _reg_policy,
        register_role as _reg_role,
    )
    from dynastore.models.protocols.policies import PermissionProtocol

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
    _reg_policy(admin_policy)

    # Attach to sysadmin and admin roles only — anonymous and 'user' are NOT granted this
    for role_name in _ADMIN_ROLES:
        _reg_role(Role(name=role_name, policies=["admin_access"], is_system=True))

    logger.debug("Admin policies pre-registered into in-memory registry.")

    # If PermissionProtocol is already live, push immediately.
    policy_manager = get_protocol(PermissionProtocol)
    if policy_manager:
        policy_manager.register_policy(admin_policy)
        for role_name in _ADMIN_ROLES:
            policy_manager.register_role(
                Role(name=role_name, policies=["admin_access"], is_system=True)
            )
        logger.debug("Admin policies also applied to live PermissionProtocol.")
