#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_ADMIN_ROLES = ["sysadmin"]


def register_configs_policies():
    """Register configs-access policy (sysadmin only).

    Always registers into the module-level in-memory registry so that
    provision_default_policies() picks it up at lifespan startup,
    regardless of whether PermissionProtocol is already available.
    """
    from dynastore.modules.apikey.policies import (
        register_policy as _reg_policy,
        register_role as _reg_role,
    )
    from dynastore.models.protocols.policies import PermissionProtocol

    configs_policy = Policy(
        id="configs_access",
        description="Grants full access to configuration management endpoints.",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[
            "/configs",
            "/configs/",
            "/configs/.*",
            "/web/pages/configs_editor",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    _reg_policy(configs_policy)

    # Attach to sysadmin only — admins see catalog-level configs only via admin_panel
    for role_name in _ADMIN_ROLES:
        _reg_role(Role(name=role_name, policies=["configs_access"], is_system=True))

    logger.debug("Configs policies pre-registered into in-memory registry.")

    # If PermissionProtocol is already live, push immediately.
    policy_manager = get_protocol(PermissionProtocol)
    if policy_manager:
        policy_manager.register_policy(configs_policy)
        for role_name in _ADMIN_ROLES:
            policy_manager.register_role(
                Role(name=role_name, policies=["configs_access"], is_system=True)
            )
        logger.debug("Configs policies also applied to live PermissionProtocol.")
