#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_REINDEX_ADMIN_ROLES = ["sysadmin", "admin"]


def register_search_policies():
    """Register admin-only policy for the reindex endpoints.

    Registers into the module-level in-memory registry so that
    provision_default_policies() picks it up at lifespan startup.
    """
    from dynastore.modules.apikey.policies import (
        register_policy as _reg_policy,
        register_role as _reg_role,
    )
    from dynastore.models.protocols.policies import PermissionProtocol

    reindex_policy = Policy(
        id="search_reindex_admin",
        description="Grants access to the bulk reindex trigger endpoints (admin only).",
        actions=["POST"],
        resources=[
            "/search/reindex",
            "/search/reindex/",
            "/search/reindex/.*",
        ],
        effect="ALLOW",
    )
    _reg_policy(reindex_policy)

    for role_name in _REINDEX_ADMIN_ROLES:
        _reg_role(Role(name=role_name, policies=["search_reindex_admin"], is_system=True))

    logger.debug("Search reindex policies pre-registered into in-memory registry.")

    # If PermissionProtocol is already live, push immediately.
    policy_manager = get_protocol(PermissionProtocol)
    if policy_manager:
        policy_manager.register_policy(reindex_policy)
        for role_name in _REINDEX_ADMIN_ROLES:
            policy_manager.register_role(
                Role(name=role_name, policies=["search_reindex_admin"], is_system=True)
            )
        logger.debug("Search reindex policies also applied to live PermissionProtocol.")
