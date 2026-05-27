#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_search_policies():
    """Register admin-only policies for search admin endpoints via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; search policies not registered.")
        return

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
    pm.register_policy(reindex_policy)

    backfill_policy = Policy(
        id="search_envelope_backfill_sysadmin",
        description=(
            "Grants access to the envelope-attrs backfill endpoint (sysadmin only). "
            "This endpoint stamps _attrs onto pre-existing ES envelope-driver docs "
            "written before #1441 shipped."
        ),
        actions=["POST"],
        resources=[
            "/search/catalogs/.*/collections/.*/backfill-envelope-attrs",
        ],
        effect="ALLOW",
    )
    pm.register_policy(backfill_policy)

    cfg = IamRolesConfig()
    for role_name in (cfg.sysadmin_role_name, cfg.admin_role_name):
        pm.register_role(Role(name=role_name, policies=["search_reindex_admin"]))

    # Backfill is sysadmin-only (destructive batch write).
    pm.register_role(
        Role(name=cfg.sysadmin_role_name, policies=["search_envelope_backfill_sysadmin"])
    )

    logger.debug("Search admin policies registered via PermissionProtocol.")
