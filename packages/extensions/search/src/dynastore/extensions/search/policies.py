#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_REINDEX_ADMIN_ROLES = (DefaultRole.SYSADMIN.value, DefaultRole.ADMIN.value)
_LOOKUP_PUBLIC_CONDITION = [{"type": "catalog_lookup_public_allowed"}]


def register_search_policies():
    """Register all PermissionProtocol policies owned by the search extension:

    1. Admin gate on the bulk reindex trigger endpoints.
    2. Anonymous ALLOW on /search/catalogs/{cat}/geoid/* — gated on
       CatalogLookupAudience.is_public via the catalog_lookup_public_allowed
       condition.
    3. Anonymous DENY on /stac/catalogs/{cat}/* and /features/catalogs/{cat}/*
       under the SAME condition. With deny precedence in the policy engine,
       this collapses anonymous access on opt-in catalogs to exactly the
       /search/catalogs/{cat}/geoid/* lookup window. Authenticated callers
       are unaffected — those paths are governed by their own policies.
    """
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; search policies not registered.")
        return

    # 1. Reindex (admin only) — unchanged
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
    for role_name in _REINDEX_ADMIN_ROLES:
        pm.register_role(Role(name=role_name, policies=["search_reindex_admin"]))

    # 2. Anonymous ALLOW on lookup endpoint (conditional on catalog opt-in)
    anon_lookup = Policy(
        id="search_anonymous_lookup_per_catalog",
        description=(
            "Anonymous access to /search/catalogs/{cat}/geoid lookups when the "
            "catalog has opted in via CatalogLookupAudience.is_public."
        ),
        actions=["GET", "POST"],
        resources=[r"/search/catalogs/[^/]+/geoid(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="ALLOW",
    )
    pm.register_policy(anon_lookup)

    # 3. Anonymous DENY on STAC + Features for opt-in catalogs (lookup-only mode)
    stac_deny = Policy(
        id="search_anonymous_stac_deny_lookup_only",
        description=(
            "Block anonymous access to STAC enumeration on catalogs that have "
            "opted into lookup-only mode (CatalogLookupAudience.is_public=True). "
            "Authenticated callers retain normal access; the dedicated "
            "/search/catalogs/{cat}/geoid endpoint remains anonymously accessible."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=[r"/stac/catalogs/[^/]+(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    )
    pm.register_policy(stac_deny)

    features_deny = Policy(
        id="search_anonymous_features_deny_lookup_only",
        description=(
            "Block anonymous access to OGC Features on catalogs that have "
            "opted into lookup-only mode."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=[r"/features/catalogs/[^/]+(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    )
    pm.register_policy(features_deny)

    pm.register_role(Role(
        name=DefaultRole.ANONYMOUS.value,
        policies=[
            "search_anonymous_lookup_per_catalog",
            "search_anonymous_stac_deny_lookup_only",
            "search_anonymous_features_deny_lookup_only",
        ],
    ))

    logger.debug(
        "Search policies registered: reindex admin + anonymous lookup ALLOW "
        "+ STAC/Features DENY for opt-in catalogs."
    )
