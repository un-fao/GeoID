"""Geoid-extension PermissionProtocol policies.

Registered at extension lifespan startup. Only operators that load the
geoid extension are affected — none of these policies fire by default;
they only become active when an operator opts a catalog into
``CatalogLookupAudience.is_public=True``.
"""
import logging

from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_LOOKUP_PUBLIC_CONDITION = [{"type": "catalog_lookup_public_allowed"}]


def register_geoid_policies():
    """Register all geoid-extension PermissionProtocol policies + role grants.

    1. ALLOW anonymous on /search/catalogs/{cat}/geoid/* — gated on
       CatalogLookupAudience.is_public via catalog_lookup_public_allowed.
    2. DENY anonymous on /stac/catalogs/{cat}/* under the same condition
       (lookup-only mode locks anonymous out of STAC enumeration).
    3. DENY anonymous on /features/catalogs/{cat}/* under the same condition.

    Authenticated callers are unaffected — those paths are governed by
    their own (out-of-this-extension) policies.
    """
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; geoid policies not registered.")
        return

    pm.register_policy(Policy(
        id="geoid_anonymous_lookup_per_catalog",
        description=(
            "Anonymous access to /search/catalogs/{cat}/geoid lookups when the "
            "catalog has opted in via CatalogLookupAudience.is_public."
        ),
        actions=["GET", "POST"],
        resources=[r"/search/catalogs/[^/]+/geoid(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="ALLOW",
    ))
    pm.register_policy(Policy(
        id="geoid_anonymous_stac_deny_lookup_only",
        description=(
            "Block anonymous access to STAC enumeration on catalogs that have "
            "opted into lookup-only mode (CatalogLookupAudience.is_public=True)."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=[r"/stac/catalogs/[^/]+(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    ))
    pm.register_policy(Policy(
        id="geoid_anonymous_features_deny_lookup_only",
        description=(
            "Block anonymous access to OGC Features on catalogs that have "
            "opted into lookup-only mode."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=[r"/features/catalogs/[^/]+(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    ))
    pm.register_role(Role(
        name=DefaultRole.ANONYMOUS.value,
        policies=[
            "geoid_anonymous_lookup_per_catalog",
            "geoid_anonymous_stac_deny_lookup_only",
            "geoid_anonymous_features_deny_lookup_only",
        ],
    ))
    logger.debug("Geoid policies registered (anonymous lookup ALLOW + STAC/Features DENY).")
