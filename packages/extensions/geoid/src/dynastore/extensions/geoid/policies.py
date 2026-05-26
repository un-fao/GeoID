"""Geoid-extension PermissionProtocol policies — DEPRECATED.

⚠️ DEPRECATED: This module is kept for backwards compatibility only.
Policy registration has moved to per-catalog hooks in geoid/hooks.py.

The policies are now registered on a per-catalog basis during the
CATALOG_CREATION event, rather than globally at extension startup.

This allows each catalog to have its own isolated policy instances,
enabling multi-tenant configurations where different catalogs can have
different permission settings.

Legacy imports should refer to hooks.py for the new per-catalog implementation.
"""
import logging

logger = logging.getLogger(__name__)


def register_geoid_policies():
    """DEPRECATED: Global policy registration.

    This function is kept for backwards compatibility but does nothing.
    Policy registration has moved to per-catalog hooks in geoid/hooks.py.

    The new architecture registers policies when each catalog is created
    (CATALOG_CREATION event), rather than globally at extension startup.
    This enables multi-tenancy and reduces overhead on catalogs that don't
    use the geoid preset.

    Callers should use register_geoid_hooks() from geoid/hooks.py instead.
    """
    logger.warning(
        "register_geoid_policies() is deprecated and does nothing. "
        "Use register_geoid_hooks() from geoid/hooks.py for per-catalog policy registration."
    )
