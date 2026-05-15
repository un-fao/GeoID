"""Geoid-extension ConditionHandlers — RE-EXPORT SHIM.

The audience ConditionHandlers (``CatalogLookupAudienceHandler``,
``CollectionWriteAudienceHandler``) have been migrated to core IAM at
``dynastore.modules.iam.audience_handlers`` (#286). They auto-register
into the default ``ConditionRegistry`` via the core IAM module's startup
path, so the geoid extension no longer needs to register them itself.

This module remains as a back-compat re-export so existing imports keep
working until the geoid extension is fully removed (#287). After that
removal, callers should import from ``dynastore.modules.iam.audience_handlers``
directly.
"""
from dynastore.modules.iam.audience_handlers import (  # noqa: F401  (re-exports)
    CatalogLookupAudienceHandler,
    CollectionWriteAudienceHandler,
    _resolve_collection_id,
)
