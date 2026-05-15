"""Geoid-extension PluginConfigs — RE-EXPORT SHIM.

The audience PluginConfigs (``CatalogLookupAudience``,
``CollectionWriteAudience``) have been migrated to core IAM at
``dynastore.modules.iam.audience_configs`` (#286). The ``_address``
tuples and ``plugin_id`` slugs are unchanged, so operator-persisted
configs continue to resolve without migration.

This module remains as a back-compat re-export so existing imports keep
working until the geoid extension is fully removed (#287). After that
removal, callers should import from ``dynastore.modules.iam.audience_configs``
directly.
"""
from dynastore.modules.iam.audience_configs import (  # noqa: F401  (re-exports)
    CatalogLookupAudience,
    CollectionWriteAudience,
)
