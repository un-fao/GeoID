#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

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
