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
