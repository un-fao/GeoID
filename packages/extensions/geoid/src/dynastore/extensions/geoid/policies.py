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
