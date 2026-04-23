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

"""StacModule — registers the STAC PG metadata drivers.

The STAC capability sub-Protocols (``StacCollectionMetadataCapability`` /
``StacCatalogMetadataCapability``) are owned by ``extensions/stac/protocols.py``
and are structurally satisfied by the drivers registered here. Splitting
the STAC code into its own module means SCOPE/entry-point installation
controls whether STAC fields can be persisted at all — without this
module loaded, ``get_protocols(StacCollectionMetadataCapability)``
returns nothing and ``stac_service._has_stac()`` reports False.

Not yet wired into ``[project.entry-points."dynastore.modules"]`` —
that activation happens in PR 1b together with the canonical-definition
move out of ``modules/storage/drivers/metadata_domain_postgresql.py``.
"""

import logging
from contextlib import asynccontextmanager

from dynastore.modules import ModuleProtocol
from dynastore.tools.discovery import register_plugin

logger = logging.getLogger(__name__)


class StacModule(ModuleProtocol):
    """Lifespan owner for the STAC PG metadata drivers.

    On lifespan entry: instantiates and registers the relocated STAC
    PG drivers via ``register_plugin(...)`` so that
    ``get_protocols(CollectionMetadataStore)`` /
    ``get_protocols(StacCollectionMetadataCapability)`` find them.
    """

    priority: int = 50

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.modules.stac.drivers.metadata_postgresql import (
            CatalogStacPostgresqlDriver,
            CollectionStacPostgresqlDriver,
        )

        register_plugin(CollectionStacPostgresqlDriver())
        register_plugin(CatalogStacPostgresqlDriver())
        logger.info("StacModule: registered STAC PG metadata drivers.")

        try:
            yield
        finally:
            pass
