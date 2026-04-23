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

"""StacModule — registers the STAC PG metadata drivers + applies global DDL.

The STAC capability sub-Protocols (``StacCollectionMetadataCapability`` /
``StacCatalogMetadataCapability``) are owned by ``extensions/stac/protocols.py``
and are structurally satisfied by the drivers registered here. Splitting
the STAC code into its own module means SCOPE/entry-point installation
controls whether STAC fields can be persisted at all — without this
module loaded, ``get_protocols(StacCollectionMetadataCapability)``
returns nothing and ``stac_service._has_stac()`` reports False.

Module priority is 60 — runs after ``CatalogModule`` (default 50) so the
``catalog.catalogs`` table (FK target for ``catalog.catalog_metadata_stac``)
exists before this module's lifespan applies the global STAC DDL.
"""

import logging
from contextlib import asynccontextmanager

from dynastore.modules import ModuleProtocol
from dynastore.tools.discovery import get_protocol, register_plugin

logger = logging.getLogger(__name__)


class StacModule(ModuleProtocol):
    """Lifespan owner for the STAC catalog-tier PG metadata driver + global DDL.

    On lifespan entry:
    - Imports the per-tenant lifecycle hook so its
      ``@lifecycle_registry.sync_catalog_initializer`` decorator fires.
    - Applies the global ``catalog.catalog_metadata_stac`` DDL.
    - Registers ``CatalogStacPostgresqlDriver`` via ``register_plugin(...)``.
      The collection-tier STAC slice is no longer surfaced as a standalone
      ``CollectionMetadataStore`` plugin — it is composed inside
      ``CollectionPostgresqlDriver`` via the
      ``MetadataPgSidecarRegistry`` try-import (PR 1e step 3b).  Until
      step 3c lands ``CatalogPostgresqlDriver``, the catalog-tier STAC
      driver still ships as its own plugin.
    """

    priority: int = 60

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        # Import the db_init module so its lifecycle decorator registers
        # the per-tenant STAC DDL initializer with lifecycle_registry.
        from dynastore.modules.stac.db_init import metadata_stac_tables  # noqa: F401
        from dynastore.modules.stac.db_init.metadata_stac_tables import (
            ensure_global_stac_metadata_tables,
        )
        from dynastore.modules.stac.drivers.metadata_postgresql import (
            CatalogStacPostgresqlDriver,
        )

        # Apply global DDL (catalog.catalog_metadata_stac) — once, idempotent.
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.db_config.query_executor import managed_transaction

        db = get_protocol(DatabaseProtocol)
        if db and db.engine:
            async with managed_transaction(db.engine) as conn:
                await ensure_global_stac_metadata_tables(conn)
            logger.info(
                "StacModule: global STAC metadata DDL applied "
                "(catalog.catalog_metadata_stac)."
            )
        else:
            logger.warning(
                "StacModule: DatabaseProtocol unavailable — global STAC "
                "DDL skipped; STAC catalog metadata persistence will fail."
            )

        register_plugin(CatalogStacPostgresqlDriver())
        logger.info(
            "StacModule: registered catalog-tier STAC PG metadata driver "
            "(collection-tier STAC slice is composed inside "
            "CollectionPostgresqlDriver via MetadataPgSidecarRegistry).",
        )

        try:
            yield
        finally:
            pass
