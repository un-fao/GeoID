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

# src/dynastore/extensions/notebooks/tenant_initialization.py
"""Lifecycle hook: creates/migrates the notebooks table in each tenant schema."""
import logging
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.notebooks.notebooks_db import init_notebooks_storage

logger = logging.getLogger(__name__)


@lifecycle_registry.sync_catalog_initializer  # type: ignore[arg-type]
async def _initialize_notebooks_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Create/migrate the notebooks table when a catalog is provisioned."""
    logger.info(f"Initializing notebooks tenant slice for {schema} (Catalog: {catalog_id})")
    await init_notebooks_storage(conn, schema, catalog_id)
