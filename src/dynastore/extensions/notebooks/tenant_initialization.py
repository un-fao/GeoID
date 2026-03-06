import logging
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DDLQuery, DbResource
from dynastore.modules.notebooks.notebooks_db import init_notebooks_storage

logger = logging.getLogger(__name__)

@lifecycle_registry.sync_catalog_initializer
async def _initialize_notebooks_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """
    Initializes the notebooks extension's slice of the tenant schema.
    This registers the callback that creates the notebooks table when a new catalog is created.
    """
    logger.info(f"Initializing notebooks tenant slice for {schema} (Catalog: {catalog_id})")
    
    # Delegate to the module's initialization logic
    await init_notebooks_storage(conn, schema, catalog_id)
