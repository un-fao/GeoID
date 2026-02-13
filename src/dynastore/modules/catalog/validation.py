import logging
from typing import Set, List, Optional
from dynastore.modules.db_config.exceptions import InternalValidationError
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig, COLLECTION_PLUGIN_CONFIG_ID
from dynastore.modules.db_config.shared_queries import get_table_column_names, DbResource
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

async def get_valid_properties(conn: DbResource, catalog_id: str, collection_id: str) -> Set[str]:
    """
    Returns a set of all valid property names for a collection, 
    including physical columns and schema-defined attributes.
    """
    # 1. Get physical columns using logical IDs
    catalogs = get_protocol(CatalogsProtocol)
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
    phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
    
    physical_columns = set()
    if phys_schema and phys_table:
        physical_columns = await get_table_column_names(conn, phys_schema, phys_table)
    
    # 2. Get attribute schema from config
    configs = get_protocol(ConfigsProtocol)
    if not configs:
        print("CRITICAL: ConfigsProtocol not found in validation.py!")
        # Debugging: print available protocols
        from dynastore.tools.discovery import _registry
        print(f"Available protocols: {_registry.keys()}")
    
    config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
    
    schema_properties = set()
    if config and config.attribute_schema:
        schema_properties = {entry.name for entry in config.attribute_schema}
        
    # Combine them
    return physical_columns.union(schema_properties)

async def validate_filter_properties(conn: DbResource, catalog_id: str, collection_id: str, property_names: List[str]):
    """
    Validates that all provided property names are valid for the collection.
    Raises HTTPException 400 if any property is unknown.
    """
    valid_props = await get_valid_properties(conn, catalog_id, collection_id)
    
    unknown_props = [p for p in property_names if p not in valid_props]
    if unknown_props:
        logger.warning(f"Unknown properties filtered for {catalog_id}.{collection_id}: {unknown_props}")
        raise InternalValidationError(
            f"Unknown filter properties: {', '.join(unknown_props)}. Valid properties are: {', '.join(sorted(valid_props))}"
        )
