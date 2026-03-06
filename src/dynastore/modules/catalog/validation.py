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
        logger.error("ConfigsProtocol not found in validation.py!")
        # Fallback empty config if protocol missing
        config = None
    else:
        config = await configs.get_config(COLLECTION_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
    
    schema_properties = set()
    if config:
        # 2a. Legacy Attribute Schema
        if config.attribute_schema:
            schema_properties.update({entry.name for entry in config.attribute_schema})

        # 2b. Sidecar Fields (New)
        if hasattr(config, "sidecars") and config.sidecars:
            from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
            for sc_config in config.sidecars:
                if not sc_config.enabled:
                    continue
                try:
                    # Instantiate sidecar to access dynamic field definitions
                    sidecar = SidecarRegistry.get_sidecar(sc_config)
                    field_defs = sidecar.get_field_definitions()
                    schema_properties.update(field_defs.keys())
                except Exception as e:
                    logger.warning(f"Failed to get field definitions from sidecar {type(sc_config).__name__}: {e}")
        
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
