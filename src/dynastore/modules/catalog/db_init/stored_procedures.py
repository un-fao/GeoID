import logging
import asyncio
from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from dynastore.modules.db_config.locking_tools import acquire_lock_if_needed

logger = logging.getLogger(__name__)

UPDATE_COLLECTION_EXTENTS_SQL = """
CREATE OR REPLACE FUNCTION update_collection_extents()
RETURNS TRIGGER AS $$
DECLARE
    master_table_name TEXT := TG_ARGV[0];
    schema_name TEXT := TG_TABLE_SCHEMA;
    catalog_id_val TEXT;
    collection_id_val TEXT;
    new_spatial_extent JSONB;
    new_temporal_extent JSONB;
BEGIN
    catalog_id_val := schema_name;
    -- master_table_name is the Hub physical table.
    -- We need to find the collection_id for this physical table.
    -- Assuming 1:1 mapping in this schema.
    SELECT id INTO collection_id_val FROM collections WHERE physical_table = master_table_name LIMIT 1;

    IF collection_id_val IS NULL THEN
        -- Fallback or error? If we can't find the collection, we can't update metadata.
        RETURN NULL;
    END IF;

    -- Recalculate the spatial extent (bounding box) for the entire collection from the master table
    EXECUTE format('SELECT jsonb_build_object(''bbox'', ARRAY[ST_Extent(geom)]) FROM %I.%I', schema_name, master_table_name)
    INTO new_spatial_extent;

    -- Recalculate the temporal extent for the entire collection from the master table
    EXECUTE format('SELECT jsonb_build_object(''interval'', ARRAY[ARRAY[MIN(valid_from), MAX(valid_to)]]) FROM %I.%I', schema_name, master_table_name)
    INTO new_temporal_extent;

    -- Update the metadata JSONB in the collections table
    IF new_spatial_extent IS NOT NULL AND new_temporal_extent IS NOT NULL THEN
        UPDATE collections
        SET metadata = jsonb_set(
                        jsonb_set(metadata, '{{extent,spatial}}', new_spatial_extent),
                        '{{extent,temporal}}', new_temporal_extent
                       )
        WHERE id = collection_id_val AND catalog_id = catalog_id_val;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

ASSET_CLEANUP_SQL = """
CREATE OR REPLACE FUNCTION asset_cleanup() RETURNS TRIGGER AS $$
DECLARE
    hub_physical_table TEXT := TG_ARGV[0];
    asset_id_val TEXT;
    remaining_count INTEGER;
    target_collection_id TEXT;
BEGIN
    -- Only proceed if the asset_id actually changed or was deleted
    IF (TG_OP = 'UPDATE' AND NEW.asset_id IS NOT DISTINCT FROM OLD.asset_id) THEN
        RETURN NULL;
    END IF;

    asset_id_val := OLD.asset_id;

    -- If asset_id is null, nothing to cleanup
    IF asset_id_val IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Resolve Logical Collection ID from Hub Physical Table Name
    -- The collections table is in the same schema (tenant schema).
    EXECUTE format('SELECT id FROM %I.collections WHERE physical_table = $1 LIMIT 1', TG_TABLE_SCHEMA)
    INTO target_collection_id
    USING hub_physical_table;
    
    -- If we can't resolve the collection, we can't safely clean up
    IF target_collection_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Check if the asset is still referenced in this table (the sidecar)
    EXECUTE format('SELECT 1 FROM %I.%I WHERE asset_id = $1 LIMIT 1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
    INTO remaining_count
    USING asset_id_val;

    -- If remaining_count is NULL (no rows found), then proceed
    IF remaining_count IS NULL THEN
        -- Delete from assets table if it matches the collection scope.
        -- Note: using asset_id (logical) not id (PK) to match assets table if that's the intention
        -- In init.sql it was deleted by asset_id and collection_id. 
        EXECUTE format('DELETE FROM %I.assets WHERE id = $1 AND collection_id = $2', TG_TABLE_SCHEMA)
        USING asset_id_val, target_collection_id;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

async def ensure_stored_procedures(conn: DbResource) -> None:
    """Ensures all required stored procedures exist in the tenant schema."""
    async with acquire_lock_if_needed(conn, "ensure_stored_procedures", lambda: asyncio.sleep(0, result=False)):
        # We use acquire_lock_if_needed to ensure DDL safety.
        # Note: TG_TABLE_SCHEMA will resolve to the current schema at runtime in the trigger.
        
        # 1. Update Collection Extents
        await DQLQuery(UPDATE_COLLECTION_EXTENTS_SQL, result_handler=ResultHandler.ROWCOUNT).execute(conn)
        
        # 2. Asset Cleanup
        await DQLQuery(ASSET_CLEANUP_SQL, result_handler=ResultHandler.ROWCOUNT).execute(conn)
        
    logger.info("Catalog stored procedures ensured.")
