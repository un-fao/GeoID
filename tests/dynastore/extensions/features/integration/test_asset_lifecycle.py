import pytest
from uuid import uuid4
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction, DQLQuery, ResultHandler



@pytest.mark.asyncio
async def test_create_collection_attaches_asset_trigger_integration(app_lifespan, catalog_id, collection_id):
    """
    Integration test verifying that creating a collection 
    correctly attaches the asset cleanup trigger.
    """
    catalogs = get_protocol(CatalogsProtocol)
    
    # 1. Setup Catalog
    await catalogs.ensure_catalog_exists(catalog_id)
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    
    # 2. Create Collection (this triggers lifecycle initializers)
    await catalogs.create_collection(catalog_id, {
        "id": collection_id,
        "title": {"en": "Trigger Test Collection"},
        "layer_config": {
            "sidecars": [
                {
                    "sidecar_type": "attributes",
                    "enable_asset_id": True, 
                    "storage_mode": "jsonb"
                }
            ]
        }
    }, lang="*", physical_table=collection_id)
    
    # 3. Verify Trigger Existence in Database
    async with managed_transaction(app_lifespan.engine) as conn:
        # Check if the trigger exists on the attribute sidecar table
        # sidecar table is named {collection_id}_attributes
        sidecar_table = f"{collection_id}_attributes"
        
        trigger_res = await DQLQuery(
            """
            SELECT trigger_name 
            FROM information_schema.triggers 
            WHERE event_object_schema = :schema 
              AND event_object_table = :table
              AND trigger_name = 'trg_asset_cleanup';
            """,
            result_handler=ResultHandler.ALL
        ).execute(conn, schema=phys_schema, table=sidecar_table)

        # DEBUG: List all triggers in schema
        all_triggers = await DQLQuery(
            "SELECT trigger_name, event_object_table FROM information_schema.triggers WHERE event_object_schema = :schema",
            result_handler=ResultHandler.ALL
        ).execute(conn, schema=phys_schema)
        
        all_tables = await DQLQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema",
            result_handler=ResultHandler.ALL
        ).execute(conn, schema=phys_schema)
        
        assert len(trigger_res) > 0, f"Asset cleanup trigger not found on {phys_schema}.{sidecar_table}. Triggers={all_triggers}, Tables={all_tables}"
