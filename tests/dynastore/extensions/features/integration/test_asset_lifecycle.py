import pytest
from uuid import uuid4
from sqlalchemy import text
from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol
from dynastore.modules.catalog.asset_service import AssetBase, AssetTypeEnum
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)



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
        # Lazy-activation: a freshly-created collection has no physical_table
        # assigned until the first POST /items (or explicit activate). The
        # asset cleanup trigger is attached at activation time, so without
        # this call resolve_physical_table returns None and the test asserts
        # against `None_attributes`. Activate inside this transaction first.
        from dynastore.models.driver_context import DriverContext
        await catalogs.activate_collection(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        # Resolve the physical table name (may differ from collection_id).
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn,
        )
        # Sidecar table is named {physical_table}_attributes
        sidecar_table = f"{phys_table}_attributes"
        
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


@pytest.mark.asyncio
async def test_sidecar_delete_cascades_to_assets_row(app_lifespan, catalog_id, collection_id):
    """
    Regression test for the trg_asset_cleanup trigger cascade. Verifies that
    deleting the last sidecar row referencing an asset deletes the asset row
    in the partitioned ``assets`` table.

    Before the id→asset_id fix this trigger raised SQLSTATE 42703
    ('column "id" does not exist') because the assets PK is
    ``(collection_id, asset_id)`` and there is no ``id`` column.
    """
    from dynastore.models.driver_context import DriverContext

    catalogs = get_protocol(CatalogsProtocol)
    assets = get_protocol(AssetsProtocol)
    assert catalogs is not None and assets is not None

    await catalogs.ensure_catalog_exists(catalog_id)
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)

    await catalogs.create_collection(catalog_id, {
        "id": collection_id,
        "title": {"en": "Cascade Test Collection"},
        "layer_config": {
            "sidecars": [
                {
                    "sidecar_type": "attributes",
                    "enable_asset_id": True,
                    "storage_mode": "jsonb",
                }
            ]
        }
    }, lang="*", physical_table=collection_id)

    asset_id = f"asset_{uuid4().hex[:8]}"
    sidecar_pk = f"sc_{uuid4().hex[:8]}"

    async with managed_transaction(app_lifespan.engine) as conn:
        await catalogs.activate_collection(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn,
        )
        sidecar_table = f"{phys_table}_attributes"

        await assets.create_asset(
            catalog_id,
            asset=AssetBase(
                asset_id=asset_id,
                uri="file:///tmp/cascade_test.bin",
                asset_type=AssetTypeEnum.ASSET,
                metadata={"test": "cascade"},
            ),
            collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )

        await conn.execute(
            text(
                f'INSERT INTO "{phys_schema}"."{sidecar_table}" (id, asset_id) '
                "VALUES (:pk, :aid)"
            ),
            {"pk": sidecar_pk, "aid": asset_id},
        )

        await conn.execute(
            text(
                f'DELETE FROM "{phys_schema}"."{sidecar_table}" WHERE id = :pk'
            ),
            {"pk": sidecar_pk},
        )

        remaining = await DQLQuery(
            f'SELECT 1 FROM "{phys_schema}".assets '
            "WHERE collection_id = :cid AND asset_id = :aid",
            result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
        ).execute(conn, cid=collection_id, aid=asset_id)

        assert remaining is None, (
            f"Cascade trigger failed to delete asset row "
            f"{collection_id}/{asset_id} after sidecar removal"
        )
