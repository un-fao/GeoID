
import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from dynastore.modules.catalog.models import Catalog
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.models.driver_context import DriverContext

@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "httpx", "proxy", "stats", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("proxy")
@pytest.mark.asyncio
async def test_proxy_ownership_and_cleanup(app_lifespan, catalog_obj, collection_obj):
    """
    Verifies the proxy URL ownership tracking and automatic cleanup.
    """
    app = app_lifespan.app
    engine = app_lifespan.engine
    catalog_id = catalog_obj.id
    collection_id = collection_obj.id
    
    # 1. Create Catalog
    catalogs = get_protocol(CatalogsProtocol)
    async with managed_transaction(engine) as conn:
        phys_schema_before = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True)
        if phys_schema_before:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE'))
            await conn.commit()
            
        await catalogs.create_catalog(catalog_obj, ctx=DriverContext(db_resource=conn))
        await lifecycle_registry.wait_for_all_tasks()
    
    # 2. Verify parent table exists in tenant schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    assert phys_schema is not None

    async with managed_transaction(engine) as conn:
        res = await conn.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = 'short_urls'"
        ), {"schema": phys_schema})
        assert res.scalar() == 1, f"Parent table 'short_urls' not found in tenant schema '{phys_schema}'"

    # 3. Create Collection
    async with managed_transaction(engine) as conn:
        await catalogs.create_collection(catalog_id, collection_obj, ctx=DriverContext(db_resource=conn))
        await lifecycle_registry.wait_for_all_tasks()
    
    # 3a. Verify partition existence
    partition_name = f"short_urls_{collection_id}"
    async with managed_transaction(engine) as conn:
        res = await conn.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :partition"
        ), {"schema": phys_schema, "partition": partition_name})
        assert res.scalar() == 1, f"Partition '{partition_name}' not found in tenant schema '{phys_schema}'"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        
        # 4. Create collection-scoped Proxy URL
        long_url = "https://example.com/collection-data"
        payload = {
            "long_url": long_url,
            "comment": "Collection URL"
        }
        
        create_resp = await ac.post(
            f"/proxy/catalogs/{catalog_id}/collections/{collection_id}/urls", 
            json=payload
        )
        assert create_resp.status_code == 201, f"Create failed: {create_resp.text}"
        data = create_resp.json()
        short_key = data["short_key"]
        
        # 5. Verify entries in DB
        async with managed_transaction(engine) as conn:
            # Check tenant short_urls table (should be in the partition)
            q_partition = text(f'SELECT count(*) FROM "{phys_schema}"."{partition_name}" WHERE short_key = :key')
            res_partition = await conn.execute(q_partition, {"key": short_key})
            assert res_partition.scalar() == 1, "Short URL not found in collection partition"

        # 6. Delete Collection (programmatically, not via REST API)
        async with managed_transaction(engine) as conn:
            await catalogs.delete_collection(catalog_id, collection_id, force=True, ctx=DriverContext(db_resource=conn))
        
        # Wait for background tasks (cleanup hook)
        await lifecycle_registry.wait_for_all_tasks()
        
        # 7. Verify automatic cleanup
        async with managed_transaction(engine) as conn:
            # Partition should be gone
            res_part = await conn.execute(text(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :partition"
            ), {"schema": phys_schema, "partition": partition_name})
            assert res_part.scalar() is None, f"Partition {partition_name} was NOT dropped"
            
            # Short URL should be gone from parent table
            q_parent = text(f'SELECT count(*) FROM "{phys_schema}".short_urls WHERE short_key = :key')
            res_parent = await conn.execute(q_parent, {"key": short_key})
            assert res_parent.scalar() == 0, f"Proxy URL {short_key} still present in parent table"

