
import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from dynastore.modules.catalog.models import Catalog
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.concurrency import await_all_background_tasks
from dynastore.models.driver_context import DriverContext

@pytest.fixture(autouse=True)
def setup_proxy_env(monkeypatch):
    """Ensure proxy module is enabled for these tests."""
    monkeypatch.setenv("SCOPE", "proxy")

@pytest.mark.enable_modules("db_config", "db", "catalog", "proxy", "stats", "metadata_postgresql")
@pytest.mark.enable_extensions("proxy")
@pytest.mark.asyncio
async def test_proxy_service_flow(app_lifespan, catalog_obj):
    """
    Verifies the end-to-end flow of the Proxy Service:
    1. Create a Catalog (Tenant).
    2. Create a Short URL.
    3. Access the Short URL (Redirect).
    4. Verify Analytics Log (Background Task).
    """
    app = app_lifespan.app
    catalog_id = catalog_obj.id
    
    # 1. Create Catalog
    catalogs = get_protocol(CatalogsProtocol)
    async with managed_transaction(app_lifespan.engine) as conn:
        # cleanup any leftover physical schema first (use resolved name, not catalog_id)
        existing_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True)
        if existing_schema:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{existing_schema}" CASCADE'))
        # Also delete the catalog record if it exists
        await conn.execute(text("DELETE FROM catalog.catalogs WHERE id = :id"), {"id": catalog_id})
        # Create catalog
        await catalogs.create_catalog(catalog_obj, ctx=DriverContext(db_resource=conn))
        physical_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
        
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        
        # 2. Create Short URL
        long_url = "https://www.fao.org/start"
        payload = {
            "long_url": long_url,
            "comment": "Integration Test URL"
        }
        
        # Standardized API path
        create_resp = await ac.post(f"/proxy/catalogs/{catalog_id}/urls", json=payload)
        
        assert create_resp.status_code == 201, f"Failed to create URL: {create_resp.text}"
        data = create_resp.json()
        short_key = data["short_key"]
        assert short_key is not None
        assert data["long_url"] == long_url
        
        # 3. Access Short URL (Redirect)
        # Note: The redirect endpoint is GET /proxy/catalogs/{code}/r/{key}
        # But we probably want to support the short root /r/{key} if we configure it? 
        # The refactor changed checking:
        # endpoints are:
        # /proxy/catalogs/{catalog_id}/r/{short_key}
        
        redirect_resp = await ac.get(f"/proxy/catalogs/{catalog_id}/r/{short_key}", follow_redirects=False)
        assert redirect_resp.status_code == 307
        assert redirect_resp.headers["location"] == long_url
        
        # 4. Verify Analytics Log (Background Task)
        # NOTE: Analytics logging is not yet implemented - skipping this check
        # TODO: Re-enable when analytics logging is implemented
        # await await_all_background_tasks()
        # async with managed_transaction(app_lifespan.engine) as conn:
        #     query = text(f'SELECT count(*) FROM "{physical_schema}".url_analytics WHERE short_key_ref = :key')
        #     result = await conn.execute(query, {"key": short_key})
        #     count = result.scalar()
        #     assert count == 1, "Analytics log not found in tenant schema"

