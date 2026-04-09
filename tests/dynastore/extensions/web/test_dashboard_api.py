"""
Integration tests for the Web extension's dashboard API endpoints.

All tests run against a real in-process FastAPI application (no mocks) with a
live database, using the sysadmin_in_process_client fixture which already
implies app_lifespan startup.

Enabled extensions: web, logs, features, stac
Enabled modules:    db_config, db, catalog, stats, iam

Fixture notes:
- sysadmin_in_process_client  — authenticated HTTP client; implicitly starts app
- my_catalog_id               — creates/tears down a real catalog around the test
"""

import pytest
from httpx import AsyncClient
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.modules.catalog.models import Catalog

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_extensions("web", "logs", "features", "stac"),
    pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "iam"),
]


@pytest.fixture
async def my_catalog_id(app_lifespan, catalog_id):
    """Fixture that actually creates a catalog in the database for these tests."""
    catalogs = get_protocol(CatalogsProtocol)
    cat = Catalog(id=catalog_id, title={"en": "Test Dashboard Catalog"})
    await catalogs.create_catalog(cat.model_dump(), lang="*")
    yield catalog_id
    try:
        await catalogs.delete_catalog(catalog_id, force=True)
    except Exception:
        pass

# @pytest.mark.skip()
async def test_dashboard_stats_endpoint(
    sysadmin_in_process_client: AsyncClient, my_catalog_id: str
):
    """Test the dashboard stats endpoint returns 200."""
    response = await sysadmin_in_process_client.get(
        f"/web/dashboard/stats?catalog_id={my_catalog_id}"
    )
    assert response.status_code == 200
    data = response.json()
    assert "total_requests" in data
    assert "average_latency_ms" in data


async def test_dashboard_logs_endpoint(
    sysadmin_in_process_client: AsyncClient, my_catalog_id: str
):
    """Test the dashboard logs endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client.get(
        f"/web/dashboard/logs?limit=5&catalog_id={my_catalog_id}"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_dashboard_events_endpoint(
    sysadmin_in_process_client: AsyncClient, my_catalog_id: str
):
    """Test the dashboard events endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client.get(
        f"/web/dashboard/events?limit=5&catalog_id={my_catalog_id}"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_dashboard_catalogs_endpoint(
    sysadmin_in_process_client: AsyncClient,
):
    """Test the dashboard catalogs endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client.get("/web/dashboard/catalogs")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_dashboard_collections_endpoint(
    sysadmin_in_process_client: AsyncClient, my_catalog_id: str
):
    """Test the dashboard collections endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client.get(
        f"/web/dashboard/catalogs/{my_catalog_id}/collections"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_health_endpoint(sysadmin_in_process_client: AsyncClient):
    """Test the health endpoint returns ok status."""
    response = await sysadmin_in_process_client.get("/web/health")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "ok"


async def test_web_pages_config_endpoint(sysadmin_in_process_client: AsyncClient):
    """Test the web pages config endpoint returns a list."""
    response = await sysadmin_in_process_client.get("/web/config/pages?language=en")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_docs_manifest_endpoint(sysadmin_in_process_client: AsyncClient):
    """Test the docs manifest endpoint returns a dict bucketed by category."""
    response = await sysadmin_in_process_client.get("/web/docs-manifest")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    for category, items in data.items():
        assert isinstance(items, list)
        for item in items:
            assert "id" in item
            assert "title" in item


async def test_dashboard_logs_pagination(sysadmin_in_process_client: AsyncClient, my_catalog_id: str):
    """Test that the logs endpoint respects the limit parameter."""
    response = await sysadmin_in_process_client.get(
        f"/web/dashboard/logs?limit=1&catalog_id={my_catalog_id}"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 1


async def test_dashboard_collections_unknown_catalog(
    sysadmin_in_process_client: AsyncClient,
):
    """
    Requesting collections for a non-existent catalog returns 200 with an empty
    list — the endpoint delegates to CollectionsProtocol which returns [] when
    the catalog has no collections (or does not exist), rather than raising 404.
    """
    response = await sysadmin_in_process_client.get(
        "/web/dashboard/catalogs/nonexistent-catalog-xyz/collections"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data == []
