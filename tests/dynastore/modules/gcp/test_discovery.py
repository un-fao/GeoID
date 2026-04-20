import pytest
import logging
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "processes")
@pytest.mark.enable_tasks()
async def test_gcp_job_runner_discovery_integration(app_lifespan, caplog, base_url):
    """GCP jobs discovered at runtime appear in GET /processes?scope=all."""
    caplog.set_level(logging.DEBUG)

    mock_job_mappings = {"ingestion": "job-abc"}

    with patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = mock_job_mappings

        from dynastore.modules.gcp.gcp_runner import GcpJobRunner

        runner = GcpJobRunner()
        await runner.setup(app_lifespan)

        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            response = await client.get("/processes/processes?scope=all")

            assert response.status_code == 200
            data = response.json()
            process_ids = [p["id"] for p in data.get("processes", [])]
            assert "ingestion" in process_ids, f"Expected 'ingestion' in {process_ids}"


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "processes")
@pytest.mark.enable_tasks()
async def test_gcp_job_runner_no_jobs(app_lifespan, base_url):
    """When no jobs are discovered, no Cloud Run processes appear in the list."""
    with patch(
        "dynastore.modules.gcp.tools.jobs.load_job_config",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = {}

        from dynastore.modules.gcp.gcp_runner import GcpJobRunner

        runner = GcpJobRunner()
        await runner.setup(app_lifespan)

        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            response = await client.get("/processes/processes?scope=all")
            assert response.status_code == 200
            data = response.json()
            process_ids = [p["id"] for p in data.get("processes", [])]
            assert "custom_cloud_task" not in process_ids
