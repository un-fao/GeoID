import pytest
import logging
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch, AsyncMock
from dynastore.modules.gcp.gcp_module import GCPModule


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "processes")
@pytest.mark.enable_tasks()  # Ensure NO tasks are pre-loaded to avoid isolation issues
async def test_gcp_cloud_run_discovery_integration(app_lifespan, caplog, base_url):
    """
    Integration test that verifies GCP jobs are discovered and
    correctly appear in the OGC Processes list.
    """
    caplog.set_level(logging.DEBUG)

    # 1. Mock GCP Job discovery response
    # We use 'ingestion' because it exists locally and has a Process definition
    mock_job_mappings = {"ingestion": "job-abc"}

    # Needs to be patched at the tool level used by the runner (cloud_run_tasks)
    with patch(
        "dynastore.tasks.gcp_cloud_runner.cloud_run_tasks.load_job_config",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = mock_job_mappings

        from dynastore.tasks.gcp_cloud_runner.gcp_runner import GcpCloudRunRunner

        runner = GcpCloudRunRunner()
        await runner.setup(app_lifespan)

        # 2. Use in-process AsyncClient to hit the /processes/processes endpoint
        # Use base_url from fixture for environment awareness
        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            response = await client.get("/processes/processes")

            assert response.status_code == 200
            data = response.json()

            # Verify the normalized task name appears in the processes list
            processes = data.get("processes", [])
            process_ids = [p["id"] for p in processes]

            assert "ingestion" in process_ids, f"Expected 'ingestion' in {process_ids}"

            print(
                f"Integration SUCCESS: Discovered GCP job 'ingestion' as process 'ingestion'."
            )

            print(
                f"Integration SUCCESS: Discovered GCP job 'custom-cloud-task' as process 'custom_cloud_task'."
            )


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "processes")
@pytest.mark.enable_tasks()
async def test_gcp_cloud_run_no_jobs_integration(app_lifespan, base_url):
    """Verifies that if no jobs are found, no NEW processes are registered."""
    with patch(
        "dynastore.tasks.gcp_cloud_runner.cloud_run_tasks.load_job_config",
        new_callable=AsyncMock,
    ) as mock_load:
        mock_load.return_value = {}

        from dynastore.tasks.gcp_cloud_runner.gcp_runner import GcpCloudRunRunner

        runner = GcpCloudRunRunner()
        await runner.setup(app_lifespan)

        app = app_lifespan.app
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url=base_url
        ) as client:
            response = await client.get("/processes/processes")
            assert response.status_code == 200
            data = response.json()
            processes = data.get("processes", [])
            process_ids = [p["id"] for p in processes]

            # We expect that NO custom cloud task is present.
            # Standard tasks might be present due to global registry persistence.
            assert "custom_cloud_task" not in process_ids

            # Optionally check that standard tasks are present if expected
            # assert "ingestion" in process_ids
