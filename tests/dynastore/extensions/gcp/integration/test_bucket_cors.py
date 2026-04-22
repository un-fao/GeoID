import pytest
import logging
from unittest.mock import MagicMock, patch
from httpx import AsyncClient, ASGITransport
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules import get_protocol


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "configs")
async def test_bucket_config_hook_trigger(app_lifespan, catalog_obj):
    """
    Integration test to verify that PUTing a config via /configs/catalogs/{id}/plugins/{plugin_id}
    triggers the GCP side effect (GCPModule.apply_bucket_config).
    """
    catalog_id = catalog_obj.id
    app = app_lifespan.app

    # Mock GCPModule.apply_storage_config
    with patch(
        "dynastore.modules.gcp.gcp_module.GCPModule.apply_storage_config"
    ) as mock_apply:
        # Also need to mock the actual GCS client stuff to avoid real network calls
        with patch("google.cloud.storage.Client"):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as ac:
                # 1. Prepare CORS config
                cors_config = {
                    "cors": [
                        {
                            "origin": ["https://app.example.com"],
                            "method": ["GET", "OPTIONS"],
                        }
                    ]
                }

                # This should trigger the hook in ConfigService -> on_apply_gcp_bucket_config -> GCPModule.apply_storage_config
                response = await ac.put(
                    f"/configs/catalogs/{catalog_id}/classes/{GcpCatalogBucketConfig.class_key()}",
                    json=cors_config,
                )

                assert response.status_code == 200

                # 3. Verify that the hook was triggered
                mock_apply.assert_called_once()
                # Check that the config passed to apply_storage_config has the CORS settings
                called_config = mock_apply.call_args[0][1]
                assert len(called_config.cors) == 1
                assert called_config.cors[0].origin == ["https://app.example.com"]


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "gcp")
@pytest.mark.enable_extensions("gcp", "configs")
async def test_eventing_config_hook_trigger(app_lifespan, catalog_obj):
    """
    Verify that updating eventing config triggers the GCPModule.apply_eventing_config hook.
    """
    catalog_id = catalog_obj.id
    app = app_lifespan.app
    from dynastore.modules.gcp.gcp_config import GcpEventingConfig

    # Mock GCPModule.apply_eventing_config
    with patch(
        "dynastore.modules.gcp.gcp_module.GCPModule.apply_eventing_config"
    ) as mock_apply:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            eventing_config = {"managed_eventing": {"enabled": True}}

            response = await ac.put(
                f"/configs/catalogs/{catalog_id}/classes/{GcpEventingConfig.class_key()}",
                json=eventing_config,
            )

            assert response.status_code == 200
            mock_apply.assert_called_once()
