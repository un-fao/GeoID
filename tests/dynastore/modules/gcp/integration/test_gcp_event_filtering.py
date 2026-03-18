import pytest
import logging
import os
import asyncio

logger = logging.getLogger(__name__)
from dynastore.modules.gcp.gcp_module import GCPModule
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
    GCP_EVENTING_CONFIG_ID,
)
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from google.cloud import storage


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks", "gcp")
@pytest.mark.enable_tasks("gcp_provision")
async def test_gcp_event_filtering_multiple_prefixes(app_lifespan, monkeypatch):
    """
    Verifies that Managed Eventing creates multiple GCS notifications for the configured prefixes.
    """
    import uuid

    catalog_id = f"test_event_multi_{uuid.uuid4().hex[:12]}"
    from dynastore.models.protocols import StorageProtocol, EventingProtocol, ConfigsProtocol
    from dynastore.modules import get_protocol
    gcp_module = get_protocol(StorageProtocol)
    if not gcp_module:
        pytest.skip("GCPModule (StorageProtocol) not initialized.")

    # Ensure provisioning is allowed for this integration test
    monkeypatch.setenv("DYNASTORE_GCP_FORCE_PROVISIONING", "true")

    # Provide a dummy push endpoint so Pub/Sub subscriptions can be created
    # locally (the test only verifies GCS notification prefixes, not delivery).
    if not os.getenv("K_SERVICE") and not os.getenv("SERVICE_URL"):
        monkeypatch.setenv("SERVICE_URL", "http://localhost:8080")

    # Override the conftest default: enable eventing for this test
    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GCP_EVENTING_CONFIG_ID,
            GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True)),
        )

    # 1. Create the catalog in the database first (mandatory for linkage)
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    catalog_def = {
        "id": catalog_id,
        "title": {"en": "Test Event Multi"},
        "description": {"en": "Test Event Multi Description"},
        "conformsTo": [],
        "links": []
    }
    await catalogs.create_catalog(catalog_def, lang="*")

    # 2. Ensure catalog GCP setup matches expectations
    # triggers background setup, but for reliable verification in this test
    # we call setup_catalog_gcp_resources directly to get the updated config.
    await gcp_module.prepare_upload_target(catalog_id)

    # Wait for background initialization to complete (the default one)
    await lifecycle_registry.wait_for_all_tasks()

    # Trigger/Verify resource setup
    bucket_name, updated_config = await gcp_module.setup_catalog_gcp_resources(
        catalog_id
    )

    assert bucket_name is not None
    assert updated_config is not None

    managed_config = updated_config.managed_eventing

    assert managed_config is not None
    assert len(managed_config.blob_name_prefixes) == 2
    assert "catalog/" in managed_config.blob_name_prefixes
    assert "collections/" in managed_config.blob_name_prefixes
    assert len(managed_config.gcs_notification_ids) == 2

    # 2. Verify notifications on the bucket
    storage_client = gcp_module.get_storage_client()

    # Handle GCS eventual consistency (404 NotFound can occur briefly between clients)
    import time
    from google.api_core.exceptions import NotFound

    bucket = None
    max_retries = 20
    retry_interval = 1.0
    for i in range(max_retries):
        try:
            bucket = storage_client.get_bucket(bucket_name)
            break
        except NotFound:
            if i == max_retries - 1:
                raise
            logger.info(f"Test: Bucket {bucket_name} not yet visible, retrying... ({i+1}/{max_retries})")
            time.sleep(retry_interval)

    notifications = list(bucket.list_notifications())

    prefixes_found = [
        n.blob_name_prefix
        for n in notifications
        if n.notification_id in managed_config.gcs_notification_ids
    ]
    assert "catalog/" in prefixes_found
    assert "collections/" in prefixes_found

    print(f"Verified multiple GCS notifications for prefixes: {prefixes_found}")


@pytest.mark.gcp
@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks", "gcp")
@pytest.mark.enable_tasks("gcp_provision")
async def test_gcp_event_filtering_custom_prefixes(app_lifespan, monkeypatch):
    """
    Verifies that Managed Eventing creates GCS notifications for custom configured prefixes.
    """
    import uuid

    catalog_id = f"test_event_custom_{uuid.uuid4().hex[:12]}"
    from dynastore.models.protocols import StorageProtocol, ConfigsProtocol
    from dynastore.modules import get_protocol
    gcp_module = get_protocol(StorageProtocol)
    if not gcp_module:
        pytest.skip("GCPModule (StorageProtocol) not initialized.")

    # Ensure provisioning is allowed for this integration test
    monkeypatch.setenv("DYNASTORE_GCP_FORCE_PROVISIONING", "true")

    # Provide a dummy push endpoint so Pub/Sub subscriptions can be created
    # locally (the test only verifies GCS notification prefixes, not delivery).
    if not os.getenv("K_SERVICE") and not os.getenv("SERVICE_URL"):
        monkeypatch.setenv("SERVICE_URL", "http://localhost:8080")

    # Override the conftest default: enable eventing for this test
    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GCP_EVENTING_CONFIG_ID,
            GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True)),
        )

    # 1. Create the catalog in the database first (mandatory for linkage)
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    catalog_def = {
        "id": catalog_id,
        "title": {"en": "Test Event Custom"},
        "description": {"en": "Test Event Custom Description"},
        "conformsTo": [],
        "links": []
    }
    await catalogs.create_catalog(catalog_def, lang="*")

    # 2. Ensure catalog GCP setup exists (triggers background setup)
    await gcp_module.prepare_upload_target(catalog_id)

    # Wait for default setup to complete
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
    await lifecycle_registry.wait_for_all_tasks()

    # Configure custom prefixes
    custom_prefixes = ["data/input/", "data/output/"]
    eventing_config = GcpEventingConfig(
        managed_eventing=ManagedBucketEventing(
            enabled=True, blob_name_prefixes=custom_prefixes
        )
    )

    from dynastore.modules.db_config.query_executor import managed_transaction

    async with managed_transaction(gcp_module.engine) as conn:
        await gcp_module.get_config_service().set_config(
            GCP_EVENTING_CONFIG_ID,
            eventing_config,
            catalog_id=catalog_id,
            db_resource=conn,
        )

    # Trigger resource update with new config
    bucket_name, updated_config = await gcp_module.setup_catalog_gcp_resources(
        catalog_id
    )
    managed_config = updated_config.managed_eventing

    assert managed_config is not None
    assert managed_config.blob_name_prefixes == custom_prefixes
    assert len(managed_config.gcs_notification_ids) == 2

    # Verify notifications on the bucket
    storage_client = gcp_module.get_storage_client()

    # Handle GCS eventual consistency (404 NotFound can occur briefly between clients)
    import time
    from google.api_core.exceptions import NotFound

    bucket = None
    max_retries = 20
    retry_interval = 1.0
    for i in range(max_retries):
        try:
            bucket = storage_client.get_bucket(bucket_name)
            break
        except NotFound:
            if i == max_retries - 1:
                raise
            logger.info(f"Test: Bucket {bucket_name} not yet visible, retrying... ({i+1}/{max_retries})")
            time.sleep(retry_interval)

    notifications = list(bucket.list_notifications())

    prefixes_found = [
        n.blob_name_prefix
        for n in notifications
        if n.notification_id in managed_config.gcs_notification_ids
    ]
    for p in custom_prefixes:
        assert p in prefixes_found

    print(f"Verified custom GCS notifications for prefixes: {prefixes_found}")
