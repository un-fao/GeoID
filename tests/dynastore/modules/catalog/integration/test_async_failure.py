import pytest
import asyncio
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.catalog.event_service import event_service, CatalogEventType
from dynastore.modules.catalog.models import Catalog
from dynastore.modules.catalog.log_manager import logger


@pytest.mark.asyncio
@pytest.mark.enable_extensions("logs")
async def test_async_hard_deletion_failure(app_lifespan):
    """
    Verify that if the async cleanup (Phase 2) fails:
    1. The function returns True (Soft delete successful).
    2. A CATALOG_HARD_DELETION_FAILURE event is emitted.
    """

    catalog_id = "test_async_fail"

    from dynastore.modules.concurrency import await_all_background_tasks

    catalogs = get_protocol(CatalogsProtocol)
    catalog_def = Catalog(id=catalog_id, description="Test Async Fail").model_dump(
        exclude_none=True
    )
    await catalogs.create_catalog(catalog_def, lang="*")
    logger.info(f"Catalog {catalog_id} created successfully.")

    # Register a failing lifecycle destroyer for the ASYNC phase
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

    async def failing_async_destroyer(catalog_id, context):
        if catalog_id == "test_async_fail":
            logger.info(f"Failing Async Destroyer Triggered for {catalog_id}!")
            raise RuntimeError("Async Cleanup Failed!")

    lifecycle_registry.async_catalog_destroyer(failing_async_destroyer)

    # Track whether the failure event was emitted via a sync listener
    failure_events = []

    async def _capture_failure(catalog_id: str, **kwargs):
        failure_events.append(catalog_id)

    event_service.sync_event_listener(
        CatalogEventType.CATALOG_HARD_DELETION_FAILURE
    )(_capture_failure)

    try:
        # Trigger Hard Deletion
        success = await catalogs.delete_catalog(catalog_id, force=True)
        assert success is True

        # Wait for background tasks (the async destroyer runs in background)
        await await_all_background_tasks()

        # The sync listener should have captured the failure event
        assert "test_async_fail" in failure_events, (
            "Should have received a CATALOG_HARD_DELETION_FAILURE event"
        )
    finally:
        lifecycle_registry.unregister_async_catalog_destroyer(failing_async_destroyer)
        # Clean up the listener
        listeners = event_service._sync_listeners.get(
            CatalogEventType.CATALOG_HARD_DELETION_FAILURE, []
        )
        if _capture_failure in listeners:
            listeners.remove(_capture_failure)
