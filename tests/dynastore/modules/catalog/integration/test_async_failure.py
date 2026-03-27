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
    2. The catalog remains Soft Deleted in DB.
    3. The schema is NOT dropped.
    4. A CATALOG_HARD_DELETION_FAILURE event is emitted/logged.
    """

    catalog_id = "test_async_fail"

    # 0. Setup: Create Catalog
    from dynastore.modules.concurrency import await_all_background_tasks

    catalogs = get_protocol(CatalogsProtocol)
    # Use model_dump for dictionary input as expected by create_catalog
    catalog_def = Catalog(id=catalog_id, description="Test Async Fail").model_dump(
        exclude_none=True
    )
    # Use lang="*" because model_dump() produces localized dicts (e.g. {'en': '...'})
    await catalogs.create_catalog(catalog_def, lang="*")
    logger.info(f"Catalog {catalog_id} created successfully.")

    # 1. Register a failing lifecycle destroyer for the ASYNC phase
    from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

    async def failing_async_destroyer(schema, catalog_id, config_snapshot):
        if catalog_id == "test_async_fail":
            logger.info(f"Failing Async Destroyer Triggered for {catalog_id}!")
            raise RuntimeError("Async Cleanup Failed!")

    # Register via the registry
    lifecycle_registry.async_catalog_destroyer(failing_async_destroyer)

    try:
        # 2. Trigger Hard Deletion
        # This should return True immediately after soft-delete commit
        # The schema IS dropped synchronously, but the async cleanup (bucket removal etc) runs after.
        success = await catalogs.delete_catalog(catalog_id, force=True)
        assert success is True

        # 3. Wait for Async Task (with Retry)
        # Since it's fire-and-forget in the module, we need to wait a bit.
        await await_all_background_tasks()

        found_logs = False
        logs = []

        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        # Retry loop (max 5 seconds)
        import time

        start_time = time.time()

        while time.time() - start_time < 5:
            async with managed_transaction(app_lifespan.engine) as conn:
                # C. Check Event Store for Failure Event
                # Platform-scoped events go to the global events.events outbox
                logs = await DQLQuery(
                    """
                     SELECT event_id FROM events.events
                     WHERE event_type = 'catalog_hard_deletion_failure'
                       AND payload->'kwargs'->>'catalog_id' = :id
                     """,
                    result_handler=ResultHandler.ALL_DICTS,
                ).execute(conn, id=catalog_id)

            if len(logs) > 0:
                found_logs = True
                break
            await asyncio.sleep(0.5)

        assert found_logs, "Should have a failure log after waiting"
    finally:
        # Cleanup: Remove the failing destroyer
        lifecycle_registry.unregister_async_catalog_destroyer(failing_async_destroyer)
        pass
