import pytest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.catalog.models import Catalog
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry


class TrackingHandler:
    def __init__(self):
        self.init_calls = []
        self.destroy_calls = []

    async def on_init(self, catalog_id, context):
        self.init_calls.append(catalog_id)

    async def on_destroy(self, catalog_id, context):
        self.destroy_calls.append(catalog_id)


@pytest.fixture
def tracker():
    handler = TrackingHandler()
    # Register real handlers
    lifecycle_registry.async_catalog_initializer(handler.on_init)
    lifecycle_registry.async_catalog_destroyer(handler.on_destroy)
    yield handler
    
    # Targeted cleanup: remove ONLY our test handlers to avoid poisoning other tests
    # while preserving core system initializers (like Table Creation)
    lifecycle_registry.unregister_async_catalog_initializer(handler.on_init)
    lifecycle_registry.unregister_async_catalog_destroyer(handler.on_destroy)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.enable_extensions("stac")
@pytest.mark.asyncio
async def test_create_catalog_triggers_async_init(app_lifespan, tracker):
    """
    Verifies that creating a catalog triggers the async_catalog_initializer hook.
    """
    import uuid

    catalog_id = f"test_async_init_{uuid.uuid4().hex[:8]}"

    catalogs = get_protocol(CatalogsProtocol)
    cat = Catalog(id=catalog_id, title={"en": "Test Async Init"})
    await catalogs.create_catalog(cat.model_dump(), lang="*")

    # Wait for background tasks if any

    await lifecycle_registry.wait_for_all_tasks()

    # Verify hook was called
    assert catalog_id in tracker.init_calls


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.enable_extensions("stac")
@pytest.mark.asyncio
async def test_hard_delete_catalog_triggers_async_destroy(app_lifespan, tracker):
    """
    Verifies that hard deleting a catalog triggers the async_catalog_destroyer hook.
    """
    import uuid

    catalog_id = f"test_async_dest_{uuid.uuid4().hex[:8]}"

    catalogs = get_protocol(CatalogsProtocol)
    # Setup: Create catalog first
    cat = Catalog(id=catalog_id, title={"en": "Test Async Destroy"})
    await catalogs.create_catalog(cat.model_dump(), lang="*")
    await lifecycle_registry.wait_for_all_tasks()

    # Perform hard delete
    await catalogs.delete_catalog(catalog_id, force=True)

    # Wait for background handlers (they are triggered via events/lifecycle)
    await lifecycle_registry.wait_for_all_tasks()

    # Verify hook was called
    assert catalog_id in tracker.destroy_calls
