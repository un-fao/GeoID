#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import pytest
from tests.dynastore.test_utils import generate_test_id
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

    catalog_id = f"test_async_init_{generate_test_id()}"

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

    catalog_id = f"test_async_dest_{generate_test_id()}"

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
