#    Copyright 2025 FAO
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

# dynastore/modules/events/catalog_integration.py

import logging
from dynastore.modules import get_protocol
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.catalog.event_service import CatalogEventType, register_event_listener
from dynastore.models.protocols import CatalogsProtocol

from . import events_module

logger = logging.getLogger(__name__)

# --- Event Listener Implementations ---
# These are the "glue" functions. They are listeners that, when called
# by the catalog_module, turn around and call the generic `create_event` function.

async def on_catalog_creation(catalog_id: str, *args, **kwargs):
    """Listens for catalog creation and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id}
        await events_module.create_event(event_type=CatalogEventType.CATALOG_CREATION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.CATALOG_CREATION.value}: {e}", exc_info=True)

async def on_catalog_deletion(catalog_id: str, *args, **kwargs):
    """Listens for catalog soft deletion and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id}
        await events_module.create_event(event_type=CatalogEventType.CATALOG_DELETION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.CATALOG_DELETION.value}: {e}", exc_info=True)

async def on_catalog_hard_deletion(catalog_id: str, *args, **kwargs):
    """Listens for catalog hard deletion and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id}
        await events_module.create_event(event_type=CatalogEventType.CATALOG_HARD_DELETION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.CATALOG_HARD_DELETION.value}: {e}", exc_info=True)

async def on_collection_creation(catalog_id: str, collection_id: str, *args, **kwargs):
    """Listens for collection creation and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id, "collection_id": collection_id}
        await events_module.create_event(event_type=CatalogEventType.COLLECTION_CREATION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.COLLECTION_CREATION.value}: {e}", exc_info=True)

async def on_collection_deletion(catalog_id: str, collection_id: str, *args, **kwargs):
    """Listens for collection soft deletion and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id, "collection_id": collection_id}
        await events_module.create_event(event_type=CatalogEventType.COLLECTION_DELETION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.COLLECTION_DELETION.value}: {e}", exc_info=True)

async def on_collection_hard_deletion(catalog_id: str, collection_id: str, *args, **kwargs):
    """Listens for collection hard deletion and dispatches an event."""
    try:
        payload = {"catalog_id": catalog_id, "collection_id": collection_id}
        await events_module.create_event(event_type=CatalogEventType.COLLECTION_HARD_DELETION.value, payload=payload)
    except Exception as e:
        logger.error(f"Failed to dispatch event for {CatalogEventType.COLLECTION_HARD_DELETION.value}: {e}", exc_info=True)
        

def register_all_listeners():
    catalog_module = get_protocol(CatalogsProtocol)
    if not catalog_module:
        logger.warning("EventsModule: CatalogsProtocol not found. Cannot register listeners.")
        return

    logger.info("Registering catalog event listeners to generate persistent events...")
    
    register_event_listener(
        CatalogEventType.CATALOG_CREATION,
        on_catalog_creation
    )
    
    register_event_listener(
        CatalogEventType.CATALOG_DELETION,
        on_catalog_deletion
    )

    register_event_listener(
        CatalogEventType.CATALOG_HARD_DELETION,
        on_catalog_hard_deletion
    )
    
    register_event_listener(
        CatalogEventType.COLLECTION_CREATION,
        on_collection_creation
    )
    
    register_event_listener(
        CatalogEventType.COLLECTION_DELETION,
        on_collection_deletion
    )
    
    register_event_listener(
        CatalogEventType.COLLECTION_HARD_DELETION,
        on_collection_hard_deletion
    )