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

import os
import logging
import asyncio
from typing import Dict, Any, Optional, List, Callable, Coroutine, Union
from contextlib import asynccontextmanager
from enum import Enum

from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.tools.protocol_helpers import resolve, get_engine
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import ConfigsProtocol, PropertiesProtocol, DatabaseProtocol, EventsProtocol
from dynastore.models.shared_models import EventType
from .models import (
    EventSubscription,
    EventSubscriptionCreate,
    AuthMethod,
    AuthConfigAPIKey,
    API_KEY_NAME,
)
from .primitives import (
    EventScope,
    EventRegistry,
    define_event,
    SystemEventType,
)
from . import catalog_integration

logger = logging.getLogger(__name__)

SUBSCRIPTIONS_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS "platform";
CREATE TABLE IF NOT EXISTS platform.event_subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscriber_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    webhook_url VARCHAR(2048) NOT NULL,
    auth_config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_name, event_type)
);
"""
SUBSCRIPTIONS_SCHEMA_INDEX = """
CREATE INDEX IF NOT EXISTS idx_event_subscriptions_event_type
ON platform.event_subscriptions (event_type);
"""

PLATFORM_API_KEY = os.getenv(API_KEY_NAME)

# --- Internal Query Objects ---
_upsert_subscription_query = DQLQuery(
    """
    INSERT INTO platform.event_subscriptions
        (subscriber_name, event_type, webhook_url, auth_config)
    VALUES
        (:subscriber_name, :event_type, :webhook_url, :auth_config)
    ON CONFLICT (subscriber_name, event_type) DO UPDATE SET
        webhook_url = EXCLUDED.webhook_url,
        auth_config = EXCLUDED.auth_config
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT
)
_get_subscriptions_for_event_query = DQLQuery(
    "SELECT * FROM platform.event_subscriptions WHERE event_type = :event_type;",
    result_handler=ResultHandler.ALL_DICTS
)
_delete_subscription_query = DQLQuery(
    "DELETE FROM platform.event_subscriptions WHERE subscriber_name = :subscriber_name AND event_type = :event_type RETURNING *;",
    result_handler=ResultHandler.ONE_OR_NONE
)
class EventsModule(ModuleProtocol):
    priority: int = 100
    """
    Manages event subscriptions and defines the event dispatching logic.
    This is a Pillar I module that owns the `event_subscriptions` table.
    """
    
    def __init__(self, app_state: object):
        self.engine: Optional[DbResource] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.tools.protocol_helpers import get_engine
        try:
            self.engine = get_engine()
        except RuntimeError as e:
            logger.critical(f"EventsModule cannot initialize: {e}")
            yield; return

        logger.info("EventsModule: Initializing subscriptions schema...")
        try:
            # 1. Create the subscriptions table
            async with managed_transaction(self.engine) as conn:
                await DDLQuery(SUBSCRIPTIONS_SCHEMA).execute(conn)
                await DDLQuery(SUBSCRIPTIONS_SCHEMA_INDEX).execute(conn)

            # This key MUST be set in the environment for production.
            # It is a shared secret between all services in the platform.

            global PLATFORM_API_KEY
            if not PLATFORM_API_KEY:
                try:
                    props = resolve(PropertiesProtocol)
                    persisted_key = await props.get_property(API_KEY_NAME)
                    if persisted_key:
                        PLATFORM_API_KEY = persisted_key
                        logger.info(f"Loaded '{API_KEY_NAME}' from database.")
                    else:
                        logger.warning(f"!!! SECURITY WARNING !!! '{API_KEY_NAME}' is not set.")
                        import secrets
                        PLATFORM_API_KEY = secrets.token_hex(32)
                        logger.info(f"Persisting '{API_KEY_NAME}' to database for the first time.")
                        await props.set_property(API_KEY_NAME, PLATFORM_API_KEY, "system")
                except RuntimeError as e:
                    logger.warning(f"PropertiesProtocol not available: {e}. Cannot load/persist '{API_KEY_NAME}'.")

            # 2. Register listeners to defer tasks, if the catalog module is enabled.
            # We use discovery to check if catalog is loaded
            from dynastore.models.protocols import CatalogsProtocol
            if get_protocol(CatalogsProtocol):
                try:
                    catalog_integration.register_all_listeners()
                    logger.info("EventsModule: Successfully registered catalog listeners.")
                except Exception as e:
                    logger.error(f"EventsModule: Failed to register catalog listeners: {e}", exc_info=True)
            else:
                logger.info("EventsModule: 'catalog' module not loaded. Skipping registration of catalog event listeners.")

            logger.info("EventsModule: Initialization complete. Event bus is active.")
        except Exception as e:
            logger.error(f"CRITICAL: EventsModule initialization failed: {e}", exc_info=True)

        yield
        logger.info("EventsModule: Shutdown complete.")

    async def create_event(self, event_type: str, payload: Dict[str, Any]):
        """
        Creates an event by enqueuing an internal dispatch task.
        """
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.tasks import tasks_module
        from dynastore.modules.tasks.models import TaskCreate

        db = get_protocol(DatabaseProtocol)
        if not db:
            logger.error("EventsModule: DatabaseProtocol not found. Cannot dispatch event.")
            return

        try:
            await tasks_module.create_task(
                engine=db,
                task_data=TaskCreate(
                    caller_id="system:events",
                    task_type="event_dispatch",
                    inputs={
                        "event_type": event_type,
                        "payload": payload
                    }
                ),
                schema=tasks_module.get_task_schema()
            )
            logger.debug(f"EventsModule: Dispatched event_dispatch task for '{event_type}'")
        except Exception as e:
            logger.error(f"EventsModule: Failed to dispatch event task for '{event_type}': {e}")

    async def subscribe(self, subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None) -> EventSubscription:
        """Creates or updates a webhook subscription."""
        db_engine = engine or self.engine or get_engine()

        async with managed_transaction(db_engine) as conn:
            sub_dict = await _upsert_subscription_query.execute(
                conn,
                subscriber_name=subscription_data.subscriber_name,
                event_type=subscription_data.event_type,
                webhook_url=str(subscription_data.webhook_url),
                auth_config=subscription_data.auth_config.model_dump_json()
            )
        logger.info(f"Subscription registered for '{subscription_data.subscriber_name}' on event '{subscription_data.event_type}'")
        return EventSubscription.model_validate(sub_dict)

    async def unsubscribe(self, subscriber_name: str, event_type: str, engine: Optional[DbResource] = None) -> Optional[EventSubscription]:
        """Deletes a webhook subscription."""
        db_engine = engine or self.engine or get_engine()

        async with managed_transaction(db_engine) as conn:
            sub_dict = await _delete_subscription_query.execute(
                conn,
                subscriber_name=subscriber_name,
                event_type=event_type
            )
        if sub_dict:
            logger.info(f"Subscription removed for '{subscriber_name}' on event '{event_type}'")
            return EventSubscription.model_validate(sub_dict)
        return None

    async def get_subscriptions_for_event_type(self, event_type: str, engine: Optional[DbResource] = None) -> List[EventSubscription]:
        """Gets all subscribers for a given event type."""
        db_engine = engine or self.engine or get_engine()

        async with managed_transaction(db_engine) as conn:
            sub_dicts = await _get_subscriptions_for_event_query.execute(conn, event_type=event_type)
            return [EventSubscription.model_validate(s) for s in sub_dicts]

# --- Module-level wrappers for compatibility ---
async def create_event(event_type: str, payload: Dict[str, Any]):
    events = get_protocol(EventsProtocol)
    if events:
        await events.create_event(event_type, payload)

async def subscribe(subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None) -> EventSubscription:
    events = get_protocol(EventsProtocol)
    return await events.subscribe(subscription_data, engine)

async def unsubscribe(subscriber_name: str, event_type: str, engine: Optional[DbResource] = None) -> Optional[EventSubscription]:
    events = get_protocol(EventsProtocol)
    return await events.unsubscribe(subscriber_name, event_type, engine)

async def get_subscriptions_for_event_type(event_type: str, engine: Optional[DbResource] = None) -> List[EventSubscription]:
    events = get_protocol(EventsProtocol)
    return await events.get_subscriptions_for_event_type(event_type, engine)
