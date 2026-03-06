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

# dynastore/modules/events/events_module.py
import logging
from uuid import UUID
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
import asyncio
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.modules.db_config.query_executor import (DDLQuery, DQLQuery, managed_transaction, ResultHandler, DbResource)
from dynastore.tools.protocol_helpers import get_engine, resolve
from .models import (EventSubscription, EventSubscriptionCreate, AuthMethod, AuthConfigAPIKey)
from . import catalog_integration
from dynastore.models.protocols import PropertiesProtocol, ConfigsProtocol
import os
from dynastore.modules.events.models import API_KEY_NAME

logger = logging.getLogger(__name__)

# --- Decoupled Event Creation ---
# This section implements an inversion of control pattern. The events module
# defines the *interface* for creating an event, but the *implementation* is
# injected by another module (in this case, the procrastinate module) at runtime.

_event_creator_func = None

# def register_event_creator(func: callable):
#     """Allows another module to register its event creation implementation."""
#     global _event_creator_func
#     logger.info(f"Registering event creator function: {func.__name__}")
#     _event_creator_func = func

SUBSCRIPTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_subscriptions (
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
ON event_subscriptions (event_type);
"""

PLATFORM_API_KEY = os.getenv(API_KEY_NAME)

async def create_event(event_type: str, payload: Dict[str, Any]):
    from dynastore.modules.procrastinate.tools import  procrastinate_event
    return await procrastinate_event(app, event_type=event_type, payload=payload)

from procrastinate import App
app: App = None
# --- Module Implementation ---

class EventsModule(ModuleProtocol):
    priority: int = 100
    """
    Manages event subscriptions and defines the event dispatching logic.
    This is a Pillar I module that owns the `event_subscriptions` table.
    The background worker logic is implemented as a Procrastinate task.
    """
    
    def __init__(self, app_state: object):
        self.engine: Optional[DbResource] = None
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._leader_lock_conn: Optional[Any] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        try:
            self.engine = get_engine()
        except RuntimeError as e:
            logger.critical(f"EventsModule cannot initialize: {e}")
            yield; return

        # Procrastinate application and connection pool
        global app
        from dynastore.modules.procrastinate.module import get_app
        app, db_config = get_app(resolve(ConfigsProtocol).db_config)
        
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

            # 2. Register listeners to defer Procrastinate tasks, if the catalog module is enabled.
            enabled_modules_str = os.getenv("DYNASTORE_MODULES", "")
            enabled_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]

            if "catalog" in enabled_modules:
                try:
                    catalog_integration.register_all_listeners()
                    logger.info("EventsModule: Successfully registered catalog listeners because 'catalog' module is enabled.")
                except Exception as e:
                    logger.error(f"EventsModule: Failed to register catalog listeners: {e}", exc_info=True)
            else:
                logger.info("EventsModule: 'catalog' module not enabled. Skipping registration of catalog event listeners.")

            logger.info("EventsModule: Initialization complete. Event bus is active.")
        except Exception as e:
            logger.error(f"CRITICAL: EventsModule initialization failed: {e}", exc_info=True)

        yield
        logger.info("EventsModule: Shutdown complete.")


# --- Internal Query Objects (Merged) ---

# ... Subscription Queries ...
_upsert_subscription_query = DQLQuery(
    """
    INSERT INTO event_subscriptions
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
    "SELECT * FROM event_subscriptions WHERE event_type = :event_type;",
    result_handler=ResultHandler.ALL_DICTS
)
_delete_subscription_query = DQLQuery(
    "DELETE FROM event_subscriptions WHERE subscriber_name = :subscriber_name AND event_type = :event_type RETURNING *;",
    result_handler=ResultHandler.ONE_OR_NONE
)


# --- Public API Functions (Merged) ---

# ... Subscription Functions ...
async def subscribe(subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None) -> EventSubscription:
    """Creates or updates a webhook subscription."""
    db_engine = engine
    if db_engine is None:
        db_engine = get_engine()

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

async def unsubscribe(subscriber_name: str, event_type: str, engine: Optional[DbResource] = None) -> Optional[EventSubscription]:
    """Deletes a webhook subscription."""
    db_engine = engine
    if db_engine is None:
        db_engine = get_engine()

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

async def get_subscriptions_for_event_type(event_type: str, engine: Optional[DbResource] = None) -> List[EventSubscription]:
    """Gets all subscribers for a given event type."""
    db_engine = engine
    if db_engine is None:
        db_engine = get_engine()

    async with managed_transaction(db_engine) as conn:
        sub_dicts = await _get_subscriptions_for_event_query.execute(conn, event_type=event_type)
        return [EventSubscription.model_validate(s) for s in sub_dicts]
