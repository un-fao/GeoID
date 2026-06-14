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

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, FrozenSet, List, Optional

import orjson
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.tools.protocol_helpers import resolve
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DbEngine,
    managed_transaction,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import (
    PropertiesProtocol,
    EventDriverProtocol,
)
from dynastore.models.protocols.event_driver import (
    AccumulationPolicy,
    DeliveryMode,
    EventDriverCapability,
)
from .models import (
    EventSubscription,
    EventSubscriptionCreate,
    API_KEY_NAME,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subscription table DDL
# ---------------------------------------------------------------------------

_EVENTS_SCHEMA = os.getenv("DYNASTORE_EVENTS_SCHEMA", "events")

SUBSCRIPTIONS_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS "{_EVENTS_SCHEMA}";
CREATE TABLE IF NOT EXISTS {_EVENTS_SCHEMA}.event_subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscriber_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    webhook_url VARCHAR(2048) NOT NULL,
    auth_config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_name, event_type)
);
"""
SUBSCRIPTIONS_SCHEMA_INDEX = f"""
CREATE INDEX IF NOT EXISTS idx_event_subscriptions_event_type
ON {_EVENTS_SCHEMA}.event_subscriptions (event_type);
"""

PLATFORM_API_KEY = os.getenv(API_KEY_NAME)

# ---------------------------------------------------------------------------
# Internal query objects (subscriptions)
# ---------------------------------------------------------------------------

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
    result_handler=ResultHandler.ONE_DICT,
)
_get_subscriptions_for_event_query = DQLQuery(
    "SELECT * FROM platform.event_subscriptions WHERE event_type = :event_type;",
    result_handler=ResultHandler.ALL_DICTS,
)
_delete_subscription_query = DQLQuery(
    "DELETE FROM platform.event_subscriptions "
    "WHERE subscriber_name = :subscriber_name AND event_type = :event_type RETURNING *;",
    result_handler=ResultHandler.ONE_OR_NONE,
)

# ---------------------------------------------------------------------------
# Internal event store constants
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
#: Public alias for the maximum event retry count.  Consumed by the
#: maintenance supervisor to keep the stuck-event reaper threshold in sync
#: with the accumulation-policy value set here.
MAX_RETRIES: int = _MAX_RETRIES


# ---------------------------------------------------------------------------
# Catalog event listeners (inlined from catalog_integration.py)
# ---------------------------------------------------------------------------

async def _on_catalog_creation(catalog_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="catalog_creation",
            payload={"catalog_id": catalog_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event catalog_creation: %s", e, exc_info=True)


async def _on_catalog_deletion(catalog_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="catalog_deletion",
            payload={"catalog_id": catalog_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event catalog_deletion: %s", e, exc_info=True)


async def _on_catalog_hard_deletion(catalog_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="catalog_hard_deletion",
            payload={"catalog_id": catalog_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event catalog_hard_deletion: %s", e, exc_info=True)


async def _on_collection_creation(catalog_id: str, collection_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="collection_creation",
            payload={"catalog_id": catalog_id, "collection_id": collection_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event collection_creation: %s", e, exc_info=True)


async def _on_collection_deletion(catalog_id: str, collection_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="collection_deletion",
            payload={"catalog_id": catalog_id, "collection_id": collection_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event collection_deletion: %s", e, exc_info=True)


async def _on_collection_hard_deletion(catalog_id: str, collection_id: str, *args, **kwargs):
    try:
        await _module_publish(
            event_type="collection_hard_deletion",
            payload={"catalog_id": catalog_id, "collection_id": collection_id},
        )
    except Exception as e:
        logger.error("Failed to dispatch event collection_hard_deletion: %s", e, exc_info=True)


async def _module_publish(event_type: str, payload: Dict[str, Any]) -> None:
    """Publish to the global outbox via the module instance."""
    driver = get_protocol(EventDriverProtocol)
    if driver:
        await driver.publish(
            event_type=event_type,
            payload=payload,
            scope="PLATFORM",
            catalog_id=payload.get("catalog_id"),
        )


def register_catalog_listeners() -> None:
    """Register EventsModule's lifecycle → outbox listeners.

    Other modules extend the bus via register_event_listener() in their own
    lifespan.  This function is intentionally separate so the GCP module (and
    future modules) can register additional catalog-event listeners without
    coupling to catalog_integration.py.
    """
    from dynastore.modules.catalog.event_service import (
        register_event_listener,
        CatalogEventType,
    )

    register_event_listener(CatalogEventType.CATALOG_CREATION, _on_catalog_creation)
    register_event_listener(CatalogEventType.CATALOG_DELETION, _on_catalog_deletion)
    register_event_listener(CatalogEventType.CATALOG_HARD_DELETION, _on_catalog_hard_deletion)
    register_event_listener(CatalogEventType.COLLECTION_CREATION, _on_collection_creation)
    register_event_listener(CatalogEventType.COLLECTION_DELETION, _on_collection_deletion)
    register_event_listener(CatalogEventType.COLLECTION_HARD_DELETION, _on_collection_hard_deletion)
    logger.info("EventsModule: Registered catalog event listeners.")


# ---------------------------------------------------------------------------
# EventsModule
# ---------------------------------------------------------------------------


class EventsModule(ModuleProtocol):
    """
    Owns webhook subscription storage and provides the EventDriverProtocol.

    Responsibilities:
    - Manage webhook subscriptions (platform.event_subscriptions)
    - Implement publish / search_events / wait_for_events; events are written
      to ``tasks.events`` (the WorkClass global hot plane) and drained by
      the control-plane EventDrainTask — not an in-module loop
    - Register catalog lifecycle listeners

    The legacy ``events.events`` global outbox and its DDL, shard partitions,
    backlog monitor, and in-process consumer have been removed.  All event
    writes go directly to ``tasks.events`` via ``emit_event_row``.

    Priority 11: starts after DBService (10), before TasksModule (15) and CatalogModule (20).
    """

    priority: int = 11

    def __init__(self, app_state: object):
        self._engine: Optional[DbEngine] = None

    # ------------------------------------------------------------------
    # EventDriverProtocol — capability declaration
    # ------------------------------------------------------------------

    @property
    def capabilities(self) -> FrozenSet[str]:
        return frozenset({
            EventDriverCapability.PERSISTENCE,
            EventDriverCapability.NOTIFICATION,
            EventDriverCapability.SUBSCRIBE,
            EventDriverCapability.DEAD_LETTER,
        })

    def has_capability(self, cap: str) -> bool:
        return cap in self.capabilities

    @property
    def delivery_mode(self) -> str:
        return DeliveryMode.AT_LEAST_ONCE

    @property
    def accumulation_policy(self) -> AccumulationPolicy:
        return AccumulationPolicy(
            retention_days=int(os.getenv("EVENT_RETENTION_DAYS", "7")),
            dead_letter_days=int(os.getenv("GLOBAL_EVENT_RETENTION_DAYS", "30")),
            max_retries=_MAX_RETRIES,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        from dynastore.tools.protocol_helpers import get_engine
        try:
            self._engine = get_engine()
        except RuntimeError as e:
            logger.critical("EventsModule cannot initialise: %s", e)
            yield
            return

        # Create webhook subscriptions table
        async with managed_transaction(self._engine) as conn:
            from dynastore.modules.db_config.locking_tools import check_table_exists
            if not await check_table_exists(conn, "event_subscriptions", _EVENTS_SCHEMA):
                await DDLQuery(SUBSCRIPTIONS_SCHEMA).execute(conn)
                await DDLQuery(SUBSCRIPTIONS_SCHEMA_INDEX).execute(conn)

        # Load / generate platform API key
        global PLATFORM_API_KEY
        if not PLATFORM_API_KEY:
            try:
                props = resolve(PropertiesProtocol)
                persisted_key = await props.get_property(API_KEY_NAME)
                if persisted_key:
                    PLATFORM_API_KEY = persisted_key
                    logger.info("Loaded '%s' from database.", API_KEY_NAME)
                else:
                    import secrets
                    PLATFORM_API_KEY = secrets.token_hex(32)
                    logger.warning(
                        "!!! SECURITY WARNING !!! '%s' is not set. Generating ephemeral key.",
                        API_KEY_NAME,
                    )
                    await props.set_property(API_KEY_NAME, PLATFORM_API_KEY, "system")
            except RuntimeError as e:
                logger.warning(
                    "PropertiesProtocol not available: %s. Cannot load '%s'.", e, API_KEY_NAME
                )

        # Register catalog integration listeners (deferred until CatalogsProtocol is present)
        from dynastore.models.protocols import CatalogsProtocol
        if get_protocol(CatalogsProtocol):
            try:
                register_catalog_listeners()
            except Exception:
                logger.exception("EventsModule: Failed to register catalog listeners.")
        else:
            logger.info(
                "EventsModule: CatalogsProtocol not loaded — skipping catalog listeners."
            )

        logger.info("EventsModule: Initialisation complete. Event storage is active.")

        try:
            yield
        finally:
            logger.info("EventsModule: Shutdown complete.")

    # ------------------------------------------------------------------
    # EventDriverProtocol — DDL lifecycle
    # ------------------------------------------------------------------

    async def initialize(self, conn: Any) -> None:
        """No-op: tasks.events DDL is owned by the tasks module."""
        pass

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """No-op. The global partitioned tasks.events outbox serves all catalogs."""
        pass

    async def init_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """No-op. The global partitioned tasks.events outbox serves all collections."""
        pass

    async def drop_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """No-op. The global outbox does not maintain per-collection partitions."""
        pass

    # ------------------------------------------------------------------
    # EventDriverProtocol — produce
    # ------------------------------------------------------------------

    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        scope: str = "PLATFORM",
        schema_name: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> str:
        """Insert an event into tasks.events. Returns event_id."""
        from dynastore.modules.events.events_emit import (  # noqa: PLC0415
            emit_event_row,
        )

        async def _run(conn: Any) -> str:
            payload_str = orjson.dumps(payload).decode()
            # Compute shard value in Python to avoid asyncpg type inference conflicts
            shard_key = catalog_id or "PLATFORM"
            shard = abs(hash(shard_key)) % 16
            return await emit_event_row(
                conn,
                event_type=event_type,
                scope=scope,
                schema_name=schema_name,
                catalog_id=catalog_id,
                collection_id=collection_id,
                identity_id=identity_id,
                payload_str=payload_str,
                shard=shard,
            )

        if db_resource is not None:
            return await _run(db_resource)

        from dynastore.tools.protocol_helpers import get_engine
        engine = self._engine or get_engine()
        async with managed_transaction(engine) as conn:
            return await _run(conn)

    async def search_events(
        self,
        engine: Any,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        identity_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Search for events in tasks.events (the active event store)."""
        from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415

        task_schema = get_task_schema()
        async with managed_transaction(engine) as conn:
            clauses = []
            params: Dict[str, Any] = {"limit": limit, "offset": offset}

            if catalog_id and catalog_id != "_system_":
                clauses.append("schema_name = :schema_name")
                params["schema_name"] = catalog_id
            if event_type:
                clauses.append("event_type = :event_type")
                params["event_type"] = event_type

            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            sql = (
                f"SELECT event_id::text as id, event_type, schema_name, "
                f"scope, payload, created_at, status "
                f"FROM {task_schema}.events {where} "
                f"ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
            )
            try:
                rows = await DQLQuery(
                    sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, **params)
                return rows or []
            except Exception as e:
                logger.debug("Event search failed: %s", e)
                return []

    # ------------------------------------------------------------------
    # EventDriverProtocol — consumer notification
    # ------------------------------------------------------------------

    async def wait_for_events(self, timeout: float = 10.0) -> None:
        """Wait until an event signal arrives or *timeout* seconds elapse."""
        from dynastore.tools.async_utils import signal_bus
        await signal_bus.wait_for("dynastore_events_channel", timeout=timeout)

    # ------------------------------------------------------------------
    # EventsModule — create_event (top-level API)
    # ------------------------------------------------------------------

    async def create_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Publish an event to tasks.events."""
        try:
            await self.publish(event_type=event_type, payload=payload, scope="PLATFORM")
            logger.debug("EventsModule: published event '%s'.", event_type)
        except Exception:
            logger.exception("EventsModule: Failed to publish event '%s'.", event_type)

    # ------------------------------------------------------------------
    # Webhook subscription management
    # ------------------------------------------------------------------

    async def subscribe(
        self, subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None
    ) -> EventSubscription:
        """Create or update a webhook subscription."""
        db_engine = engine or self._engine
        async with managed_transaction(db_engine) as conn:
            sub_dict = await _upsert_subscription_query.execute(
                conn,
                subscriber_name=subscription_data.subscriber_name,
                event_type=subscription_data.event_type,
                webhook_url=str(subscription_data.webhook_url),
                auth_config=subscription_data.auth_config.model_dump_json(),
            )
        logger.info(
            "Subscription registered for '%s' on event '%s'.",
            subscription_data.subscriber_name,
            subscription_data.event_type,
        )
        return EventSubscription.model_validate(sub_dict)

    async def unsubscribe(
        self,
        subscriber_name: str,
        event_type: str,
        engine: Optional[DbResource] = None,
    ) -> Optional[EventSubscription]:
        """Delete a webhook subscription."""
        db_engine = engine or self._engine
        async with managed_transaction(db_engine) as conn:
            sub_dict = await _delete_subscription_query.execute(
                conn, subscriber_name=subscriber_name, event_type=event_type
            )
        if sub_dict:
            logger.info(
                "Subscription removed for '%s' on event '%s'.", subscriber_name, event_type
            )
            return EventSubscription.model_validate(sub_dict)
        return None

    async def get_subscriptions_for_event_type(
        self, event_type: str, engine: Optional[DbResource] = None
    ) -> List[EventSubscription]:
        """Return all webhook subscribers for an event type."""
        db_engine = engine or self._engine
        async with managed_transaction(db_engine) as conn:
            sub_dicts = await _get_subscriptions_for_event_query.execute(
                conn, event_type=event_type
            )
        return [EventSubscription.model_validate(s) for s in sub_dicts]


# ---------------------------------------------------------------------------
# Module-level convenience wrappers (backward compat)
# ---------------------------------------------------------------------------

async def create_event(event_type: str, payload: Dict[str, Any]) -> None:
    driver = get_protocol(EventDriverProtocol)
    if driver:
        await driver.publish(event_type=event_type, payload=payload, scope="PLATFORM")


async def subscribe(
    subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None
) -> EventSubscription:
    driver = get_protocol(EventDriverProtocol)
    if driver is None:
        raise RuntimeError("EventDriverProtocol not available.")
    return await driver.subscribe(subscription_data, engine)


async def unsubscribe(
    subscriber_name: str, event_type: str, engine: Optional[DbResource] = None
) -> Optional[EventSubscription]:
    driver = get_protocol(EventDriverProtocol)
    if driver is None:
        raise RuntimeError("EventDriverProtocol not available.")
    return await driver.unsubscribe(subscriber_name, event_type, engine)


async def get_subscriptions_for_event_type(
    event_type: str, engine: Optional[DbResource] = None
) -> List[EventSubscription]:
    driver = get_protocol(EventDriverProtocol)
    if driver is None:
        raise RuntimeError("EventDriverProtocol not available.")
    return await driver.get_subscriptions_for_event_type(event_type, engine)
