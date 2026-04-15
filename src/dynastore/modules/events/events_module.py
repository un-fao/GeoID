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

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional

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
from dynastore.modules.db_config.locking_tools import (
    acquire_startup_lock,
    _get_stable_lock_id,
)
from dynastore.models.protocols import (
    CatalogsProtocol,
    ConfigsProtocol,
    PropertiesProtocol,
    DatabaseProtocol,
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
from .primitives import (
    EventScope,
    EventRegistry,
    define_event,
    SystemEventType,
)
from sqlalchemy import text as _sql_text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global events schema DDL
# ---------------------------------------------------------------------------

_EVENTS_SCHEMA = os.getenv("DYNASTORE_EVENTS_SCHEMA", "events")

GLOBAL_EVENTS_TABLE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS "{_EVENTS_SCHEMA}";
CREATE TABLE IF NOT EXISTS {_EVENTS_SCHEMA}.events (
    event_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
    event_type    VARCHAR       NOT NULL,
    scope         VARCHAR(50)   NOT NULL DEFAULT 'PLATFORM',
    schema_name   VARCHAR(255),
    catalog_id    VARCHAR(255),
    collection_id VARCHAR(255),
    identity_id   VARCHAR(255),
    payload       JSONB         NOT NULL DEFAULT '{{}}',
    status        VARCHAR       NOT NULL DEFAULT 'PENDING',
    dedup_key     VARCHAR(512),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ,
    error_message TEXT,
    retry_count   INT           NOT NULL DEFAULT 0,
    shard         SMALLINT      GENERATED ALWAYS AS (
                      (abs(hashtext(coalesce(catalog_id, 'PLATFORM'))) % 16)::smallint
                  ) STORED,
    PRIMARY KEY (shard, created_at, event_id)
) PARTITION BY LIST (shard);
"""

GLOBAL_EVENTS_INDEXES_DDL = f"""
CREATE INDEX IF NOT EXISTS idx_events_queue
    ON {_EVENTS_SCHEMA}.events (status, created_at)
    WHERE status = 'PENDING';
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup
    ON {_EVENTS_SCHEMA}.events (dedup_key, shard)
    WHERE dedup_key IS NOT NULL AND status NOT IN ('DEAD_LETTER');
CREATE INDEX IF NOT EXISTS idx_events_schema
    ON {_EVENTS_SCHEMA}.events (schema_name, event_type);
CREATE INDEX IF NOT EXISTS idx_events_event_id
    ON {_EVENTS_SCHEMA}.events (event_id);
CREATE INDEX IF NOT EXISTS idx_events_catalog
    ON {_EVENTS_SCHEMA}.events (catalog_id, event_type, created_at)
    WHERE catalog_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_collection
    ON {_EVENTS_SCHEMA}.events (collection_id, event_type, created_at)
    WHERE collection_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_identity
    ON {_EVENTS_SCHEMA}.events (identity_id, event_type, created_at)
    WHERE identity_id IS NOT NULL;

CREATE OR REPLACE FUNCTION {_EVENTS_SCHEMA}.notify_event_ready()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('dynastore_events_channel', NEW.event_type);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_event_insert ON {_EVENTS_SCHEMA}.events;
CREATE TRIGGER on_event_insert
    AFTER INSERT ON {_EVENTS_SCHEMA}.events
    FOR EACH ROW
    WHEN (NEW.status = 'PENDING')
    EXECUTE FUNCTION {_EVENTS_SCHEMA}.notify_event_ready();
"""

# ---------------------------------------------------------------------------
# Subscription table DDL
# ---------------------------------------------------------------------------

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
# Internal event store query objects
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3

_publish_query = DQLQuery(
    f"""
    INSERT INTO {_EVENTS_SCHEMA}.events
        (event_type, scope, schema_name, catalog_id, collection_id, identity_id,
         payload, dedup_key)
    VALUES
        (:event_type, :scope, :schema_name, :catalog_id, :collection_id,
         :identity_id, :payload, :dedup_key)
    ON CONFLICT (dedup_key, shard)
        WHERE dedup_key IS NOT NULL AND status NOT IN ('DEAD_LETTER')
        DO NOTHING
    RETURNING event_id::text;
    """,
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

_consume_query = DQLQuery(
    f"""
    UPDATE {_EVENTS_SCHEMA}.events
    SET status = 'PROCESSING',
        processed_at = NOW()
    WHERE (shard, created_at, event_id) IN (
        SELECT shard, created_at, event_id FROM {_EVENTS_SCHEMA}.events
        WHERE status = 'PENDING'
          AND (:shard IS NULL OR shard = :shard)
          AND (:scope = 'ALL' OR scope = :scope)
        ORDER BY created_at ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    RETURNING event_id::text, event_type, scope, schema_name, catalog_id,
              collection_id, identity_id, payload, created_at, dedup_key, retry_count;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_ack_query = DQLQuery(
    f"DELETE FROM {_EVENTS_SCHEMA}.events WHERE event_id = ANY(:event_ids);",
    result_handler=ResultHandler.NONE,
)

_nack_query = DQLQuery(
    f"""
    UPDATE {_EVENTS_SCHEMA}.events
    SET status = CASE
            WHEN retry_count + 1 >= :max_retries THEN 'DEAD_LETTER'
            ELSE 'PENDING'
        END,
        retry_count   = retry_count + 1,
        error_message = :error,
        processed_at  = NOW()
    WHERE event_id = :event_id::uuid;
    """,
    result_handler=ResultHandler.NONE,
)


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
    Owns all event storage and provides the EventDriverProtocol.

    Responsibilities:
    - Create and manage events.events (global outbox, 16-shard partitioned)
    - Implement publish / consume_batch / ack / nack / wait_for_events
    - Manage webhook subscriptions (platform.event_subscriptions)
    - Expose distributed advisory lock via acquire_consumer_lock
    - Register catalog lifecycle listeners

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
            EventDriverCapability.LOCKING,
            EventDriverCapability.NOTIFICATION,
            EventDriverCapability.SUBSCRIBE,
            EventDriverCapability.DEAD_LETTER,
            EventDriverCapability.EXACTLY_ONCE,
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

        # 1. Create global events table (guarded by advisory lock for multi-instance safety)
        logger.info(
            "EventsModule: Initialising global events storage (%s.events)…", _EVENTS_SCHEMA
        )
        try:
            async with managed_transaction(self._engine) as conn:
                await DDLQuery(GLOBAL_EVENTS_TABLE_DDL).execute(
                    conn, lock_key=f"events_storage_init_table.{_EVENTS_SCHEMA}"
                )
                await DDLQuery(GLOBAL_EVENTS_INDEXES_DDL).execute(
                    conn, lock_key=f"events_storage_init_idx.{_EVENTS_SCHEMA}"
                )
            logger.info("EventsModule: %s.events ready.", _EVENTS_SCHEMA)

            # Create 16 shard sub-partitions and per-shard maintenance jobs
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.maintenance_tools import (
                    ensure_future_partitions,
                    register_retention_policy,
                    register_partition_creation_policy,
                )

                global_retention = int(os.getenv("GLOBAL_EVENT_RETENTION_DAYS", "30"))

                for shard_id in range(16):
                    await DDLQuery(
                        f"""
                        CREATE TABLE IF NOT EXISTS {_EVENTS_SCHEMA}.events_s{shard_id}
                        PARTITION OF {_EVENTS_SCHEMA}.events
                        FOR VALUES IN ({shard_id})
                        PARTITION BY RANGE (created_at);
                        """
                    ).execute(conn)

                    await ensure_future_partitions(
                        conn,
                        schema=_EVENTS_SCHEMA,
                        table=f"events_s{shard_id}",
                        interval="monthly",
                        periods_ahead=3,
                        column="created_at",
                    )
                    await register_partition_creation_policy(
                        conn,
                        schema=_EVENTS_SCHEMA,
                        table=f"events_s{shard_id}",
                        interval="monthly",
                        periods_ahead=3,
                    )
                    await register_retention_policy(
                        conn,
                        schema=_EVENTS_SCHEMA,
                        table=f"events_s{shard_id}",
                        policy="prune",
                        interval="daily",
                        retention_period=f"{global_retention} days",
                        column="created_at",
                    )

            logger.info("EventsModule: Global events shard partitions configured.")
        except Exception:
            logger.exception("EventsModule: Failed to initialise global events storage.")
            raise

        # 2. Create webhook subscriptions table
        try:
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.locking_tools import check_table_exists
                if not await check_table_exists(conn, "event_subscriptions", _EVENTS_SCHEMA):
                    await DDLQuery(SUBSCRIPTIONS_SCHEMA).execute(conn)
                    await DDLQuery(SUBSCRIPTIONS_SCHEMA_INDEX).execute(conn)
        except Exception:
            logger.exception("EventsModule: Failed to initialise subscriptions schema.")
            raise

        # 3. Load / generate platform API key
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

        # 4. Register catalog integration listeners (deferred until CatalogsProtocol is present)
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
        yield
        logger.info("EventsModule: Shutdown complete.")

    # ------------------------------------------------------------------
    # EventDriverProtocol — DDL lifecycle
    # ------------------------------------------------------------------

    async def initialize(self, conn: Any) -> None:
        """Create global events table (idempotent). Called by lifespan; exposed for tests."""
        await DDLQuery(GLOBAL_EVENTS_TABLE_DDL).execute(conn)
        await DDLQuery(GLOBAL_EVENTS_INDEXES_DDL).execute(conn)

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """No-op. The global shard-partitioned outbox serves all catalogs."""
        pass

    async def init_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """No-op. The global shard-partitioned outbox serves all collections."""
        pass

    async def drop_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """No-op. The global outbox does not maintain per-collection partitions."""
        pass

    # ------------------------------------------------------------------
    # EventDriverProtocol — distributed lock
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire_consumer_lock(self, key: str) -> AsyncIterator[bool]:
        """
        Acquire a PostgreSQL session-scoped advisory lock on a dedicated
        AUTOCOMMIT connection.

        Yields True if this worker became the leader; the lock is held for
        the lifetime of the context.  On connection drop (pod/worker death)
        the lock is released automatically — no heartbeat needed.
        """
        lock_id = _get_stable_lock_id(key)
        async with self._engine.connect() as conn:
            conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(
                _sql_text("SELECT pg_advisory_lock(:id)"), {"id": lock_id}
            )
            logger.info("EventsModule: consumer lock acquired (key=%s).", key)
            try:
                yield True
            finally:
                try:
                    await conn.execute(
                        _sql_text("SELECT pg_advisory_unlock(:id)"), {"id": lock_id}
                    )
                except Exception:
                    pass  # connection drop releases lock automatically

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
        dedup_key: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Insert an event into the global outbox. Returns event_id or None (dedup)."""

        async def _run(conn: Any) -> Optional[str]:
            payload_str = orjson.dumps(payload).decode()
            return await _publish_query.execute(
                conn,
                event_type=event_type,
                scope=scope,
                schema_name=schema_name,
                catalog_id=catalog_id,
                collection_id=collection_id,
                identity_id=identity_id,
                payload=payload_str,
                dedup_key=dedup_key,
            )

        if db_resource is not None:
            return await _run(db_resource)

        from dynastore.tools.protocol_helpers import get_engine
        engine = self._engine or get_engine()
        async with managed_transaction(engine) as conn:
            return await _run(conn)

    # ------------------------------------------------------------------
    # EventDriverProtocol — consume / ack / nack
    # ------------------------------------------------------------------

    async def consume_batch(
        self,
        scope: str = "PLATFORM",
        batch_size: int = 100,
        shard: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Claim a batch of PENDING events for *scope* (and optionally *shard*) using SKIP LOCKED."""
        from dynastore.tools.protocol_helpers import get_engine
        engine = self._engine or get_engine()
        async with managed_transaction(engine) as conn:
            rows = await _consume_query.execute(
                conn, scope=scope, batch_size=batch_size, shard=shard
            )
        return rows or []

    async def ack(self, event_ids: List[str]) -> None:
        """Delete successfully processed events from the outbox."""
        if not event_ids:
            return
        from dynastore.tools.protocol_helpers import get_engine
        engine = self._engine or get_engine()
        async with managed_transaction(engine) as conn:
            await _ack_query.execute(conn, event_ids=event_ids)

    async def nack(self, event_id: str, error: str) -> None:
        """Increment retry_count; move to DEAD_LETTER when retries exhausted."""
        from dynastore.tools.protocol_helpers import get_engine
        engine = self._engine or get_engine()
        async with managed_transaction(engine) as conn:
            await _nack_query.execute(
                conn,
                event_id=event_id,
                error=error,
                max_retries=_MAX_RETRIES,
            )

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
        """Search for events in the global outbox."""
        async with managed_transaction(engine) as conn:
            clauses = []
            params: Dict[str, Any] = {"limit": limit, "offset": offset}

            if catalog_id and catalog_id != "_system_":
                clauses.append("catalog_id = :catalog_id")
                params["catalog_id"] = catalog_id
            if collection_id:
                clauses.append("collection_id = :collection_id")
                params["collection_id"] = collection_id
            if identity_id:
                clauses.append("identity_id = :identity_id")
                params["identity_id"] = identity_id
            if event_type:
                clauses.append("event_type = :event_type")
                params["event_type"] = event_type

            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            sql = (
                f"SELECT event_id::text as id, event_type, catalog_id, collection_id, "
                f"identity_id, payload, created_at, status "
                f"FROM {_EVENTS_SCHEMA}.events {where} "
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
        """Publish an event to the global outbox."""
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
