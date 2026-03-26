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
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import orjson
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.tools.protocol_helpers import resolve
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config.locking_tools import acquire_startup_lock
from dynastore.models.protocols import (
    ConfigsProtocol,
    PropertiesProtocol,
    DatabaseProtocol,
    EventsProtocol,
    EventStorageProtocol,
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
from . import catalog_integration

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
    collection_id VARCHAR(255),
    payload       JSONB         NOT NULL DEFAULT '{{}}',
    status        VARCHAR       NOT NULL DEFAULT 'PENDING',
    dedup_key     VARCHAR(512),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ,
    error_message TEXT,
    retry_count   INT           NOT NULL DEFAULT 0,
    PRIMARY KEY (created_at, event_id)
) PARTITION BY RANGE (created_at);
"""

GLOBAL_EVENTS_INDEXES_DDL = f"""
CREATE INDEX IF NOT EXISTS idx_events_queue
    ON {_EVENTS_SCHEMA}.events (status, created_at)
    WHERE status = 'PENDING';
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup
    ON {_EVENTS_SCHEMA}.events (dedup_key, created_at)
    WHERE dedup_key IS NOT NULL AND status NOT IN ('DEAD_LETTER');
CREATE INDEX IF NOT EXISTS idx_events_schema
    ON {_EVENTS_SCHEMA}.events (schema_name, event_type);
CREATE INDEX IF NOT EXISTS idx_events_event_id
    ON {_EVENTS_SCHEMA}.events (event_id);

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

from dynastore.modules.events.tenant_ddl import (
    TENANT_CATALOG_EVENTS_DDL,
    TENANT_CATALOG_EVENTS_DL_DDL,
    TENANT_COLLECTION_EVENTS_DDL,
    TENANT_COLLECTION_EVENTS_DEFAULT_PARTITION_DDL,
    TENANT_COLLECTION_EVENTS_DL_DDL,
    build_tenant_notify_ddl,
    build_tenant_cron_ddl,
)

# ---------------------------------------------------------------------------
# Subscription table DDL (unchanged from original)
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
        (event_type, scope, schema_name, collection_id, payload, dedup_key)
    VALUES
        (:event_type, :scope, :schema_name, :collection_id, :payload, :dedup_key)
    ON CONFLICT (dedup_key, created_at)
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
    WHERE (created_at, event_id) IN (
        SELECT created_at, event_id FROM {_EVENTS_SCHEMA}.events
        WHERE status = 'PENDING'
          AND (:scope = 'ALL' OR scope = :scope)
        ORDER BY created_at ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    RETURNING event_id::text, event_type, scope, schema_name, collection_id,
              payload, created_at, dedup_key, retry_count;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_ack_query = DQLQuery(
    f"DELETE FROM {_EVENTS_SCHEMA}.events WHERE event_id = ANY(:event_ids);",
    result_handler=ResultHandler.NONE,
)

_tenant_catalog_event_insert_query = DQLQuery(
    """
    INSERT INTO {schema}.catalog_events (event_type, catalog_id, payload)
    VALUES (:event_type, :catalog_id, :payload)
    RETURNING id;
    """,
    result_handler=ResultHandler.SCALAR_ONE,
)

_tenant_collection_event_insert_query = DQLQuery(
    """
    INSERT INTO {schema}.collection_events (event_type, catalog_id, collection_id, item_id, payload)
    VALUES (:event_type, :catalog_id, :collection_id, :item_id, :payload)
    RETURNING id;
    """,
    result_handler=ResultHandler.SCALAR_ONE,
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
# EventsModule
# ---------------------------------------------------------------------------


class EventsModule(ModuleProtocol):
    """
    Owns all event storage and provides the EventStorageProtocol.

    Responsibilities:
    - Create and manage events.events (global outbox) + per-catalog tables
    - Implement publish / consume_batch / ack / nack / wait_for_events
    - Manage webhook subscriptions (platform.event_subscriptions)
    - Register catalog lifecycle listeners (when CatalogsProtocol is present)

    Priority 5: starts before DBService (10), TasksModule (15), CatalogModule (20).
    """

    priority: int = 5
    supports_notify: bool = True

    def __init__(self, app_state: object):
        self._engine: Optional[Any] = None

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
        logger.info("EventsModule: Initialising global events storage (%s.events)…", _EVENTS_SCHEMA)
        try:
            async with managed_transaction(self._engine) as conn:
                await DDLQuery(GLOBAL_EVENTS_TABLE_DDL).execute(
                    conn, lock_key=f"events_storage_init_table.{_EVENTS_SCHEMA}"
                )
                await DDLQuery(GLOBAL_EVENTS_INDEXES_DDL).execute(
                    conn, lock_key=f"events_storage_init_idx.{_EVENTS_SCHEMA}"
                )
            logger.info("EventsModule: %s.events ready.", _EVENTS_SCHEMA)

            # Ensure RANGE partitions exist for the global events table
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.maintenance_tools import (
                    ensure_future_partitions,
                    register_retention_policy,
                    register_partition_creation_policy,
                )

                global_retention = int(
                    os.getenv("GLOBAL_EVENT_RETENTION_DAYS", "30")
                )
                await ensure_future_partitions(
                    conn,
                    schema=_EVENTS_SCHEMA,
                    table="events",
                    interval="monthly",
                    periods_ahead=3,
                    column="created_at",
                )
                await register_retention_policy(
                    conn,
                    schema=_EVENTS_SCHEMA,
                    table="events",
                    policy="prune",
                    interval="daily",
                    retention_period=f"{global_retention} days",
                    column="created_at",
                )
                await register_partition_creation_policy(
                    conn,
                    schema=_EVENTS_SCHEMA,
                    table="events",
                    interval="monthly",
                    periods_ahead=3,
                )
            logger.info("EventsModule: Global events partition management configured.")
        except Exception:
            logger.exception("EventsModule: Failed to initialise global events storage.")

        # 2. Create webhook subscriptions table
        try:
            async with managed_transaction(self._engine) as conn:
                from dynastore.modules.db_config.locking_tools import check_table_exists
                if not await check_table_exists(conn, "event_subscriptions", _EVENTS_SCHEMA):
                    await DDLQuery(SUBSCRIPTIONS_SCHEMA).execute(conn)
                    await DDLQuery(SUBSCRIPTIONS_SCHEMA_INDEX).execute(conn)
        except Exception:
            logger.exception("EventsModule: Failed to initialise subscriptions schema.")

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
                logger.warning("PropertiesProtocol not available: %s. Cannot load '%s'.", e, API_KEY_NAME)

        # 4. Register catalog integration listeners (deferred until CatalogsProtocol is present)
        from dynastore.models.protocols import CatalogsProtocol
        if get_protocol(CatalogsProtocol):
            try:
                catalog_integration.register_all_listeners()
                logger.info("EventsModule: Registered catalog event listeners.")
            except Exception:
                logger.exception("EventsModule: Failed to register catalog listeners.")
        else:
            logger.info("EventsModule: CatalogsProtocol not loaded — skipping catalog listeners.")

        logger.info("EventsModule: Initialisation complete. Event storage is active.")
        yield
        logger.info("EventsModule: Shutdown complete.")

    # ------------------------------------------------------------------
    # EventStorageProtocol — DDL
    # ------------------------------------------------------------------

    async def initialize(self, conn: Any) -> None:
        """Create global events table (idempotent). Called by lifespan; exposed for tests."""
        await DDLQuery(GLOBAL_EVENTS_TABLE_DDL).execute(conn)
        await DDLQuery(GLOBAL_EVENTS_INDEXES_DDL).execute(conn)

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """Create per-catalog event table and indexes inside *catalog_schema*."""
        from dynastore.modules.db_config.maintenance_tools import register_cron_job
        from dynastore.modules.db_config.locking_tools import check_table_exists, check_function_exists
        from dynastore.modules.events.tenant_ddl import (
            TENANT_CATALOG_EVENTS_DDL,
            TENANT_CATALOG_EVENTS_DL_DDL,
            TENANT_COLLECTION_EVENTS_DDL,
            TENANT_COLLECTION_EVENTS_DL_DDL,
            TENANT_COLLECTION_EVENTS_DEFAULT_PARTITION_DDL,
            build_tenant_notify_ddl,
            build_tenant_cron_ddl,
        )

        schema = catalog_schema

        async def _check_all_tables_exist(active_conn=None, params=None):
            target = active_conn or conn
            exists_1 = await check_table_exists(target, "catalog_events", schema)
            exists_2 = await check_table_exists(target, "catalog_events_dead_letter", schema)
            exists_3 = await check_table_exists(target, "collection_events", schema)
            exists_4 = await check_table_exists(target, "collection_events_dead_letter", schema)
            exists_5 = await check_table_exists(target, "collection_events_default", schema)
            return all([exists_1, exists_2, exists_3, exists_4, exists_5])

        combined_ddl = (
            TENANT_CATALOG_EVENTS_DDL
            + TENANT_CATALOG_EVENTS_DL_DDL
            + TENANT_COLLECTION_EVENTS_DDL
            + TENANT_COLLECTION_EVENTS_DL_DDL
            + TENANT_COLLECTION_EVENTS_DEFAULT_PARTITION_DDL
        )

        await DDLQuery(
            combined_ddl,
            check_query=_check_all_tables_exist,
            lock_key=f"{schema}_event_tables_init",
        ).execute(conn, schema=schema)

        # --- NOTIFY function + triggers (catalog and collection share one function) ---
        func_name, notify_func, triggers = build_tenant_notify_ddl(schema)

        async def check_notify_resources(active_conn=None, params=None):
            target_conn = active_conn or conn
            return await check_function_exists(target_conn, func_name, schema)

        await DDLQuery(
            notify_func + triggers,
            check_query=check_notify_resources,
            lock_key=f"init_{schema}_{func_name}",
        ).execute(conn, schema=schema)

        # --- pg_cron maintenance jobs ---
        for job_name, schedule, command in build_tenant_cron_ddl(schema):
            await register_cron_job(conn, job_name=job_name, schedule=schedule, command=command)

    async def init_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """Creates a per-collection LIST partition in collection_events."""
        from dynastore.modules.db_config.locking_tools import check_table_exists

        # Sanitise collection_id for use as a table name suffix
        safe_suffix = collection_id.replace("-", "_").replace(".", "_")
        partition_table = f"collection_events_{safe_suffix}"

        # PostgreSQL identifier limit is 63 chars. Apply stable hash truncation if needed.
        if len(partition_table) > 63:
            import hashlib

            h = hashlib.sha1(collection_id.encode()).hexdigest()[:8]
            partition_table = f"collection_events_{h}"

        async def partition_exists(active_conn=None, params=None):
            return await check_table_exists(
                active_conn or conn, partition_table, catalog_schema
            )

        create_ddl = (
            f'CREATE TABLE IF NOT EXISTS "{catalog_schema}"."{partition_table}" '
            f'PARTITION OF "{catalog_schema}".collection_events '
            f"FOR VALUES IN ('{collection_id}');"
        )

        await DDLQuery(
            create_ddl,
            check_query=partition_exists,
            lock_key=f"{catalog_schema}_{partition_table}_init",
        ).execute(conn)
        logger.info(
            "EventsModule: Created collection_events partition '%s.%s'.",
            catalog_schema,
            partition_table,
        )

    async def drop_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """Removes a per-collection LIST partition from collection_events."""
        # Sanitise collection_id
        safe_suffix = collection_id.replace("-", "_").replace(".", "_")
        partition_table = f"collection_events_{safe_suffix}"

        if len(partition_table) > 63:
            import hashlib

            h = hashlib.sha1(collection_id.encode()).hexdigest()[:8]
            partition_table = f"collection_events_{h}"

        # DETACH and DROP
        # We use CONCURRENTLY if possible? No, DDLQuery uses regular EXECUTE.
        # Idempotent drop:
        drop_ddl = f'DROP TABLE IF EXISTS "{catalog_schema}"."{partition_table}";'

        await DDLQuery(
            drop_ddl, lock_key=f"{catalog_schema}_{partition_table}_drop"
        ).execute(conn)
        logger.info(
            "EventsModule: Dropped collection_events partition '%s.%s'.",
            catalog_schema,
            partition_table,
        )

    # ------------------------------------------------------------------
    # EventStorageProtocol — produce
    # ------------------------------------------------------------------

    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        scope: str = "PLATFORM",
        schema_name: Optional[str] = None,
        collection_id: Optional[str] = None,
        dedup_key: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[str]:
        """Insert an event into the global outbox. Returns event_id or None (dedup)."""
        import orjson

        async def _run(conn: Any) -> Optional[str]:
            payload_str = orjson.dumps(payload).decode()
            
            # 1. Always insert into the global outbox (events.events)
            event_id = await _publish_query.execute(
                conn,
                event_type=event_type,
                scope=scope,
                schema_name=schema_name,
                collection_id=collection_id,
                payload=payload_str,
                dedup_key=dedup_key,
            )
            
            # 2. If it's a tenant event, insert into the tenant's localized history table
            if schema_name and scope in ("CATALOG", "COLLECTION"):
                try:
                    # catalog_id is usually derived from schema_name in this context or passed in payload
                    # We'll extract catalog_id from payload if available, else derive from schema
                    # Wait, payload usually has catalog_id
                    cat_id = payload.get("catalog_id") or schema_name.split("_")[0]
                    
                    if scope == "COLLECTION" or collection_id:
                        await _tenant_collection_event_insert_query.execute(
                            conn,
                            schema=schema_name,
                            catalog_id=cat_id,
                            collection_id=collection_id,
                            item_id=payload.get("item_id"),
                            event_type=event_type,
                            payload=payload_str,
                        )
                    else:
                        await _tenant_catalog_event_insert_query.execute(
                            conn,
                            schema=schema_name,
                            catalog_id=cat_id,
                            event_type=event_type,
                            payload=payload_str,
                        )
                except Exception as e:
                    # Tenant table might not exist yet if catalog isn't fully initialized; log and continue
                    logger.warning(f"Failed to insert tenant event {event_type} into {schema_name}: {e}")
                    
            return event_id

        if db_resource is not None:
            return await _run(db_resource)

        engine = self._engine or get_protocol(DatabaseProtocol)
        async with managed_transaction(engine) as conn:
            return await _run(conn)

    # ------------------------------------------------------------------
    # EventStorageProtocol — consume / ack / nack
    # ------------------------------------------------------------------

    async def consume_batch(
        self,
        scope: str = "PLATFORM",
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Claim a batch of PENDING events for *scope* using SKIP LOCKED."""
        engine = self._engine or get_protocol(DatabaseProtocol)
        async with managed_transaction(engine) as conn:
            rows = await _consume_query.execute(conn, scope=scope, batch_size=batch_size)
        return rows or []

    async def ack(self, event_ids: List[str]) -> None:
        """Delete successfully processed events from the outbox."""
        if not event_ids:
            return
        engine = self._engine or get_protocol(DatabaseProtocol)
        async with managed_transaction(engine) as conn:
            await _ack_query.execute(conn, event_ids=event_ids)

    async def nack(self, event_id: str, error: str) -> None:
        """Increment retry_count; move to DEAD_LETTER when retries exhausted."""
        engine = self._engine or get_protocol(DatabaseProtocol)
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
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Search for events across global and tenant tables."""
        async with managed_transaction(engine) as conn:
            base_clauses = ["catalog_id = :catalog_id"]
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}

            if collection_id:
                base_clauses.append("collection_id = :collection_id")
                params["collection_id"] = collection_id
            if event_type:
                base_clauses.append("event_type = :event_type")
                params["event_type"] = event_type

            where_clause = " AND ".join(base_clauses)
            queries = []

            # Global Events (for _system_ or matching payload->>'catalog_id')
            sys_where = where_clause
            if catalog_id == "_system_":
                sys_where = where_clause.replace("catalog_id = :catalog_id", "TRUE")
            else:
                # payload->>'catalog_id' = :catalog_id
                sys_where = sys_where.replace("catalog_id = :catalog_id", "(payload->>'catalog_id' = :catalog_id)")

            queries.append(
                f"SELECT event_id::text as id, event_type, payload->>'catalog_id' as catalog_id, collection_id, NULL as item_id, payload, created_at, status "
                f"FROM {_EVENTS_SCHEMA}.events WHERE {sys_where}"
            )

            # Tenant Events
            if catalog_id != "_system_":
                catalogs_provider = get_protocol(CatalogsProtocol)
                if catalogs_provider:
                    physical_schema = await catalogs_provider.resolve_physical_schema(catalog_id)
                    if physical_schema:
                        if not collection_id:
                            queries.append(
                                f"SELECT id::text, event_type, catalog_id, NULL as collection_id, NULL as item_id, payload, created_at, status "
                                f'FROM "{physical_schema}".catalog_events WHERE {where_clause}'
                            )
                        queries.append(
                            f"SELECT id::text, event_type, catalog_id, collection_id, item_id, payload, created_at, status "
                            f'FROM "{physical_schema}".collection_events WHERE {where_clause}'
                        )

            final_sql = " UNION ALL ".join(queries)
            final_sql = f"SELECT * FROM ({final_sql}) AS combined ORDER BY created_at DESC LIMIT :limit OFFSET :offset"

            try:
                rows = await DQLQuery(
                    final_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, **params)
                return rows
            except Exception as e:
                logger.debug(f"Event Search failed: {e}")
                
            return []

    # ------------------------------------------------------------------
    # EventStorageProtocol — consumer notification
    # ------------------------------------------------------------------

    async def wait_for_events(self, timeout: float = 10.0) -> None:
        """
        Wait until an event signal arrives on 'dynastore_events_channel' or
        *timeout* seconds elapse.

        Allows the event consumer to avoid tight polling. Implementations
        without pub/sub will naturally fall back to sleeping for `timeout` seconds.
        """
        from dynastore.tools.async_utils import signal_bus
        await signal_bus.wait_for("dynastore_events_channel", timeout=timeout)

    # ------------------------------------------------------------------
    # EventsModule — create_event (top-level API, publish to own outbox)
    # ------------------------------------------------------------------

    async def create_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Publish an event to the global outbox (replaces Procrastinate task dispatch)."""
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
            logger.info("Subscription removed for '%s' on event '%s'.", subscriber_name, event_type)
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
    events = get_protocol(EventStorageProtocol)
    if events:
        await events.publish(event_type=event_type, payload=payload, scope="PLATFORM")


async def subscribe(
    subscription_data: EventSubscriptionCreate, engine: Optional[DbResource] = None
) -> EventSubscription:
    events = get_protocol(EventsProtocol)
    return await events.subscribe(subscription_data, engine)


async def unsubscribe(
    subscriber_name: str, event_type: str, engine: Optional[DbResource] = None
) -> Optional[EventSubscription]:
    events = get_protocol(EventsProtocol)
    return await events.unsubscribe(subscriber_name, event_type, engine)


async def get_subscriptions_for_event_type(
    event_type: str, engine: Optional[DbResource] = None
) -> List[EventSubscription]:
    events = get_protocol(EventsProtocol)
    return await events.get_subscriptions_for_event_type(event_type, engine)
