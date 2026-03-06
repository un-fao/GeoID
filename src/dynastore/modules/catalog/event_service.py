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

import logging
import asyncio
import json
from typing import (
    List,
    Optional,
    Any,
    Callable,
    Coroutine,
    Dict,
    Tuple,
    Protocol,
    runtime_checkable,
    Awaitable,
    cast,
    Set,
    Union,
)
from collections import defaultdict
from contextlib import asynccontextmanager
from enum import Enum

from dynastore.modules.catalog.models import EventType
from dynastore.models.protocols import EventsProtocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
    QueryExecutionError,
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    acquire_startup_lock,
    check_trigger_exists,
    check_function_exists,
    check_table_exists,
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.maintenance_tools import register_cron_job

logger = logging.getLogger(__name__)

# Import system schema constant
from dynastore.models.shared_models import SYSTEM_SCHEMA

# ==============================================================================
#  TENANT INITIALIZATION (Events Slice)
#
#  Design: Two flat tables per tenant schema (no partitions).
#  - catalog_events:    for catalog-scoped events
#  - collection_events: for collection-scoped events
#
#  Tables:
#   - catalog_events:    flat table for catalog-scoped events.
#   - collection_events: LIST partitioned table by collection_id.
#
#  Maintenance (pg_cron):
#   - Daily: Prune events older than 30 days.
# ==============================================================================

# ----- Catalog-level events (flat, no partitions) -----
TENANT_CATALOG_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  VARCHAR      NOT NULL,
    catalog_id  VARCHAR,
    payload     JSONB,
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    status      VARCHAR      DEFAULT 'PENDING'
);
CREATE INDEX IF NOT EXISTS idx_catalog_events_type ON {schema}.catalog_events(event_type);
CREATE INDEX IF NOT EXISTS idx_catalog_events_cat_id ON {schema}.catalog_events(catalog_id);
"""

TENANT_CATALOG_EVENTS_DL_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_events_dead_letter (
    id                  BIGINT       NOT NULL,
    event_type          VARCHAR      NOT NULL,
    catalog_id          VARCHAR,
    payload             JSONB,
    created_at          TIMESTAMPTZ  NOT NULL,
    archived_at         TIMESTAMPTZ  DEFAULT NOW(),
    original_status     VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_catalog_events_dl_type ON {schema}.catalog_events_dead_letter(event_type);
CREATE INDEX IF NOT EXISTS idx_catalog_events_dl_cat_id ON {schema}.catalog_events_dead_letter(catalog_id);
"""

# ----- Collection-level events (partitioned by collection_id via LIST) -----
# Each collection gets its own partition created at collection-creation time.
# A DEFAULT partition captures events before the per-collection partition exists.
# The dead letter stays flat and is rotated by pg_cron monthly.
TENANT_COLLECTION_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events (
    id              BIGSERIAL       NOT NULL,
    event_type      VARCHAR         NOT NULL,
    catalog_id      VARCHAR,
    collection_id   VARCHAR         NOT NULL,
    item_id         VARCHAR,
    payload         JSONB,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    status          VARCHAR         DEFAULT 'PENDING',
    PRIMARY KEY (collection_id, id)
) PARTITION BY LIST (collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_type ON {schema}.collection_events(event_type);
CREATE INDEX IF NOT EXISTS idx_collection_events_cat_id ON {schema}.collection_events(catalog_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_col_id ON {schema}.collection_events(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_item_id ON {schema}.collection_events(item_id);
"""

TENANT_COLLECTION_EVENTS_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events_default
    PARTITION OF {schema}.collection_events DEFAULT;
"""

TENANT_COLLECTION_EVENTS_DL_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events_dead_letter (
    id                  BIGINT       NOT NULL,
    event_type          VARCHAR      NOT NULL,
    catalog_id          VARCHAR,
    collection_id       VARCHAR,
    item_id             VARCHAR,
    payload             JSONB,
    created_at          TIMESTAMPTZ  NOT NULL,
    archived_at         TIMESTAMPTZ  DEFAULT NOW(),
    original_status     VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_type ON {schema}.collection_events_dead_letter(event_type);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_cat_id ON {schema}.collection_events_dead_letter(catalog_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_col_id ON {schema}.collection_events_dead_letter(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_item_id ON {schema}.collection_events_dead_letter(item_id);
"""


def _build_tenant_notify_ddl(schema: str) -> tuple[str, str, str]:
    """Returns (func_name, notify_func_ddl, triggers_ddl) for catalog and collection tables.

    NOTE: Statement-level triggers are no longer required for collection_events (partitioned)
    since we are on PostgreSQL 16+. Row-level triggers on the parent table fire
    individually for each partition insertion.
    """
    func_name = f"notify_tenant_event_{schema}"
    notify_func = f"""
    CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('dynastore_events_channel', TG_TABLE_SCHEMA);
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
    triggers = f"""
    CREATE TRIGGER trg_catalog_events_insert
    AFTER INSERT ON "{schema}".catalog_events
    FOR EACH ROW EXECUTE FUNCTION "{schema}"."{func_name}"();

    CREATE TRIGGER trg_collection_events_insert
    AFTER INSERT ON "{schema}".collection_events
    FOR EACH ROW EXECUTE FUNCTION "{schema}"."{func_name}"();
    """
    return func_name, notify_func, triggers


def _build_tenant_cron_ddl(schema: str) -> list[tuple[str, str, str]]:
    """
    Returns a list of (job_name, schedule, sql_command) tuples for tenant cron jobs.

    Jobs registered:
      - archive_catalog_events_{schema}:    move PENDING > 30 days → dead_letter
      - archive_collection_events_{schema}: move PENDING > 30 days → dead_letter
      - cleanup_catalog_events_dl_{schema}: delete dead_letter > 1 year
      - cleanup_collection_events_dl_{schema}: delete dead_letter > 1 year
    """
    s = schema
    return [
        (
            f"archive_catalog_events_{s}",
            "0 2 * * 0",  # Sundays 02:00
            (
                f'INSERT INTO "{s}".catalog_events_dead_letter '
                f"(id, event_type, catalog_id, payload, created_at, original_status) "
                f"SELECT id, event_type, catalog_id, payload, created_at, status "
                f'FROM "{s}".catalog_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days'; "
                f'DELETE FROM "{s}".catalog_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days';"
            ),
        ),
        (
            f"archive_collection_events_{s}",
            "0 2 * * 0",  # Sundays 02:00
            (
                f'INSERT INTO "{s}".collection_events_dead_letter '
                f"(id, event_type, catalog_id, collection_id, item_id, payload, created_at, original_status) "
                f"SELECT id, event_type, catalog_id, collection_id, item_id, payload, created_at, status "
                f'FROM "{s}".collection_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days'; "
                f'DELETE FROM "{s}".collection_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days';"
            ),
        ),
        (
            f"cleanup_catalog_events_dl_{s}",
            "0 3 1 * *",  # 1st of month 03:00
            f"DELETE FROM \"{s}\".catalog_events_dead_letter WHERE archived_at < NOW() - INTERVAL '1 year';",
        ),
        (
            f"cleanunocp_collection_events_dl_{s}",
            "0 3 1 * *",  # 1st of month 03:00
            f"DELETE FROM \"{s}\".collection_events_dead_letter WHERE archived_at < NOW() - INTERVAL '1 year';",
        ),
    ]


@lifecycle_registry.sync_catalog_initializer(priority=50)
async def _initialize_events_tenant_slice(
    conn: DbResource, schema: str, catalog_id: str
):
    """Initializes per-tenant event tables and cron jobs (no partitions)."""

    async def _check_all_tables_exist(active_conn=None, params=None):
        target = active_conn or conn
        exists_1 = await check_table_exists(target, "catalog_events", schema)
        exists_2 = await check_table_exists(
            target, "catalog_events_dead_letter", schema
        )
        exists_3 = await check_table_exists(target, "collection_events", schema)
        exists_4 = await check_table_exists(
            target, "collection_events_dead_letter", schema
        )
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
    func_name, notify_func, triggers = _build_tenant_notify_ddl(schema)

    async def check_notify_resources(active_conn=None, params=None):
        target_conn = active_conn or conn
        return await check_function_exists(target_conn, func_name, schema)

    await DDLQuery(
        notify_func + triggers,
        check_query=check_notify_resources,
        lock_key=f"init_{schema}_{func_name}",
    ).execute(conn, schema=schema)

    # --- pg_cron maintenance jobs ---
    for job_name, schedule, command in _build_tenant_cron_ddl(schema):
        await register_cron_job(
            conn, job_name=job_name, schedule=schedule, command=command
        )


# ==============================================================================
#  COLLECTION-LEVEL PARTITION LIFECYCLE
#
#  When a collection is created  → attach a dedicated LIST partition for it.
#  When a collection is deleted  → detach and drop its partition.
#  The DEFAULT partition catches events until the specific one is ready.
# ==============================================================================

from dynastore.modules.catalog.lifecycle_manager import (
    sync_collection_initializer,
    sync_collection_destroyer,
)


@sync_collection_initializer
async def _create_collection_events_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str, **kwargs
) -> None:
    """Creates a per-collection LIST partition in collection_events."""
    # Sanitise collection_id for use as a table name suffix (replace non-alpha)
    safe_suffix = collection_id.replace("-", "_").replace(".", "_")
    partition_table = f"collection_events_{safe_suffix}"

    # PostgreSQL identifier limit is 63 chars. Apply stable hash truncation if needed.
    if len(partition_table) > 63:
        import hashlib
        h = hashlib.sha1(collection_id.encode()).hexdigest()[:8]
        partition_table = f"collection_events_{h}"

    async def partition_exists(active_conn=None, params=None):
        return await check_table_exists(active_conn or conn, partition_table, schema)

    create_ddl = (
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{partition_table}" '
        f'PARTITION OF "{schema}".collection_events '
        f"FOR VALUES IN ('{collection_id}');"
    )

    await DDLQuery(
        create_ddl,
        check_query=partition_exists,
        lock_key=f"{schema}_{partition_table}_init",
    ).execute(conn)
    logger.info("Created collection_events partition '%s.%s'.", schema, partition_table)


@sync_collection_destroyer
async def _drop_collection_events_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str
) -> None:
    """Archives pending events then drops the per-collection partition."""
    safe_suffix = collection_id.replace("-", "_").replace(".", "_")
    partition_table = f"collection_events_{safe_suffix}"

    async def partition_exists(active_conn=None, params=None):
        return await check_table_exists(active_conn or conn, partition_table, schema)

    exists = await partition_exists()
    if not exists:
        logger.debug(
            "No collection_events partition to drop for collection '%s'.", collection_id
        )
        return

    # Archive any remaining PENDING events to dead letter before dropping
    # (Note: simpler execution than checking DL existence, as DL is static)
    archive_sql = (
        f'INSERT INTO "{schema}".collection_events_dead_letter '
        f"(id, event_type, catalog_id, collection_id, item_id, payload, created_at, original_status) "
        f"SELECT id, event_type, catalog_id, collection_id, item_id, payload, created_at, status "
        f'FROM "{schema}"."{partition_table}" WHERE status = \'PENDING\';'
    )
    drop_sql = f'DROP TABLE IF EXISTS "{schema}"."{partition_table}";'

    # No lock needed: collection deletion is already serialised by the caller
    await DDLQuery(archive_sql + drop_sql).execute(conn)
    logger.info("Dropped collection_events partition '%s.%s'.", schema, partition_table)


class EventScope(str, Enum):
    PLATFORM = "platform"
    CATALOG = "catalog"
    COLLECTION = "collection"


class EventRegistry:
    """Central registry for dynamic event definitions."""

    _events: Dict[str, EventScope] = {}

    @classmethod
    def register(cls, name: str, scope: EventScope):
        if name in cls._events and cls._events[name] != scope:
            logger.warning(
                f"Event '{name}' re-registered with different scope: {scope} (was {cls._events[name]})"
            )
        cls._events[name] = scope
        logger.debug(f"Registered event '{name}' with scope '{scope.value}'")

    @classmethod
    def is_valid(cls, name: str) -> bool:
        return name in cls._events


def define_event(name: str, scope: EventScope = EventScope.CATALOG):
    """
    Decorator/Function to register a new event type dynamically.
    Usage:
        MY_EVENT = define_event("my.custom.event", EventScope.CATALOG)
    """
    EventRegistry.register(name, scope)
    return name


# --- Standard Events ---
# These are pre-registered for backward compatibility and core functionality.


class CatalogEventType(EventType):
    # Catalog Lifecycle
    BEFORE_CATALOG_CREATION = define_event(
        "before_catalog_creation", EventScope.PLATFORM
    )
    CATALOG_CREATION = define_event("catalog_creation", EventScope.PLATFORM)
    AFTER_CATALOG_CREATION = define_event("after_catalog_creation", EventScope.PLATFORM)

    BEFORE_CATALOG_UPDATE = define_event("before_catalog_update", EventScope.PLATFORM)
    CATALOG_UPDATE = define_event("catalog_update", EventScope.PLATFORM)
    AFTER_CATALOG_UPDATE = define_event("after_catalog_update", EventScope.PLATFORM)

    BEFORE_CATALOG_DELETION = define_event(
        "before_catalog_deletion", EventScope.PLATFORM
    )
    CATALOG_DELETION = define_event("catalog_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_DELETION = define_event("after_catalog_deletion", EventScope.PLATFORM)

    BEFORE_CATALOG_HARD_DELETION = define_event(
        "before_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION = define_event("catalog_hard_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_HARD_DELETION = define_event(
        "after_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION_FAILURE = define_event(
        "catalog_hard_deletion_failure", EventScope.PLATFORM
    )

    # Collection Lifecycle
    BEFORE_COLLECTION_CREATION = define_event(
        "before_collection_creation", EventScope.CATALOG
    )
    COLLECTION_CREATION = define_event("collection_creation", EventScope.CATALOG)
    AFTER_COLLECTION_CREATION = define_event(
        "after_collection_creation", EventScope.CATALOG
    )

    BEFORE_COLLECTION_UPDATE = define_event(
        "before_collection_update", EventScope.CATALOG
    )
    COLLECTION_UPDATE = define_event("collection_update", EventScope.CATALOG)
    AFTER_COLLECTION_UPDATE = define_event(
        "after_collection_update", EventScope.CATALOG
    )

    BEFORE_COLLECTION_DELETION = define_event(
        "before_collection_deletion", EventScope.CATALOG
    )
    COLLECTION_DELETION = define_event("collection_deletion", EventScope.CATALOG)
    AFTER_COLLECTION_DELETION = define_event(
        "after_collection_deletion", EventScope.CATALOG
    )

    BEFORE_COLLECTION_HARD_DELETION = define_event(
        "before_collection_hard_deletion", EventScope.CATALOG
    )
    COLLECTION_HARD_DELETION = define_event(
        "collection_hard_deletion", EventScope.CATALOG
    )
    AFTER_COLLECTION_HARD_DELETION = define_event(
        "after_collection_hard_deletion", EventScope.CATALOG
    )

    # Asset Lifecycle
    BEFORE_ASSET_CREATION = define_event("before_asset_creation", EventScope.COLLECTION)
    ASSET_CREATION = define_event("asset_creation", EventScope.COLLECTION)
    AFTER_ASSET_CREATION = define_event("after_asset_creation", EventScope.COLLECTION)

    BEFORE_ASSET_UPDATE = define_event("before_asset_update", EventScope.COLLECTION)
    ASSET_UPDATE = define_event("asset_update", EventScope.COLLECTION)
    AFTER_ASSET_UPDATE = define_event("after_asset_update", EventScope.COLLECTION)

    BEFORE_ASSET_DELETION = define_event("before_asset_deletion", EventScope.COLLECTION)
    ASSET_DELETION = define_event("asset_deletion", EventScope.COLLECTION)
    AFTER_ASSET_DELETION = define_event("after_asset_deletion", EventScope.COLLECTION)

    BEFORE_ASSET_HARD_DELETION = define_event(
        "before_asset_hard_deletion", EventScope.COLLECTION
    )
    ASSET_HARD_DELETION = define_event("asset_hard_deletion", EventScope.COLLECTION)
    AFTER_ASSET_HARD_DELETION = define_event(
        "after_asset_hard_deletion", EventScope.COLLECTION
    )


    # Item Lifecycle
    BEFORE_ITEM_CREATION = define_event("before_item_creation", EventScope.COLLECTION)
    ITEM_CREATION = define_event("item_creation", EventScope.COLLECTION)
    AFTER_ITEM_CREATION = define_event("after_item_creation", EventScope.COLLECTION)

    BEFORE_ITEM_UPDATE = define_event("before_item_update", EventScope.COLLECTION)
    ITEM_UPDATE = define_event("item_update", EventScope.COLLECTION)
    AFTER_ITEM_UPDATE = define_event("after_item_update", EventScope.COLLECTION)

    BEFORE_ITEM_DELETION = define_event("before_item_deletion", EventScope.COLLECTION)
    ITEM_DELETION = define_event("item_deletion", EventScope.COLLECTION)
    AFTER_ITEM_DELETION = define_event("after_item_deletion", EventScope.COLLECTION)

    BEFORE_ITEM_HARD_DELETION = define_event(
        "before_item_hard_deletion", EventScope.COLLECTION
    )
    ITEM_HARD_DELETION = define_event("item_hard_deletion", EventScope.COLLECTION)
    AFTER_ITEM_HARD_DELETION = define_event(
        "after_item_hard_deletion", EventScope.COLLECTION
    )

    # Bulk Event Lifecycle
    BEFORE_BULK_ITEM_CREATION = define_event("before_bulk_item_creation", EventScope.COLLECTION)
    BULK_ITEM_CREATION = define_event("bulk_item_creation", EventScope.COLLECTION)
    AFTER_BULK_ITEM_CREATION = define_event("after_bulk_item_creation", EventScope.COLLECTION)

    # Task Lifecycle Events (platform-scoped, fired by runners - no module coupling)
    TASK_FAILED = define_event("task.failed", EventScope.PLATFORM)

Listener = Callable[..., Coroutine[Any, Any, None]]


@runtime_checkable
class BackgroundRunner(Protocol):
    def run_coro(self, coro: Coroutine[Any, Any, None]) -> None: ...


class DefaultRunner:
    def run_coro(self, coro: Coroutine[Any, Any, None]) -> None:
        from dynastore.modules.concurrency import get_background_executor

        get_background_executor().submit(coro, task_name="event_listener_runner")


# --- Event Storage Extension Point ---


@runtime_checkable
class EventStorageSPI(Protocol):
    async def initialize(self, conn: DbResource) -> None: ...
    async def ensure_partition(
        self, conn: DbResource, catalog_id: str, schema: Optional[str] = None
    ) -> None: ...
    async def store(
        self,
        conn: DbResource,
        catalog_id: Optional[str],
        event_type: str,
        payload: Dict[str, Any],
        schema: Optional[str] = None,
    ) -> None: ...
    async def consume(
        self, conn: DbResource, limit: int = 100
    ) -> List[Dict[str, Any]]: ...
    async def search_events(
        self,
        engine: DbResource,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]: ...


class GlobalSystemEventStore:
    """
    Implementation of the Outbox Pattern for GLOBAL SYSTEM EVENTS.
    Includes explicit notification trigger on insert.
    """

    GLOBAL_SYSTEM_EVENTS_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.system_events (
        id BIGSERIAL,
        catalog_id VARCHAR,
        event_type VARCHAR NOT NULL,
        payload JSONB NOT NULL,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        status VARCHAR DEFAULT 'PENDING',
        PRIMARY KEY (id)
    );
    """

    # Notify trigger for the global table (distinct from tenant trigger)
    GLOBAL_TRIGGER_DDL = f"""
    CREATE OR REPLACE TRIGGER trigger_notify_global_event
    AFTER INSERT ON {SYSTEM_SCHEMA}.system_events
    FOR EACH ROW EXECUTE FUNCTION {SYSTEM_SCHEMA}.notify_system_event();
    """

    GLOBAL_NOTIFY_FUNC = f"""
    CREATE OR REPLACE FUNCTION {SYSTEM_SCHEMA}.notify_system_event() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('dynastore_events_channel', TG_TABLE_SCHEMA);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    _insert_query = DQLQuery(
        f"""
        INSERT INTO {SYSTEM_SCHEMA}.system_events (catalog_id, event_type, payload)
        VALUES (:catalog_id, :event_type, :payload)
        RETURNING id;
        """,
        result_handler=ResultHandler.SCALAR_ONE,
    )

    _consume_query = DQLQuery(
        f"""
        DELETE FROM {SYSTEM_SCHEMA}.system_events
        WHERE id IN (
            SELECT id
            FROM {SYSTEM_SCHEMA}.system_events
            WHERE status = 'PENDING'
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
        )
        RETURNING *;
        """,
        result_handler=ResultHandler.ALL_DICTS,
    )

    def __init__(self):
        super().__init__()
        from dynastore.tools.async_utils import AsyncBufferAggregator
        
        # Flushes up to 1000 items every 1 second
        self.aggregator = AsyncBufferAggregator(
            flush_callback=self._flush_batch,
            threshold=1000,
            interval=1.0,
            name="GlobalEventAggregator"
        )
        self._aggregator_started = False

    async def _flush_batch(self, items: List[Dict[str, Any]], engine: DbResource):
        # We use executemany for high throughput
        async with managed_transaction(engine) as conn:
            query = f"""
            INSERT INTO {SYSTEM_SCHEMA}.system_events (catalog_id, event_type, payload)
            VALUES (:catalog_id, :event_type, :payload);
            """
            # `execute_many` isn't formalized in DQLQuery optimally for simple DBAPI,
            # so we'll run a standard execute_many if underlying connection supports it,
            # or just loop if using synchronous wrapper. DQLQuery doesn't have execute_many exposed directly.
            # We'll run the inserts in a single transaction loop for now (still vastly faster than separate TXs)
            
            # Use raw connection executemany if available (asyncpg/psycopg)
            if hasattr(conn, "executemany"):
                await conn.executemany(query, items)
            else:
                for item in items:
                     await self._insert_query.execute(conn, **item)

    async def initialize(self, conn: DbResource) -> None:
        async def check_all(c=None, p=None):
            target = c or conn
            return await check_table_exists(
                target, "system_events", SYSTEM_SCHEMA
            ) and await check_function_exists(
                target, "notify_system_event", SYSTEM_SCHEMA
            )

        # Atomic initialization of all event store resources
        await DDLQuery(
            self.GLOBAL_SYSTEM_EVENTS_SCHEMA
            + self.GLOBAL_NOTIFY_FUNC
            + self.GLOBAL_TRIGGER_DDL,
            check_query=check_all,
            lock_key="global_system_event_store_init",
        ).execute(conn)

    async def store(
        self,
        conn: DbResource,
        catalog_id: Optional[str],
        event_type: str,
        payload: Dict[str, Any],
        schema: Optional[str] = None,
    ) -> None:
        
        item = {
            "catalog_id": catalog_id,
            "event_type": event_type,
            "payload": json.dumps(payload, cls=CustomJSONEncoder),
        }
        
        # If a transaction connection specifies explicitly, insert synchronously
        # to ensure it's part of the transaction.
        from dynastore.modules.db_config.query_executor import _is_in_transaction
        
        if _is_in_transaction(conn):
            await self._insert_query.execute(conn, **item)
        else:
            if not self._aggregator_started:
                from dynastore.modules.concurrency import default_executor
                default_executor.submit(self.aggregator.start(), "event_aggregator_start")
                self._aggregator_started = True
            await self.aggregator.add(item)

    async def consume(self, conn: DbResource, limit: int = 100) -> List[Dict[str, Any]]:
        return await self._consume_query.execute(conn, limit=limit)

    async def search_events(
        self,
        engine: DbResource,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
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

            # Global Events (always queried, since they might match)
            sys_where = where_clause
            if catalog_id == "_system_":
                sys_where = where_clause.replace("catalog_id = :catalog_id", "TRUE")

            queries.append(
                f"SELECT id, event_type, catalog_id, NULL as collection_id, NULL as item_id, payload, timestamp as created_at, status "
                f"FROM {SYSTEM_SCHEMA}.system_events WHERE {sys_where}"
            )

            # Tenant Events
            if catalog_id != "_system_":
                from dynastore.models.protocols import CatalogsProtocol
                from dynastore.tools.discovery import get_protocol
                from fastapi import HTTPException

                catalogs_provider: CatalogsProtocol = get_protocol(CatalogsProtocol)
                if not catalogs_provider:
                    raise HTTPException(
                        status_code=500, detail="CatalogsProtocol not available."
                    )

                physical_schema = await catalogs_provider.resolve_physical_schema(
                    catalog_id
                )
                if not physical_schema:
                    raise HTTPException(
                        status_code=404, detail=f"Catalog '{catalog_id}' not found."
                    )

                # Catalog scoped
                if not collection_id:
                    queries.append(
                        f"SELECT id, event_type, catalog_id, NULL as collection_id, NULL as item_id, payload, created_at, status "
                        f'FROM "{physical_schema}".catalog_events WHERE {where_clause}'
                    )

                # Collection scoped
                queries.append(
                    f"SELECT id, event_type, catalog_id, collection_id, item_id, payload, created_at, status "
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


class EventConsumerWorker:
    """
    Optimized background worker that listens for DB notifications (LISTEN/NOTIFY)
    to process events immediately, falling back to polling only as a health check.
    """

    CHANNEL_NAME = "dynastore_events_channel"

    def __init__(
        self,
        engine: DbResource,
        storage: EventStorageSPI,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        batch_size: int = 100,
        poll_interval: float = 10.0,  # Increased poll interval since we have NOTIFY
    ):
        self.engine = engine
        self.storage = storage
        self.handler = handler
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._task: Optional[asyncio.Task] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._running = False
        self._notify_event = asyncio.Event()

    async def start(self):
        """Starts the background worker loops."""
        if self._running:
            return
        self._running = True
        from dynastore.modules.concurrency import get_background_executor

        executor = get_background_executor()
        self._task = executor.submit(
            self._processing_loop(), task_name="EventProcessingLoop"
        )
        self._listen_task = executor.submit(
            self._listener_loop(), task_name="EventListenerLoop"
        )
        logger.info("EventConsumerWorker started with LISTEN/NOTIFY optimization.")

    async def stop(self):
        """Stops the background worker gracefully."""
        if not self._running:
            return
        self._running = False

        if self._listen_task:
            self._listen_task.cancel()

        if self._task:
            self._task.cancel()
            # Trigger event to wake up processing loop if it's waiting
            self._notify_event.set()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventConsumerWorker stopped.")

    async def _listener_loop(self):
        """Dedicated connection loop to LISTEN for notifications."""
        logger.info(f"Starting DB Listener on channel '{self.CHANNEL_NAME}'...")
        while self._running:
            conn = None
            try:
                # We need a raw asyncpg connection for LISTEN
                # Accessing the raw pool from the SQLAlchemy engine
                raw_conn = await self.engine.raw_connection()
                try:
                    conn = raw_conn.driver_connection

                    # Define callback
                    def notify_handler(connection, pid, channel, payload):
                        # logger.debug(f"Received notification on {channel}: {payload}")
                        self._notify_event.set()

                    await conn.add_listener(self.CHANNEL_NAME, notify_handler)

                    # Keep connection open and waiting
                    while self._running:
                        await asyncio.sleep(self.poll_interval)
                        # Ping or check connection health if needed
                        if conn.is_closed():
                            break

                finally:
                    if conn and not conn.is_closed():
                        try:
                            await conn.remove_listener(
                                self.CHANNEL_NAME, notify_handler
                            )
                        except Exception:
                            import traceback

                            traceback.print_exc()
                            raise
                    raw_conn.close()  # Return to pool/close wrapper

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DB Listener connection lost: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5.0)

    async def _processing_loop(self):
        """Main loop that processes events when notified or on timeout."""
        logger.info("EventConsumerWorker processing loop running.")
        while self._running:
            try:
                # 1. Process pending events
                has_more = await self._process_batch()

                # 2. If we processed events, there might be more (backlog). Don't wait.
                if has_more:
                    continue

                # 3. If no events, wait for notification OR timeout (heartbeat)
                try:
                    await asyncio.wait_for(
                        self._notify_event.wait(), timeout=self.poll_interval
                    )
                    # Clear event immediately to detect subsequent signals
                    self._notify_event.clear()
                except asyncio.TimeoutError:
                    # Timeout just means "check anyway" (heartbeat)
                    pass

            except asyncio.CancelledError:
                logger.debug("EventConsumerWorker processing task cancelled.")
                break
            except Exception as e:
                logger.error(
                    f"EventConsumerWorker crashed: {e}. Restarting loop in 5s...",
                    exc_info=True,
                )
                await asyncio.sleep(5.0)

    async def _process_batch(self) -> bool:
        """Fetch and process a batch. Returns True if a full batch was processed (implying more work)."""
        events = []
        async with managed_transaction(self.engine) as conn:
            events = await self.storage.consume(conn, limit=self.batch_size)

        if not events:
            return False

        for event in events:
            try:
                await self.handler(event)
            except Exception as e:
                logger.error(
                    f"Error processing event {event.get('id')}: {e}", exc_info=True
                )

        # If we fetched a full batch, return True to indicate immediate retry
        return len(events) >= self.batch_size


class EventService(EventsProtocol):
    """
    Manages event registration, emission, and persistence.
    Supports pluggable background execution strategies and persistent storage.
    """

    # Shared state for listeners across all instances (Singleton behavior for registration)
    _sync_listeners: Dict[str, List[Listener]] = defaultdict(list)
    _async_listeners: Dict[str, List[Listener]] = defaultdict(list)

    def __init__(
        self,
        runner: Optional[BackgroundRunner] = None,
        storage: Optional[EventStorageSPI] = None
    ):
        from dynastore.tools.discovery import get_protocol
        
        if not runner:
            discovered_runner = get_protocol(BackgroundRunner)
            self._runner: BackgroundRunner = discovered_runner if discovered_runner else DefaultRunner()
        else:
            self._runner = runner
            
        if not storage:
            discovered_storage = get_protocol(EventStorageSPI)
            self.storage: Optional[EventStorageSPI] = discovered_storage if discovered_storage else GlobalSystemEventStore()
        else:
            self.storage = storage

    def set_runner(self, runner: BackgroundRunner) -> None:
        self._runner = runner
        logger.info(f"EventService configured with runner: {type(runner).__name__}")

    def set_storage(self, storage: EventStorageSPI) -> None:
        self.storage = storage
        logger.info(f"EventService configured with storage: {type(storage).__name__}")

    def register(self, event_type: Union[EventType, str], listener: Listener):
        """Register an async listener for an event type (legacy, defaults to sync)."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        self._sync_listeners[e_val].append(listener)
        logger.debug(f"Registered listener {listener} for event {e_val}")

    def sync_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register a synchronous (in-transaction) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._sync_listeners[e_val].append(func)
            logger.info(
                f"Registered sync event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    def async_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register an asynchronous (background) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._async_listeners[e_val].append(func)
            logger.info(
                f"Registered async event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    async def emit(
        self,
        event_type: Union[EventType, str],
        *args,
        db_resource: Optional[DbResource] = None,
        raise_on_error: bool = False,
        **kwargs,
    ):
        """
        Emit an event.
        Persists to Outbox if db_resource is provided.
        Fires sync listeners immediately.
        Schedules async listeners.
        """
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        # Optional: Validate event existence
        if not EventRegistry.is_valid(e_val):
            logger.debug(f"Emitting unregistered event type: {e_val}")

        # 1. Persistence (Outbox Pattern)
        # Determine scope based on registry or heuristics
        scope = EventRegistry._events.get(e_val, EventScope.PLATFORM)

        # Override scope logic based on known types for backward compat if registry is empty/partial
        is_global_event = scope == EventScope.PLATFORM

        if db_resource:
            catalog_id = kwargs.get("catalog_id")
            payload = {"args": args, "kwargs": kwargs}

            async with managed_transaction(db_resource) as tx_conn:
                if is_global_event and self.storage:
                    await self.storage.store(tx_conn, catalog_id, e_val, payload)
                elif not is_global_event and catalog_id:
                    # For tenant-specific events, we need the physical schema
                    from dynastore.models.protocols import CatalogsProtocol
                    from dynastore.tools.discovery import get_protocol

                    catalogs = get_protocol(CatalogsProtocol)
                    phys_schema = (
                        await catalogs.resolve_physical_schema(
                            catalog_id, db_resource=tx_conn
                        )
                        if catalogs
                        else None
                    )

                    if phys_schema:
                        try:
                            collection_id = kwargs.get("collection_id")
                            if collection_id:
                                await _tenant_collection_event_insert_query.execute(
                                    tx_conn,
                                    schema=phys_schema,
                                    catalog_id=catalog_id,
                                    collection_id=collection_id,
                                    item_id=kwargs.get("item_id"),
                                    event_type=e_val,
                                    payload=json.dumps(payload, cls=CustomJSONEncoder),
                                )
                            else:
                                await _tenant_catalog_event_insert_query.execute(
                                    tx_conn,
                                    schema=phys_schema,
                                    catalog_id=catalog_id,
                                    asset_code=kwargs.get("asset_code"),
                                    event_type=e_val,
                                    payload=json.dumps(payload, cls=CustomJSONEncoder),
                                )
                        except QueryExecutionError as e:
                            logger.error(
                                f"Failed to store tenant event {e_val} for catalog {catalog_id} in schema {phys_schema}: {e}",
                                exc_info=True,
                            )
                            original = getattr(e, "original_exception", None)
                            # Check for 'check_violation' (23514) which occurs when no partition exists
                            if (
                                hasattr(original, "pgcode")
                                and original.pgcode == "23514"
                            ):
                                logger.warning(
                                    f"Tenant event storage skipped for {catalog_id}: partition missing."
                                )
                                return
                            raise
                    else:
                        logger.warning(
                            f"Cannot persist tenant event {e_val} for catalog {catalog_id}: physical schema not resolved."
                        )
                else:
                    pass  # Global event but no storage, or tenant event but no catalog code

        # 2. Sync Listeners (In-Transaction, Blocking)
        logger.debug(f"Emitting sync event {e_val}")
        for listener in self._sync_listeners.get(e_val, []):
            try:
                await listener(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Error in sync event listener for '{e_val}': {e}", exc_info=True
                )
                if raise_on_error:
                    raise e

        # 3. Async Listeners (Background, Non-Blocking)
        async_listeners = self._async_listeners.get(e_val, [])
        if async_listeners:
            logger.debug(
                f"Scheduling {len(async_listeners)} async listeners for {e_val}"
            )
            for listener in async_listeners:
                try:
                    coro = listener(*args, **kwargs)
                    self._runner.run_coro(coro)
                except Exception as e:
                    logger.error(
                        f"Error scheduling async listener for '{e_val}': {e}",
                        exc_info=True,
                    )

    def emit_side_effect(self, event_type: Union[EventType, str], *args, **kwargs):
        self.emit_detached(event_type, *args, **kwargs)

    def emit_detached(self, event_type: Union[EventType, str], *args, **kwargs):
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        listeners = self._async_listeners.get(e_val, [])
        if not listeners:
            return

        logger.debug(
            f"Scheduling detached event {e_val} for {len(listeners)} async listeners"
        )

        for listener in listeners:
            try:
                coro = listener(*args, **kwargs)
                self._runner.run_coro(coro)
            except Exception as e:
                logger.error(
                    f"Error scheduling detached listener for '{e_val}': {e}",
                    exc_info=True,
                )

    async def search_events(
        self,
        engine: DbResource,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Searches events across system and tenant schemas."""
        if self.storage:
            return await self.storage.search_events(
                engine=engine,
                catalog_id=catalog_id,
                collection_id=collection_id,
                event_type=event_type,
                limit=limit,
                offset=offset,
            )
        return []

    def create_consumer(
        self,
        engine: DbResource,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        batch_size: int = 100,
        poll_interval: float = 10.0,
    ) -> EventConsumerWorker:
        if not self.storage:
            raise RuntimeError("Cannot create consumer: No EventStorageSPI configured.")

        return EventConsumerWorker(
            engine, self.storage, handler, batch_size, poll_interval
        )

    @asynccontextmanager
    async def transaction(
        self, detached: bool = False, db_resource: Optional[DbResource] = None
    ):
        buffer: List[Tuple[str, tuple, dict]] = []

        async def emitter(event_type: Union[EventType, str], *args, **kwargs):
            e_val = (
                event_type.value if isinstance(event_type, EventType) else event_type
            )
            if db_resource and "db_resource" not in kwargs:
                kwargs["db_resource"] = db_resource

            # Store immediately if persistent
            await self.emit(event_type, *args, **kwargs)

            # Buffer for potential future use (though emit handles async dispatch already)
            # If 'detached' logic was intended to defer ALL processing, we'd need to block emit here.
            # But standard 'emit' separates sync/async.
            # This transaction manager seems to be for grouping logic primarily.
            buffer.append((e_val, args, kwargs))

        yield emitter


event_service = EventService()


def register_event_listener(event_type: Union[EventType, str], listener: Listener):
    event_service.register(event_type, listener)


sync_event_listener = event_service.sync_event_listener
async_event_listener = event_service.async_event_listener


async def emit_event(
    event_type: Union[EventType, str],
    *args,
    db_resource: Optional[DbResource] = None,
    **kwargs,
):
    await event_service.emit(event_type, *args, db_resource=db_resource, **kwargs)


async def process_queued_event(event: Dict[str, Any]):
    """
    Handler for the background worker.
    Deserializes events from the Outbox and processes them.
    """
    event_type_str = event.get("event_type")
    payload = event.get("payload", {})
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})

    logger.debug(f"Worker processing event: {event_type_str} (ID: {event.get('id')})")
    event_service.emit_detached(event_type_str, *args, **kwargs)
