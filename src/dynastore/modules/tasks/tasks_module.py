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

# dynastore/modules/tasks/tasks_module.py

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, AsyncGenerator
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext
from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    managed_transaction,
    ResultHandler,
    DbResource,
    run_in_event_loop,
)
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.partition_tools import (
    ensure_hierarchical_partitions_exist,
    PartitionDefinition,
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists,
    register_retention_policy,
    ensure_future_partitions,
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    check_table_exists,
)

from .models import Task, TaskCreate, TaskUpdate

logger = logging.getLogger(__name__)


def get_task_schema() -> str:
    """Returns the default schema for global tasks."""
    return os.getenv("DYNASTORE_TASK_SCHEMA", "tasks")


def get_task_lookback() -> timedelta:
    """Returns the lookback window for claim queries.

    Controls which partitions the planner scans — only partitions whose
    timestamp range overlaps [now - lookback, now] are touched.
    Configure via DYNASTORE_TASK_LOOKBACK_DAYS (default: 30).
    Set to match the retention period to avoid scanning pruned partitions.
    """
    days = int(os.getenv("DYNASTORE_TASK_LOOKBACK_DAYS", "30"))
    return timedelta(days=days)

# --- DDL Definitions ---

# --- Step 1: Table creation only (IF NOT EXISTS safe) ---

GLOBAL_TASKS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.tasks (
    task_id           UUID          NOT NULL,
    schema_name       VARCHAR(255)  NOT NULL,
    scope             VARCHAR(50)   NOT NULL DEFAULT 'CATALOG',
    caller_id         VARCHAR(255),
    task_type         VARCHAR       NOT NULL,
    type              VARCHAR       NOT NULL DEFAULT 'task',
    execution_mode    VARCHAR       NOT NULL DEFAULT 'ASYNCHRONOUS',
    status            VARCHAR       NOT NULL DEFAULT 'PENDING',
    progress          INT           DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    inputs            JSONB,
    outputs           JSONB,
    error_message     TEXT,
    dedup_key         VARCHAR(512),
    timestamp         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    collection_id     VARCHAR(255),
    locked_until      TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    owner_id          VARCHAR(255),
    retry_count       INT           NOT NULL DEFAULT 0,
    max_retries       INT           NOT NULL DEFAULT 3,
    PRIMARY KEY (timestamp, task_id)
) PARTITION BY RANGE (timestamp);
"""

# --- Step 2: Indexes and triggers (run AFTER migration so all columns exist) ---

GLOBAL_TASKS_INDEXES_DDL = """
-- Queue claim index: optimizes claim_next() SKIP LOCKED query
CREATE INDEX IF NOT EXISTS idx_tasks_queue
    ON {schema}.tasks (status, task_type, execution_mode, locked_until)
    WHERE status IN ('PENDING', 'ACTIVE');
CREATE INDEX IF NOT EXISTS idx_tasks_schema_status
    ON {schema}.tasks (schema_name, status);
-- Dedup index: includes timestamp (partition key) as PG requires it for
-- unique indexes on partitioned tables. Per-partition uniqueness.
-- cross-partition dedup enforced at the application layer in enqueue().
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_dedup
    ON {schema}.tasks (schema_name, dedup_key, timestamp)
    WHERE dedup_key IS NOT NULL AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER');
CREATE INDEX IF NOT EXISTS idx_tasks_caller
    ON {schema}.tasks (caller_id);
CREATE INDEX IF NOT EXISTS idx_tasks_timestamp
    ON {schema}.tasks (timestamp DESC);
-- task_id lookup index: enables complete/fail/heartbeat without full partition scan
CREATE INDEX IF NOT EXISTS idx_tasks_task_id
    ON {schema}.tasks (task_id);

CREATE OR REPLACE FUNCTION {schema}.notify_task_ready()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('new_task_queued', NEW.task_type);
    RETURN NEW;
END;
$$;

-- Guard trigger creation behind pg_trigger existence check.
-- DROP+CREATE TRIGGER takes AccessExclusiveLock on tasks and deadlocks against
-- a concurrently-live dispatcher in another pod (RowExclusiveLock from
-- claim_batch DML). Changes to trigger body are a migration concern.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'on_task_insert'
          AND tgrelid = '{schema}.tasks'::regclass
    ) THEN
        CREATE TRIGGER on_task_insert
            AFTER INSERT ON {schema}.tasks
            FOR EACH ROW
            WHEN (NEW.status = 'PENDING')
            EXECUTE FUNCTION {schema}.notify_task_ready();
    END IF;
END $do$;

CREATE OR REPLACE FUNCTION {schema}.notify_task_status_changed()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('task_status_changed', NEW.task_type || ':' || NEW.status);
    RETURN NEW;
END;
$$;

DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'on_task_status_update'
          AND tgrelid = '{schema}.tasks'::regclass
    ) THEN
        CREATE TRIGGER on_task_status_update
            AFTER UPDATE ON {schema}.tasks
            FOR EACH ROW
            WHEN (OLD.status IS DISTINCT FROM NEW.status)
            EXECUTE FUNCTION {schema}.notify_task_status_changed();
    END IF;
END $do$;
"""





from dynastore.modules import ModuleProtocol
from dynastore.models.protocols import TasksProtocol
from dynastore.models.protocols.task_queue import TaskQueueProtocol


class TasksModule(TaskQueueProtocol, ModuleProtocol):
    priority: int = 15  # Must start before CatalogModule (20) to create global tables

    # --- TasksProtocol CRUD (backward compat) ---

    async def create_task(
        self, engine: DbResource, task_data: Any, schema: str, initial_status: str = "PENDING"
    ) -> Any:
        return await create_task(engine, task_data, schema, initial_status=initial_status)

    async def update_task(
        self, conn: DbResource, task_id: uuid.UUID, update_data: Any, schema: str
    ) -> Optional[Any]:
        return await update_task(conn, task_id, update_data, schema)

    async def get_task(
        self, conn: DbResource, task_id: uuid.UUID, schema: str
    ) -> Optional[Any]:
        return await get_task(conn, task_id, schema)

    async def list_tasks(
        self, conn: DbResource, schema: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        return await list_tasks(conn, schema, limit, offset)

    # Catalog-aware versions
    async def create_task_for_catalog(
        self, engine: DbResource, task_data: Any, catalog_id: str
    ) -> Any:
        return await create_task_for_catalog(engine, task_data, catalog_id)

    async def get_task_for_catalog(
        self, conn: DbResource, task_id: uuid.UUID, catalog_id: str
    ) -> Optional[Any]:
        return await get_task_for_catalog(conn, task_id, catalog_id)

    async def list_tasks_for_catalog(
        self, conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        return await list_tasks_for_catalog(conn, catalog_id, limit, offset)

    # --- TaskQueueProtocol queue operations ---

    async def enqueue(
        self,
        engine: Any,
        task_data: Any,
        schema_name: str,
        dedup_key: Optional[str] = None,
        execution_mode: str = "ASYNCHRONOUS",
        scope: str = "CATALOG",
    ) -> Optional[Any]:
        return await enqueue(engine, task_data, schema_name, dedup_key, execution_mode, scope)

    async def claim_next(
        self,
        engine: Any,
        async_task_types: List[str],
        sync_task_types: List[str],
        visibility_timeout: timedelta,
        owner_id: str,
    ) -> Optional[Dict[str, Any]]:
        return await claim_next(engine, async_task_types, sync_task_types, visibility_timeout, owner_id)

    async def claim_batch_tasks(
        self,
        engine: Any,
        async_task_types: List[str],
        sync_task_types: List[str],
        visibility_timeout: timedelta,
        owner_id: str,
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        return await claim_batch(engine, async_task_types, sync_task_types, visibility_timeout, owner_id, batch_size)

    async def complete(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        outputs: Optional[Any] = None,
    ) -> None:
        return await complete_task(engine, task_id, timestamp, outputs)

    async def fail(
        self,
        engine: Any,
        task_id: uuid.UUID,
        timestamp: Any,
        error_message: str,
        retry: bool = True,
    ) -> None:
        return await fail_task(engine, task_id, timestamp, error_message, retry)

    async def heartbeat(
        self,
        engine: Any,
        task_ids: List[uuid.UUID],
        visibility_timeout: timedelta,
    ) -> None:
        return await heartbeat_tasks(engine, task_ids, visibility_timeout)

    async def find_stale(
        self,
        engine: Any,
        stale_threshold: timedelta,
        schema_name: Optional[str] = None,
    ) -> List[Any]:
        return await find_stale_tasks(engine, stale_threshold, schema_name)

    async def cleanup_orphans(self, engine: Any, grace_period: timedelta) -> int:
        return await cleanup_orphan_tasks(engine, grace_period)

    async def get_capable_task_types(self) -> Dict[str, List[str]]:
        from dynastore.modules.tasks.runners import capability_map
        return {
            "ASYNCHRONOUS": capability_map.async_types,
            "SYNCHRONOUS": capability_map.sync_types,
        }
    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Full lifecycle for the tasks subsystem:
          1. Initialise task singletons (runners, startup hooks) via manage_tasks.
          2. Start QueueListener and Dispatcher background loops (if a DB engine is available).
          3. On shutdown: signal dispatcher/listener to stop, then teardown singletons.
        """
        import asyncio
        from dynastore.modules.concurrency import get_background_executor
        from dynastore.modules.tasks.queue import start_queue_listener
        from dynastore.modules.tasks.dispatcher import run_dispatcher
        from dynastore.tools.protocol_helpers import get_engine
        from dynastore.tasks import manage_tasks

        logger.info("TasksModule: Initialising task singletons …")

        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.tasks import manage_tasks

        shutdown_event = asyncio.Event()

        try:
            db = resolve(DatabaseProtocol)
            engine = db.get_any_engine()
            logger.debug(f"TasksModule: Resolved engine: {engine}")
        except (RuntimeError, AttributeError) as e:
            logger.warning(f"TasksModule: Failed to resolve engine: {e}")
            engine = None

        async with manage_tasks(app_state):
            logger.info("TasksModule: Task singletons active.")

            if engine is not None:
                executor = get_background_executor()
                schema = get_task_schema()

                # Ensure the tasks table + current-month partition exist before
                # the dispatcher starts. The advisory lock must be held on the
                # SAME connection as the DDL, otherwise two concurrent revisions
                # can both observe "table missing" and race to create it (and
                # its partitions). Using the locked_conn yielded by
                # acquire_startup_lock guarantees that.
                from dynastore.modules.db_config.locking_tools import acquire_startup_lock
                async with acquire_startup_lock(
                    engine, f"tasks_storage_init.{schema}"
                ) as locked_conn:
                    if locked_conn is None:
                        raise RuntimeError(
                            f"TasksModule: could not acquire startup lock for '{schema}.tasks' "
                            "initialization — refusing to start dispatcher."
                        )
                    await ensure_task_storage_exists(locked_conn, schema)

                # Post-condition: verify current-month partition is visible on a
                # fresh connection. If it's not, crash loud — Cloud Run will
                # restart the pod rather than letting the dispatcher spin on
                # "relation does not exist".
                async with managed_transaction(engine) as probe_conn:
                    await _assert_current_partition_ready(probe_conn, schema)

                from dynastore.tools.discovery import get_protocol
                from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
                from dynastore.modules.tasks.tasks_config import TasksPluginConfig
                
                poll_interval = 30.0
                config_mgr = get_protocol(PlatformConfigsProtocol)
                if config_mgr:
                    try:
                        # get_config can fetch from cache or DB; since we have an engine inside manage_tasks, we pass it safely
                        tasks_config = await config_mgr.get_config(TasksPluginConfig, ctx=DriverContext(db_resource=engine))
                        if isinstance(tasks_config, TasksPluginConfig):
                            poll_interval = tasks_config.queue_poll_interval
                    except Exception as e:
                        logger.warning(f"TasksModule: Failed to load TasksPluginConfig, defaulting to {poll_interval}s: {e}")

                executor.submit(start_queue_listener(engine, shutdown_event, poll_timeout=poll_interval), task_name="service:queue_listener")
                executor.submit(run_dispatcher(engine, None, shutdown_event), task_name="service:dispatcher")
                logger.info(f"TasksModule: QueueListener (poll_interval={poll_interval}s) and Multi-Tenant Dispatcher launched.")
            else:
                logger.warning(
                    "TasksModule: No database engine available — "
                    "running without Dispatcher/QueueListener (on-premise / test mode)."
                )

            try:
                yield
            finally:
                shutdown_event.set()
                logger.info("TasksModule: Shutdown event set — QueueListener/Dispatcher stopping.")


# --- Internal Query Objects ---
# All queries target the global tasks table. The `schema_name` column
# distinguishes tenants; `get_task_schema()` returns the PostgreSQL schema
# that hosts the global table (default: "tasks").


async def _assert_current_partition_ready(conn: DbResource, schema: str) -> None:
    """
    Readiness probe: confirm the current-month partition of {schema}.tasks is
    visible before starting the dispatcher. Raises RuntimeError on failure.

    Uses to_regclass() on the fully-qualified child partition name (tasks_YYYY_MM)
    so that the check is reliable under concurrent DDL — unlike pg_tables, which
    can briefly lag.
    """
    now = datetime.now(timezone.utc)
    partition_name = f"tasks_{now.strftime('%Y_%m')}"
    fq_name = f'"{schema}"."{partition_name}"'
    result = await DQLQuery(
        "SELECT to_regclass(:fq)",
        result_handler=ResultHandler.SCALAR,
    ).execute(conn, fq=fq_name)
    if result is None:
        raise RuntimeError(
            f"TasksModule: current-month partition {schema}.{partition_name} is "
            "missing after ensure_task_storage_exists — refusing to start dispatcher."
        )
    logger.info(f"TasksModule: partition {schema}.{partition_name} is ready.")


async def ensure_task_storage_exists(conn: DbResource, schema: str):
    """
    Ensures that the global tasks table exists in the specified schema.
    Called at every startup. All steps are idempotent and must run every time:
    table/index DDL use IF NOT EXISTS, partition + retention/cron helpers all
    check-then-create. The table-existence check is NOT used to short-circuit
    the rest of this function — otherwise a restart after a month rollover
    would never create the current-month partition and the dispatcher would
    hit "relation does not exist" on claim_batch.

    Note: events table is now owned by EventsModule (priority=11).
    """
    from dynastore.modules.db_config import maintenance_tools

    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)

    async def tasks_table_exists():
        return await check_table_exists(conn, "tasks", schema)

    # Step 1: Create tasks table under advisory lock + existence guard to
    # serialize concurrent startups. Without this, two workers racing on
    # `CREATE TABLE tasks` both fall through IF NOT EXISTS's toctou window
    # and collide on the implicit composite type row in pg_type.
    await DDLQuery(
        GLOBAL_TASKS_TABLE_DDL,
        check_query=tasks_table_exists,
        lock_key=f"{schema}_tasks",
    ).execute(conn, schema=schema)

    # Step 2: Create indexes and triggers (idempotent: IF NOT EXISTS / OR REPLACE)
    await DDLQuery(GLOBAL_TASKS_INDEXES_DDL).execute(conn, schema=schema)

    # Step 3: Ensure current + future partitions exist.
    # Critical path — must succeed for the dispatcher to start.
    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=schema,
        table="tasks",
        interval="monthly",
        periods_ahead=12,
        column="timestamp",
    )

    # Steps 4+: pg_cron registration — operational, not structural.
    # If pg_cron is not installed or misconfigured these steps warn rather than
    # rolling back the table and partition creation above.
    try:
        await maintenance_tools.register_retention_policy(
            conn,
            schema=schema,
            table="tasks",
            policy="prune",
            interval="daily",
            retention_period="1 month",
            column="timestamp",
        )
    except Exception as e:
        logger.warning(
            f"TasksModule: register_retention_policy failed for {schema}.tasks "
            f"(pg_cron unavailable?): {e}"
        )

    try:
        await maintenance_tools.register_partition_creation_policy(
            conn,
            schema=schema,
            table="tasks",
            interval="monthly",
            periods_ahead=3,
        )
    except Exception as e:
        logger.warning(
            f"TasksModule: register_partition_creation_policy failed for {schema}.tasks "
            f"(pg_cron unavailable?): {e}"
        )



# --- Public API Functions ---


# Catalog-aware helper functions using CatalogsProtocol
async def _resolve_catalog_schema(
    catalog_id: str, db_resource: Optional[DbResource] = None
) -> str:
    """
    Resolves the physical schema for a catalog using CatalogsProtocol.
    This decouples tasks from direct catalog module dependencies.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalog_protocol = get_protocol(CatalogsProtocol)
    if not catalog_protocol:
        raise RuntimeError(
            "CatalogsProtocol not available - CatalogModule not initialized"
        )

    schema = await catalog_protocol.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None
    )
    if not schema:
        raise ValueError(f"Cannot resolve schema for catalog '{catalog_id}'")
    return schema


async def create_task_for_catalog(
    engine: DbResource, task_data: TaskCreate, catalog_id: str
) -> Task:
    """
    Creates a new task within a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    async with managed_transaction(engine) as conn:
        schema = await _resolve_catalog_schema(catalog_id, conn)
        return await create_task(engine, task_data, schema)


async def get_task_for_catalog(
    conn: DbResource, task_id: uuid.UUID, catalog_id: str
) -> Optional[Task]:
    """
    Retrieves a task from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await get_task(conn, task_id, schema)


async def list_tasks_for_catalog(
    conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0
) -> List[Task]:
    """
    Lists tasks from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await list_tasks(conn, schema, limit, offset)


async def update_task_for_catalog(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, catalog_id: str
) -> Optional[Task]:
    """
    Updates a task in a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await update_task(conn, task_id, update_data, schema)


# --- Low-level functions ---
# The `schema` parameter in these functions refers to the `schema_name` column
# value (e.g. tenant schema "s_abc123" or "system"), NOT the PostgreSQL schema
# that hosts the table.  The actual table lives in `get_task_schema()`.tasks.

async def create_task(
    engine: DbResource,
    task_data: TaskCreate,
    schema: str,
    initial_status: str = "PENDING",
) -> Task:
    """
    Creates a new task in the global tasks table with schema_name = `schema`.

    Pass initial_status='RUNNING' to bypass the dispatcher queue (e.g. for
    audit tasks created by BackgroundRunner that are already being executed
    in-process and must not be re-claimed by the dispatcher).
    """
    from dynastore.tools.identifiers import generate_uuidv7

    task_id = generate_uuidv7()
    creation_time = datetime.now(timezone.utc)
    task_schema = get_task_schema()

    async with managed_transaction(engine) as conn:
        sql = f"""
            INSERT INTO {task_schema}.tasks
                (task_id, schema_name, scope, caller_id, task_type, type,
                 execution_mode, inputs, timestamp, collection_id, dedup_key,
                 status)
            VALUES
                (:task_id, :schema_name, :scope, :caller_id, :task_type, :type,
                 :execution_mode, :inputs, :timestamp, :collection_id, :dedup_key,
                 :status)
            RETURNING *;
        """

        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            schema_name=schema,
            scope=task_data.scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=task_data.type,
            execution_mode=task_data.execution_mode,
            inputs=json.dumps(task_data.inputs) if task_data.inputs else None,
            timestamp=creation_time,
            collection_id=task_data.collection_id,
            dedup_key=task_data.dedup_key,
            status=initial_status,
        )
        get_task.cache_invalidate(conn, task_id, schema)
        task = Task.model_validate(task_dict)

    return task


async def update_task(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """
    Updates fields of an existing task in the global tasks table.
    """
    task_schema = get_task_schema()
    update_fields = update_data.model_dump(exclude_unset=True)

    if "outputs" in update_fields and update_fields["outputs"] is not None:
        from dynastore.tools.json import CustomJSONEncoder
        update_fields["outputs"] = json.dumps(update_fields["outputs"], cls=CustomJSONEncoder)

    set_clauses = [f"{key} = :{key}" for key in update_fields.keys()]
    if not set_clauses:
        return await get_task(conn, task_id, schema)

    set_sql = ", ".join(set_clauses)

    sql = f'UPDATE {task_schema}.tasks SET {set_sql} WHERE task_id = :task_id AND schema_name = :schema_name RETURNING *;'

    query_params = {**update_fields, "task_id": task_id, "schema_name": schema}

    updated_task_dict = await DQLQuery(
        sql, result_handler=ResultHandler.ONE_DICT
    ).execute(conn, **query_params)

    get_task.cache_invalidate(conn, task_id, schema)
    return Task.model_validate(updated_task_dict) if updated_task_dict else None


@cached(maxsize=256, namespace="tasks", ignore=["conn"])
async def get_task(conn: DbResource, task_id: uuid.UUID, schema: str) -> Optional[Task]:
    """Retrieves a single task by its ID from the global tasks table."""
    task_schema = get_task_schema()
    sql = f'SELECT * FROM {task_schema}.tasks WHERE task_id = :task_id AND schema_name = :schema_name;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
        conn, task_id=task_id, schema_name=schema
    )
    return Task.model_validate(task_dict) if task_dict else None


async def list_tasks(
    conn: DbResource, schema: str, limit: int = 20, offset: int = 0
) -> List[Task]:
    """Lists tasks filtered by schema_name, ordered by creation date."""
    task_schema = get_task_schema()
    sql = f'SELECT * FROM {task_schema}.tasks WHERE schema_name = :schema_name ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;'
    task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, schema_name=schema, limit=limit, offset=offset
    )
    return [Task.model_validate(t) for t in task_dicts]


# --- Synchronous Wrappers for Task Runners ---
def update_task_sync(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """Synchronous wrapper for updating a task."""
    return run_in_event_loop(update_task(conn, task_id, update_data, schema))


# --- TaskQueueProtocol implementation functions ---

async def enqueue(
    engine: DbResource,
    task_data: TaskCreate,
    schema_name: str,
    dedup_key: Optional[str] = None,
    execution_mode: str = "ASYNCHRONOUS",
    scope: str = "CATALOG",
) -> Optional[Task]:
    """
    Enqueue a task into the global task queue.

    If dedup_key is provided and already exists (for a non-terminal task),
    returns None instead of creating a duplicate.
    """
    # Override task_data fields with explicit parameters
    task_data.execution_mode = execution_mode
    task_data.scope = scope
    if dedup_key is not None:
        task_data.dedup_key = dedup_key

    from dynastore.tools.identifiers import generate_uuidv7

    task_id = generate_uuidv7()
    creation_time = datetime.now(timezone.utc)
    task_schema = get_task_schema()

    async with managed_transaction(engine) as conn:
        if dedup_key is not None:
            # Cross-partition dedup check: the UNIQUE index is per-partition
            # (PG requires partition key in unique indexes), so we do an
            # explicit check across all partitions before inserting.
            check_sql = f"""
                SELECT task_id FROM {task_schema}.tasks
                WHERE dedup_key = :dedup_key
                  AND schema_name = :schema_name
                  AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                LIMIT 1;
            """
            existing = await DQLQuery(
                check_sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, dedup_key=dedup_key, schema_name=schema_name)
            if existing:
                return None

            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, schema_name, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id, dedup_key)
                VALUES
                    (:task_id, :schema_name, :scope, :caller_id, :task_type, :type,
                     :execution_mode, :inputs, :timestamp, :collection_id, :dedup_key)
                ON CONFLICT (schema_name, dedup_key, timestamp)
                    WHERE dedup_key IS NOT NULL
                    AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
                DO NOTHING
                RETURNING *;
            """
        else:
            sql = f"""
                INSERT INTO {task_schema}.tasks
                    (task_id, schema_name, scope, caller_id, task_type, type,
                     execution_mode, inputs, timestamp, collection_id)
                VALUES
                    (:task_id, :schema_name, :scope, :caller_id, :task_type, :type,
                     :execution_mode, :inputs, :timestamp, :collection_id)
                RETURNING *;
            """

        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            schema_name=schema_name,
            scope=scope,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=task_data.type,
            execution_mode=execution_mode,
            inputs=json.dumps(task_data.inputs) if task_data.inputs else None,
            timestamp=creation_time,
            collection_id=task_data.collection_id,
            dedup_key=dedup_key,
        )

        if task_dict is None:
            # Dedup conflict — task already exists
            return None

        get_task.cache_invalidate(conn, task_id, schema_name)
        return Task.model_validate(task_dict)


async def claim_next(
    engine: DbResource,
    async_task_types: List[str],
    sync_task_types: List[str],
    visibility_timeout: timedelta,
    owner_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Atomically claim the next available task matching the given types and
    execution modes using FOR UPDATE SKIP LOCKED.
    """
    if not async_task_types and not sync_task_types:
        return None

    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout

    # Build WHERE conditions for execution mode + task type pairs
    conditions = []
    now = datetime.now(timezone.utc)
    # Partition pruning hint: only scan partitions within the lookback window.
    # Configurable via DYNASTORE_TASK_LOOKBACK_DAYS (default: 30).
    lookback = now - get_task_lookback()
    params: Dict[str, Any] = {
        "locked_until": locked_until,
        "owner_id": owner_id,
        "now": now,
        "lookback": lookback,
    }

    if async_task_types:
        conditions.append(
            "(execution_mode = 'ASYNCHRONOUS' AND task_type = ANY(:async_types))"
        )
        params["async_types"] = async_task_types

    if sync_task_types:
        conditions.append(
            "(execution_mode = 'SYNCHRONOUS' AND task_type = ANY(:sync_types))"
        )
        params["sync_types"] = sync_task_types

    mode_filter = " OR ".join(conditions)

    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE (timestamp, task_id) = (
            SELECT timestamp, task_id FROM {task_schema}.tasks
            WHERE status = 'PENDING'
              AND timestamp >= :lookback
              AND (locked_until IS NULL OR locked_until <= :now)
              AND ({mode_filter})
            ORDER BY timestamp ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, schema_name, scope, task_type, execution_mode,
                  caller_id, inputs, collection_id, retry_count, max_retries,
                  timestamp, dedup_key;
    """

    async with managed_transaction(engine) as conn:
        result = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, **params
        )

    return result


async def claim_batch(
    engine: DbResource,
    async_task_types: List[str],
    sync_task_types: List[str],
    visibility_timeout: timedelta,
    owner_id: str,
    batch_size: int = 10,
) -> List[Dict[str, Any]]:
    """
    Atomically claim up to ``batch_size`` available tasks matching the given
    types and execution modes using FOR UPDATE SKIP LOCKED.

    Returns a list of claimed task rows (may be empty).
    """
    if not async_task_types and not sync_task_types:
        return []

    task_schema = get_task_schema()
    locked_until = datetime.now(timezone.utc) + visibility_timeout

    conditions = []
    now = datetime.now(timezone.utc)
    lookback = now - get_task_lookback()
    params: Dict[str, Any] = {
        "locked_until": locked_until,
        "owner_id": owner_id,
        "now": now,
        "lookback": lookback,
        "batch_size": batch_size,
    }

    if async_task_types:
        conditions.append(
            "(execution_mode = 'ASYNCHRONOUS' AND task_type = ANY(:async_types))"
        )
        params["async_types"] = async_task_types

    if sync_task_types:
        conditions.append(
            "(execution_mode = 'SYNCHRONOUS' AND task_type = ANY(:sync_types))"
        )
        params["sync_types"] = sync_task_types

    mode_filter = " OR ".join(conditions)

    # Fairness: pick the oldest PENDING task per tenant (schema_name) first,
    # then fill remaining batch slots from those results. This prevents a
    # single high-volume tenant from monopolising all claim slots.
    # DISTINCT ON (schema_name) ORDER BY schema_name, timestamp ASC
    # returns exactly one row per tenant — the oldest eligible task.
    # DISTINCT ON and FOR UPDATE SKIP LOCKED cannot be combined in the same
    # SELECT (PostgreSQL forbids FOR UPDATE with DISTINCT). Use a two-step
    # approach: a CTE picks one candidate per tenant (oldest PENDING task via
    # DISTINCT ON), then the outer SELECT locks those specific rows.
    sql = f"""
        WITH candidates AS (
            SELECT DISTINCT ON (schema_name) timestamp, task_id
            FROM {task_schema}.tasks
            WHERE status = 'PENDING'
              AND timestamp >= :lookback
              AND (locked_until IS NULL OR locked_until <= :now)
              AND ({mode_filter})
            ORDER BY schema_name, timestamp ASC
        )
        UPDATE {task_schema}.tasks
        SET status = 'ACTIVE',
            locked_until = :locked_until,
            owner_id = :owner_id,
            started_at = COALESCE(started_at, NOW()),
            last_heartbeat_at = NOW()
        WHERE (timestamp, task_id) IN (
            SELECT timestamp, task_id
            FROM {task_schema}.tasks
            WHERE (timestamp, task_id) IN (SELECT timestamp, task_id FROM candidates)
              AND status = 'PENDING'
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, schema_name, scope, task_type, execution_mode,
                  caller_id, inputs, collection_id, retry_count, max_retries,
                  timestamp, dedup_key;
    """

    async with managed_transaction(engine) as conn:
        result = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )

    return result or []


async def complete_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    outputs: Optional[Any] = None,
) -> None:
    """Mark a claimed task as COMPLETED."""
    task_schema = get_task_schema()
    serialized_outputs = None
    if outputs is not None:
        from dynastore.tools.json import CustomJSONEncoder
        serialized_outputs = json.dumps(outputs, cls=CustomJSONEncoder)

    sql = f"""
        UPDATE {task_schema}.tasks
        SET status = 'COMPLETED',
            finished_at = :finished_at,
            outputs = :outputs,
            locked_until = NULL,
            owner_id = NULL
        WHERE task_id = :task_id;
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, task_id=task_id, finished_at=timestamp, outputs=serialized_outputs
        )


async def fail_task(
    engine: DbResource,
    task_id: uuid.UUID,
    timestamp: Any,
    error_message: str,
    retry: bool = True,
) -> None:
    """
    Mark a claimed task as failed. If retry=True and retries remain,
    requeue with exponential backoff. Otherwise move to DEAD_LETTER.
    """
    task_schema = get_task_schema()

    if retry:
        # Attempt retry: increment retry_count, reset to PENDING with backoff
        sql = f"""
            UPDATE {task_schema}.tasks
            SET status = CASE
                    WHEN retry_count + 1 < max_retries THEN 'PENDING'
                    ELSE 'DEAD_LETTER'
                END,
                error_message = :error_message,
                retry_count = retry_count + 1,
                locked_until = CASE
                    WHEN retry_count + 1 < max_retries
                    THEN NOW() + (POWER(2, retry_count + 1) || ' seconds')::INTERVAL
                    ELSE NULL
                END,
                finished_at = CASE
                    WHEN retry_count + 1 >= max_retries THEN :finished_at
                    ELSE finished_at
                END,
                owner_id = CASE
                    WHEN retry_count + 1 < max_retries THEN NULL
                    ELSE owner_id
                END
            WHERE task_id = :task_id;
        """
    else:
        sql = f"""
            UPDATE {task_schema}.tasks
            SET status = 'FAILED',
                error_message = :error_message,
                finished_at = :finished_at,
                locked_until = NULL,
                owner_id = NULL
            WHERE task_id = :task_id;
        """

    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, task_id=task_id, error_message=error_message, finished_at=timestamp
        )


async def heartbeat_tasks(
    engine: DbResource,
    task_ids: List[uuid.UUID],
    visibility_timeout: timedelta,
) -> None:
    """Extend locked_until for active tasks (batched heartbeat)."""
    if not task_ids:
        return

    task_schema = get_task_schema()
    new_locked_until = datetime.now(timezone.utc) + visibility_timeout

    sql = f"""
        UPDATE {task_schema}.tasks
        SET locked_until = :locked_until,
            last_heartbeat_at = NOW()
        WHERE task_id = ANY(:task_ids)
          AND status = 'ACTIVE';
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, locked_until=new_locked_until, task_ids=list(task_ids)
        )


async def find_stale_tasks(
    engine: DbResource,
    stale_threshold: timedelta,
    schema_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Find active tasks with expired locks (janitor use).
    If schema_name is provided, scopes to that tenant.
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - stale_threshold

    schema_filter = ""
    params: Dict[str, Any] = {"cutoff": cutoff}
    if schema_name is not None:
        schema_filter = "AND schema_name = :schema_name"
        params["schema_name"] = schema_name

    sql = f"""
        SELECT task_id, schema_name, task_type, execution_mode, retry_count, max_retries,
               owner_id, locked_until, last_heartbeat_at
        FROM {task_schema}.tasks
        WHERE status = 'ACTIVE'
          AND locked_until < :cutoff
          {schema_filter}
        ORDER BY locked_until ASC
        LIMIT 500;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )
    return rows or []


async def cleanup_orphan_tasks(
    engine: DbResource,
    grace_period: timedelta,
) -> int:
    """
    Move tasks for deleted catalogs to DEAD_LETTER.

    Checks schema_name against existing catalog schemas. Tasks whose
    schema_name no longer exists and whose creation timestamp is older
    than grace_period are dead-lettered.
    """
    task_schema = get_task_schema()
    cutoff = datetime.now(timezone.utc) - grace_period

    async with managed_transaction(engine) as conn:
        # catalog.catalogs may not exist on new DBs or partial inits — skip if absent
        if not await check_table_exists(conn, "catalogs", "catalog"):
            return 0

        # Find orphaned tasks: schema_name not in any active catalog schema
        # and task is not already in a terminal state
        sql = f"""
            WITH active_schemas AS (
                SELECT DISTINCT physical_schema
                FROM catalog.catalogs
                WHERE deleted_at IS NULL
            )
            UPDATE {task_schema}.tasks t
            SET status = 'DEAD_LETTER',
                error_message = 'Orphaned: catalog schema no longer exists',
                finished_at = NOW(),
                locked_until = NULL
            WHERE t.status IN ('PENDING', 'ACTIVE')
              AND t.scope = 'CATALOG'
              AND t.timestamp < :cutoff
              AND t.schema_name NOT IN (SELECT physical_schema FROM active_schemas)
              AND t.schema_name != 'system';
        """

        result = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, cutoff=cutoff
        )
    return result or 0
