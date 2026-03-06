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
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Optional
from typing import AsyncGenerator
from async_lru import alru_cache
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
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
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

# --- DDL Definitions ---

TENANT_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS \"{schema}\".tasks (
    task_id           UUID          NOT NULL,
    caller_id         VARCHAR(255),
    task_type         VARCHAR       NOT NULL,
    type              VARCHAR       NOT NULL DEFAULT 'task',
    status            VARCHAR       NOT NULL DEFAULT 'PENDING',
    progress          INT           DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    inputs            JSONB,
    outputs           JSONB,
    error_message     TEXT,
    timestamp         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    collection_id     VARCHAR(255),
    -- Durable queue fields (populated by dispatcher / heartbeat manager)
    locked_until      TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    owner_id          VARCHAR(255),
    retry_count       INT           NOT NULL DEFAULT 0,
    max_retries       INT           NOT NULL DEFAULT 3,
    PRIMARY KEY (timestamp, task_id)
) PARTITION BY RANGE (timestamp);
CREATE INDEX IF NOT EXISTS idx_tasks_caller    ON \"{schema}\".tasks (caller_id);
CREATE INDEX IF NOT EXISTS idx_tasks_timestamp ON \"{schema}\".tasks (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_queue     ON \"{schema}\".tasks (status, locked_until)
    WHERE status IN ('PENDING', 'ACTIVE');

CREATE OR REPLACE FUNCTION "{schema}".notify_task_ready()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('new_task_queued', '{schema}');
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_task_insert ON "{schema}".tasks;
CREATE TRIGGER on_task_insert
    AFTER INSERT ON "{schema}".tasks
    FOR EACH ROW EXECUTE FUNCTION "{schema}".notify_task_ready();
"""


@lifecycle_registry.sync_catalog_initializer(priority=100)
async def _initialize_tasks_tenant_slice(
    conn: DbResource, schema: str, catalog_id: str
):
    """Initializes the tasks module's slice of the tenant schema."""
    try:
        await ensure_task_storage_exists(conn, schema)
    except Exception:
        import traceback

        traceback.print_exc()
        raise


class TasksModule(ModuleProtocol):
    priority: int = 100
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

        logger.info("TasksModule: Entering lifespan...")

        try:
            db = resolve(DatabaseProtocol)
            engine = db.get_any_engine()
            logger.warning(f"DEBUG: TasksModule: Resolved engine: {engine}")
        except (RuntimeError, AttributeError) as e:
            logger.warning(f"DEBUG: TasksModule: Failed to resolve engine: {e}")
            engine = None

        logger.warning("DEBUG: TasksModule: Entering manage_tasks context...")
        async with manage_tasks(app_state):
            logger.warning("DEBUG: TasksModule: Task singletons active.")

            if engine is not None:
                executor = get_background_executor()
                schema = get_task_schema()

                # Ensure the tasks table exists before the dispatcher starts
                async with managed_transaction(engine) as conn:
                    await ensure_task_storage_exists(conn, schema)

                executor.submit(start_queue_listener(engine, shutdown_event))
                executor.submit(run_dispatcher(engine, None, shutdown_event))
                logger.info("TasksModule: QueueListener and Multi-Tenant Dispatcher launched.")
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
# These are for the default 'tasks' schema.
# For dynamic schemas, we will construct queries on the fly or use a dynamic builder.
# We keep these for backward compatibility and default usage.
_create_task_query = DQLQuery(
    "INSERT INTO {schema}.tasks (task_id, caller_id, task_type, inputs, timestamp) VALUES (:task_id, :caller_id, :task_type, :inputs, :timestamp) RETURNING *;",
    result_handler=ResultHandler.ONE_DICT,
)
_get_task_query = DQLQuery(
    "SELECT * FROM {schema}.tasks WHERE task_id = :task_id;",
    result_handler=ResultHandler.ONE_DICT,
)
_list_tasks_query = DQLQuery(
    "SELECT * FROM {schema}.tasks ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL_DICTS,
)


async def ensure_task_storage_exists(conn: DbResource, schema: str):
    """
    Ensures that the task table exists in the specified schema.
    Uses centralized DDL checking to avoid unnecessary logging during JIT calls.
    """
    from dynastore.modules.db_config.locking_tools import execute_safe_ddl
    from dynastore.modules.db_config import maintenance_tools
    
    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)
    
    async def table_exists_check():
        from dynastore.modules.db_config.locking_tools import check_table_exists
        return await check_table_exists(conn, "tasks", schema)

    # Create table formatted with the schema name securely.
    from dynastore.modules.db_config.query_executor import DDLQuery
    await DDLQuery(TENANT_TASKS_DDL).execute(
        conn,
        schema=schema,
        lock_key=f"{schema}_tasks",
        existence_check=table_exists_check,
    )

    # Ensure partitions for the task table in this schema
    await maintenance_tools.ensure_future_partitions(
        conn,
        schema=schema,
        table="tasks",
        interval="monthly",
        periods_ahead=12,
        column="timestamp",
    )
    await maintenance_tools.register_retention_policy(
        conn,
        schema=schema,
        table="tasks",
        policy="prune",
        interval="daily",
        retention_period="1 month",
        column="timestamp",
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

    schema = await catalog_protocol.resolve_physical_schema(catalog_id, db_resource)
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


# --- Low-level schema-based functions (for internal use) ---
async def create_task(engine: DbResource, task_data: TaskCreate, schema: str) -> Task:
    """
    Creates a new task within a specific physical schema.
    """
    from dynastore.tools.identifiers import generate_uuidv7

    task_id = generate_uuidv7()
    creation_time = datetime.now(timezone.utc)

    async with managed_transaction(engine) as conn:
        # Storage existence is ensured during module lifespan or tenant creation, 
        # not on every task submission to avoid excessive DDL overhead and logs.

        # Dynamic Insert - catalog_id is no longer needed in the table as it's isolated by schema
        sql = f"""
            INSERT INTO "{schema}".tasks (task_id, caller_id, task_type, type, inputs, timestamp, collection_id) 
            VALUES (:task_id, :caller_id, :task_type, :type, :inputs, :timestamp, :collection_id) 
            RETURNING *;
        """

        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn,
            task_id=task_id,
            caller_id=task_data.caller_id,
            task_type=task_data.task_type,
            type=task_data.type,
            inputs=json.dumps(task_data.inputs) if task_data.inputs else None,
            timestamp=creation_time,
            collection_id=task_data.collection_id,
        )
        get_task.cache_invalidate(conn, task_id, schema)
        task = Task.model_validate(task_dict)

        # Wake the dispatcher ONLY after the transaction actually commits.
        # If this is a nested transaction, it will wait for the outermost commit.
        from sqlalchemy import event
        from dynastore.modules.tasks.queue import NEW_TASK_QUEUED
        from dynastore.tools.async_utils import signal_bus
        
        # We need the sync connection from the async connection
        sync_conn = conn.sync_connection

        @event.listens_for(sync_conn, "commit")
        def _on_commit(connection):
            try:
                from dynastore.modules.tasks.queue import mark_schema_dirty
                mark_schema_dirty(schema)
                loop = asyncio.get_running_loop()
                loop.create_task(signal_bus.emit(NEW_TASK_QUEUED))
            except RuntimeError:
                pass 

    return task



async def update_task(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """
    Updates the status and other fields of an existing task within a tenant schema.
    """
    # Get only the fields that were explicitly set in the update request.
    update_fields = update_data.model_dump(exclude_unset=True)

    # The 'outputs' field needs to be explicitly serialized to a JSON string
    if "outputs" in update_fields and update_fields["outputs"] is not None:
        from dynastore.tools.json import CustomJSONEncoder
        update_fields["outputs"] = json.dumps(update_fields["outputs"], cls=CustomJSONEncoder)

    # Dynamically build the SET clause
    set_clauses = [f"{key} = :{key}" for key in update_fields.keys()]
    if not set_clauses:
        return await get_task(conn, task_id, schema)

    set_sql = ", ".join(set_clauses)

    sql = f'UPDATE "{schema}".tasks SET {set_sql} WHERE task_id = :task_id RETURNING *;'

    # Add the task_id to the parameters
    query_params = {**update_fields, "task_id": task_id}

    # Execute
    updated_task_dict = await DQLQuery(
        sql, result_handler=ResultHandler.ONE_DICT
    ).execute(conn, **query_params)

    get_task.cache_invalidate(conn, task_id, schema)
    return Task.model_validate(updated_task_dict) if updated_task_dict else None


@alru_cache(maxsize=256)
async def get_task(conn: DbResource, task_id: uuid.UUID, schema: str) -> Optional[Task]:
    """Retrieves a single task by its ID from a tenant schema."""
    sql = f'SELECT * FROM "{schema}".tasks WHERE task_id = :task_id;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
        conn, task_id=task_id
    )

    return Task.model_validate(task_dict) if task_dict else None


async def list_tasks(
    conn: DbResource, schema: str, limit: int = 20, offset: int = 0
) -> List[Task]:
    """Lists all tasks for a tenant, ordered by creation date."""
    sql = f'SELECT * FROM "{schema}".tasks ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;'
    task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, limit=limit, offset=offset
    )

    return [Task.model_validate(t) for t in task_dicts]


# --- Synchronous Wrappers for Task Runners ---
def update_task_sync(
    conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str
) -> Optional[Task]:
    """Synchronous wrapper for updating a task."""
    return run_in_event_loop(update_task(conn, task_id, update_data, schema))
