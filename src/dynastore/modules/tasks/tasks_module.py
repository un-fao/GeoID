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

import logging
import uuid
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import List, Optional
from typing import AsyncGenerator
from async_lru import alru_cache
from dynastore.modules import ModuleProtocol, dynastore_module
from dynastore.modules.db_config.query_executor import (DDLQuery, DQLQuery, managed_transaction,
                                                        ResultHandler, DbResource, run_in_event_loop)
from dynastore.modules.db_config.tools import get_any_engine
from dynastore.modules.catalog.tenant_schema import register_tenant_initializer
from dynastore.modules.db_config.partition_tools import ensure_hierarchical_partitions_exist, PartitionDefinition
from dynastore.modules.db_config.maintenance_tools import ensure_future_partitions, acquire_startup_lock, ensure_schema_exists, execute_ddl_block, register_retention_policy
from dynastore.modules.db_config.locking_tools import acquire_lock_if_needed, check_table_exists

from .models import Task, TaskCreate, TaskUpdate

logger = logging.getLogger(__name__)

# --- DDL Definitions ---

TENANT_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.tasks (
    task_id UUID NOT NULL,
    caller_id VARCHAR(255),
    task_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'PENDING',
    progress INT DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    inputs JSONB,
    outputs JSONB,
    error_message TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    collection_id VARCHAR(255),
    PRIMARY KEY (timestamp, task_id)
) PARTITION BY RANGE (timestamp);
CREATE INDEX IF NOT EXISTS idx_tasks_caller ON {schema}.tasks (caller_id);
CREATE INDEX IF NOT EXISTS idx_tasks_timestamp ON {schema}.tasks (timestamp DESC);
"""

@register_tenant_initializer
async def _initialize_tasks_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the tasks module's slice of the tenant schema."""
    from dynastore.modules.db_config.locking_tools import execute_safe_ddl
    from dynastore.modules.db_config import maintenance_tools
    try:
        # Tasks table creation
        async def table_exists_check():
            return await check_table_exists(conn, "tasks", schema)

        await execute_safe_ddl(
            conn=conn,
            ddl_statement=TENANT_TASKS_DDL,
            lock_key=f"{schema}_tasks",
            existence_check=table_exists_check,
            schema=schema
        )
        
        await maintenance_tools.ensure_future_partitions(conn, schema=schema, table="tasks", interval="monthly", periods_ahead=12, column="timestamp")
        await maintenance_tools.register_retention_policy(conn, schema=schema, table="tasks", policy="prune", interval="daily", retention_period="1 month", column="timestamp")
    except Exception:
        import traceback
        traceback.print_exc()
        raise

@dynastore_module
class TasksModule(ModuleProtocol):
    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """
        Initializes the tasks module. 
        """
        logger.info("TasksModule: Initialized (Cellular Mode).")
        yield

# --- Internal Query Objects ---
# These are for the default 'tasks' schema. 
# For dynamic schemas, we will construct queries on the fly or use a dynamic builder.
# We keep these for backward compatibility and default usage.
_create_task_query = DQLQuery("INSERT INTO {schema}.tasks (task_id, caller_id, task_type, inputs, timestamp) VALUES (:task_id, :caller_id, :task_type, :inputs, :timestamp) RETURNING *;", result_handler=ResultHandler.ONE_DICT)
_get_task_query = DQLQuery("SELECT * FROM {schema}.tasks WHERE task_id = :task_id;", result_handler=ResultHandler.ONE_DICT)
_list_tasks_query = DQLQuery("SELECT * FROM {schema}.tasks ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;", result_handler=ResultHandler.ALL_DICTS)

async def ensure_task_storage_exists(conn: DbResource, schema: str):
    """
    Ensures that the task table exists in the specified schema.
    """
    # Ensure schema exists first
    await ensure_schema_exists(conn, schema)
    # Create table formatted with the schema name.
    await execute_ddl_block(conn, TENANT_TASKS_DDL, schema=schema)
    
    # Ensure partitions for the task table in this schema
    await ensure_future_partitions(conn, schema=schema, table="tasks", interval="monthly", periods_ahead=12, column="timestamp")
    await register_retention_policy(conn, schema=schema, table="tasks", policy="prune", interval="daily", retention_period="1 month", column="timestamp")


# --- Public API Functions ---

# Catalog-aware helper functions using CatalogsProtocol
async def _resolve_catalog_schema(catalog_id: str, db_resource: Optional[DbResource] = None) -> str:
    """
    Resolves the physical schema for a catalog using CatalogsProtocol.
    This decouples tasks from direct catalog module dependencies.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol
    
    catalog_protocol = get_protocol(CatalogsProtocol)
    if not catalog_protocol:
        raise RuntimeError("CatalogsProtocol not available - CatalogModule not initialized")
    
    schema = await catalog_protocol.resolve_physical_schema(catalog_id, db_resource)
    if not schema:
        raise ValueError(f"Cannot resolve schema for catalog '{catalog_id}'")
    return schema

async def create_task_for_catalog(engine: DbResource, task_data: TaskCreate, catalog_id: str) -> Task:
    """
    Creates a new task within a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    async with managed_transaction(engine) as conn:
        schema = await _resolve_catalog_schema(catalog_id, conn)
        return await create_task(engine, task_data, schema)

async def get_task_for_catalog(conn: DbResource, task_id: uuid.UUID, catalog_id: str) -> Optional[Task]:
    """
    Retrieves a task from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await get_task(conn, task_id, schema)

async def list_tasks_for_catalog(conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0) -> List[Task]:
    """
    Lists tasks from a catalog's schema.
    Uses CatalogsProtocol to resolve the physical schema.
    """
    schema = await _resolve_catalog_schema(catalog_id, conn)
    return await list_tasks(conn, schema, limit, offset)

async def update_task_for_catalog(conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, catalog_id: str) -> Optional[Task]:
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
    task_id = uuid.uuid4()
    creation_time = datetime.now(timezone.utc)

    async with managed_transaction(engine) as conn:
        await ensure_task_storage_exists(conn, schema)
        
        # Dynamic Insert - catalog_id is no longer needed in the table as it's isolated by schema
        sql = f"""
            INSERT INTO "{schema}".tasks (task_id, caller_id, task_type, inputs, timestamp, collection_id) 
            VALUES (:task_id, :caller_id, :task_type, :inputs, :timestamp, :collection_id) 
            RETURNING *;
        """
        
        task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, 
            task_id=task_id, 
            caller_id=task_data.caller_id,
            task_type=task_data.task_type, 
            inputs=json.dumps(task_data.inputs) if task_data.inputs else None,
            timestamp=creation_time,
            collection_id=task_data.collection_id
        )
        get_task.cache_invalidate(conn, task_id, schema)
        return Task.model_validate(task_dict)

async def update_task(conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str) -> Optional[Task]:
    """
    Updates the status and other fields of an existing task within a tenant schema.
    """
    # Get only the fields that were explicitly set in the update request.
    update_fields = update_data.model_dump(exclude_unset=True)

    # The 'outputs' field needs to be explicitly serialized to a JSON string
    if 'outputs' in update_fields and update_fields['outputs'] is not None:
        update_fields['outputs'] = json.dumps(update_fields['outputs'])
    
    # Dynamically build the SET clause
    set_clauses = [f"{key} = :{key}" for key in update_fields.keys()]
    if not set_clauses:
        return await get_task(conn, task_id, schema)
    
    set_sql = ", ".join(set_clauses)
    
    sql = f'UPDATE "{schema}".tasks SET {set_sql} WHERE task_id = :task_id RETURNING *;'
    
    # Add the task_id to the parameters
    query_params = {**update_fields, "task_id": task_id}
    
    # Execute
    updated_task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn, **query_params)
    
    get_task.cache_invalidate(conn, task_id, schema)
    return Task.model_validate(updated_task_dict) if updated_task_dict else None

@alru_cache(maxsize=256)
async def get_task(conn: DbResource, task_id: uuid.UUID, schema: str) -> Optional[Task]:
    """Retrieves a single task by its ID from a tenant schema."""
    sql = f'SELECT * FROM "{schema}".tasks WHERE task_id = :task_id;'
    task_dict = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn, task_id=task_id)

    return Task.model_validate(task_dict) if task_dict else None

async def list_tasks(conn: DbResource, schema: str, limit: int = 20, offset: int = 0) -> List[Task]:
    """Lists all tasks for a tenant, ordered by creation date."""
    sql = f'SELECT * FROM "{schema}".tasks ORDER BY timestamp DESC LIMIT :limit OFFSET :offset;'
    task_dicts = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn, limit=limit, offset=offset)
        
    return [Task.model_validate(t) for t in task_dicts]

# --- Synchronous Wrappers for Task Runners ---
def update_task_sync(conn: DbResource, task_id: uuid.UUID, update_data: TaskUpdate, schema: str) -> Optional[Task]:
    """Synchronous wrapper for updating a task."""
    return run_in_event_loop(update_task(conn, task_id, update_data, schema))
