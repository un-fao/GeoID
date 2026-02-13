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
#    See the License for the apecific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import hashlib
import logging
import asyncio
import functools
from contextlib import asynccontextmanager
from typing import Optional, Callable, Awaitable, Any, TypeVar, Dict, AsyncGenerator
from sqlalchemy import text, Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from dynastore.modules.db_config.query_executor import DQLQuery, DDLQuery, DbResource, ResultHandler, managed_transaction

logger = logging.getLogger(__name__)

T = TypeVar("T")

def retry_on_lock_conflict(max_retries: int = 5, base_delay: float = 0.5):
    """
    Decorator to retry database operations when encountering lock contention 
    or asyncpg protocol 'operation in progress' errors.
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_err = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_err = e
                    err_str = str(e).lower()
                    # Catch lock timeouts (55P03), deadlocks (40P01), 
                    # asyncpg InterfaceErrors (protocol busy),
                    # and DatabaseConnectionError/closed connection errors
                    retryable = any(x in err_str for x in [
                        "55p03", "40p01", "lock_timeout", "deadlock", 
                        "operation is in progress", "interfaceerror",
                        "connection does not exist", "connection was closed", "08003",
                        "databaseconnectionerror", "connection is closed", "connectionerror"
                    ])
                    
                    if not retryable or attempt == max_retries - 1:
                        raise
                    
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Conflict on wire/DB (attempt {attempt + 1}/{max_retries}): {e}\nRetrying in {delay:.2f}s...")
                    await asyncio.sleep(delay)
            if last_err:
                raise last_err
        return wrapper
    return decorator

# Global coordinator to dedupe identical startup tasks within the same process.
class _StartupCoordinator:
    _tasks: Dict[str, asyncio.Future] = {}
    _lock = asyncio.Lock()

    @classmethod
    async def run_once(cls, key: str, coro_func: Callable[[], Awaitable[T]]) -> T:
        async with cls._lock:
            if key in cls._tasks:
                return await cls._tasks[key]
            
            future = asyncio.Future()
            cls._tasks[key] = future

        # Try-finally block to ensure we cleanup on failure
        cleanup_task_ref = None
        try:
            result = await coro_func()
            if not future.done():
                future.set_result(result) 
            
            # Schedule cleanup for success case (keep result briefly)
            async def _cleanup():
                await asyncio.sleep(5)
                async with cls._lock:
                    # Only pop if it's still the SAME future
                    if cls._tasks.get(key) is future:
                        cls._tasks.pop(key, None)
            cleanup_task_ref = asyncio.create_task(_cleanup())
            
            return result
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            
            # Immediate cleanup on failure so retries can happen
            async with cls._lock:
                # Only pop if it's still the SAME future
                if cls._tasks.get(key) is future:
                    cls._tasks.pop(key, None)
            raise

def _get_stable_lock_id(key: str) -> int:
    """Generates a stable 64-bit integer from a string key for Postgres advisory locks."""
    hashed = hashlib.sha256(key.encode('utf-8')).digest()
    return int.from_bytes(hashed[:8], byteorder='big', signed=True)

@asynccontextmanager
async def acquire_startup_lock(conn: DbResource, lock_key: str, timeout: str = "30s") -> AsyncGenerator[Optional[DbResource], None]:
    """
    Acquires an advisory lock for coordination. 
    Serialization is handled internally by Query Executor.
    Ensures all operations happen on the same connection if an engine is provided.
    """
    if isinstance(conn, (AsyncEngine, Engine)):
        async with managed_transaction(conn) as tx_conn:
            async with acquire_startup_lock(tx_conn, lock_key, timeout) as active:
                yield active
        return

    # Below here conn is guaranteed to be a Connection resource
    lock_id = _get_stable_lock_id(lock_key)
    
    # First try non-blocking to be fast
    q_try = DQLQuery("SELECT pg_try_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.SCALAR)
    acquired = await q_try.execute(conn, lock_id=lock_id)
    
    if not acquired:
        # If busy, wait with a timeout to prevent deadlocks
        logger.debug(f"Lock {lock_key} busy, waiting up to {timeout}...")
        # We use a local session timeout for safety during the lock wait.
        await conn.execute(text(f"SET LOCAL lock_timeout = '{timeout}'"))
        
        q_wait = DQLQuery("SELECT pg_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.NONE)
        try:
            await q_wait.execute(conn, lock_id=lock_id)
            acquired = True
        except Exception as e:
            logger.warning(f"Failed to acquire advisory lock {lock_key} within {timeout}: {e}")
            raise # Re-raise to ensure transaction rollback
            
    if acquired:
        logger.debug(f"Acquired advisory lock: {lock_key}")
        yield conn
    else:
        yield None

@asynccontextmanager
async def acquire_lock_if_needed(conn: DbResource, lock_key: str, check_fn: Callable[[], Awaitable[bool]]):
    """
    High-level coordination for DDL blocks.
    Re-entrant and safe for cross-process (advisory) and in-process use.
    Handles its own transaction for the lock but yields the active connection.
    """
    # 1. Quick optimistic check
    try:
        if await check_fn():
            yield False
            return
    except Exception:
        pass

    # 2. Acquire DB-level lock and re-check
    async with managed_transaction(conn) as tx_conn:
        async with acquire_startup_lock(tx_conn, lock_key) as active:
            if active and not await check_fn():
                yield active
            else:
                yield False

async def execute_safe_ddl(
    conn: DbResource,
    ddl_statement: str,
    lock_key: Optional[str] = None,
    existence_check: Optional[Callable[[], Awaitable[bool]]] = None,
    **ddl_params
):
    """
    Safely executes a DDL block with granular locking and deduplication.
    If no lock_key is provided, one is generated from the DDL statement hash.
    """
    if existence_check and await existence_check():
        return

    # Use query hash if no key provided
    if not lock_key:
        stmt_hash = hashlib.sha256(ddl_statement.strip().encode()).hexdigest()[:16]
        lock_key = f"ddl.{stmt_hash}"

    async with acquire_lock_if_needed(conn, lock_key, existence_check or (lambda: asyncio.sleep(0, result=False))) as should_run:
        if should_run:
            # Execute individual statements
            statements = [s.strip() for s in ddl_statement.split(';') if s.strip()]
            async with managed_transaction(conn) as tx_conn:
                for stmt in statements:
                    await DDLQuery(stmt).execute(tx_conn, **ddl_params)

async def check_table_exists(conn: DbResource, table_name: str, schema: str = "public") -> bool:
    """Checks if a table exists in the given schema."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM pg_tables WHERE schemaname = :schema AND tablename = :table",
        result_handler=ResultHandler.SCALAR
    )
    try:
        res = await query.execute(conn, schema=schema, table=table_name)
        return res is not None
    except Exception:
        return False

async def check_schema_exists(conn: DbResource, schema_name: str) -> bool:
    """Checks if a schema exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM pg_namespace WHERE nspname = :schema",
        result_handler=ResultHandler.SCALAR
    )
    try:
        return await query.execute(conn, schema=schema_name) is not None
    except Exception:
        return False

async def check_extension_exists(conn: DbResource, extension_name: str) -> bool:
    """Checks if an extension is installed."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM pg_extension WHERE extname = :extension",
        result_handler=ResultHandler.SCALAR
    )
    try:
        return await query.execute(conn, extension=extension_name) is not None
    except Exception:
        return False

async def check_trigger_exists(conn: DbResource, trigger_name: str, schema: str = "public") -> bool:
    """Checks if a trigger exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :schema AND t.tgname = :name",
        result_handler=ResultHandler.SCALAR
    )
    try:
        return await query.execute(conn, schema=schema, name=trigger_name) is not None
    except Exception:
        return False

async def check_cron_job_exists(conn: DbResource, job_name: str) -> bool:
    """Checks if a pg_cron job exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM cron.job WHERE jobname = :job_name",
        result_handler=ResultHandler.SCALAR
    )
    try:
        return await query.execute(conn, job_name=job_name) is not None
    except Exception:
        return False

async def check_function_exists(conn: DbResource, function_name: str, schema: str = "public") -> bool:
    """Checks if a function exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler
    query = DQLQuery(
        "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = :schema AND p.proname = :name",
        result_handler=ResultHandler.SCALAR
    )
    try:
        return await query.execute(conn, schema=schema, name=function_name) is not None
    except Exception:
        return False

# --- Termination Helpers ---

async def terminate_backends_locking_schema(conn: DbResource, schema_name: str) -> int:
    """
    Terminates all backend processes holding locks on any object within a schema.
    Excludes the current connection's backend.
    """
    sql = """
    SELECT pg_terminate_backend(pid)
    FROM pg_locks l
    JOIN pg_class c ON l.relation = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = :schema
      AND pid <> pg_backend_pid();
    """
    q = DQLQuery(sql, result_handler=ResultHandler.ALL_SCALARS)
    try:
        results = await q.execute(conn, schema=schema_name)
        count = len(results)
        if count > 0:
            logger.warning(f"Terminated {count} backends locking objects in schema '{schema_name}'")
        return count
    except Exception as e:
        logger.error(f"Failed to terminate backends for schema '{schema_name}': {e}")
        return 0

async def terminate_backends_locking_table(conn: DbResource, schema_name: str, table_name: str) -> int:
    """
    Terminates all backend processes holding locks on a specific table.
    Excludes the current connection's backend.
    """
    sql = """
    SELECT pg_terminate_backend(pid)
    FROM pg_locks l
    JOIN pg_class c ON l.relation = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = :schema
      AND c.relname = :table
      AND pid <> pg_backend_pid();
    """
    q = DQLQuery(sql, result_handler=ResultHandler.ALL_SCALARS)
    try:
        results = await q.execute(conn, schema=schema_name, table=table_name)
        count = len(results)
        if count > 0:
            logger.warning(f"Terminated {count} backends locking table '{schema_name}.{table_name}'")
        return count
    except Exception as e:
        logger.error(f"Failed to terminate backends for table '{schema_name}.{table_name}': {e}")
        return 0

async def force_truncate_table(conn: DbResource, schema_name: str, table_name: str):
    """
    Forcefully truncates a table by terminating any blocking backends first.
    """
    await terminate_backends_locking_table(conn, schema_name, table_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'TRUNCATE TABLE "{schema_name}"."{table_name}" CASCADE;').execute(conn)

async def force_drop_schema(conn: DbResource, schema_name: str):
    """
    Forcefully drops a schema by terminating any blocking backends first.
    """
    await terminate_backends_locking_schema(conn, schema_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'DROP SCHEMA "{schema_name}" CASCADE;').execute(conn)
