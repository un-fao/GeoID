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
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Optional, Callable, Awaitable, Any, TypeVar, Dict, AsyncGenerator, Set
from sqlalchemy import text, Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
    sync_managed_transaction,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Tracks which advisory lock keys are already held in the current async call stack.
# This makes acquire_lock_if_needed re-entrant: if the same key is requested again
# within the same coroutine chain, we skip the lock attempt (PostgreSQL advisory
# xact locks are re-entrant at the DB level too).
_held_lock_keys: ContextVar[Set[str]] = ContextVar("_held_lock_keys", default=None)


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
                    retryable = any(
                        x in err_str
                        for x in [
                            "55p03",
                            "40p01",
                            "lock_timeout",
                            "deadlock",
                            "operation is in progress",
                            "interfaceerror",
                            "connection does not exist",
                            "connection was closed",
                            "08003",
                            "databaseconnectionerror",
                            "connection is closed",
                            "connectionerror",
                        ]
                    )

                    if not retryable or attempt == max_retries - 1:
                        raise

                    delay = base_delay * (2**attempt)
                    logger.warning(
                        f"Conflict on wire/DB (attempt {attempt + 1}/{max_retries}): {e}\nRetrying in {delay:.2f}s..."
                    )
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
    hashed = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(hashed[:8], byteorder="big", signed=True)


@contextmanager
def sync_acquire_startup_lock(conn: DbResource, lock_key: str, timeout: str = "30s"):
    """
    Synchronous version of acquire_startup_lock for DDL coordination.
    """
    if isinstance(conn, Engine):
        with sync_managed_transaction(conn) as tx_conn:
            with sync_acquire_startup_lock(tx_conn, lock_key, timeout) as active:
                yield active
        return

    # Below here conn is guaranteed to be a Connection resource
    lock_id = _get_stable_lock_id(lock_key)

    # First try non-blocking
    # We use execute directly as DQLQuery is async-oriented or we need to check if it supports sync
    # DQLQuery executor is BaseExecutor which supports sync.

    q_try = DQLQuery(
        "SELECT pg_try_advisory_xact_lock(:lock_id)",
        result_handler=ResultHandler.SCALAR,
    )
    acquired = q_try._executor._execute_sync(
        conn,
        q_try._executor.query_builder_strategy.build(conn, {"lock_id": lock_id})[0],
        {"lock_id": lock_id},
    )

    if not acquired:
        logger.debug(f"Lock {lock_key} busy, waiting up to {timeout}...")
        conn.execute(text(f"SET LOCAL lock_timeout = '{timeout}'"))

        q_wait = DQLQuery(
            "SELECT pg_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.NONE
        )
        try:
            q_wait._executor._execute_sync(
                conn,
                q_wait._executor.query_builder_strategy.build(
                    conn, {"lock_id": lock_id}
                )[0],
                {"lock_id": lock_id},
            )
            acquired = True
        except Exception as e:
            logger.warning(
                f"Failed to acquire advisory lock {lock_key} within {timeout}: {e}"
            )
            raise

    if acquired:
        logger.debug(f"Acquired advisory lock (sync): {lock_key}")
        yield conn
    else:
        yield None


@asynccontextmanager
async def acquire_startup_lock(
    conn: DbResource, lock_key: str, timeout: str = "30s"
) -> AsyncGenerator[Optional[DbResource], None]:
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
    q_try = DQLQuery(
        "SELECT pg_try_advisory_xact_lock(:lock_id)",
        result_handler=ResultHandler.SCALAR,
    )
    acquired = await q_try.execute(conn, lock_id=lock_id)

    if not acquired:
        # If busy, wait with a timeout to prevent deadlocks
        logger.debug(f"Lock {lock_key} busy, waiting up to {timeout}...")
        # We use a local session timeout for safety during the lock wait.
        await conn.execute(text(f"SET LOCAL lock_timeout = '{timeout}'"))

        q_wait = DQLQuery(
            "SELECT pg_advisory_xact_lock(:lock_id)", result_handler=ResultHandler.NONE
        )
        try:
            await q_wait.execute(conn, lock_id=lock_id)
            acquired = True
        except Exception as e:
            logger.warning(
                f"Failed to acquire advisory lock {lock_key} within {timeout}: {e}"
            )
            raise  # Re-raise to ensure transaction rollback

    if acquired:
        logger.debug(f"Acquired advisory lock: {lock_key}")
        yield conn
    else:
        yield None


@asynccontextmanager
async def acquire_lock_if_needed(
    conn: DbResource, lock_key: str, check_fn: Callable[[], Awaitable[bool]]
):
    """
    Acquires an advisory lock only if the resource doesn't already exist.
    Uses acquire_startup_lock with the provided lock_key for correct per-resource
    lock coordination (avoids lock contention between different resources).

    Re-entrant: if the same lock_key is already held in the current async call
    stack (e.g. nested initialization), the lock attempt is skipped and the
    connection is yielded directly — matching PostgreSQL's own re-entrant
    advisory lock semantics.

    Yields the active connection if the lock was acquired (resource needs creation),
    or False if the resource already exists (no lock needed).
    """
    # Fast path: check without locking first
    if await check_fn():
        yield False
        return

    # Re-entrancy check: if this key is already held in the current call stack,
    # skip the lock attempt (PostgreSQL advisory xact locks are re-entrant at DB level).
    held = _held_lock_keys.get()
    if held is None:
        held = set()
        _held_lock_keys.set(held)

    if lock_key in held:
        logger.debug("Re-entrant lock request for key '%s' — skipping lock acquisition.", lock_key)
        yield conn
        return

    # Slow path: acquire the lock scoped to this specific resource key,
    # then re-check inside the lock to avoid TOCTOU races.
    held.add(lock_key)
    try:
        async with acquire_startup_lock(conn, lock_key) as active_conn:
            if active_conn is None:
                # Lock timed out — another process likely holds it.
                # Re-check: if it now exists, we can skip.
                if await check_fn():
                    yield False
                    return
                raise RuntimeError(f"Failed to acquire advisory lock for key: {lock_key}")

            # Re-check inside the lock (double-checked locking pattern)
            if await check_fn():
                yield False
                return

            yield active_conn
    finally:
        held.discard(lock_key)


async def execute_safe_ddl(
    conn: DbResource,
    ddl_statement: str,
    lock_key: Optional[str] = None,
    existence_check: Optional[Callable[[], Awaitable[bool]]] = None,
    **ddl_params,
):
    """
    [DEPRECATED] Safely executes a DDL block with granular locking and deduplication.
    Use DDLQuery(...) directly instead.
    """
    import warnings
    warnings.warn(
        "execute_safe_ddl is deprecated and will be removed in a future version. "
        "Use DDLQuery directly instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    # Simply use DDLQuery which now handles existence checks and locking centrally
    # We pass existence_check as a wrapped function if it exists
    query = DDLQuery(
        ddl_statement, 
        check_query=existence_check, 
        lock_key=lock_key
    )
    return await query.execute(conn, **ddl_params)


async def check_table_exists(
    conn: DbResource, table_name: str, schema: str = "platform"
) -> bool:
    """Checks if a table exists in the given schema."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    query = DQLQuery(
        "SELECT 1 FROM pg_tables WHERE schemaname = :schema AND tablename = :table",
        result_handler=ResultHandler.SCALAR,
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
        result_handler=ResultHandler.SCALAR,
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
        result_handler=ResultHandler.SCALAR,
    )
    try:
        return await query.execute(conn, extension=extension_name) is not None
    except Exception:
        return False


async def check_trigger_exists(
    conn: DbResource, trigger_name: str, schema: str = "platform"
) -> bool:
    """Checks if a trigger exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    query = DQLQuery(
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = :schema AND t.tgname = :name",
        result_handler=ResultHandler.SCALAR,
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
        result_handler=ResultHandler.SCALAR,
    )
    try:
        return await query.execute(conn, job_name=job_name) is not None
    except Exception:
        return False


async def check_function_exists(
    conn: DbResource, function_name: str, schema: str = "platform"
) -> bool:
    """Checks if a function exists."""
    from dynastore.modules.db_config.maintenance_tools import DQLQuery, ResultHandler

    query = DQLQuery(
        "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = :schema AND p.proname = :name",
        result_handler=ResultHandler.SCALAR,
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
            logger.warning(
                f"Terminated {count} backends locking objects in schema '{schema_name}'"
            )
        return count
    except Exception as e:
        logger.error(f"Failed to terminate backends for schema '{schema_name}': {e}")
        return 0


async def terminate_backends_locking_table(
    conn: DbResource, schema_name: str, table_name: str
) -> int:
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
            logger.warning(
                f"Terminated {count} backends locking table '{schema_name}.{table_name}'"
            )
        return count
    except Exception as e:
        logger.error(
            f"Failed to terminate backends for table '{schema_name}.{table_name}': {e}"
        )
        return 0


async def force_truncate_table(conn: DbResource, schema_name: str, table_name: str):
    """
    Forcefully clears a table using DELETE instead of TRUNCATE to avoid deadlocks.
    """
    await terminate_backends_locking_table(conn, schema_name, table_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'DELETE FROM "{schema_name}"."{table_name}";').execute(
        conn
    )


async def force_drop_schema(conn: DbResource, schema_name: str):
    """
    Forcefully drops a schema by terminating any blocking backends first.
    """
    await terminate_backends_locking_schema(conn, schema_name)
    # Give a small window for backends to actually exit
    await asyncio.sleep(0.1)
    await DDLQuery(f'DROP SCHEMA "{schema_name}" CASCADE;').execute(conn)
