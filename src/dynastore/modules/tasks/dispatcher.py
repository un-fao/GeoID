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

"""
tasks/dispatcher.py

Durable task dispatcher and Janitor for the DynaStore task system.

The Dispatcher:
  - Waits on the signal-bus for ``new_task_queued`` events (from QueueListener).
  - Atomically claims a PENDING task using SELECT … SKIP LOCKED.
  - Dispatches it to the appropriate runner (via runners.py).
  - On CancelledError, resets the task to PENDING so another instance can pick it up.

The Janitor:
  - Runs on the same event cycle as the Dispatcher (no separate thread needed).
  - After each Dispatcher wakeup, scans for ACTIVE tasks with expired
    visibility windows (dead runner, killed worker instance, etc.).
  - Resets tasks to PENDING (up to max_retries) or moves them to DEAD_LETTER.

Both are stateless: any worker instance can resume any task after a crash.
"""

import asyncio
import logging
import os
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks.queue import NEW_TASK_QUEUED
from dynastore.tools.async_utils import signal_bus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Runner identity (owner_id for task claiming)
# ---------------------------------------------------------------------------

def _runner_id() -> str:
    """
    Returns a stable runner identity for this process.
    Prioritizes RUNNER_ID, then NAME, falling back to hostname + pid.
    """
    runner_id = os.getenv("RUNNER_ID", "")
    if runner_id:
        return runner_id

    name = os.getenv("NAME", "")
    if name:
        import socket
        return f"{name}@{socket.gethostname()}"

    import socket
    return f"{socket.gethostname()}:{os.getpid()}"


_RUNNER_ID = _runner_id()


# ---------------------------------------------------------------------------
# Atomic task claim  (SKIP LOCKED — no lock contention, no deadlocks)
# ---------------------------------------------------------------------------

async def _claim_next_task(
    engine: AsyncEngine,
    schema: str,
    visibility_timeout: timedelta,
    task_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Atomically claims the next PENDING task for this runner.

    Uses SELECT … FOR UPDATE SKIP LOCKED so that multiple concurrent dispatcher
    instances never fight over the same row.  Returns the claimed task row, or
    None if the queue is empty.

    Args:
        task_types: If provided, only claim tasks whose task_type is in this list.
                    This implements task affinity — each service instance only
                    processes the task types it has loaded via DYNASTORE_TASK_MODULES.
    """
    if task_types is not None and len(task_types) == 0:
        return None  # No task types loaded — nothing to claim

    locked_until = datetime.now(timezone.utc) + visibility_timeout

    # Build the task_type filter clause using individual named params
    # (asyncpg doesn't support list binding via ANY(:param) through SQLAlchemy text())
    type_filter = ""
    type_params = {}
    if task_types is not None:
        placeholders = ", ".join(f":t_{i}" for i in range(len(task_types)))
        type_filter = f"AND task_type IN ({placeholders})"
        type_params = {f"t_{i}": t for i, t in enumerate(task_types)}

    sql = f"""
        UPDATE "{schema}".tasks
        SET status            = 'ACTIVE',
            locked_until      = :locked_until,
            last_heartbeat_at = NOW(),
            owner_id          = :owner_id,
            started_at        = COALESCE(started_at, NOW())
        WHERE (timestamp, task_id) = (
            SELECT timestamp, task_id
            FROM   "{schema}".tasks
            WHERE  status = 'PENDING'
              AND  (locked_until IS NULL OR locked_until <= NOW())
              {type_filter}
            ORDER  BY timestamp ASC
            LIMIT  1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING task_id, task_type, caller_id, inputs, collection_id,
                  retry_count, max_retries, timestamp;
    """
    bind_params = dict(locked_until=locked_until, owner_id=_RUNNER_ID, **type_params)

    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, **bind_params
        )
        if row:
            logger.debug(f"Dispatcher: Claimed task {row['task_id']} ({row['task_type']}).")
        return row


# ---------------------------------------------------------------------------
# Task state transitions
# ---------------------------------------------------------------------------

async def _complete_task(
    engine: AsyncEngine,
    schema: str,
    task_id: str,
    timestamp: datetime,
    outputs: Any = None,
) -> None:
    sql = f"""
        UPDATE "{schema}".tasks
        SET status      = 'COMPLETED',
            finished_at = NOW(),
            outputs     = CAST(:outputs AS jsonb),
            locked_until = NULL
        WHERE task_id = :task_id AND timestamp = :timestamp;
    """
    import json
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            task_id=str(task_id),
            timestamp=timestamp,
            outputs=json.dumps(outputs) if outputs is not None else "null",
        )
    
    # If this was a provisioning task, we might need a post-completion hook
    # But currently the task itself handles marking 'ready'.


async def _fail_task(
    engine: AsyncEngine,
    schema: str,
    task_id: str,
    timestamp: datetime,
    error: str,
    reset_to_pending: bool,
    retry_count: int,
    task_type: Optional[str] = None,
    inputs: Optional[Dict[str, Any]] = None,
) -> None:
    """Resets to PENDING (retry) or moves to DEAD_LETTER (exhausted)."""
    new_status = "PENDING" if reset_to_pending else "DEAD_LETTER"
    # If resetting to PENDING, add a brief exponential-ish backoff to avoid tight loops
    locked_until = None
    if reset_to_pending:
        # Simple backoff: 5s, 10s, 20s, 40s...
        backoff_seconds = 5 * (2 ** min(retry_count, 6))
        locked_until = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)

    sql = f"""
        UPDATE "{schema}".tasks
        SET status        = '{new_status}',
            error_message = :error,
            retry_count   = retry_count + 1,
            locked_until  = :locked_until,
            finished_at   = CASE WHEN '{new_status}' = 'DEAD_LETTER' THEN NOW() ELSE NULL END
        WHERE task_id = :task_id AND timestamp = :timestamp;
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            task_id=str(task_id),
            timestamp=timestamp,
            error=error,
            locked_until=locked_until,
        )
    
    # --- GENERIC TASK FAILURE EVENT ---
    if new_status == "DEAD_LETTER":
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols import EventsProtocol
            events_svc = get_protocol(EventsProtocol)
            if events_svc:
                await events_svc.emit(
                    "task.failed",
                    db_resource=engine,
                    task_id=str(task_id),
                    task_type=task_type or "unknown",
                    error_message=error,
                    inputs=inputs or {}
                )
        except Exception as e:
            logger.error(f"Dispatcher: Failed to emit task.failed event: {e}")


    logger.warning(
        f"Dispatcher: Task {task_id} ({task_type or 'unknown'}) → {new_status} "
        f"(error: {error[:120]}{'…' if len(error) > 120 else ''})"
    )


# ---------------------------------------------------------------------------
# Janitor — recovery of stale ACTIVE tasks
# ---------------------------------------------------------------------------

async def _run_janitor(engine: AsyncEngine, schema: str) -> None:
    """
    Finds ACTIVE tasks with expired visibility windows (dead runners) and
    either resets them to PENDING (up to max_retries) or sends to DEAD_LETTER.
    Called once per Dispatcher wakeup cycle.

    Uses pg_try_advisory_xact_lock to elect a single Janitor leader across
    all Cloud Run instances. The lock is application-level (not object-level)
    so all instances competing for the same schema share a single lock key.
    """
    from dynastore.modules.tasks.maintenance import find_stale_active_tasks

    # Derive a stable 63-bit lock key from the schema name so different
    # schemas/tenants each get an independent Janitor leader election.
    _JANITOR_LOCK_KEY = hash(f"dynastore.janitor.{schema}") & 0x7FFFFFFFFFFFFFFF

    async with managed_transaction(engine) as conn:
        # NON-BLOCKING advisory lock — returns immediately.
        # Only one instance across all Cloud Run replicas wins per cycle.
        # The lock is auto-released when this transaction ends.
        lock_acquired = await DQLQuery(
            "SELECT pg_try_advisory_xact_lock(:lock_key);",
            result_handler=ResultHandler.SCALAR
        ).execute(conn, lock_key=_JANITOR_LOCK_KEY)

        if not lock_acquired:
            # We didn't get the lock, so another active Janitor is running.
            pass  # Removed noisy debug log: "Janitor: Another instance holds the advisory lock. Skipping run."
            return

        # Removed noisy debug log: "Janitor: Advisory lock acquired. Running janitor tasks."

        stale = await find_stale_active_tasks(conn, schema) # Pass conn directly
        if not stale:
            return

        logger.info(f"Janitor: Found {len(stale)} stale ACTIVE task(s).")
        for row in stale:
            task_id   = str(row["task_id"])
            retry_count = int(row.get("retry_count", 0))
            max_retries = int(row.get("max_retries", 3))
            reset = retry_count < max_retries
            await _fail_task(
                engine, # Keep engine for _fail_task as it manages its own transaction
                schema,
                task_id=task_id,
                timestamp=row["timestamp"] if "timestamp" in row else datetime.now(timezone.utc),
                error=f"Janitor: visibility window expired (owner={row.get('owner_id')!r})",
                reset_to_pending=reset,
                retry_count=retry_count,
                task_type=row.get("task_type"),
                inputs=row["inputs"] if isinstance(row.get("inputs"), dict) else (json.loads(row["inputs"]) if row.get("inputs") else None),
            )
            if not reset:
                logger.error(
                    f"Janitor: Task {task_id} moved to DEAD_LETTER after "
                    f"{retry_count}/{max_retries} retries."
                )


# ---------------------------------------------------------------------------
# HeartbeatManager — prevents Janitor from reclaiming in-progress tasks
# ---------------------------------------------------------------------------

class HeartbeatManager:
    """
    Periodically refreshes the locked_until timestamp for an ACTIVE task.
    Run as a background asyncio task alongside the runner coroutine.

    Example (inside a runner)::

        async with HeartbeatManager(engine, schema, task_id, timestamp):
            result = await do_long_work()
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        task_id: str,
        timestamp: datetime,
        interval: timedelta = timedelta(seconds=30),
        visibility_timeout: timedelta = timedelta(minutes=5),
    ):
        self._engine = engine
        self._schema = schema
        self._task_id = task_id
        self._timestamp = timestamp
        self._interval = interval.total_seconds()
        self._visibility_timeout = visibility_timeout
        self._task: Optional[asyncio.Task] = None

    async def _beat(self) -> None:
        sql = f"""
            UPDATE "{self._schema}".tasks
            SET locked_until      = :locked_until,
                last_heartbeat_at = NOW()
            WHERE task_id  = :task_id
              AND timestamp = :timestamp
              AND status    = 'ACTIVE';
        """
        while True:
            await asyncio.sleep(self._interval)
            try:
                new_lock = datetime.now(timezone.utc) + self._visibility_timeout
                async with managed_transaction(self._engine) as conn:
                    await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                        conn,
                        locked_until=new_lock,
                        task_id=str(self._task_id),
                        timestamp=self._timestamp,
                    )
                logger.debug(
                    f"HeartbeatManager: Task {self._task_id} heartbeat extended to {new_lock.isoformat()}."
                )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"HeartbeatManager: Heartbeat failed for {self._task_id}: {e}")

    async def __aenter__(self):
        self._task = asyncio.create_task(self._beat())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# ---------------------------------------------------------------------------
# Dispatcher loop
# ---------------------------------------------------------------------------

async def run_dispatcher(
    engine: AsyncEngine,
    schema: Optional[str],
    shutdown_event: asyncio.Event,
    visibility_timeout: timedelta = timedelta(minutes=5),
    signal_timeout: float = 35.0,
) -> None:
    """
    Main dispatcher loop.

    Waits for a ``new_task_queued`` signal-bus event, atomically claims a
    PENDING task, dispatches it to the registered runners, and updates the
    task state on completion or failure.

    Also runs the Janitor on every wakeup cycle.

    Args:
        engine:             Async database engine.
        schema:             Schema that contains the tasks table (e.g. ``"tasks"``).
                            If None, enables Multi-Tenant mode (auto-discovers schemas).
        shutdown_event:     Set this to stop the dispatcher cleanly.
        visibility_timeout: How long a claimed task stays ACTIVE before the
                            Janitor can reclaim it (heartbeat extends this).
        signal_timeout:     Max seconds to wait for a signal before running
                            the Janitor anyway (defensive polling).
    """
    from dynastore.modules.tasks.runners import get_runners
    from dynastore.modules.tasks.models import TaskExecutionMode, RunnerContext
    from dynastore.tasks import get_loaded_task_types
    from dynastore.modules.tasks.queue import get_and_clear_dirty_schemas

    # Resolve which task types this instance can execute.
    # In a distributed deployment each service loads a different subset.
    loaded_types = list(get_loaded_task_types())
    is_multi_tenant = schema is None

    logger.info(
        f"Dispatcher: Started (schema={schema!r}, multi_tenant={is_multi_tenant}, "
        f"runner={_RUNNER_ID!r}, task_types={loaded_types or 'ALL'})."
    )
    _last_janitor_run: datetime = datetime.now(timezone.utc) - timedelta(seconds=signal_timeout)
    
    # Track which schemas we've already discovered in multi-tenant mode to avoid 
    # hitting CatalogsProtocol on every single wakeup.
    known_tenant_schemas: Set[str] = set()
    _last_schema_refresh: datetime = datetime.now(timezone.utc) - timedelta(hours=1)

    while not shutdown_event.is_set():
        try:
            # Wait for a notification or periodic wakeup
            got_signal = await signal_bus.wait_for(NEW_TASK_QUEUED, timeout=signal_timeout)
            
            # Collect schemas that need checking
            dirty_schemas = get_and_clear_dirty_schemas()
            schemas_to_check: Set[str] = set(dirty_schemas)
            
            if not is_multi_tenant:
                schemas_to_check.add(schema)
            else:
                # In multi-tenant mode, if we haven't seen a specific signal recently,
                # or if it's a periodic janitor wakeup, we refresh all catalog schemas.
                now = datetime.now(timezone.utc)
                if not got_signal or (now - _last_schema_refresh).total_seconds() > 300: # 5 min refresh
                    from dynastore.modules import get_protocol
                    from dynastore.models.protocols import CatalogsProtocol
                    catalogs_proto = get_protocol(CatalogsProtocol)
                    if catalogs_proto:
                        try:
                            # Avoid importing CatalogModule directly here to prevent circular deps
                            catalog_list = await catalogs_proto.list_catalogs(limit=1000)
                            for cat in catalog_list:
                                s = await catalogs_proto.resolve_physical_schema(cat.id)
                                if s: known_tenant_schemas.add(s)
                            _last_schema_refresh = now
                        except Exception as e:
                            logger.warning(f"Dispatcher: Failed to refresh catalog schemas: {e}")
                
                # If we're waking up defensively (timeout), check all known schemas.
                # If we're waking up on signal, we only check the dirty ones (already handled).
                if not got_signal:
                    schemas_to_check.update(known_tenant_schemas)

            # ── Process each schema ──────────────────────────────────────
            # We use a set of processed tasks to avoid duplicates if 
            # signals arrive while we are already processing.
            for s in schemas_to_check:
                if shutdown_event.is_set(): break

                # ── Janitor pass (throttled: once per signal_timeout at most) ──
                now = datetime.now(timezone.utc)
                if (now - _last_janitor_run).total_seconds() >= signal_timeout / 2:
                    await _run_janitor(engine, s)
                    _last_janitor_run = datetime.now(timezone.utc)

                # ── Claim + dispatch ─────────────────────────────────────────
                # Claim as many as we can from this schema before moving on
                while not shutdown_event.is_set():
                    row = await _claim_next_task(
                        engine, s, visibility_timeout,
                        task_types=loaded_types if loaded_types else None,
                    )
                    if row is None:
                        break  # Queue empty for this schema
                    
                    task_id   = row["task_id"]
                    task_type = row["task_type"]
                    timestamp = row["timestamp"]
                    logger.info(f"Dispatcher: Claimed task {task_id} ({task_type}) from schema {s!r}.")

                    from dynastore.tasks import get_task_instance, hydrate_task_payload

                    # Define raw_payload here so it's available for error handling/hooks
                    raw_payload = {
                        "task_id": task_id,
                        "caller_id": row.get("caller_id") or "",
                        "inputs": row["inputs"] if isinstance(row.get("inputs"), dict) else (json.loads(row["inputs"]) if row.get("inputs") else {}),
                    }

                    runners = get_runners(TaskExecutionMode.ASYNCHRONOUS)
                    if not runners:
                        logger.warning(
                            f"Dispatcher: No ASYNC runners registered — resetting task {task_id}."
                        )
                        await _fail_task(engine, s, str(task_id), timestamp,
                                         "No runners registered", reset_to_pending=True,
                                         retry_count=int(row.get("retry_count", 0)),
                                         task_type=task_type,
                                         inputs=raw_payload.get("inputs"))
                        continue

                    context = RunnerContext(
                        engine=engine,
                        task_type=task_type,
                        caller_id=raw_payload["caller_id"],
                        inputs=raw_payload["inputs"],
                        db_schema=s, # Use the specific schema from the loop
                        extra_context={"task_id": str(task_id)},
                    )

                    try:
                        async with HeartbeatManager(engine, s, str(task_id), timestamp,
                                                    visibility_timeout=visibility_timeout):
                            result = None

                            # 1. Try specialized Runners first.
                            for runner in runners:
                                caps = getattr(runner, 'capabilities', None)
                                if caps is not None and getattr(caps, 'requires_request_context', False):
                                    continue
                                # Also skip pure in-process initiators
                                if runner.__class__.__name__ in ("BackgroundRunner", "SyncRunner", "GcpCloudRunRunner"):
                                    continue

                                logger.debug(f"Dispatcher: Running task {task_id} via runner '{runner.__class__.__name__}'.")
                                result = await runner.run(context)
                                if result is not None:
                                    logger.info(f"Dispatcher: Task {task_id} handled by runner '{runner.__class__.__name__}'.")
                                    break
                            
                            # 2. Fallback: Direct task execution via TaskProtocol singleton
                            if result is None:
                                task_instance = get_task_instance(task_type)
                                if task_instance:
                                    logger.info(f"Dispatcher: Executing task '{task_type}' via TaskProtocol singleton.")
                                    hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
                                    result = await task_instance.run(hydrated_payload)
                                    logger.info(f"Dispatcher: Task {task_id} via TaskProtocol singleton completed.")
                                else:
                                    raise RuntimeError(f"No runner or task implementation found for '{task_type}'.")

                        await _complete_task(engine, s, str(task_id), timestamp, outputs=result)
                        logger.info(f"Dispatcher: Task {task_id} completed successfully in schema {s!r}.")

                    except asyncio.CancelledError:
                        logger.warning(
                            f"Dispatcher: Task {task_id} interrupted (CancelledError) — resetting to PENDING."
                        )
                        await _fail_task(engine, s, str(task_id), timestamp,
                                         "Runner interrupted (SIGTERM)", reset_to_pending=True,
                                         retry_count=int(row.get("retry_count", 0)),
                                         task_type=task_type,
                                         inputs=raw_payload.get("inputs"))
                        raise

                    except Exception as e:
                        import traceback
                        logger.error(f"Dispatcher: Task {task_id} failed with error: {e}\n{traceback.format_exc()}")
                        retry_count = int(row.get("retry_count", 0))
                        max_retries = int(row.get("max_retries", 3))
                        reset = retry_count < max_retries
                        await _fail_task(engine, s, str(task_id), timestamp,
                                         str(e), reset_to_pending=reset,
                                         retry_count=retry_count,
                                         task_type=task_type,
                                         inputs=raw_payload.get("inputs"))

        except asyncio.CancelledError:
            logger.info("Dispatcher: Cancelled — shutting down.")
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(f"Dispatcher: Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(2.0)

    logger.info("Dispatcher: Stopped.")
