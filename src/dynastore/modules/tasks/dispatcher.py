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

Uses a single global ``tasks.tasks`` table. No per-schema discovery is
needed: the ``claim_next()`` query filters by runner-aware capability map
types and returns the ``schema_name`` column so runners know which tenant
context to operate in.

The Dispatcher:
  - Waits on the signal-bus for ``new_task_queued`` events (from QueueListener).
  - Atomically claims a PENDING task via ``claim_next()`` (SKIP LOCKED).
  - Dispatches it to the appropriate runner (via runners.py).
  - On CancelledError, resets the task to PENDING so another instance can pick it up.

The Janitor:
  - Runs on the same event cycle as the Dispatcher (no separate thread needed).
  - After each Dispatcher wakeup, scans for ACTIVE tasks with expired
    visibility windows (dead runner, killed worker instance, etc.).
  - Resets tasks to PENDING (up to max_retries) or moves them to DEAD_LETTER.
  - Also cleans up orphaned tasks for deleted catalogs.

Both are stateless: any worker instance can resume any task after a crash.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy.ext.asyncio import AsyncEngine

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
# Batched heartbeat — one coroutine per dispatcher, one UPDATE per interval
# ---------------------------------------------------------------------------

class BatchedHeartbeat:
    """
    Refreshes ``locked_until`` for all tasks owned by this dispatcher in a
    single batched UPDATE via ``heartbeat_tasks()``, rather than one
    transaction per task.

    Usage::

        hb = BatchedHeartbeat(engine, visibility_timeout=timedelta(minutes=5))
        await hb.start()
        ...
        await hb.register(task_id, timestamp)
        try:
            result = await run_task(...)
        finally:
            await hb.unregister(task_id)
        ...
        await hb.stop()
    """

    def __init__(
        self,
        engine: AsyncEngine,
        interval: timedelta = timedelta(seconds=30),
        visibility_timeout: timedelta = timedelta(minutes=5),
    ):
        self._engine = engine
        self._interval = interval.total_seconds()
        self._visibility_timeout = visibility_timeout
        # {task_id: timestamp}
        self._owned: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None

    async def register(self, task_id: str, timestamp: datetime) -> None:
        async with self._lock:
            self._owned[task_id] = timestamp

    async def unregister(self, task_id: str) -> None:
        async with self._lock:
            self._owned.pop(task_id, None)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._beat_loop(), name="batched_heartbeat")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _beat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            snapshot = dict(self._owned)

        if not snapshot:
            return

        from dynastore.modules.tasks.tasks_module import heartbeat_tasks
        import uuid

        task_ids = [uuid.UUID(tid) for tid in snapshot.keys()]

        try:
            await heartbeat_tasks(self._engine, task_ids, self._visibility_timeout)
            logger.debug(
                f"BatchedHeartbeat: extended {len(task_ids)} task(s) "
                f"until +{self._visibility_timeout}."
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"BatchedHeartbeat: failed: {e}")


# ---------------------------------------------------------------------------
# Janitor — recovery of stale ACTIVE tasks + orphan cleanup
# ---------------------------------------------------------------------------

async def _run_janitor(
    engine: AsyncEngine,
    visibility_timeout: timedelta,
    orphan_grace_period: timedelta = timedelta(hours=1),
) -> None:
    """
    Global janitor:
    1. Finds ACTIVE tasks with expired locks and requeues or dead-letters them.
    2. Cleans up orphaned tasks for deleted catalogs.

    Uses pg_try_advisory_xact_lock to elect a single Janitor leader across
    all Cloud Run instances.
    """
    from dynastore.modules.tasks.tasks_module import find_stale_tasks, fail_task, cleanup_orphan_tasks
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction

    _JANITOR_LOCK_KEY = hash("dynastore.janitor.global") & 0x7FFFFFFFFFFFFFFF

    async with managed_transaction(engine) as conn:
        try:
            lock_acquired = await DQLQuery(
                "SELECT pg_try_advisory_xact_lock(:lock_key);",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn, lock_key=_JANITOR_LOCK_KEY)

            if not lock_acquired:
                return
        except Exception:
            return

    # Find stale tasks (expired locks) — global scan
    try:
        stale = await find_stale_tasks(engine, visibility_timeout)
        if stale:
            logger.info(f"Janitor: Found {len(stale)} stale ACTIVE task(s).")
            for row in stale:
                task_id = row["task_id"]
                retry_count = int(row.get("retry_count", 0))
                max_retries = int(row.get("max_retries", 3))
                should_retry = retry_count < max_retries
                await fail_task(
                    engine,
                    task_id=task_id,
                    timestamp=datetime.now(timezone.utc),
                    error_message=f"Janitor: visibility window expired (owner={row.get('owner_id')!r})",
                    retry=should_retry,
                )
                if not should_retry:
                    logger.error(
                        f"Janitor: Task {task_id} moved to DEAD_LETTER after "
                        f"{retry_count}/{max_retries} retries."
                    )
    except Exception as e:
        logger.warning(f"Janitor: Error scanning stale tasks: {e}")

    # Orphan cleanup
    try:
        orphaned = await cleanup_orphan_tasks(engine, orphan_grace_period)
        if orphaned:
            logger.info(f"Janitor: Dead-lettered {orphaned} orphaned task(s).")
    except Exception as e:
        logger.warning(f"Janitor: Error cleaning orphans: {e}")


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
    PENDING task from the global queue via ``claim_next()``, dispatches it
    to the registered runners, and updates the task state on completion or
    failure.

    Also runs the Janitor on every timeout-based wakeup.

    Args:
        engine:             Async database engine.
        schema:             Ignored (kept for backward compat). The global
                            ``tasks.tasks`` table is always used.
        shutdown_event:     Set this to stop the dispatcher cleanly.
        visibility_timeout: How long a claimed task stays ACTIVE before the
                            Janitor can reclaim it (heartbeat extends this).
        signal_timeout:     Max seconds to wait for a signal before running
                            the Janitor anyway (defensive polling).
    """
    from dynastore.modules.tasks.runners import capability_map, get_runners
    from dynastore.modules.tasks.models import TaskExecutionMode, RunnerContext, PermanentTaskFailure
    from dynastore.tasks import get_task_instance, hydrate_task_payload
    from dynastore.modules.tasks.tasks_module import claim_next, complete_task, fail_task

    # Refresh capability map at startup
    await capability_map.refresh()

    logger.info(
        f"Dispatcher: Started (runner={_RUNNER_ID!r}, "
        f"async_types={capability_map.async_types}, "
        f"sync_types={capability_map.sync_types})."
    )
    _last_janitor_run = datetime.now(timezone.utc) - timedelta(seconds=signal_timeout)

    heartbeat = BatchedHeartbeat(engine, visibility_timeout=visibility_timeout)
    await heartbeat.start()

    while not shutdown_event.is_set():
        try:
            # Wait for a notification or periodic wakeup
            got_signal = await signal_bus.wait_for(NEW_TASK_QUEUED, timeout=signal_timeout)

            # Janitor pass (throttled: once per signal_timeout/2 at most)
            now = datetime.now(timezone.utc)
            if (now - _last_janitor_run).total_seconds() >= signal_timeout / 2:
                await _run_janitor(engine, visibility_timeout)
                _last_janitor_run = datetime.now(timezone.utc)

            # Claim as many tasks as we can from the global queue
            while not shutdown_event.is_set():
                row = await claim_next(
                    engine,
                    async_task_types=capability_map.async_types,
                    sync_task_types=capability_map.sync_types,
                    visibility_timeout=visibility_timeout,
                    owner_id=_RUNNER_ID,
                )
                if row is None:
                    break  # Queue empty for our capability set

                task_id = row["task_id"]
                task_type = row["task_type"]
                timestamp = row["timestamp"]
                schema_name = row["schema_name"]
                execution_mode = row.get("execution_mode", TaskExecutionMode.ASYNCHRONOUS)

                logger.info(
                    f"Dispatcher: Claimed task {task_id} ({task_type}) "
                    f"schema={schema_name!r} mode={execution_mode}."
                )

                raw_payload = {
                    "task_id": task_id,
                    "caller_id": row.get("caller_id") or "",
                    "inputs": row["inputs"] if isinstance(row.get("inputs"), dict) else (
                        json.loads(row["inputs"]) if row.get("inputs") else {}
                    ),
                }

                # Select runners by execution mode
                runner_mode = (
                    TaskExecutionMode.SYNCHRONOUS
                    if execution_mode == "SYNCHRONOUS"
                    else TaskExecutionMode.ASYNCHRONOUS
                )
                runners = get_runners(runner_mode)
                if not runners:
                    logger.warning(
                        f"Dispatcher: No {runner_mode} runners registered — "
                        f"resetting task {task_id}."
                    )
                    await fail_task(
                        engine, task_id, timestamp,
                        f"No {runner_mode} runners registered",
                        retry=True,
                    )
                    continue

                context = RunnerContext(
                    engine=engine,
                    task_type=task_type,
                    caller_id=raw_payload["caller_id"],
                    inputs=raw_payload["inputs"],
                    db_schema=schema_name,
                    extra_context={"task_id": str(task_id)},
                )

                await heartbeat.register(str(task_id), timestamp)
                try:
                    result = None

                    # 1. Try runners that can handle this task type
                    for runner in runners:
                        if not runner.can_handle(task_type):
                            continue
                        caps = getattr(runner, 'capabilities', None)
                        if caps is not None and getattr(caps, 'requires_request_context', False):
                            continue

                        logger.debug(
                            f"Dispatcher: Running task {task_id} via "
                            f"runner '{runner.__class__.__name__}'."
                        )
                        result = await runner.run(context)
                        if result is not None:
                            logger.info(
                                f"Dispatcher: Task {task_id} handled by "
                                f"runner '{runner.__class__.__name__}'."
                            )
                            break

                    # 2. Fallback: Direct task execution via TaskProtocol singleton
                    if result is None:
                        task_instance = get_task_instance(task_type)
                        if task_instance:
                            logger.info(
                                f"Dispatcher: Executing task '{task_type}' "
                                f"via TaskProtocol singleton."
                            )
                            hydrated_payload = hydrate_task_payload(
                                task_instance, raw_payload
                            )
                            result = await task_instance.run(hydrated_payload)
                            logger.info(
                                f"Dispatcher: Task {task_id} via TaskProtocol "
                                f"singleton completed."
                            )
                        else:
                            raise RuntimeError(
                                f"No runner or task implementation found "
                                f"for '{task_type}'."
                            )

                    await complete_task(engine, task_id, timestamp, outputs=result)
                    logger.info(
                        f"Dispatcher: Task {task_id} completed successfully."
                    )

                except asyncio.CancelledError:
                    logger.warning(
                        f"Dispatcher: Task {task_id} interrupted "
                        f"(CancelledError) — resetting to PENDING."
                    )
                    await fail_task(
                        engine, task_id, timestamp,
                        "Runner interrupted (SIGTERM)",
                        retry=True,
                    )
                    raise

                except PermanentTaskFailure as e:
                    logger.error(
                        f"Dispatcher: Task {task_id} permanently failed "
                        f"(no retries): {e}"
                    )
                    await fail_task(
                        engine, task_id, timestamp,
                        str(e),
                        retry=False,
                    )

                except Exception as e:
                    import traceback
                    logger.error(
                        f"Dispatcher: Task {task_id} failed with error: "
                        f"{e}\n{traceback.format_exc()}"
                    )
                    await fail_task(
                        engine, task_id, timestamp,
                        str(e),
                        retry=True,
                    )

                finally:
                    await heartbeat.unregister(str(task_id))

        except asyncio.CancelledError:
            logger.info("Dispatcher: Cancelled — shutting down.")
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(f"Dispatcher: Unexpected error: {e}", exc_info=True)
            await asyncio.sleep(2.0)

    await heartbeat.stop()
    logger.info("Dispatcher: Stopped.")
