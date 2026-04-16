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
import os
from typing import Any, Callable, Coroutine, List, Union, Dict, Tuple, Type, Protocol, runtime_checkable, AsyncGenerator
from contextlib import asynccontextmanager

from dynastore.modules.tasks.models import (Task, TaskCreate, TaskExecutionMode,
                                            TaskPayload, TaskStatusEnum,
                                            TaskUpdate, RunnerContext)
from dynastore.tasks import get_task_instance
import asyncio
from dynastore.tools.plugin import ProtocolPlugin
from dynastore.tools.discovery import register_plugin
from dynastore.modules.concurrency import get_background_executor

logger = logging.getLogger(__name__)


async def _emit_task_failure(
    context: "RunnerContext",
    job: Any,
    error_message: str,
    exc: Exception,
) -> None:
    """Shared helper: emits a generic platform 'task.failed' event after a task fails.

    Severity is derived from the exception type:
    - 'recoverable'   — transient errors (timeout, OSError, etc.)
    - 'unrecoverable' — all other exceptions

    The event carries the full input context so listeners can perform rollback
    without any knowledge of the specific module that dispatched the task.
    Also logs via tenant log_manager if catalog_id is resolvable.
    """
    # Determine severity from exception type
    severity = "unrecoverable"
    if isinstance(exc, (TimeoutError, OSError, asyncio.TimeoutError)):
        severity = "recoverable"

    catalog_id = (context.inputs or {}).get("catalog_id")
    originating_event = (context.extra_context or {}).get("originating_event")

    # Tenant-scoped structured log (best-effort, never blocks failure path)
    if catalog_id:
        try:
            from dynastore.modules.catalog.log_manager import log_error
            await log_error(
                catalog_id,
                event_type="task.failed",
                message=f"Task '{context.task_type}' ({job.task_id}) failed [{severity}]: {error_message}",
                details={"task_type": context.task_type, "severity": severity},
            )
        except Exception as log_exc:
            logger.debug(f"log_manager unavailable for task failure logging: {log_exc}")

    # Emit generic platform event — all rollback logic lives in module listeners
    try:
        from dynastore.modules.catalog.event_service import emit_event
        await emit_event(
            "task.failed",
            task_id=str(job.task_id),
            task_type=context.task_type,
            error_message=error_message,
            severity=severity,
            inputs=context.inputs,
            originating_event=originating_event,
            catalog_id=catalog_id,
        )
    except Exception as emit_exc:
        logger.error(f"Failed to emit task.failed event: {emit_exc}")

@runtime_checkable
class RunnerProtocol(Protocol):
    """Defines the contract for a class-based task runner."""

    priority: int
    mode: TaskExecutionMode

    async def setup(self, app_state: Any) -> None:
        """
        An optional, async method called at application startup to allow the
        runner to initialize resources or perform discovery.
        """
        pass  # Default implementation does nothing.

    @property
    def capabilities(self) -> Any:
        """Returns the capabilities of this runner (e.g. max_concurrency, tags)."""
        ...

    def can_handle(self, task_type: str) -> bool:
        """
        Returns True if this runner can execute tasks of the given type.

        Used by the CapabilityMap at startup (and on refresh) to build the
        set of task types this instance can claim from the global queue.

        Default: returns True (runner accepts all task types).
        """
        return True

    async def run(self, context: RunnerContext) -> Union[Task, Any]:
        """
        The core execution logic for the runner. This method is required.
        """
        ...

def get_runners(mode: TaskExecutionMode) -> List[RunnerProtocol]:
    """
    Retrieves a prioritized list of registered runner instances for a given mode.
    """
    # Ensure default runners are registered
    register_default_runners()
    
    from dynastore.tools.discovery import get_protocols
    return [r for r in get_protocols(RunnerProtocol) if getattr(r, "mode", None) == mode]

def get_all_runners_with_setup() -> List[Tuple[int, RunnerProtocol]]:
    """
    Returns a prioritized list of all runners that have implemented
    a custom setup method.
    """
    from dynastore.tools.discovery import get_protocols
    all_runners = []
    # get_protocols already sorts by priority desc
    for runner in get_protocols(RunnerProtocol):
        # Check if the instance's setup method is not the default one from the base class
        if getattr(runner.setup, "__func__", runner.setup) is not RunnerProtocol.setup:
            all_runners.append((getattr(runner, "priority", 0), runner))
    return all_runners

# --- Default Runner Implementations ---

class SyncRunner(RunnerProtocol, ProtocolPlugin[Any]):
    mode = TaskExecutionMode.SYNCHRONOUS
    priority = 100
    """
    An in-process, synchronous runner that executes the job immediately.
    """

    async def setup(self, app_state: Any) -> None:
        pass

    @property
    def capabilities(self) -> Any:
        from dynastore.modules.tasks.models import RunnerCapabilities
        return RunnerCapabilities(max_concurrency=100)

    def can_handle(self, task_type: str) -> bool:
        return get_task_instance(task_type) is not None

    async def run(self, context: RunnerContext) -> Any:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol
        
        # Resolve the task management protocol
        tasks_mgr = resolve(TasksProtocol)
        
        # This is an in-process runner, so it's responsible for getting the task instance.
        task_instance = get_task_instance(context.task_type)
        logger.debug(f"SyncRunner: lookup for '{context.task_type}' -> {task_instance}")
        if not task_instance:
            return None

        # Create a task record to track this synchronous execution for auditing.
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        
        job = await tasks_mgr.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created audit task '{job.task_id}' for synchronous process '{context.task_type}'.")
        
        # ... (rest of the logic remains the same, but using tasks_mgr) ...
        from dynastore.tasks import hydrate_task_payload

        raw_payload = {
            "task_id": job.task_id, 
            "caller_id": context.caller_id, 
            "inputs": context.inputs,
            "asset": context.asset
        }

        try:
            logger.info(f"Executing sync task '{job.task_id}'...")
            await tasks_mgr.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)

            # Hydrate and execute
            hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
            result = await task_instance.run(hydrated_payload)

            # OGC sync processes return the result directly; it's not stored in 'outputs'.
            update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100)
            await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            logger.info(f"Sync task '{job.task_id}' completed successfully.")
            return result

        except Exception as e:
            logger.error(f"Sync task '{job.task_id}' failed: {e}", exc_info=True)
            error_message = f"Synchronous execution failed: {str(e)}"
            update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
            await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            await _emit_task_failure(context, job, error_message, e)
            # Re-raise to allow the service layer to return a 500 error.
            raise

class BackgroundRunner(RunnerProtocol, ProtocolPlugin[Any]):
    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 100
    """
    Runs a task asynchronously in the background.
    Uses Starlette/FastAPI BackgroundTasks if available in context, otherwise asyncio.create_task.
    Returns a StatusInfo object immediately (201 Created pattern).
    """
    def __init__(self):
        self._running_tasks = set()
        self._max_concurrency = int(os.getenv("BACKGROUND_RUNNER_CONCURRENCY", "100"))
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

    def can_handle(self, task_type: str) -> bool:
        return get_task_instance(task_type) is not None

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        try:
            yield
        finally:
            # Shutdown: wait for running tasks to finish or timeout
            if self._running_tasks:
                pending_count = len(self._running_tasks)
                logger.info(f"BackgroundRunner: Waiting for {pending_count} background tasks to complete...")
                # We give them a decent timeout as some might be provisioning resources
                _, pending = await asyncio.wait(list(self._running_tasks), timeout=10.0)
                if pending:
                    logger.warning(f"BackgroundRunner: {len(pending)} tasks did not finish in time and will be cancelled.")
                    for p in pending:
                        p.cancel()
                    await asyncio.gather(*list(pending), return_exceptions=True)
                logger.info("BackgroundRunner: Cleaned up background tasks.")

    @property
    def capabilities(self) -> Any:
        from dynastore.modules.tasks.models import RunnerCapabilities
        return RunnerCapabilities(max_concurrency=self._max_concurrency)

    async def run(self, context: RunnerContext) -> Any:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol
        
        # Resolve the task management protocol
        tasks_mgr = resolve(TasksProtocol)
        
        # This is an in-process runner
        logger.debug(f"BackgroundRunner.run called for task_type: {context.task_type}, mode: {self.mode}")
        task_instance = get_task_instance(context.task_type)
        if not task_instance:
            logger.error(f"Failed to find task instance for type: {context.task_type}")
            return None
        logger.debug(f"Found task instance: {task_instance.__class__.__name__} for task_type: {context.task_type}")

        # Create a task record (PENDING)
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        job = await tasks_mgr.create_task(
            context.engine, task_create_request, schema=context.db_schema,
            initial_status="RUNNING",
        )
        logger.info(f"Created audit task '{job.task_id}' for async process '{context.task_type}'.")

        from dynastore.tasks import hydrate_task_payload
        
        raw_payload = {
            "task_id": job.task_id, 
            "caller_id": context.caller_id, 
            "inputs": context.inputs,
            "asset": context.asset
        }

        background_tasks = context.extra_context.get("background_tasks")
        
        # Define the background execution logic (semaphore-guarded)
        async def _execute_background():
            async with self._semaphore:
                try:
                    logger.info(f"Executing async task '{job.task_id}' in background...")

                    # Hydrate and execute
                    hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
                    result = await task_instance.run(hydrated_payload)

                    # Update to COMPLETED with outputs
                    update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100, outputs=result)
                    await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                    logger.info(f"Async task '{job.task_id}' completed successfully.")

                except asyncio.CancelledError:
                    logger.warning(f"Async task '{job.task_id}' was cancelled (SIGTERM?). Resetting to PENDING.")
                    try:
                        # Reset to PENDING so another worker can pick it up
                        await tasks_mgr.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.PENDING), schema=context.db_schema)
                    except Exception as e:
                        logger.error(f"Failed to reset cancelled task '{job.task_id}': {e}")
                    raise
                except Exception as e:
                    logger.error(f"Async task '{job.task_id}' failed: {e}", exc_info=True)
                    error_message = f"Asynchronous execution failed: {str(e)}"
                    try:
                        update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
                        await tasks_mgr.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                    except Exception as update_error:
                        logger.critical(f"Failed to update task '{job.task_id}' status to FAILED: {update_error}")
                    await _emit_task_failure(context, job, error_message, e)

        # Submit to background
        if background_tasks:
             # Starlette BackgroundTasks — not semaphore-guarded (Starlette manages lifecycle)
             background_tasks.add_task(_execute_background)
             logger.info(f"Task '{job.task_id}' submitted to Starlette BackgroundTasks.")
        else:
             # Use BackgroundExecutor for tracked execution with GC prevention + error logging
             executor = get_background_executor()
             t = executor.submit(_execute_background(), task_name=f"task:{job.task_id}")
             self._running_tasks.add(t)
             t.add_done_callback(self._running_tasks.discard)
             logger.info(f"Task '{job.task_id}' submitted via BackgroundExecutor.")

        # For ASYNC, we return a StatusInfo object (or similar) immediately.
        # Check what the processes module expects.
        # It expects whatever the runner returns.
        # The OGC Process API expects a generic result that it can format.
        # Usually for async, it needs a job ID.
        
        # We can return the Job object or a dict with jobID.
        # The service layer usually wraps this. 
        # But looking at SyncRunner, it returns the result of task_instance.run(payload).
        # For Async, we should return the initial status info.
        
        from dynastore.modules.processes.models import StatusInfo
        return StatusInfo(
            jobID=job.task_id,
            status="accepted",
            message="Task accepted for asynchronous execution.",
            progress=0,
            links=[] 
        )

class CapabilityMap:
    """
    In-memory map of task_type to capable runners, grouped by execution mode.

    Built at dispatcher startup by querying each runner's can_handle() method.
    Refreshable at runtime when Cloud Run Jobs are added/removed.
    """

    def __init__(self):
        self._async_types: set = set()
        self._sync_types: set = set()
        self._lock = asyncio.Lock()

    async def refresh(self) -> None:
        """Rebuild capability map from current runners and loaded task types."""
        from dynastore.tasks import get_loaded_task_types
        async with self._lock:
            self._async_types.clear()
            self._sync_types.clear()
            for task_type in get_loaded_task_types():
                for runner in get_runners(TaskExecutionMode.ASYNCHRONOUS):
                    if runner.can_handle(task_type):
                        self._async_types.add(task_type)
                        break
                for runner in get_runners(TaskExecutionMode.SYNCHRONOUS):
                    if runner.can_handle(task_type):
                        self._sync_types.add(task_type)
                        break
            logger.info(
                f"CapabilityMap refreshed: async={sorted(self._async_types)}, "
                f"sync={sorted(self._sync_types)}"
            )

    @property
    def async_types(self) -> List[str]:
        return list(self._async_types)

    @property
    def sync_types(self) -> List[str]:
        return list(self._sync_types)

    @property
    def all_types(self) -> List[str]:
        return list(self._async_types | self._sync_types)

    def can_claim(self, task_type: str, execution_mode: str) -> bool:
        """Check if this instance can claim a task of the given type and mode."""
        if execution_mode == TaskExecutionMode.ASYNCHRONOUS:
            return task_type in self._async_types
        elif execution_mode == TaskExecutionMode.SYNCHRONOUS:
            return task_type in self._sync_types
        return False


# Singleton capability map instance
capability_map = CapabilityMap()


def register_default_runners() -> None:
    """Ensures that default runners are registered in the global plugin registry.
    Safe to call multiple times.
    """
    register_plugin(SyncRunner())
    register_plugin(BackgroundRunner())

# Register default runners on module load
register_default_runners()
