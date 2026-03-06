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
from typing import Any, Callable, Coroutine, List, Union, Dict, Tuple, Type, Protocol, runtime_checkable

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import (Task, TaskCreate, TaskExecutionMode,
                                            TaskPayload, TaskStatusEnum,
                                            TaskUpdate, RunnerContext)
from dynastore.tasks import get_task_instance
import asyncio
from dynastore.tools.plugin import ProtocolPlugin
from dynastore.tools.discovery import register_plugin

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

    async def run(self, context: RunnerContext) -> Union[Task, Any]:
        """
        The core execution logic for the runner. This method is required.
        """
        ...

def get_runners(mode: TaskExecutionMode) -> List[RunnerProtocol]:
    """
    Retrieves a prioritized list of registered runner instances for a given mode.
    """
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

    async def run(self, context: RunnerContext) -> Any:
        # This is an in-process runner, so it's responsible for getting the task instance.
        task_instance = get_task_instance(context.task_type)
        logger.warning(f"DEBUG: SyncRunner lookup for '{context.task_type}' -> {task_instance}")
        if not task_instance:
            return None

        # Create a task record to track this synchronous execution for auditing.
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        job = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created audit task '{job.task_id}' for synchronous process '{context.task_type}'.")

        # FIX: Pydantic v2 requires model_validate for casting, or just passing fields.
        # But 'inputs' in TaskPayload is a generic InputsType.
        # In context.inputs, we have a Dict[str, Any] which is actually an ExecuteRequest.model_dump().
        # However, TaskPayload expects 'inputs' to be of type ExecuteRequest (the Pydantic model), NOT a dict.
        # So we need to reconstruct the Pydantic model from the dict.
        
        # We need to know the type of 'inputs' expected by the TaskPayload[T].
        # In this generic runner, we don't know T.
        # But wait, the task_instance.run(payload) expects a specific T.
        # If we pass a dict, Pydantic v2 validation MIGHT handle it if configured correctly,
        # OR we might be hitting a validation error if TaskPayload expects a model instance.
        
        # Actually, looking at the error: "AttributeError: 'dict' object has no attribute 'inputs'"
        # This happened inside IngestionTask.run(), where it tried `ogc_process_inputs.inputs`.
        # This implies `ogc_process_inputs` was a dict, not an ExecuteRequest object.
        # So `payload.inputs` was a dict.
        
        # Why?
        # context.inputs is a Dict (ExecuteRequest.model_dump()).
        # TaskPayload.inputs is defined as InputsType (Generic).
        # When we create TaskPayload(..., inputs=context.inputs), Pydantic sees a Dict.
        # Since InputsType is generic, Pydantic might just be accepting the Dict as is, 
        # effectively resolving InputsType to Dict.
        
        # But IngestionTask expects TaskPayload[ExecuteRequest].
        # So inside run(), it expects payload.inputs to be an ExecuteRequest object.
        
        # Ideally, we should convert the dict back to the model if we know the model type.
        # But the Runner is generic.
        
        # OPTION 1: The Runner shouldn't care, but the Task implementation should handle both Dict and Model?
        # OPTION 2: We should try to hydrate the model if possible.
        # OPTION 3: In IngestionTask.run, we check if it's a dict and convert/use it.
        
        # The error was: `catalog_id = ogc_process_inputs.inputs['catalog_id']`
        # `ogc_process_inputs` is `payload.inputs`.
        # If it's a dict, `ogc_process_inputs['inputs']['catalog_id']` would work.
        # But the code tried `ogc_process_inputs.inputs`. This implies it expected an object.
        
        # Let's check IngestionTask again.
        # `async def run(self, payload: TaskPayload[ExecuteRequest])`
        # It types payload.inputs as ExecuteRequest.
        
        # If we pass a dict to TaskPayload constructor for a field typed as a Model, Pydantic v2 usually validates/converts it.
        # BUT here TaskPayload is Generic[InputsType].
        # In `SyncRunner`, we execute `TaskPayload(...)`. We are NOT instantiating `TaskPayload[ExecuteRequest]`.
        # We are instantiating a raw `TaskPayload` (effectively `TaskPayload[Any]`).
        # So Pydantic doesn't know it should convert `inputs` to `ExecuteRequest`.
        
        # So `payload.inputs` remains a `dict`.
        # Then we pass this `payload` to `task_instance.run(payload)`.
        # Python's runtime type hints don't cast data.
        
        # FIX: In IngestionTask.run, we must handle the fact that payload.inputs might be a dict (from generic runners) 
        # or properly ensure it's converted.
        # Since fixing it in every Task is annoying, maybe we can fix it here?
        # But we don't know the type.
        
        # So the fix MUST be in IngestionTask.run (or a base class helper) to be robust.
        # Or, we change how we construct payload to use the specific type if known? No, runner is generic.
        
        # Let's fix IngestionTask.run to handle dicts. I tried that but I made a mistake in the Edit.
        # "inputs_dict = ogc_process_inputs.inputs" -> This was the error line I tried to introduce?
        # No, the previous code was: `catalog_id = ogc_process_inputs.inputs['catalog_id']`
        # If ogc_process_inputs is a dict, it has no attribute `.inputs`. It has key `['inputs']`.
        
        # So `inputs_dict = ogc_process_inputs['inputs']` would work if it's a dict.
        # `inputs_dict = ogc_process_inputs.inputs` works if it's an object.
        
        # Robust way:
        # if isinstance(ogc_process_inputs, dict):
        #    inputs_dict = ogc_process_inputs['inputs']
        # else:
        #    inputs_dict = ogc_process_inputs.inputs
        
        # However, I should probably just leave this Runner alone and fix IngestionTask.
        
        from dynastore.tasks import hydrate_task_payload

        raw_payload = {
            "task_id": job.task_id, 
            "caller_id": context.caller_id, 
            "inputs": context.inputs,
            "asset": context.asset
        }

        try:
            logger.info(f"Executing sync task '{job.task_id}'...")
            await tasks_module.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)

            # Hydrate and execute
            hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
            result = await task_instance.run(hydrated_payload)

            # OGC sync processes return the result directly; it's not stored in 'outputs'.
            update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100)
            await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            logger.info(f"Sync task '{job.task_id}' completed successfully.")
            return result

        except Exception as e:
            logger.error(f"Sync task '{job.task_id}' failed: {e}", exc_info=True)
            error_message = f"Synchronous execution failed: {str(e)}"
            update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
            await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
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
    @property
    def capabilities(self) -> Any:
        from dynastore.modules.tasks.models import RunnerCapabilities
        return RunnerCapabilities(max_concurrency=20)

    async def run(self, context: RunnerContext) -> Any:
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
        job = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created audit task '{job.task_id}' for async process '{context.task_type}'.")

        from dynastore.tasks import hydrate_task_payload
        
        raw_payload = {
            "task_id": job.task_id, 
            "caller_id": context.caller_id, 
            "inputs": context.inputs,
            "asset": context.asset
        }

        background_tasks = context.extra_context.get("background_tasks")
        
        # Define the background execution logic
        async def _execute_background():
            try:
                logger.info(f"Executing async task '{job.task_id}' in background...")
                # We need a new engine wrapper or ensure the existing one is thread-safe/async-safe context
                # The context.engine is likely an AsyncEngine wrapper.
                
                await tasks_module.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)
                
                # Hydrate and execute
                hydrated_payload = hydrate_task_payload(task_instance, raw_payload)
                result = await task_instance.run(hydrated_payload)
                
                # Update to COMPLETED with outputs
                update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100, outputs=result)
                await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                logger.info(f"Async task '{job.task_id}' completed successfully.")
                
            except asyncio.CancelledError:
                logger.warning(f"Async task '{job.task_id}' was cancelled (SIGTERM?). Resetting to PENDING.")
                try:
                    # Reset to PENDING so another worker can pick it up
                    await tasks_module.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.PENDING), schema=context.db_schema)
                except Exception as e:
                    logger.error(f"Failed to reset cancelled task '{job.task_id}': {e}")
                raise
            except Exception as e:
                logger.error(f"Async task '{job.task_id}' failed: {e}", exc_info=True)
                error_message = f"Asynchronous execution failed: {str(e)}"
                try:
                    update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
                    await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                except Exception as update_error:
                    logger.critical(f"Failed to update task '{job.task_id}' status to FAILED: {update_error}")
                await _emit_task_failure(context, job, error_message, e)

        # Submit to background
        if background_tasks:
             # Starlette BackgroundTasks expects a sync callable or async coroutine? 
             # It handles async functions. add_task(func, *args, **kwargs)
             background_tasks.add_task(_execute_background)
             logger.info(f"Task '{job.task_id}' submitted to Starlette BackgroundTasks.")
        else:
             # Fallback to asyncio
             asyncio.create_task(_execute_background())
             logger.info(f"Task '{job.task_id}' submitted to asyncio event loop.")

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
            jobID=str(job.task_id),
            status="accepted",
            message="Task accepted for asynchronous execution.",
            progress=0,
            links=[] 
        )

def register_default_runners() -> None:
    """Ensures that default runners are registered in the global plugin registry.
    Safe to call multiple times.
    """
    register_plugin(SyncRunner())
    register_plugin(BackgroundRunner())

# Register default runners on module load
register_default_runners()
