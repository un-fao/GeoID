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
from typing import Any, Callable, Coroutine, List, Union, Dict, Tuple, Type, Protocol

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import (Task, TaskCreate, TaskExecutionMode,
                                            TaskPayload, TaskStatusEnum,
                                            TaskUpdate, RunnerContext)
from dynastore.tasks import get_task_instance
import asyncio
logger = logging.getLogger(__name__)

class RunnerProtocol(Protocol):
    """Defines the contract for a class-based task runner."""

    async def setup(self, app_state: Any) -> None:
        """
        An optional, async method called at application startup to allow the
        runner to initialize resources or perform discovery.
        """
        pass  # Default implementation does nothing.

    async def run(self, context: RunnerContext) -> Union[Task, Any]:
        """
        The core execution logic for the runner. This method is required.
        """
        ...

# The registry now stores instantiated runner objects, sorted by priority.
_RUNNERS: Dict[TaskExecutionMode, List[Tuple[int, RunnerProtocol]]] = {
    TaskExecutionMode.ASYNCHRONOUS: [],
    TaskExecutionMode.SYNCHRONOUS: [],
}

def register_runner(mode: TaskExecutionMode, priority: int = 100) -> Callable[[Type[RunnerProtocol]], Type[RunnerProtocol]]:
    """
    A class decorator to register a class as a runner for a specific TaskExecutionMode.
    It instantiates the class and stores the instance in the registry.

    Example:
        @register_runner(TaskExecutionMode.ASYNCHRONOUS)
        class MyRunner(RunnerProtocol):
            async def setup(self, app_state):
                print("Setting up!")
            async def run(self, context: RunnerContext):
                ...
    """
    def decorator(cls: Type[RunnerProtocol]) -> Type[RunnerProtocol]:
        instance = cls()
        _RUNNERS[mode].append((priority, instance))
        _RUNNERS[mode].sort(key=lambda item: item[0]) # Sort by priority
        logger.info(f"Registered runner class '{cls.__name__}' for '{mode.value}' with priority {priority}.")
        return cls # Return the original class, not the instance
    return decorator

def get_runners(mode: TaskExecutionMode) -> List[RunnerProtocol]:
    """
    Retrieves a prioritized list of registered runner instances for a given mode.
    """
    return [runner for priority, runner in _RUNNERS.get(mode, [])]

def get_all_runners_with_setup() -> List[Tuple[int, RunnerProtocol]]:
    """
    Returns a flattened, prioritized list of all runners that have implemented
    a custom setup method.
    """
    all_runners = []
    for mode in _RUNNERS:
        for priority, runner_func in _RUNNERS[mode]:
            # Correctly check if the instance's setup method is not the default one from the protocol.
            if runner_func.setup.__func__ is not RunnerProtocol.setup:
                all_runners.append((priority, runner_func))
    all_runners.sort(key=lambda item: item[0]) # Sort globally by priority
    return all_runners

# --- Default Runner Implementations ---

@register_runner(TaskExecutionMode.SYNCHRONOUS)
class SyncRunner(RunnerProtocol):
    """
    Runs a task synchronously within the request-response cycle.
    It creates a task record for auditing, executes the task's run method,
    updates the task status, and returns the final result directly.
    """
    async def run(self, context: RunnerContext) -> Any:
        # This is an in-process runner, so it's responsible for getting the task instance.
        task_instance = get_task_instance(context.task_type)
        if not task_instance:
            # Raise a recoverable error to allow fallback to other runners.
            raise RuntimeError(f"SyncRunner: No task instance found for '{context.task_type}'.")

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
        
        payload = TaskPayload(
            task_id=job.task_id, 
            caller_id=context.caller_id, 
            inputs=context.inputs,
            asset=context.asset
        )

        try:
            logger.info(f"Executing sync task '{job.task_id}'...")
            await tasks_module.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)

            # The task's `run` method is now async, so we can await it directly.
            result = await task_instance.run(payload)

            # OGC sync processes return the result directly; it's not stored in 'outputs'.
            update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100)
            await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            logger.info(f"Sync task '{job.task_id}' completed successfully.")
            return result

        except Exception as e:
            logger.error(f"Sync task '{job.task_id}' failed: {e}", exc_info=True) # exc_info=True is important
            error_message = f"Synchronous execution failed: {str(e)}"
            update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
            await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
            # Re-raise to allow the service layer to return a 500 error.
            raise

@register_runner(TaskExecutionMode.ASYNCHRONOUS)
class BackgroundRunner(RunnerProtocol):
    """
    Runs a task asynchronously in the background.
    Uses Starlette/FastAPI BackgroundTasks if available in context, otherwise asyncio.create_task.
    Returns a StatusInfo object immediately (201 Created pattern).
    """
    async def run(self, context: RunnerContext) -> Any:
        # This is an in-process runner
        task_instance = get_task_instance(context.task_type)
        if not task_instance:
            raise RuntimeError(f"BackgroundRunner: No task instance found for '{context.task_type}'.")

        # Create a task record (PENDING)
        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=str(context.task_type),
            inputs=context.inputs,
        )
        job = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created audit task '{job.task_id}' for async process '{context.task_type}'.")

        payload = TaskPayload(
            task_id=job.task_id, 
            caller_id=context.caller_id, 
            inputs=context.inputs,
            asset=context.asset
        )

        background_tasks = context.extra_context.get("background_tasks")
        
        # Define the background execution logic
        async def _execute_background():
            try:
                logger.info(f"Executing async task '{job.task_id}' in background...")
                # We need a new engine wrapper or ensure the existing one is thread-safe/async-safe context
                # The context.engine is likely an AsyncEngine wrapper.
                
                await tasks_module.update_task(context.engine, job.task_id, TaskUpdate(status=TaskStatusEnum.RUNNING), schema=context.db_schema)
                
                # Execute the task logic
                await task_instance.run(payload)
                
                # Update to COMPLETED
                update_data = TaskUpdate(status=TaskStatusEnum.COMPLETED, progress=100)
                await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                logger.info(f"Async task '{job.task_id}' completed successfully.")
                
            except Exception as e:
                logger.error(f"Async task '{job.task_id}' failed: {e}", exc_info=True)
                error_message = f"Asynchronous execution failed: {str(e)}"
                try:
                    update_data = TaskUpdate(status=TaskStatusEnum.FAILED, error_message=error_message)
                    await tasks_module.update_task(context.engine, job.task_id, update_data, schema=context.db_schema)
                except Exception as update_error:
                     logger.critical(f"Failed to update task '{job.task_id}' status to FAILED: {update_error}")

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
            status="ACCEPTED",
            message="Task accepted for asynchronous execution.",
            progress=0,
            links=[] 
        )
