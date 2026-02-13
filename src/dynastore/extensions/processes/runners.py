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
import asyncio
import traceback

from fastapi import BackgroundTasks

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import (Task, TaskCreate, TaskExecutionMode,
                                            TaskPayload, TaskStatusEnum,
                                            TaskUpdate, RunnerContext)
from dynastore.modules.tasks.runners import RunnerProtocol, register_runner
from dynastore.tasks import get_task_instance
from dynastore.modules.processes.models import ExecuteRequest, as_process_task_payload

logger = logging.getLogger(__name__)


async def _run_and_update_task(context: RunnerContext, task_id, task_instance):
    """
    A wrapper that executes the task's run method and handles updating the
    task status in the database upon completion or failure.
    """
    # If context.inputs is for a process, wrap as ProcessTaskPayload
    if isinstance(context.inputs, dict) and "inputs" in context.inputs:
        payload = as_process_task_payload(
            task_id=task_id,
            caller_id=context.caller_id,
            execution_request=ExecuteRequest(**context.inputs)
        )
    else:
        payload = TaskPayload(
            task_id=task_id, 
            caller_id=context.caller_id, 
            inputs=context.inputs
        )
    # --- End Integration ---

    try:
        logger.info(f"Background task '{task_id}' started.")
        update_request = TaskUpdate(
            status=TaskStatusEnum.RUNNING,
            outputs=context.extra_context.get("outputs", None),  # Optional: initial outputs
        )
        await tasks_module.update_task(context.engine, task_id, update_request, schema=context.db_schema)

        # The task's `run` method is now async, so we can await it directly.
        outputs = await task_instance.run(payload)

        # On success, update status to COMPLETED and store results
        update_request = TaskUpdate(
            status=TaskStatusEnum.COMPLETED,
            outputs=outputs,
        )
        await tasks_module.update_task(context.engine, task_id, update_request, schema=context.db_schema)
        logger.info(f"Background task '{task_id}' completed successfully.")

    except Exception as e:
        # On failure, update status to FAILED and store the error
        error_message = f"Task failed: {e}\n{traceback.format_exc()}"
        logger.error(f"Background task '{task_id}' failed: {error_message}")
        update_request = TaskUpdate(
            status=TaskStatusEnum.FAILED,
            error_message=error_message,
        )
        await tasks_module.update_task(context.engine, task_id, update_request, schema=context.db_schema)


@register_runner(TaskExecutionMode.ASYNCHRONOUS, priority=50)  # Higher priority
class FastAPIBackgroundRunner(RunnerProtocol):
    """
    An asynchronous runner that uses FastAPI's BackgroundTasks to execute the job.
    This is suitable for in-process asynchronous execution.
    """
    async def run(self, context: RunnerContext) -> Task:
        background_tasks = context.extra_context.get("background_tasks")
        if background_tasks is None:
            raise RuntimeError(
                "FastAPIBackgroundRunner requires 'background_tasks' in the extra_context, but it was not provided."
            )
        if not isinstance(background_tasks, BackgroundTasks):
            raise RuntimeError(
                "FastAPIBackgroundRunner requires 'background_tasks' of type fastapi.BackgroundTasks in the extra_context."
            )

        # This is an in-process runner, so it's responsible for getting the task instance.
        task_instance = get_task_instance(context.task_type)
        if not task_instance:
            # Raise a recoverable error to allow fallback to other runners.
            raise RuntimeError(f"FastAPIBackgroundRunner: No task instance found for '{context.task_type}'.")

        # Create the task record in the database for tracking.
        task_create_request = TaskCreate(
            caller_id=context.caller_id, task_type=str(context.task_type), inputs=context.inputs
        )
        new_task = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        logger.info(f"Created task '{new_task.task_id}' for FastAPI background execution.")

        # 3. Add the robust wrapper to FastAPI's background tasks.
        background_tasks.add_task(_run_and_update_task, context, new_task.task_id, task_instance)
        logger.info(f"Scheduled task '{new_task.task_id}' to run in the background.")

        # 4. Return the created task object immediately.
        return new_task