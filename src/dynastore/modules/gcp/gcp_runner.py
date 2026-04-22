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
from typing import Any, Optional

from dynastore.modules.processes.models import Process, ExecuteRequest, as_process_task_payload
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.runners import RunnerProtocol
from dynastore.modules.tasks.models import Task, TaskCreate, TaskUpdate, TaskExecutionMode, RunnerContext
from dynastore.modules.gcp.tools.jobs import (
    run_cloud_run_job_async,
    load_job_config,
    get_job_map_sync,
    try_load_process_definition,
)
from dynastore.tools.identifiers import generate_id_hex
from dynastore.tools.plugin import ProtocolPlugin

logger = logging.getLogger(__name__)


class GcpJobRunner(RunnerProtocol, ProtocolPlugin[Any]):
    """Runner for async processes that triggers a Google Cloud Run job."""

    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 10
    runner_type = "gcp_cloud_run"

    @property
    def capabilities(self) -> Any:
        return {"backend": "gcp_cloud_run"}

    async def setup(self, app_state: Any) -> None:
        """Warm the sync job map cache at startup."""
        try:
            await load_job_config()
        except Exception as e:
            logger.warning(f"GcpJobRunner: failed to warm job cache: {e}")

    def can_handle(self, task_type: str) -> bool:
        """Returns True if a Cloud Run Job is configured for this task type."""
        return task_type in get_job_map_sync()

    async def run(self, context: RunnerContext) -> Optional[Task]:
        """Creates a task record and dispatches the job to Cloud Run."""
        job_map = await load_job_config()
        job_name = job_map.get(context.task_type)

        if not job_name:
            return None

        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=context.task_type,
            inputs=context.inputs,
        )
        new_task = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        if new_task is None:
            raise RuntimeError("GcpCloudRunner: create_task returned None (dedup hit on a non-dedup task).")

        from datetime import datetime, timezone, timedelta
        locked_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        execution_id = generate_id_hex()

        from dynastore.models.tasks import TaskStatusEnum
        update_data = TaskUpdate(
            status=TaskStatusEnum.ACTIVE,
            owner_id=f"gcp_cloud_run_{execution_id}",
            locked_until=locked_time,
        )
        await tasks_module.update_task(context.engine, new_task.task_id, update_data, schema=context.db_schema)

        logger.info(
            f"Created and locked task '{new_task.task_id}' for GCP Cloud Run job '{job_name}' "
            f"(execution_id={execution_id})."
        )

        process_defn = try_load_process_definition(context.task_type)

        if process_defn is not None and isinstance(process_defn, Process):
            if isinstance(context.inputs, dict) and "inputs" in context.inputs:
                exec_req = ExecuteRequest(**context.inputs)
            elif isinstance(context.inputs, ExecuteRequest):
                exec_req = context.inputs
            else:
                exec_req = ExecuteRequest(inputs=context.inputs)
            payload = as_process_task_payload(
                task_id=new_task.task_id,
                caller_id=context.caller_id,
                execution_request=exec_req,
            )
        else:
            from dynastore.modules.tasks.models import TaskPayload
            payload = TaskPayload(
                task_id=new_task.task_id,
                caller_id=context.caller_id,
                inputs=context.inputs,
            )

        args = [context.task_type, payload.model_dump_json(), "--schema", context.db_schema]
        try:
            await run_cloud_run_job_async(
                job_name=job_name,
                args=args,
                env_vars={"DYNASTORE_EXECUTION_ID": execution_id},
            )
        except Exception as e:
            logger.error(
                f"Failed to trigger Cloud Run job '{job_name}' for task '{new_task.task_id}': {e}",
                exc_info=True,
            )
            await tasks_module.update_task(
                conn=context.engine,
                task_id=new_task.task_id,
                update_data=TaskUpdate(
                    status=TaskStatusEnum.FAILED,
                    error_message=str(e),
                ),
                schema=context.db_schema,
            )
            raise
        finally:
            logger.info(f"Dispatched Cloud Run job '{job_name}'.")

        return new_task
