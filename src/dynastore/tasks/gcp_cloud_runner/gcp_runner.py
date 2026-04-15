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
import json
from typing import Dict, Any, Optional
from dynastore.modules.processes.models import Process, ExecuteRequest, as_process_task_payload
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.runners import RunnerProtocol
from dynastore.modules.gcp.tools.jobs import run_cloud_run_job_async
from dynastore.modules.gcp.tools.jobs import load_job_config
from dynastore.modules.tasks.models import (Task, TaskCreate, TaskUpdate, TaskExecutionMode, RunnerContext)
from dynastore.tasks import discover_tasks, get_all_task_configs

logger = logging.getLogger(__name__)

from dynastore.tasks.gcp_cloud_runner.cloud_run_tasks import register_cloud_run_jobs_as_tasks

logger = logging.getLogger(__name__)


from dynastore.tools.plugin import ProtocolPlugin

class GcpCloudRunRunner(RunnerProtocol, ProtocolPlugin[Any]):
    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 10
    """
    A runner for asynchronous processes that triggers a Google Cloud Run job.
    """
    _job_map_cache: Dict[str, str] = {}

    @property
    def capabilities(self) -> Any:
        return {"backend": "gcp_cloud_run"}

    async def setup(self, app_state: Any):
        """
        Discovers Cloud Run jobs from the configuration and dynamically registers
        them as placeholder tasks within the DynaStore task system. This allows
        them to be treated like any other task and be exposed as OGC Processes.
        """
        await register_cloud_run_jobs_as_tasks()
        # Cache job map for can_handle() (sync method)
        try:
            self._job_map_cache = await load_job_config()
        except Exception as e:
            logger.warning(f"GcpCloudRunRunner: Failed to cache job map: {e}")
            self._job_map_cache = {}

    def can_handle(self, task_type: str) -> bool:
        """Returns True if a Cloud Run Job is configured for this task type."""
        return task_type in self._job_map_cache

    async def run(self, context: RunnerContext) -> Optional[Task]:
        """
        Creates a task record and dispatches the job to the configured Cloud Run
        environment.
        """
        job_map = await load_job_config()
        job_name = job_map.get(context.task_type)

        if not job_name:
            # Returning None delegates the task back to the dispatcher for fallback (e.g. BackgroundTasks)
            # This prevents ghost task creation loops when Cloud Run is not configured.
            return None

        task_create_request = TaskCreate(
            caller_id=context.caller_id,
            task_type=context.task_type,
            inputs=context.inputs,
        )
        new_task = await tasks_module.create_task(context.engine, task_create_request, schema=context.db_schema)
        
        # CRITICAL FIX for Distributed Execution: Prevents local worker from stealing the task before Cloud Run boots
        from datetime import datetime, timezone, timedelta
        locked_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        
        # We must cast the string to TaskStatusEnum 
        from dynastore.models.tasks import TaskStatusEnum
        update_data = TaskUpdate(
            status=TaskStatusEnum.ACTIVE, 
            owner_id="gcp_cloud_run", 
            locked_until=locked_time
        )
        await tasks_module.update_task(context.engine, new_task.task_id, update_data, schema=context.db_schema)

        logger.info(f"Created and locked task '{new_task.task_id}' for GCP Cloud Run job '{job_name}'.")

        all_configs = get_all_task_configs()
        task_config = all_configs.get(context.task_type)

        # --- Adaptation: Always wrap as ProcessTaskPayload if process, else pass as-is ---
        payload = None
        if task_config is not None and isinstance(getattr(task_config, "definition", None), Process):
            # If context.inputs is already a dict from ExecuteRequest, reconstruct the model
            if isinstance(context.inputs, dict) and "inputs" in context.inputs:
                exec_req = ExecuteRequest(**context.inputs)
            elif isinstance(context.inputs, ExecuteRequest):
                exec_req = context.inputs
            else:
                # fallback: treat as dict for inputs
                exec_req = ExecuteRequest(inputs=context.inputs)
            payload = as_process_task_payload(
                task_id=new_task.task_id,
                caller_id=context.caller_id,
                execution_request=exec_req,
            )
        else:
            # fallback for non-process tasks
            from dynastore.modules.tasks.models import TaskPayload
            payload = TaskPayload(
                task_id=new_task.task_id, 
                caller_id=context.caller_id, 
                inputs=context.inputs, 
            )
        # --- End Adaptation ---

        args = [context.task_type, payload.model_dump_json(), "--schema", context.db_schema]
        try:
            await run_cloud_run_job_async(job_name=job_name, args=args)
        except Exception as e:
            logger.error(f"Failed to trigger Cloud Run job '{job_name}' for task '{new_task.task_id}': {e}", exc_info=True)
            await tasks_module.update_task(conn=context.engine, task_id=new_task.task_id,
                                     update_data=TaskUpdate(
                                         status=TaskStatusEnum.FAILED,
                                         error_message=str(e)
                                     ),
                                     schema=context.db_schema
            )
            raise
        finally:
            logger.info(f"Dispatched Cloud Run job '{job_name}'.")

        return new_task

# Register runner
from dynastore.tools.discovery import register_plugin
register_plugin(GcpCloudRunRunner())