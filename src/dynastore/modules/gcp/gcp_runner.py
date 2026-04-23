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
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from dynastore.modules.processes.models import Process, ExecuteRequest, as_process_task_payload
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.runners import RunnerProtocol
from dynastore.modules.tasks.models import (
    Task,
    TaskCreate,
    TaskUpdate,
    TaskExecutionMode,
    RunnerContext,
    DEFERRED_COMPLETION,
)
from dynastore.tools.identifiers import generate_id_hex
from dynastore.tools.plugin import ProtocolPlugin

logger = logging.getLogger(__name__)


# Default lease for a freshly-launched Cloud Run Job. Picked to outlast typical
# job startup + run; the in-job heartbeat (main_task.py) extends it while the
# job runs, and the pg_cron reaper resets the row if the lease lapses.
_DEFAULT_TASK_TIMEOUT_SECONDS = int(os.getenv("TASK_TIMEOUT", "3600"))


class GcpJobRunner(RunnerProtocol, ProtocolPlugin[Any]):
    """Runner for async processes that triggers a Google Cloud Run job.

    Two invocation paths:

    1. **REST path** (``ExecutionEngine.execute``, no ``task_id`` in
       ``extra_context``): create a fresh PENDING row, mark ACTIVE, launch a
       Cloud Run Job with the new ``task_id`` in its payload.

    2. **Dispatcher path** (``ExecutionEngine.dispatch``, ``task_id`` provided
       in ``extra_context``): the row was already claimed by the dispatcher.
       Do **not** create a new row — extend the lease and launch a Cloud Run
       Job carrying the existing ``task_id``. Return :data:`DEFERRED_COMPLETION`
       so the dispatcher does not write COMPLETED ahead of the job container.

    Path 2 used to call ``create_task`` unconditionally, producing one new row
    + one Cloud Run Job execution per dispatcher claim. Combined with the
    pg_cron reaper resetting ACTIVE rows back to PENDING when ``locked_until``
    lapsed, this caused an unbounded re-enqueue loop. See plan
    ``geoid-when-a-job-giggly-diffie`` for the full diagnosis.
    """

    mode = TaskExecutionMode.ASYNCHRONOUS
    priority = 10
    runner_type = "gcp_cloud_run"

    @property
    def capabilities(self) -> Any:
        return {"backend": "gcp_cloud_run"}

    async def setup(self, app_state: Any) -> None:
        """Warm the sync job map cache at startup."""
        from dynastore.modules.gcp.tools.jobs import load_job_config
        try:
            await load_job_config()
        except Exception as e:
            logger.warning(f"GcpJobRunner: failed to warm job cache: {e}")

    def can_handle(self, task_type: str) -> bool:
        """Returns True if a Cloud Run Job is configured for this task type."""
        from dynastore.modules.gcp.tools.jobs import get_job_map_sync
        return task_type in get_job_map_sync()

    async def run(self, context: RunnerContext) -> Optional[Any]:
        """Dispatch the task to a Cloud Run Job.

        Returns either the freshly-created :class:`Task` (REST path) or
        :data:`DEFERRED_COMPLETION` (dispatcher path). On Cloud Run trigger
        failure, the row is marked FAILED and the exception is re-raised.
        """
        from dynastore.modules.gcp.tools.jobs import (
            load_job_config,
            run_cloud_run_job_async,
            try_load_process_definition,
            get_job_max_retries,
        )

        job_map = await load_job_config()
        job_name = job_map.get(context.task_type)
        if not job_name:
            return None
        # Coerce inputs to a plain dict — caller may have passed an
        # ExecuteRequest or a JSON string in some legacy paths.
        raw_inputs: Any = context.inputs
        if isinstance(raw_inputs, str):
            import json as _json
            inputs_dict = _json.loads(raw_inputs)
        elif isinstance(raw_inputs, dict):
            inputs_dict = dict(raw_inputs)
        elif hasattr(raw_inputs, "model_dump"):
            inputs_dict = raw_inputs.model_dump()
        else:
            inputs_dict = {}

        # Discriminator: a non-empty task_id in extra_context means the
        # dispatcher already claimed a PENDING row and is delegating
        # execution to us. Reuse that row instead of creating another.
        claimed_task_id = context.extra_context.get("task_id") if context.extra_context else None

        execution_id = generate_id_hex()
        owner_id = f"gcp_cloud_run_{execution_id}"
        task_lease = timedelta(seconds=_DEFAULT_TASK_TIMEOUT_SECONDS)
        new_locked_until = datetime.now(timezone.utc) + task_lease

        if claimed_task_id is not None:
            # Dispatcher path: extend lease on the existing row.
            import uuid
            task_id_uuid = uuid.UUID(str(claimed_task_id))
            await tasks_module.update_task(
                context.engine,
                task_id_uuid,
                TaskUpdate(
                    owner_id=owner_id,
                    locked_until=new_locked_until,
                ),
                schema=context.db_schema,
            )
            task_id_for_payload = task_id_uuid
            existing_task = None  # not needed; dispatcher already has the row
            logger.info(
                f"GcpJobRunner: dispatcher-path reuse of task '{task_id_uuid}' for "
                f"job '{job_name}' (execution_id={execution_id}, lease={task_lease.total_seconds():.0f}s)."
            )
        else:
            # REST path: create a fresh PENDING row and immediately mark ACTIVE.
            # Honour the Cloud Run job's MAX_RETRIES env (capped at job-level
            # rather than the column default of 3) so a single misbehaving job
            # cannot loop more than once by default.
            job_max_retries = get_job_max_retries(context.task_type)
            # Optional: caller may pre-supply a dedup_key in extra_context to
            # collapse at-least-once redeliveries (Pub/Sub push, retry storms).
            dedup_key = (
                context.extra_context.get("dedup_key") if context.extra_context else None
            )

            task_create_request = TaskCreate(
                caller_id=context.caller_id,
                task_type=context.task_type,
                inputs=inputs_dict,
                max_retries=job_max_retries if job_max_retries is not None else 3,
                dedup_key=dedup_key,
            )
            new_task = await tasks_module.create_task(
                context.engine, task_create_request, schema=context.db_schema
            )
            if new_task is None:
                logger.info(
                    f"GcpJobRunner: dedup hit on task_type='{context.task_type}' "
                    f"dedup_key='{dedup_key}' — skipping Cloud Run Job dispatch."
                )
                return None

            from dynastore.models.tasks import TaskStatusEnum
            update_data = TaskUpdate(
                status=TaskStatusEnum.ACTIVE,
                owner_id=owner_id,
                locked_until=new_locked_until,
            )
            await tasks_module.update_task(
                context.engine, new_task.task_id, update_data, schema=context.db_schema
            )
            task_id_for_payload = new_task.task_id
            existing_task = new_task
            logger.info(
                f"GcpJobRunner: REST-path created task '{new_task.task_id}' for "
                f"job '{job_name}' (execution_id={execution_id}, "
                f"lease={task_lease.total_seconds():.0f}s, "
                f"max_retries={job_max_retries if job_max_retries is not None else 'default'})."
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
                task_id=task_id_for_payload,
                caller_id=context.caller_id,
                execution_request=exec_req,
            )
        else:
            from dynastore.modules.tasks.models import TaskPayload
            payload = TaskPayload(
                task_id=task_id_for_payload,
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
                f"Failed to trigger Cloud Run job '{job_name}' for task '{task_id_for_payload}': {e}",
                exc_info=True,
            )
            from dynastore.models.tasks import TaskStatusEnum
            await tasks_module.update_task(
                conn=context.engine,
                task_id=task_id_for_payload,
                update_data=TaskUpdate(
                    status=TaskStatusEnum.FAILED,
                    error_message=str(e),
                ),
                schema=context.db_schema,
            )
            raise
        finally:
            logger.info(f"Dispatched Cloud Run job '{job_name}'.")

        # Dispatcher path: tell the dispatcher the row is being handled
        # asynchronously by the Cloud Run Job container — it will write
        # COMPLETED / FAILED itself via main_task.py.
        if claimed_task_id is not None:
            return DEFERRED_COMPLETION

        return existing_task
