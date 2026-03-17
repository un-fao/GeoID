import logging
from typing import Dict, Any
from dynastore.tasks import _register_task, discover_tasks, get_all_task_configs
from dynastore.modules.gcp.tools.jobs import load_job_config 

logger = logging.getLogger(__name__)

def _create_cloud_run_task_class(task_type: str, job_name: str, process_definition: Any):
    """
    A factory function that creates a dedicated, explicit class for a remote
    Cloud Run task. This improves clarity and static analysis over using type().
    """
    
    class DynamicCloudRunTask:
        """
        This is a placeholder task class dynamically created for a remote Cloud Run job.
        Its purpose is to be discoverable by the task system and to expose
        an OGC Process definition if one is available. The actual execution logic
        is handled by the gcp_cloud_run_runner, not by a 'run' method on this class.
        """
        _task_type = task_type
        _job_name = job_name
        _process_definition = process_definition
        is_placeholder = True

        @staticmethod
        def get_definition() -> Process:
            return GCP_CLOUD_RUNNER_PROCESS_DEFINITION

    return DynamicCloudRunTask

async def register_cloud_run_jobs_as_tasks():
    """
    Discovers Cloud Run jobs from the configuration and dynamically registers
    them as placeholder tasks within the DynaStore task system.
    This allows them to be treated like any other task and be exposed as
    OGC Processes.
    """
    # Load the job configuration to find all available Cloud Run jobs
    gcp_job_mappings = await load_job_config()
    remote_task_types = list(gcp_job_mappings.keys())

    if not remote_task_types:
        logger.info("No remote Cloud Run jobs discovered. Skipping placeholder registration.")
        return

    # Normalize hyphenated task types to underscores for local discovery
    local_task_types = [t.replace("-", "_") for t in remote_task_types]
    logger.info(f"Found remote jobs. Discovering definitions for: {local_task_types}")
    
    # 1. Use the refactored discover_tasks to load the definitions for the remote tasks
    #    into the central registry.
    #    The new Clean Discovery Protocol ensures that even if strict dependencies are missing,
    #    we get the Process definition from `definition.py` and a Placeholder task.
    discover_tasks(include_only=local_task_types)

    # 2. Get the now-populated registry.
    all_configs = get_all_task_configs()

    # Dynamically create and register placeholder tasks for each discovered Cloud Run job
    for task_type, job_name in gcp_job_mappings.items():
        normalized_task_type = task_type.replace("-", "_")
        
        # 3. Create the placeholder, attaching the definition if it was found in the registry.
        task_config = all_configs.get(normalized_task_type)
        
        # If the task is already registered and NOT a placeholder, skip dynamic registration.
        # This occurs when we are in a worker environment that has the full implementation.
        if task_config and not getattr(task_config.cls, 'is_placeholder', False):
             logger.info(f"Task '{normalized_task_type}' is already registered as a full implementation. Skipping dynamic Cloud Run registration.")
             continue

        process_definition = task_config.definition if task_config else None

        # Dynamically create a class for each task_type.
        DynamicCloudRunTask = _create_cloud_run_task_class(task_type, job_name, process_definition)

        # Register the dynamically created class.
        # The 'registration_name' parameter is crucial as it's what processes_service.py will use.
        _register_task(DynamicCloudRunTask, registration_name=normalized_task_type)
        logger.info(f"Dynamically registered placeholder task '{normalized_task_type}' for Cloud Run job '{job_name}'.")
