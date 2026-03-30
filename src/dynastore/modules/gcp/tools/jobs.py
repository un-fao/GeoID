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

from google.api_core.operation import Operation
from dynastore.modules import get_protocol
from dynastore.models.protocols import JobExecutionProtocol
from dynastore.tools.cache import cached
import logging

logger = logging.getLogger(__name__)


async def run_cloud_run_job_async(
    job_name: str, args: list = None, env_vars: dict = None
) -> Operation:
    """
    Triggers a serverless job asynchronously using the JobExecutionProtocol.

    Args:
        job_name (str): The name of the job to execute.
        args (list, optional): A list of command-line arguments to pass to the job's container.
        env_vars (dict, optional): A dictionary of environment variables to override in the job's container.

    Returns:
        Operation: The long-running operation object for the job execution.

    Raises:
        RuntimeError: If the JobExecutionProtocol implementation is not available.
    """
    job_runner = get_protocol(JobExecutionProtocol)
    if not job_runner:
        raise RuntimeError("JobExecutionProtocol not available. Unable to trigger job.")

    return await job_runner.run_job(job_name, args, env_vars)


@cached(maxsize=1, namespace="job_config", distributed=False)
async def load_job_config():
    """
    Discovers deployed jobs and returns a mapping of task_type to job name.
    """
    job_runner = get_protocol(JobExecutionProtocol)
    if not job_runner:
        logger.warning("JobExecutionProtocol not available. Unable to discover jobs.")
        return {}

    return await job_runner.get_job_config()


# def load_job_config():
#     """
#     Discovers deployed GCP Cloud Run jobs by querying the GCP API and returns a
#     mapping of task_type to the job's name.

#     The mapping is derived by inspecting the environment variables of each job
#     for a 'DYNASTORE_TASK_MODULES' key. The value of this key is used as the
#     task_type.
#     """
#     job_map = {}
#     credentials_result = get_credentials()
#     if not credentials_result:
#         logger.warning("Could not get GCP credentials. Unable to discover Cloud Run jobs.")
#         return job_map

#     credentials, gcp_config = credentials_result

#     if not gcp_config or not gcp_config.get("project_id"):
#         logger.warning("Could not determine GCP project. Unable to discover Cloud Run jobs.")
#         return job_map

#     project_id = gcp_config["project_id"]
#     # Use the same region fallback logic as other components
#     region = gcp_config.get("region", os.getenv("REGION", "europe-west1"))

#     try:
#         client = run_v2.JobsClient(credentials=credentials)
#         parent = f"projects/{project_id}/locations/{region}"
#         request = run_v2.ListJobsRequest(parent=parent)

#         logger.info(f"Discovering Cloud Run jobs in {parent}...")
#         for job in client.list_jobs(request=request):
#             job_name = job.name.split('/')[-1]
#             # Navigate through the job's template to find container environment variables
#             if job.template and job.template.template and job.template.template.containers:
#                 for container in job.template.template.containers:
#                     for env_var in container.env:
#                         if env_var.name == "DYNASTORE_TASK_MODULES":
#                             task_type = env_var.value
#                             if task_type:
#                                 job_map[task_type] = job_name
#                                 logger.info(f"Discovered GCP job mapping: task '{task_type}' -> job '{job_name}'")
#                             break # Found the variable in this container
#     except Exception as e:
#         logger.error(f"Error discovering Cloud Run jobs from GCP API: {e}", exc_info=True)

#     return job_map
