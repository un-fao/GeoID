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

from typing import Optional
from google.api_core.operation import Operation
from dynastore.modules import get_protocol
from dynastore.models.protocols import JobExecutionProtocol
from dynastore.tools.cache import cached
import logging

logger = logging.getLogger(__name__)


async def run_cloud_run_job_async(
    job_name: str, args: Optional[list] = None, env_vars: Optional[dict] = None
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
