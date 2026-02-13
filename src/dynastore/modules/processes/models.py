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

from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from uuid import UUID
from datetime import datetime
from dynastore.models.shared_models import Link
import uuid
from dynastore.modules.tasks.models import TaskPayload

if TYPE_CHECKING:
    from dynastore.modules.tasks.models import Task

# --- OGC Process Definition Models ---

class JobControlOptions(str, Enum):
    """OGC standard values for job control."""
    SYNC_EXECUTE = "sync-execute"
    ASYNC_EXECUTE = "async-execute"
    DISMISS = "dismiss"

class TransmissionMode(str, Enum):
    """OGC standard values for output transmission."""
    VALUE = "value"
    REFERENCE = "reference"

class ProcessSummary(BaseModel):
    id: str
    title: str
    description: Optional[str] = None
    version: str
    jobControlOptions: List[JobControlOptions] = [JobControlOptions.ASYNC_EXECUTE]
    outputTransmission: List[TransmissionMode] = [TransmissionMode.REFERENCE]
    links: List[Link] = []

class ProcessInput(BaseModel):
    title: str
    description: Optional[str] = None
    schema_: Dict[str, Any] = Field(..., alias="schema")

class ProcessOutput(BaseModel):
    title: str
    description: Optional[str] = None
    schema_: Dict[str, Any] = Field(..., alias="schema")

class Process(ProcessSummary):
    inputs: Dict[str, ProcessInput]
    outputs: Dict[str, ProcessOutput]

    model_config = ConfigDict(from_attributes=True)

# --- OGC API Endpoint Models ---

class ProcessList(BaseModel):
    processes: List[ProcessSummary]
    links: List[Link]

class StatusInfo(BaseModel):
    """OGC API - Processes StatusInfo model for job status responses."""
    jobID: UUID
    status: str
    message: Optional[str] = None
    type: str = "process"
    progress: Optional[int] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    links: List[Link]

class OutputExecutionRequest(BaseModel):
    """Describes how an output should be returned/processed."""
    format: Optional[Dict[str, Any]] = None
    transmissionMode: Optional[TransmissionMode] = None

class ExecuteRequest(BaseModel):
    """
    Represents the input for a process execution.
    This model is designed to be used as the `inputs` type for TaskPayload,
    enabling processes to be executed as tasks.
    """
    inputs: Dict[str, Any]
    outputs: Optional[Dict[str, OutputExecutionRequest]] = None
    response: str = Field("document", pattern="^(document|raw)$")

    model_config = ConfigDict(arbitrary_types_allowed=True)

# Type alias for clarity: a process execution task payload
ProcessTaskPayload = TaskPayload[ExecuteRequest] # This remains for type hinting

def as_process_task_payload(
    task_id: UUID, 
    caller_id: str,
    execution_request: "ExecuteRequest"
) -> "ProcessTaskPayload":
    """
    Utility to wrap an ExecuteRequest as a ProcessTaskPayload for runner compatibility.
    This is now the canonical way to construct a task payload for a process.
    """
    return ProcessTaskPayload(
        task_id=task_id,
        caller_id=caller_id,
        inputs=execution_request
    )

def task_to_status_info(task: "Task", links: Optional[List[Link]] = None) -> StatusInfo:
    """
    Converts a generic Task model to an OGC API - Processes StatusInfo model.
    This is the translation layer between generic task execution and OGC API.
    
    Args:
        task: Generic Task model from tasks module
        links: Optional list of HATEOAS links to include
        
    Returns:
        OGC-compliant StatusInfo model
    """
    return StatusInfo(
        jobID=task.jobID,
        status=task.status.to_api_status(),  # Generic task status to API format
        message=task.error_message,
        type="process",  # OGC processes always use "process" type
        progress=task.progress,
        created=task.timestamp,
        updated=task.finished_at or task.started_at or task.timestamp,
        links=links or task.links or []
    )
