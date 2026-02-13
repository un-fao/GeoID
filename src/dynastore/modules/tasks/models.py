# /Users/ccancellieri/work/code/dynastore/src/dynastore/modules/tasks/models.py
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
from typing import Optional, Dict, Any, TypeVar, Generic, List
from uuid import UUID, uuid4
from datetime import datetime, timezone, UTC
from enum import Enum

import uuid

from dynastore.modules.db_config.query_executor import DbResource

class RunnerContext(BaseModel):
    engine: DbResource
    task_type: str
    caller_id: str = Field(..., min_length=1, description="Identifier of the user or system that initiated the task.")
    inputs: Dict[str, Any]
    asset: Optional[Any] = None
    db_schema: str = "tasks"
    extra_context: Dict[str, Any]

    model_config = ConfigDict(arbitrary_types_allowed=True)

# --- Enumerations ---

class TaskStatusEnum(str, Enum):
    """Internal task status enumeration, mapped to standard API status strings for responses."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DISMISSED = "DISMISSED"
    
    def to_api_status(self) -> str:
        """Maps internal status to standard API status strings (lowercase)."""
        mapping = {
            TaskStatusEnum.PENDING: "accepted",
            TaskStatusEnum.RUNNING: "running",
            TaskStatusEnum.COMPLETED: "successful",
            TaskStatusEnum.FAILED: "failed",
            TaskStatusEnum.DISMISSED: "dismissed"
        }
        return mapping.get(self, "unknown")

class TaskExecutionMode(str, Enum):
    """Defines the execution strategy for a task runner."""
    SYNCHRONOUS = "SYNCHRONOUS"
    ASYNCHRONOUS = "ASYNCHRONOUS"

# --- Generic Payload Model ---

InputsType = TypeVar("InputsType")

class TaskPayload(BaseModel, Generic[InputsType]):
    """
    A standardized data-transfer object (DTO) for passing information
    to a background task's `run` method.

    Used for both tasks and process executions (process-as-task).
    """
    task_id: UUID = Field(..., description="The unique ID of the job record from the database.")
    caller_id: str = Field(..., description="Identifier of the user or system that initiated the task.")
    inputs: InputsType = Field(..., description="The specific, strongly-typed inputs for the task.")
    asset: Optional[Any] = None

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

# --- Database & API Models ---

class TaskBase(BaseModel):
    caller_id: Optional[str] = None
    task_type: str
    inputs: Optional[Dict[str, Any]] = None
    collection_id: Optional[str] = None

class TaskCreate(TaskBase):
    pass

class TaskUpdate(BaseModel):
    status: Optional[TaskStatusEnum] = None
    progress: Optional[int] = Field(None, ge=0, le=100)
    outputs: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

class Task(TaskBase):
    """
    Task model aligned with standard API job status format for simplified type mapping.
    This model can be directly converted to StatusInfo for API responses.
    """
    # Aligned with StatusInfo: jobID instead of task_id
    jobID: UUID = Field(default_factory=uuid4, alias="task_id")
    
    # Aligned with StatusInfo: type field to distinguish tasks from processes
    type: str = Field(default="task", description="Type of job: 'task' or 'process'")
    
    # Status and progress
    status: TaskStatusEnum = TaskStatusEnum.PENDING
    progress: int = Field(default=0, ge=0, le=100)
    
    # Outputs and errors
    outputs: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = Field(None, alias="message")
    
    # Timestamps
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), alias="created")
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = Field(None, alias="updated")
    
    # Aligned with StatusInfo: links field for HATEOAS
    links: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @property
    def task_id(self) -> UUID:
        """Backward compatibility alias for jobID."""
        return self.jobID
    
    def to_status_info(self) -> Dict[str, Any]:
        """Converts Task to status info dict for API responses."""
        return {
            "jobID": str(self.jobID),
            "type": self.type,
            "status": self.status.to_api_status(),
            "message": self.error_message,
            "progress": self.progress,
            "created": self.timestamp,
            "updated": self.finished_at or self.started_at or self.timestamp,
            "links": self.links
        }
