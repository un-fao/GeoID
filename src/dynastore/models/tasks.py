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
from datetime import datetime, timezone, UTC, timedelta
from enum import Enum

# --- Enumerations ---

class TaskStatusEnum(str, Enum):
    """Internal task status enumeration."""
    PENDING     = "PENDING"
    ACTIVE      = "ACTIVE"       # Claimed by a runner; heartbeat expected
    RUNNING     = "RUNNING"      # Legacy alias kept for backwards compatibility
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    DISMISSED   = "DISMISSED"
    DEAD_LETTER = "DEAD_LETTER"  # max_retries exhausted; requires manual intervention


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
    type: str = "task"  # 'task' or 'process'
    inputs: Optional[Dict[str, Any]] = None
    collection_id: Optional[str] = None

class TaskCreate(TaskBase):
    extra_context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional context passed by the dispatcher (e.g. originating_event for rollback). "
                    "Not persisted to the DB — forwarded through RunnerContext at dispatch time.",
    )

class TaskUpdate(BaseModel):
    status: Optional[TaskStatusEnum] = None
    progress: Optional[int] = Field(None, ge=0, le=100)
    outputs: Optional[Any] = None
    error_message: Optional[str] = None
    # Queue control fields (set by dispatcher / heartbeat manager)
    locked_until: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    owner_id: Optional[str] = None
    retry_count: Optional[int] = None

class Task(TaskBase):
    """
    Task model aligned with standard API job status format.
    Includes durable queue fields for dispatcher / Janitor / heartbeat.
    """
    jobID: UUID = Field(default_factory=uuid4, alias="task_id")
    type: str = Field(default="task", description="Type of job: 'task' or 'process'")

    status: TaskStatusEnum = TaskStatusEnum.PENDING
    progress: int = Field(default=0, ge=0, le=100)

    outputs: Optional[Any] = None
    error_message: Optional[str] = Field(None, alias="message")

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), alias="created")
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = Field(None, alias="updated")

    # Queue control — populated by dispatcher / heartbeat manager
    locked_until: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    owner_id: Optional[str] = None        # CloudIdentity email or caller_id
    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)

    links: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @property
    def task_id(self) -> UUID:
        """Backward compatibility alias for jobID."""
        return self.jobID
