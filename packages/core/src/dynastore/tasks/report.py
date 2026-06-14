#    Copyright 2026 FAO
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

"""Typed internal result envelope for durable tasks (#1807 P2).

Provides :class:`TaskReport`, a structured intermediate between a task's
``run()`` return value and the raw ``outputs`` JSON persisted to the tasks
table.  It is **additive** — existing tasks that return plain dicts, a
``StatusInfo``, or ``None`` are not affected.

Only tasks that *want* structured reporting need to return a ``TaskReport``.
The persistence seam in :func:`normalize_task_result` converts it to the
wire-safe ``(outputs_dict, error_message)`` pair that ``complete_task`` /
``TaskUpdate`` already expect.

OGC ``message`` contract
-------------------------
:meth:`TaskReport.to_outputs` ALWAYS places ``message`` at the top level of
the returned dict when :attr:`TaskReport.message` is set, preserving the
load-bearing contract described in ``dynastore/tasks/result_message.py`` and
read by :func:`dynastore.modules.processes.models.task_to_status_info`.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

# Import only from the canonical model package (no circular-import risk —
# TaskStatusEnum lives in dynastore.models.tasks, which has no upstream
# dependency on this module).
from dynastore.models.tasks import TaskStatusEnum


# ---------------------------------------------------------------------------
# OperationError — error detail for a single operation or for the whole task
# ---------------------------------------------------------------------------


class OperationError(BaseModel):
    """Structured error detail attached to an operation or to the whole task.

    Attributes:
        code:      Machine-readable error code (e.g. ``"indexer_unavailable"``).
        message:   Human-readable description surfaced in logs / job status.
        retryable: Whether the caller should retry this operation. Used by
                   drain loops to decide between mark_retry and mark_dead.
        details:   Arbitrary key/value pairs for structured log enrichment.
    """

    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# OperationResult — outcome for one operation within a batch/drain cycle
# ---------------------------------------------------------------------------

# Status literals mirror the storage-drain outcome vocabulary so drain tasks
# can use the same type without mapping.  An open ``str`` fallback allows
# extension (e.g. "no_op") without breaking the type.
OperationStatus = Literal["passed", "retried", "poison", "skipped"] | str


class OperationResult(BaseModel):
    """Outcome for a single operation within a task (e.g. one storage row).

    Attributes:
        op_id:     Unique identifier for the operation (e.g. a UUID).
        status:    One of ``"passed"``, ``"retried"``, ``"poison"``,
                   ``"skipped"``, or a task-specific extension.
        entity_id: The entity this operation acted on (item_id, event_id …).
        error:     Structured error when status is not ``"passed"``.
        details:   Additional metadata for log enrichment.
    """

    op_id: str | None = None
    status: OperationStatus
    entity_id: str | None = None
    error: OperationError | None = None
    details: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# TaskReport — the task-level result envelope
# ---------------------------------------------------------------------------


class TaskReport(BaseModel):
    """Typed result envelope returned by a task's ``run()`` method.

    Replaces ad-hoc ``dict`` returns with a structured object that normalizes
    smoothly to the ``outputs`` JSON column.

    The envelope is **additive**: tasks that already return plain dicts or
    ``StatusInfo`` are unaffected (see :func:`normalize_task_result`).  Tasks
    opt in by returning a ``TaskReport`` instance.

    Attributes:
        status:      Terminal task status (COMPLETED or FAILED).
        message:     Human-facing message surfaced on the OGC job-status document
                     via ``outputs["message"]``.  LOAD-BEARING — :meth:`to_outputs`
                     always promotes this to a top-level ``"message"`` key.
        outputs:     Structured payload beyond the message (e.g. counts, URLs).
                     NOT serialized as a nested ``"outputs"`` key — its entries
                     are merged flat with ``message``, ``metrics``, and
                     ``operations`` in the final dict.
        error:       Structured error summary for the whole task.
        operations:  Per-operation breakdown (e.g. drain outcomes per row).
                     Omitted from the serialized dict when empty.
        metrics:     Counters and durations (populate where cheap).
                     Omitted from the serialized dict when empty.
        correlation: Tracing context forwarded from the task context
                     (task_id, catalog_id, owner_id …).  Omitted when empty.
    """

    status: TaskStatusEnum
    message: str | None = None
    outputs: dict[str, Any] = Field(default_factory=dict)
    error: OperationError | None = None
    operations: list[OperationResult] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    correlation: dict[str, str] = Field(default_factory=dict)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_outputs(self) -> dict[str, Any]:
        """Produce the JSON object persisted to ``task.outputs``.

        Rules:
        * The ``"message"`` key is **always** set when :attr:`message` is not
          ``None`` (load-bearing OGC contract).
        * :attr:`outputs` entries are merged at the top level.
        * ``"metrics"`` and ``"operations"`` are included only when non-empty.
        * ``"correlation"`` is included only when non-empty.
        * ``"error"`` is included only when present (its ``message`` is also
          surfaced via the separate ``error_message`` column; including it here
          gives API consumers a structured payload even for failures).

        The result is always a plain, JSON-serializable dict.
        """
        result: dict[str, Any] = dict(self.outputs)
        if self.message is not None:
            result["message"] = self.message
        if self.metrics:
            result["metrics"] = self.metrics
        if self.operations:
            result["operations"] = [op.model_dump(exclude_none=True) for op in self.operations]
        if self.correlation:
            result["correlation"] = self.correlation
        if self.error is not None:
            result["error"] = self.error.model_dump(exclude_none=True)
        return result

    def error_message_str(self) -> str | None:
        """Return the error message string for the ``error_message`` column.

        Maps :attr:`error.message` (structured error on this report) to the
        flat string the ``TaskUpdate.error_message`` / ``fail_task`` paths
        expect.  Returns ``None`` when no structured error is present.
        """
        return self.error.message if self.error is not None else None

    def log_details(self) -> dict[str, Any]:
        """Structured details dict for :func:`dynastore.modules.catalog.log_manager.log_event`.

        Suitable for the ``details=`` kwarg.  Includes status, error summary,
        metric snapshot, and correlation ids so a single log entry carries
        everything an operator needs to diagnose a terminal outcome.
        """
        details: dict[str, Any] = {"status": self.status.value}
        if self.error is not None:
            details["error"] = {
                "code": self.error.code,
                "message": self.error.message,
                "retryable": self.error.retryable,
            }
        if self.metrics:
            details["metrics"] = self.metrics
        if self.correlation:
            details["correlation"] = self.correlation
        return details

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def completed(
        cls,
        message: str | None = None,
        *,
        outputs: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        operations: list[OperationResult] | None = None,
        correlation: dict[str, str] | None = None,
    ) -> "TaskReport":
        """Build a COMPLETED :class:`TaskReport`."""
        return cls(
            status=TaskStatusEnum.COMPLETED,
            message=message,
            outputs=outputs or {},
            metrics=metrics or {},
            operations=operations or [],
            correlation=correlation or {},
        )

    @classmethod
    def failed(
        cls,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        operations: list[OperationResult] | None = None,
        correlation: dict[str, str] | None = None,
    ) -> "TaskReport":
        """Build a FAILED :class:`TaskReport` with a structured error."""
        return cls(
            status=TaskStatusEnum.FAILED,
            message=message,
            error=OperationError(
                code=code,
                message=message,
                retryable=retryable,
                details=details or {},
            ),
            metrics=metrics or {},
            operations=operations or [],
            correlation=correlation or {},
        )


# ---------------------------------------------------------------------------
# normalize_task_result — persistence-seam helper
# ---------------------------------------------------------------------------


def normalize_task_result(result: Any) -> tuple[Any, str | None]:
    """Normalise a task ``run()`` return value for persistence.

    Used at every seam where ``run()``'s return value is about to be written
    to ``task.outputs`` / ``TaskUpdate.error_message``.

    Returns ``(outputs_value, error_message_str)`` where:

    * For a :class:`TaskReport`: ``outputs_value`` is :meth:`TaskReport.to_outputs`
      (a plain dict with the OGC ``"message"`` key preserved) and
      ``error_message_str`` is :meth:`TaskReport.error_message_str`.
    * For anything else: ``(result, None)`` — verbatim passthrough, identical
      to the behaviour before this helper existed.
    """
    if isinstance(result, TaskReport):
        return result.to_outputs(), result.error_message_str()
    return result, None
