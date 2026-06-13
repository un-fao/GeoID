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

"""Canonical execution hints — the per-request preference axis for task routing.

Two-axis task routing model
============================

Task dispatch separates two orthogonal concerns. Each axis answers a
different question; every concept the task routing layer deals with belongs
to exactly one of them.

::

    +---------------+--------------------------+--------------------------+
    | Axis          | Question it answers      | Mechanism                |
    +===============+==========================+==========================+
    | ``TaskKind``  | What KIND of task?       | ``task_kind()`` in the   |
    |               | (system-task vs process) | registry — not a hint    |
    +---------------+--------------------------+--------------------------+
    | ``ExecHint``  | Which RunnerTarget does  | ``ExecHint`` (this       |
    |               | the caller want?         | module).  Caller passes  |
    |               | (per-request preference) | ``hints=frozenset({...})``|
    |               |                          | to ``select_target``;    |
    |               |                          | targets self-declare     |
    |               |                          | supported hints via      |
    |               |                          | ``RunnerTarget.hints``.  |
    +---------------+--------------------------+--------------------------+

Rule of thumb
-------------

* ``ExecHint`` is an adjective on a request — adding one is cheap and
  does not touch DB schemas or migrations.  A new "kind of execution
  preference" (e.g. ``streaming``, ``batched``) is a new hint, not a
  new model field.

Why a single canonical ``ExecHint`` catalogue
----------------------------------------------

Hint strings were previously implicit in placement-config documentation.
This module is the single source of truth.  The values are snake_case
string-comparable (``ExecHint.OFFLOAD == "offload"`` holds at runtime)
so persisted ``RunnerTarget.hints`` strings remain cheap to compare.
"""

from enum import StrEnum


class ExecHint(StrEnum):
    """Per-request execution preference carried in a task/process payload.

    Mirrors the storage-router Hint axis: a soft, semantic suggestion that
    biases which ``RunnerTarget`` is chosen.  Empty request hints -> first
    viable target; no match -> empty selection and the caller degrades
    gracefully.

    Members are grouped by concern below; group order is editorial only.
    """

    # ── Latency / interactivity preferences ───────────────────────────
    # Callers that are blocking a user interaction pass INTERACTIVE or
    # SYNC so the dispatcher favours an in-process runner rather than
    # queuing behind a background worker. BACKGROUND and OFFLOAD point
    # in the opposite direction: fire-and-forget semantics.
    INTERACTIVE = "interactive"   # caller waiting interactively; prefer in-process
    SYNC = "sync"                 # caller blocks for an inline result; prefer a sync runner
    BACKGROUND = "background"     # in-process async on the claiming pod
    OFFLOAD = "offload"           # delegate to an external executor (Cloud Run Job / worker queue)

    # ── Workload weight ────────────────────────────────────────────────
    # HEAVY is a workload-weight tag, orthogonal to runner type. A heavy
    # job biases toward offload targets so it doesn't starve the in-process
    # worker pool. Use alongside OFFLOAD for clarity.
    HEAVY = "heavy"               # large/long job; bias toward offload targets
