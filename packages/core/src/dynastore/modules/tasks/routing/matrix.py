"""Pure builder for the default task routing matrix.

Shared by ``TaskRoutingConfig._materialize_if_empty`` and the deployment
presets.  Profile-independent per-entry data lives here; only the runner
type and hint set differ between cloud and onprem profiles.

Two flavours:

* ``cloud``  — processes run as GCP Cloud Run Jobs (``gcp_cloud_run``
  runner) unless they are lightweight; lightweight processes and all
  system tasks stay in-process (``background``).
* ``onprem`` — all tasks and processes run as background workers; heavy
  processes get the ``HEAVY`` hint to signal they should be served by a
  dedicated worker tier.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import RunnerTarget


@dataclass(frozen=True)
class InventoryItem:
    """One entry from the live task registry, reduced to routing-relevant fields."""

    task_key: str
    kind: str                     # "process" | "task"
    affinity_tier: Optional[str]  # e.g. "catalog"; None for tier-agnostic


# Cloud topology — consumer lists for processes that fan to non-catalog services.
# Ported from the FAO production seed and the retired routing JSON.
# The DEFAULT for any process not listed here is ``["catalog"]``.
CLOUD_PROCESS_CONSUMERS: Dict[str, List[str]] = {
    "gdal": ["catalog", "maps"],
    "tiles_preseed": ["maps"],
    "tiles_export": ["maps"],
}

# Processes that must stay in-process even under the cloud profile
# (lightweight enough that spinning up a Cloud Run Job would be wasteful).
LIGHTWEIGHT_PROCESSES: frozenset = frozenset({"requeue_dead_letter_tasks"})


def build_routing_matrix(
    inventory: Iterable[InventoryItem],
    preset: str = "cloud",
) -> Tuple[Dict[str, List[RunnerTarget]], Dict[str, List[RunnerTarget]]]:
    """Build the (tasks_map, processes_map) routing matrices for ``preset``.

    Args:
        inventory: Iterable of ``InventoryItem`` from the task registry.
        preset:    ``"cloud"`` (default), ``"onprem"``, or ``"review"``.

    Returns:
        A 2-tuple ``(tasks_map, processes_map)`` where each map is
        ``{task_key: [RunnerTarget, ...]}`` in application order.

    Selection semantics (per preset)
    ---------------------------------

    **System tasks** (``kind == "task"``), all presets:
        A single ``background`` entry whose ``consumers`` list contains the
        task's ``affinity_tier`` (falls back to ``"catalog"`` for
        tier-agnostic tasks).

    **Processes** (``kind == "process"``), cloud preset:
        Lightweight processes (``key in LIGHTWEIGHT_PROCESSES``) stay in-
        process: ``background`` runner, ``consumers=["catalog"]``,
        ``hints={BACKGROUND}``.
        All other processes offload to GCP Cloud Run Jobs: ``gcp_cloud_run``
        runner, ``consumers`` from ``CLOUD_PROCESS_CONSUMERS`` (defaulting
        to ``["catalog"]``), ``hints={OFFLOAD, HEAVY}``.  The ``options``
        dict is intentionally empty — job-name discovery is the runner's
        responsibility.

    **Processes** (``kind == "process"``), onprem preset:
        All processes run as background workers on the ``"worker"`` service
        tier.  Lightweight processes get ``hints={BACKGROUND}``; heavy
        processes get ``hints={HEAVY}`` to signal they should be deferred
        to a dedicated worker queue.

    **Processes** (``kind == "process"``), review preset:
        Mirrors the cloud preset for every process EXCEPT ``gdal``, which
        runs as an in-process async background task on the catalog pod:
        ``background`` runner, ``consumers=["catalog"]``,
        ``hints={BACKGROUND, INTERACTIVE}``.  All other processes keep the
        same cloud target (``gcp_cloud_run`` + ``{OFFLOAD, HEAVY}``).
        Intended for review and local images only; production images use the
        cloud preset unchanged.
    """
    tasks_map: Dict[str, List[RunnerTarget]] = {}
    processes_map: Dict[str, List[RunnerTarget]] = {}

    for item in inventory:
        if item.kind == "task":
            tier = item.affinity_tier or "catalog"
            tasks_map[item.task_key] = [
                RunnerTarget(
                    consumers=[tier],
                    runner="background",
                    hints={ExecHint.BACKGROUND},
                )
            ]
        else:
            # kind == "process"
            if preset == "cloud":
                if item.task_key in LIGHTWEIGHT_PROCESSES:
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=["catalog"],
                            runner="background",
                            hints={ExecHint.BACKGROUND},
                        )
                    ]
                else:
                    consumers = CLOUD_PROCESS_CONSUMERS.get(item.task_key, ["catalog"])
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=list(consumers),
                            runner="gcp_cloud_run",
                            hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
                        )
                    ]
            elif preset == "review":
                if item.task_key == "gdal":
                    # gdal runs in-process on the catalog pod in review/local images.
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=["catalog"],
                            runner="background",
                            hints={ExecHint.BACKGROUND, ExecHint.INTERACTIVE},
                        )
                    ]
                elif item.task_key in LIGHTWEIGHT_PROCESSES:
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=["catalog"],
                            runner="background",
                            hints={ExecHint.BACKGROUND},
                        )
                    ]
                else:
                    consumers = CLOUD_PROCESS_CONSUMERS.get(item.task_key, ["catalog"])
                    processes_map[item.task_key] = [
                        RunnerTarget(
                            consumers=list(consumers),
                            runner="gcp_cloud_run",
                            hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
                        )
                    ]
            else:
                # onprem — all processes to the worker tier
                if item.task_key in LIGHTWEIGHT_PROCESSES:
                    hints: frozenset = frozenset({ExecHint.BACKGROUND})
                else:
                    hints = frozenset({ExecHint.HEAVY})
                processes_map[item.task_key] = [
                    RunnerTarget(
                        consumers=["worker"],
                        runner="background",
                        hints=set(hints),
                    )
                ]

    return tasks_map, processes_map
