"""Hint-driven task routing package.

Replaces ``modules/tasks/placement`` with a richer routing model that
expresses the full intent (runner type, consumer affinity, execution hints,
and post-execution actions) rather than just a flat placement mode.

Public API (stable within this unit):

Model objects
    ExecHint, ActionVerb, Action, RunnerTarget, TaskRoutingConfig

Matrix builder
    InventoryItem, build_routing_matrix

Runtime helpers
    resolved_targets  (async, fail-open)
    select_target     (sync, pure -- no I/O)

Preset singletons
    CloudTaskRoutingPreset, OnpremTaskRoutingPreset
"""
from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
from dynastore.modules.tasks.routing.model import (
    Action,
    ActionVerb,
    RunnerTarget,
    TaskRoutingConfig,
)
from dynastore.modules.tasks.routing.presets import (
    CloudTaskRoutingPreset,
    OnpremTaskRoutingPreset,
)
from dynastore.modules.tasks.routing.resolver import resolved_targets, select_target

__all__ = [
    # hints
    "ExecHint",
    # model
    "ActionVerb",
    "Action",
    "RunnerTarget",
    "TaskRoutingConfig",
    # matrix
    "InventoryItem",
    "build_routing_matrix",
    # resolver
    "resolved_targets",
    "select_target",
    # presets
    "CloudTaskRoutingPreset",
    "OnpremTaskRoutingPreset",
]
