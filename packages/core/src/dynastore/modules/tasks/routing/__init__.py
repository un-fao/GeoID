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
