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

"""#1647 regression: cascade_cleanup must have an EXPLICIT, claimable route.

#1647 root cause: a cascade_cleanup task sat PENDING forever because no
deployed consumer service was registered to claim it — routing was implicit
and a missing consumer was silent.  The routing model fixes this by pinning
every system task to its affinity tier with a concrete runner, so a deployed
catalog service WILL claim it, and a *missing* runner surfaces as
``select_target -> None`` (fail-open + stuck-pending warner) instead of a
silent queue backlog.

These assertions encode that invariant for the real cascade_cleanup task
(``task_type='cascade_cleanup'``, kind ``'task'``, ``affinity_tier='catalog'``
— see ``tasks/cascade_cleanup/task.py``).  DB-free; run with the isolated
``--noconftest`` recipe.
"""
from __future__ import annotations

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
from dynastore.modules.tasks.routing.resolver import select_target

# Faithful to tasks/cascade_cleanup/task.py: task_type, kind, mandatory affinity.
_CASCADE = InventoryItem(task_key="cascade_cleanup", kind="task", affinity_tier="catalog")


def _build(preset: str):
    tasks_map, _ = build_routing_matrix([_CASCADE], preset=preset)
    return tasks_map["cascade_cleanup"]


def test_cascade_cleanup_has_explicit_catalog_background_route_cloud():
    targets = _build("cloud")
    assert len(targets) == 1
    t = targets[0]
    # The #1647 fix: an EXPLICIT consumer tier — not [] ("any/none") — is named.
    assert t.consumers == ["catalog"]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints


def test_cascade_cleanup_route_is_explicit_onprem_too():
    t = _build("onprem")[0]
    assert t.consumers == ["catalog"]
    assert t.runner == "background"


def test_catalog_worker_claims_cascade_cleanup():
    """A deployed catalog pod with a background runner WILL claim it (no starvation)."""
    chosen = select_target(
        _build("cloud"), frozenset(), "catalog",
        can_handle=lambda runner_type: runner_type == "background",
    )
    assert chosen is not None
    assert chosen.runner == "background"


def test_non_catalog_pod_does_not_claim_cascade_cleanup():
    """The consumers=['catalog'] gate keeps a maps pod from claiming a catalog task."""
    chosen = select_target(
        _build("cloud"), frozenset(), "maps",
        can_handle=lambda runner_type: runner_type == "background",
    )
    assert chosen is None


def test_cascade_cleanup_unclaimable_when_no_background_runner_is_observable():
    """If the catalog pod has NO background runner, select_target returns None —
    the dispatcher fail-opens and the stuck-pending warner surfaces it: the exact
    #1647 condition, now observable instead of a silent backlog."""
    chosen = select_target(
        _build("cloud"), frozenset(), "catalog",
        can_handle=lambda runner_type: False,  # no runner wired on this pod
    )
    assert chosen is None
