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

"""select_runner_for: routing config picks the right runner; fail-open to priority order.

Covers:
- When routing resolves gcp_cloud_run and that runner can_handle, select_runner_for
  returns the gcp_cloud_run runner (cloud off-load preference).
- When the gcp runner is absent but background can_handle, fall back to background.
- When no targets resolve (empty list), fall back to the highest-priority capable runner.
- Fail-open: a resolver exception leaves the queue unbricked (returns a capable runner).
"""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import execution


class _MockRunner:
    def __init__(self, runner_type: str, priority: int, can: bool = True):
        self.runner_type = runner_type
        self.priority = priority
        self._can = can
        self.mode = None  # not used in select_runner_for path

    def can_handle(self, _task_type: str) -> bool:
        return self._can


def _aval(value):
    async def _f(*a, **k):
        return value
    return _f


@pytest.mark.asyncio
async def test_routing_picks_gcp_cloud_run_over_background(monkeypatch):
    """When routing resolves gcp_cloud_run and the runner can_handle, it is selected."""
    from dynastore.modules.tasks.routing.model import RunnerTarget
    from dynastore.modules.tasks.routing.exec_hints import ExecHint

    gcp_runner = _MockRunner("gcp_cloud_run", priority=10)
    bg_runner = _MockRunner("background", priority=100)

    target = RunnerTarget(
        runner="gcp_cloud_run",
        consumers=[],
        hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
    )

    import dynastore.modules.tasks.routing.resolver as routing_resolver
    monkeypatch.setattr(routing_resolver, "resolved_targets", _aval([target]))

    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode
    monkeypatch.setattr(runners_mod, "get_runners",
                        lambda mode: [bg_runner, gcp_runner] if mode == TaskExecutionMode.ASYNCHRONOUS else [])

    monkeypatch.setattr("dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog")

    chosen = await execution.select_runner_for("gdal")
    assert chosen is not None
    assert chosen.runner_type == "gcp_cloud_run"


@pytest.mark.asyncio
async def test_routing_falls_back_to_background_when_gcp_absent(monkeypatch):
    """When gcp_cloud_run is in the routing targets but no such runner is registered,
    fall back to the highest-priority capable runner (background)."""
    from dynastore.modules.tasks.routing.model import RunnerTarget
    from dynastore.modules.tasks.routing.exec_hints import ExecHint

    bg_runner = _MockRunner("background", priority=100)

    target = RunnerTarget(
        runner="gcp_cloud_run",
        consumers=[],
        hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
    )

    import dynastore.modules.tasks.routing.resolver as routing_resolver
    monkeypatch.setattr(routing_resolver, "resolved_targets", _aval([target]))

    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode
    # Only background is registered — gcp runner absent
    monkeypatch.setattr(runners_mod, "get_runners",
                        lambda mode: [bg_runner] if mode == TaskExecutionMode.ASYNCHRONOUS else [])

    monkeypatch.setattr("dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog")

    chosen = await execution.select_runner_for("gdal")
    # Fail-open: no gcp runner available -> fall back to background
    assert chosen is not None
    assert chosen.runner_type == "background"


@pytest.mark.asyncio
async def test_routing_fail_open_on_empty_targets(monkeypatch):
    """When routing returns no targets, fall back to highest-priority capable runner."""
    import dynastore.modules.tasks.routing.resolver as routing_resolver
    monkeypatch.setattr(routing_resolver, "resolved_targets", _aval([]))

    bg_runner = _MockRunner("background", priority=100)

    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode
    monkeypatch.setattr(runners_mod, "get_runners",
                        lambda mode: [bg_runner] if mode == TaskExecutionMode.ASYNCHRONOUS else [])

    monkeypatch.setattr("dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog")

    chosen = await execution.select_runner_for("any_task")
    assert chosen is not None
    assert chosen.runner_type == "background"


@pytest.mark.asyncio
async def test_routing_fail_open_on_resolver_exception(monkeypatch):
    """When the resolver raises, fall back gracefully — the queue must not brick."""
    import dynastore.modules.tasks.routing.resolver as routing_resolver

    async def _boom(_task_key):
        raise RuntimeError("config unreachable")

    monkeypatch.setattr(routing_resolver, "resolved_targets", _boom)

    bg_runner = _MockRunner("background", priority=100)

    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode
    monkeypatch.setattr(runners_mod, "get_runners",
                        lambda mode: [bg_runner] if mode == TaskExecutionMode.ASYNCHRONOUS else [])

    monkeypatch.setattr("dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog")

    chosen = await execution.select_runner_for("any_task")
    assert chosen is not None
    assert chosen.runner_type == "background"
