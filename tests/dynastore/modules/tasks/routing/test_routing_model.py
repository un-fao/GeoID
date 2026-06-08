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

"""Unit tests for routing/model.py and routing/exec_hints.py.

Covers:
- Action validation: ROUTE requires process; non-ROUTE forbids process.
- ExecHint rejects unknown strings.
- TaskRoutingConfig.class_key() == "task_routing_config".
- _materialize_if_empty fills non-empty from the registry.
- resolved_targets bucket selection (tasks vs processes, fallback, unknown).
"""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import (
    Action,
    ActionVerb,
    RunnerTarget,
    TaskRoutingConfig,
)


# ---------------------------------------------------------------------------
# ExecHint
# ---------------------------------------------------------------------------


def test_exec_hint_values_are_string_comparable():
    assert ExecHint.BACKGROUND == "background"
    assert ExecHint.OFFLOAD == "offload"
    assert ExecHint.HEAVY == "heavy"
    assert ExecHint.SYNC == "sync"
    assert ExecHint.INTERACTIVE == "interactive"


def test_exec_hint_rejects_unknown_string():
    """RunnerTarget.hints is Set[ExecHint]; unknown strings should fail validation."""
    with pytest.raises((ValueError, Exception)):
        RunnerTarget(runner="background", hints={"totally_unknown_hint"})


def test_exec_hint_accepts_valid_strings():
    t = RunnerTarget(runner="background", hints={"background", "heavy"})
    assert ExecHint.BACKGROUND in t.hints
    assert ExecHint.HEAVY in t.hints


# ---------------------------------------------------------------------------
# Action validation
# ---------------------------------------------------------------------------


def test_action_route_requires_process():
    with pytest.raises(ValueError, match="process"):
        Action(action=ActionVerb.ROUTE)


def test_action_route_with_process_is_valid():
    a = Action(action=ActionVerb.ROUTE, process="some_task")
    assert a.process == "some_task"


def test_action_non_route_forbids_process():
    with pytest.raises(ValueError, match="process"):
        Action(action=ActionVerb.REPORT, process="some_task")


def test_action_non_route_without_process_is_valid():
    for verb in (ActionVerb.REPORT, ActionVerb.DEAD_LETTER, ActionVerb.FAIL):
        a = Action(action=verb)
        assert a.process is None


def test_action_default_is_report():
    a = Action()
    assert a.action == ActionVerb.REPORT


# ---------------------------------------------------------------------------
# RunnerTarget defaults
# ---------------------------------------------------------------------------


def test_runner_target_defaults():
    t = RunnerTarget(runner="background")
    assert t.consumers == []
    assert t.hints == set()
    assert t.options == {}
    assert t.on_success.action == ActionVerb.REPORT
    assert t.on_failure.action == ActionVerb.DEAD_LETTER


# ---------------------------------------------------------------------------
# TaskRoutingConfig.class_key
# ---------------------------------------------------------------------------


def test_task_routing_config_class_key():
    assert TaskRoutingConfig.class_key() == "task_routing_config"


# ---------------------------------------------------------------------------
# _materialize_if_empty
# ---------------------------------------------------------------------------


def test_materialize_if_empty_leaves_non_empty_config_unchanged():
    existing = {"my_task": [RunnerTarget(runner="background")]}
    cfg = TaskRoutingConfig(tasks=existing)
    # tasks was non-empty so _materialize_if_empty must not overwrite it
    assert "my_task" in cfg.tasks
    assert cfg.tasks["my_task"][0].runner == "background"


def test_materialize_with_empty_registry(monkeypatch):
    """When the registry is empty the config stays empty (no crash)."""
    import dynastore.tasks as _tasks_module
    monkeypatch.setattr(_tasks_module, "_DYNASTORE_TASKS", {})
    cfg = TaskRoutingConfig()
    # registry was empty -> both maps stay empty
    assert cfg.tasks == {}
    assert cfg.processes == {}


def test_materialize_with_populated_registry(monkeypatch):
    """When the registry has tasks and processes the maps are populated."""
    from dataclasses import dataclass
    from typing import Any, Optional

    import dynastore.tasks as _tasks_module

    @dataclass
    class _FakeCls:
        affinity_tier: Optional[str] = "catalog"

    @dataclass
    class _FakeCfg:
        cls: Any
        definition: Any = None

    fake_tasks = {
        "my_sys_task": _FakeCfg(cls=_FakeCls()),
        "my_process": _FakeCfg(cls=_FakeCls(), definition=object()),
    }
    monkeypatch.setattr(_tasks_module, "_DYNASTORE_TASKS", fake_tasks)

    cfg = TaskRoutingConfig()
    assert "my_sys_task" in cfg.tasks
    assert "my_process" in cfg.processes


# ---------------------------------------------------------------------------
# resolved_targets bucket selection
# ---------------------------------------------------------------------------


def test_resolved_targets_tasks_bucket_takes_priority():
    target = RunnerTarget(runner="background")
    cfg = TaskRoutingConfig(
        tasks={"shared": [target]},
        processes={"shared": [RunnerTarget(runner="gcp_cloud_run")]},
    )
    result = cfg.resolved_targets("shared")
    assert len(result) == 1
    assert result[0].runner == "background"


def test_resolved_targets_falls_back_to_processes():
    target = RunnerTarget(runner="gcp_cloud_run")
    cfg = TaskRoutingConfig(tasks={}, processes={"my_proc": [target]})
    result = cfg.resolved_targets("my_proc")
    assert len(result) == 1
    assert result[0].runner == "gcp_cloud_run"


def test_resolved_targets_returns_empty_for_unknown_key():
    cfg = TaskRoutingConfig(tasks={"x": [RunnerTarget(runner="background")]})
    assert cfg.resolved_targets("unknown") == []
