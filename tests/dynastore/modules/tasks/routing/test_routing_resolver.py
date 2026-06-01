"""Unit tests for routing/resolver.py select_target.

Covers:
- Superset match: request hints must be a subset of effective hints.
- Longest-hints tie-break wins.
- Empty request hints -> first viable target in order.
- Wrong-service targets are filtered out.
- consumers == [] matches any service.
- No match returns None.
- Async resolved_targets: fail-open on exception; returns list from config.
"""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import RunnerTarget, TaskRoutingConfig
from dynastore.modules.tasks.routing.resolver import resolved_targets, select_target


def _all_runners_ok(runner: str) -> bool:
    return True


def _no_runner_ok(runner: str) -> bool:
    return False


def _bg_only(runner: str) -> bool:
    return runner == "background"


# ---------------------------------------------------------------------------
# select_target — basic matching
# ---------------------------------------------------------------------------


def test_select_target_empty_request_hints_returns_longest_hints_first():
    """With empty request_hints all viable targets match; the longest-hints one wins."""
    targets = [
        RunnerTarget(runner="background", consumers=["catalog"], hints={ExecHint.BACKGROUND}),
        RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"], hints={ExecHint.OFFLOAD, ExecHint.HEAVY}),
    ]
    result = select_target(targets, frozenset(), "catalog", _all_runners_ok)
    # gcp_cloud_run has 2 hints (longer), background has 1 -- longest wins.
    assert result is targets[1]


def test_select_target_empty_request_hints_same_hint_length_preserves_order():
    """With equal-length hints, original position (first) wins as tiebreaker."""
    t1 = RunnerTarget(runner="background", consumers=[], hints={ExecHint.BACKGROUND})
    t2 = RunnerTarget(runner="background", consumers=[], hints={ExecHint.SYNC})
    result = select_target([t1, t2], frozenset(), "catalog", _all_runners_ok)
    assert result is t1


def test_select_target_hint_superset_match():
    targets = [
        RunnerTarget(runner="background", consumers=["catalog"], hints={ExecHint.BACKGROUND}),
        RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"], hints={ExecHint.OFFLOAD, ExecHint.HEAVY}),
    ]
    result = select_target(
        targets,
        frozenset({ExecHint.OFFLOAD}),
        "catalog",
        _all_runners_ok,
    )
    # Only gcp_cloud_run has OFFLOAD in its effective hints
    assert result is targets[1]


def test_select_target_no_match_returns_none():
    targets = [
        RunnerTarget(runner="background", consumers=["catalog"], hints={ExecHint.BACKGROUND}),
    ]
    result = select_target(
        targets,
        frozenset({ExecHint.OFFLOAD}),
        "catalog",
        _all_runners_ok,
    )
    assert result is None


# ---------------------------------------------------------------------------
# select_target — consumer filtering
# ---------------------------------------------------------------------------


def test_select_target_wrong_service_filtered():
    targets = [
        RunnerTarget(runner="background", consumers=["maps"], hints={ExecHint.BACKGROUND}),
    ]
    result = select_target(targets, frozenset(), "catalog", _all_runners_ok)
    assert result is None


def test_select_target_empty_consumers_matches_any_service():
    target = RunnerTarget(runner="background", consumers=[], hints={ExecHint.BACKGROUND})
    result = select_target([target], frozenset(), "any_service", _all_runners_ok)
    assert result is target


def test_select_target_service_in_consumers_matches():
    target = RunnerTarget(runner="background", consumers=["catalog", "maps"], hints={ExecHint.BACKGROUND})
    assert select_target([target], frozenset(), "catalog", _all_runners_ok) is target
    assert select_target([target], frozenset(), "maps", _all_runners_ok) is target


# ---------------------------------------------------------------------------
# select_target — can_handle filtering
# ---------------------------------------------------------------------------


def test_select_target_can_handle_false_filters_out():
    targets = [
        RunnerTarget(runner="gcp_cloud_run", consumers=[], hints={ExecHint.OFFLOAD}),
        RunnerTarget(runner="background", consumers=[], hints={ExecHint.BACKGROUND}),
    ]
    result = select_target(targets, frozenset(), "catalog", _bg_only)
    assert result is targets[1]


def test_select_target_all_can_handle_false_returns_none():
    targets = [RunnerTarget(runner="gcp_cloud_run", consumers=[], hints={ExecHint.OFFLOAD})]
    assert select_target(targets, frozenset(), "catalog", _bg_only) is None


# ---------------------------------------------------------------------------
# select_target — longest-hints tie-break
# ---------------------------------------------------------------------------


def test_select_target_longest_hints_wins():
    """When multiple targets match, the one with the most declared hints wins."""
    short = RunnerTarget(runner="background", consumers=[], hints={ExecHint.BACKGROUND})
    long = RunnerTarget(
        runner="background",
        consumers=[],
        hints={ExecHint.BACKGROUND, ExecHint.SYNC},
    )
    # Put short first; long should still win because it has more hints.
    result = select_target(
        [short, long],
        frozenset({ExecHint.BACKGROUND}),
        "catalog",
        _all_runners_ok,
    )
    assert result is long


def test_select_target_empty_hints_on_target_is_universe():
    """A target with no declared hints matches any request (full universe)."""
    universal = RunnerTarget(runner="background", consumers=[], hints=set())
    specific = RunnerTarget(runner="background", consumers=[], hints={ExecHint.BACKGROUND})
    # Both match; universal has the larger effective hint set so it wins.
    result = select_target(
        [specific, universal],
        frozenset({ExecHint.BACKGROUND}),
        "catalog",
        _all_runners_ok,
    )
    assert result is universal


# ---------------------------------------------------------------------------
# Async resolved_targets — fail-open
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolved_targets_fail_open_on_exception(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as _resolver

    async def _boom(task_key):
        raise RuntimeError("config unavailable")

    # Patch _load_config to raise
    async def _bad_load():
        raise RuntimeError("DB down")

    monkeypatch.setattr(_resolver, "_load_config", _bad_load)
    result = await resolved_targets("any_task")
    assert result == []


@pytest.mark.asyncio
async def test_resolved_targets_returns_targets_from_config(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as _resolver

    target = RunnerTarget(runner="background")
    cfg = TaskRoutingConfig(
        tasks={"my_task": [target]},
        processes={},
    )

    async def _fake_load():
        return cfg

    monkeypatch.setattr(_resolver, "_load_config", _fake_load)
    result = await resolved_targets("my_task")
    assert len(result) == 1
    assert result[0].runner == "background"


@pytest.mark.asyncio
async def test_resolved_targets_unknown_key_returns_empty(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as _resolver

    async def _fake_load():
        return TaskRoutingConfig(tasks={}, processes={})

    monkeypatch.setattr(_resolver, "_load_config", _fake_load)
    result = await resolved_targets("no_such_task")
    assert result == []
