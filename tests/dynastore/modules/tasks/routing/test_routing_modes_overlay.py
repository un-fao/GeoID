"""_overlay_routing_modes: routing config overrides observed modes, fail-open.

The registry's ``modes`` column is an informational label of how a task runs.
``_overlay_routing_modes`` replaces the in-process observed modes with the
runner name of the first resolved ``RunnerTarget`` — but only when routing has
an opinion. Any resolver error or empty target list leaves the observed modes
untouched (a degraded routing read must never distort publication).
"""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.registry import publisher
from dynastore.modules.tasks.registry.model import CapabilityRow
from dynastore.modules.tasks.routing import resolver as routing_resolver
from dynastore.modules.tasks.routing.model import RunnerTarget


def _row(task_key: str, modes):
    return CapabilityRow(
        service="catalog", task_key=task_key, kind="task", modes=list(modes),
        service_version="v", service_commit="c", version="c",
    )


@pytest.mark.asyncio
async def test_routing_runner_overrides_observed(monkeypatch):
    async def _fake_targets(task_key):
        return [RunnerTarget(runner="gcp_cloud_run", consumers=["maps"])]

    monkeypatch.setattr(routing_resolver, "resolved_targets", _fake_targets)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["gcp_cloud_run"]


@pytest.mark.asyncio
async def test_empty_targets_leave_observed_modes(monkeypatch):
    async def _none(task_key):
        return []

    monkeypatch.setattr(routing_resolver, "resolved_targets", _none)
    rows = [_row("reindex", ["async", "sync"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["async", "sync"]


@pytest.mark.asyncio
async def test_resolver_error_is_fail_open(monkeypatch):
    async def _boom(task_key):
        raise RuntimeError("routing config unreadable")

    monkeypatch.setattr(routing_resolver, "resolved_targets", _boom)
    rows = [_row("reindex", ["async"])]
    await publisher._overlay_routing_modes(rows)
    assert rows[0].modes == ["async"]  # untouched
