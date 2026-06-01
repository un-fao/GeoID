"""#1675 reconcile: catalogued tasks routed here but unclaimable are flagged + WARNed.

``reconcile_routing_capabilities`` is the consumer side of the
"runner declares what it can run" loop: a task routed to this service whose
runner cannot claim it (no background runner registered, or a Cloud Run Job not
deployed) is the #1647 starvation gap, surfaced here as a WARN + a ``starving``
entry rather than a silent PENDING backlog.

DB-free: the registry (``describe_all``), runner registry, resolver and service
name are mocked.  Run with the isolated ``--noconftest`` recipe.
"""
from __future__ import annotations

import logging

import pytest

from dynastore.modules.tasks.routing import reconcile as recon
from dynastore.modules.tasks.routing.model import RunnerTarget


class _Runner:
    def __init__(self, *, can: bool = True, declared=None):
        self._can = can
        self._declared = declared or []  # list of dicts, like declared_tasks()

    def can_handle(self, _key: str) -> bool:
        return self._can

    def declared_tasks(self) -> list:
        return self._declared


def _patch(monkeypatch, *, service, runners, targets_by_key, catalogue):
    import dynastore.modules.tasks.routing.resolver as resolver
    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode

    monkeypatch.setattr(
        runners_mod, "get_runners",
        lambda mode: runners if mode == TaskExecutionMode.ASYNCHRONOUS else [],
    )
    monkeypatch.setattr("dynastore.modules.tasks.dispatcher._SERVICE_NAME", service)

    async def _resolved(key):
        return targets_by_key.get(key, [])

    monkeypatch.setattr(resolver, "resolved_targets", _resolved)
    monkeypatch.setattr("dynastore.tasks.describe_all", lambda: catalogue)


@pytest.mark.asyncio
async def test_covered_task_not_starving(monkeypatch):
    _patch(
        monkeypatch, service="catalog",
        runners=[_Runner(can=True, declared=[])],  # declares nothing → covers via can_handle
        targets_by_key={"cascade_cleanup": [RunnerTarget(runner="background", consumers=["catalog"])]},
        catalogue=[{"task_key": "cascade_cleanup", "kind": "task"}],
    )
    res = await recon.reconcile_routing_capabilities()
    assert res["service"] == "catalog"
    assert res["starving"] == []


@pytest.mark.asyncio
async def test_routed_here_but_no_runner_is_starving(monkeypatch, caplog):
    _patch(
        monkeypatch, service="catalog",
        runners=[_Runner(can=False)],  # no runner can handle it
        targets_by_key={"cascade_cleanup": [RunnerTarget(runner="background", consumers=["catalog"])]},
        catalogue=[{"task_key": "cascade_cleanup", "kind": "task"}],
    )
    caplog.set_level(logging.WARNING)
    res = await recon.reconcile_routing_capabilities()
    assert [s["task_key"] for s in res["starving"]] == ["cascade_cleanup"]
    assert res["starving"][0]["routed_runners"] == ["background"]
    assert any("STARVE" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_routed_to_other_service_is_skipped(monkeypatch):
    _patch(
        monkeypatch, service="catalog",
        runners=[_Runner(can=False)],
        targets_by_key={"tiles_preseed": [RunnerTarget(runner="gcp_cloud_run", consumers=["maps"])]},
        catalogue=[{"task_key": "tiles_preseed", "kind": "process"}],
    )
    res = await recon.reconcile_routing_capabilities()
    assert res["starving"] == []  # routed to maps, not this (catalog) service


@pytest.mark.asyncio
async def test_gcp_manifest_excludes_key_is_starving(monkeypatch):
    """gcp runner can_handle anything, but its deployed-Job manifest omits gdal."""
    gcp = _Runner(can=True, declared=[{"task_key": "dwh_join", "runner_type": "gcp_cloud_run", "job": "x"}])
    _patch(
        monkeypatch, service="catalog",
        runners=[gcp],
        targets_by_key={"gdal": [RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"])]},
        catalogue=[{"task_key": "gdal", "kind": "process"}],
    )
    res = await recon.reconcile_routing_capabilities()
    assert [s["task_key"] for s in res["starving"]] == ["gdal"]


@pytest.mark.asyncio
async def test_gcp_manifest_includes_key_is_covered(monkeypatch):
    gcp = _Runner(can=True, declared=[{"task_key": "gdal", "runner_type": "gcp_cloud_run", "job": "gdal-job"}])
    _patch(
        monkeypatch, service="catalog",
        runners=[gcp],
        targets_by_key={"gdal": [RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"])]},
        catalogue=[{"task_key": "gdal", "kind": "process"}],
    )
    res = await recon.reconcile_routing_capabilities()
    assert res["starving"] == []


@pytest.mark.asyncio
async def test_empty_catalogue_no_starving(monkeypatch):
    _patch(monkeypatch, service="catalog", runners=[_Runner()], targets_by_key={}, catalogue=[])
    res = await recon.reconcile_routing_capabilities()
    assert res["starving"] == []


@pytest.mark.asyncio
async def test_no_routing_opinion_any_capable_service_still_checks_runner(monkeypatch):
    """No targets → 'any capable service'; if no runner here can claim it, it starves."""
    _patch(
        monkeypatch, service="catalog",
        runners=[_Runner(can=False)],
        targets_by_key={},  # no routing opinion
        catalogue=[{"task_key": "orphan", "kind": "task"}],
    )
    res = await recon.reconcile_routing_capabilities()
    assert [s["task_key"] for s in res["starving"]] == ["orphan"]
    assert res["starving"][0]["routed_runners"] == ["<unrouted>"]
