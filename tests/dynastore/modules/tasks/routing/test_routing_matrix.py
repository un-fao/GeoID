"""Unit tests for routing/matrix.py.

Covers:
- cloud preset: processes get gcp_cloud_run + {OFFLOAD, HEAVY}.
- cloud preset: gdal consumers = [catalog, maps]; tiles_* = [maps].
- cloud preset: requeue_dead_letter_tasks stays background.
- cloud preset: system tasks get background on their affinity tier.
- onprem preset: every process in-process (background) on its affinity tier
  (gdal -> [catalog, maps], tiles_* -> [maps], rest -> [catalog]); never
  gcp_cloud_run, never the bare [worker] tier.
"""
from __future__ import annotations

from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import (
    CLOUD_PROCESS_CONSUMERS,
    LIGHTWEIGHT_PROCESSES,
    InventoryItem,
    build_routing_matrix,
)


def _task(key: str, affinity: str | None = None) -> InventoryItem:
    return InventoryItem(task_key=key, kind="task", affinity_tier=affinity)


def _proc(key: str) -> InventoryItem:
    return InventoryItem(task_key=key, kind="process", affinity_tier=None)


# ---------------------------------------------------------------------------
# cloud preset
# ---------------------------------------------------------------------------


def test_cloud_system_task_gets_background():
    tasks, _ = build_routing_matrix([_task("heartbeat", affinity="catalog")], preset="cloud")
    assert "heartbeat" in tasks
    t = tasks["heartbeat"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints
    assert t.consumers == ["catalog"]


def test_cloud_system_task_affinity_fallback():
    """Tier-agnostic task (affinity_tier=None) should default to catalog."""
    tasks, _ = build_routing_matrix([_task("generic_task")], preset="cloud")
    assert tasks["generic_task"][0].consumers == ["catalog"]


def test_cloud_process_gets_gcp_cloud_run():
    _, procs = build_routing_matrix([_proc("ingestion")], preset="cloud")
    t = procs["ingestion"][0]
    assert t.runner == "gcp_cloud_run"
    assert ExecHint.OFFLOAD in t.hints
    assert ExecHint.HEAVY in t.hints


def test_cloud_gdal_consumers():
    _, procs = build_routing_matrix([_proc("gdal")], preset="cloud")
    assert procs["gdal"][0].consumers == CLOUD_PROCESS_CONSUMERS["gdal"]
    assert "catalog" in procs["gdal"][0].consumers
    assert "maps" in procs["gdal"][0].consumers


def test_cloud_tiles_preseed_consumers():
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="cloud")
    assert procs["tiles_preseed"][0].consumers == ["maps"]


def test_cloud_tiles_export_consumers():
    _, procs = build_routing_matrix([_proc("tiles_export")], preset="cloud")
    assert procs["tiles_export"][0].consumers == ["maps"]


def test_cloud_unknown_process_defaults_to_catalog():
    _, procs = build_routing_matrix([_proc("unknown_heavy_job")], preset="cloud")
    assert procs["unknown_heavy_job"][0].consumers == ["catalog"]


def test_cloud_lightweight_process_stays_background():
    for key in LIGHTWEIGHT_PROCESSES:
        _, procs = build_routing_matrix([_proc(key)], preset="cloud")
        t = procs[key][0]
        assert t.runner == "background", f"{key} should be background under cloud"
        assert ExecHint.BACKGROUND in t.hints, f"{key} should carry BACKGROUND hint"
        assert ExecHint.OFFLOAD not in t.hints
        assert t.consumers == ["catalog"]


def test_cloud_no_options_job_field():
    """Runner options dict should be empty -- job name is supplied by the runner."""
    _, procs = build_routing_matrix([_proc("ingestion")], preset="cloud")
    assert procs["ingestion"][0].options == {}


# ---------------------------------------------------------------------------
# onprem preset
# ---------------------------------------------------------------------------


def test_onprem_system_task_gets_background():
    tasks, _ = build_routing_matrix([_task("heartbeat", affinity="catalog")], preset="onprem")
    t = tasks["heartbeat"][0]
    assert t.runner == "background"
    assert ExecHint.BACKGROUND in t.hints
    assert t.consumers == ["catalog"]


def test_onprem_process_runs_in_process_on_affinity_tier():
    # A default process (not in CLOUD_PROCESS_CONSUMERS) lands on catalog,
    # in-process — never gcp_cloud_run, never the bare "worker" tier.
    _, procs = build_routing_matrix([_proc("ingestion")], preset="onprem")
    t = procs["ingestion"][0]
    assert t.runner == "background"
    assert t.consumers == ["catalog"]
    assert ExecHint.BACKGROUND in t.hints
    assert ExecHint.HEAVY not in t.hints


def test_onprem_gdal_routes_to_catalog_and_maps_in_process():
    # gdal's affinity (CLOUD_PROCESS_CONSUMERS) is [catalog, maps]; on-prem keeps
    # that consumer topology but runs it in-process so maps can claim gdalinfo.
    _, procs = build_routing_matrix([_proc("gdal")], preset="onprem")
    t = procs["gdal"][0]
    assert t.runner == "background"
    assert t.consumers == ["catalog", "maps"]
    assert ExecHint.BACKGROUND in t.hints


def test_onprem_tiles_preseed_routes_to_maps_in_process():
    _, procs = build_routing_matrix([_proc("tiles_preseed")], preset="onprem")
    t = procs["tiles_preseed"][0]
    assert t.runner == "background"
    assert t.consumers == ["maps"]


def test_onprem_never_offloads_to_cloud_run():
    for key in ("ingestion", "gdal", "tiles_preseed", "dwh_join", *LIGHTWEIGHT_PROCESSES):
        _, procs = build_routing_matrix([_proc(key)], preset="onprem")
        t = procs[key][0]
        assert t.runner == "background"
        assert ExecHint.OFFLOAD not in t.hints
        assert t.consumers != ["worker"]


# ---------------------------------------------------------------------------
# Mixed inventory
# ---------------------------------------------------------------------------


def test_tasks_and_processes_land_in_separate_maps():
    inv = [_task("sys_task"), _proc("my_proc")]
    tasks, procs = build_routing_matrix(inv, preset="cloud")
    assert "sys_task" in tasks
    assert "my_proc" in procs
    assert "sys_task" not in procs
    assert "my_proc" not in tasks


def test_empty_inventory_returns_empty_maps():
    tasks, procs = build_routing_matrix([], preset="cloud")
    assert tasks == {}
    assert procs == {}
