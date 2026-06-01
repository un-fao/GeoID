"""TaskRoutingConfig materializes a non-empty matrix and the routing structure is correct."""
from __future__ import annotations

from dynastore.modules.tasks.routing.model import RunnerTarget, TaskRoutingConfig


def test_address_and_freeze_match_platform_tier():
    assert TaskRoutingConfig._address == ("platform", "tasks")
    assert TaskRoutingConfig._freeze_at == "platform"


def test_tasks_bucket_returned_before_processes():
    target = RunnerTarget(runner="background", consumers=["catalog"])
    cfg = TaskRoutingConfig(
        tasks={"outbox_drain": [target]},
        processes={},
    )
    result = cfg.resolved_targets("outbox_drain")
    assert result == [target]


def test_processes_bucket_is_fallback():
    target = RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"])
    cfg = TaskRoutingConfig(
        tasks={},
        processes={"gdal": [target]},
    )
    result = cfg.resolved_targets("gdal")
    assert result == [target]


def test_unknown_key_returns_empty():
    cfg = TaskRoutingConfig(
        tasks={"a": [RunnerTarget(runner="background")]},
        processes={},
    )
    assert cfg.resolved_targets("no_such_key") == []


def test_class_key_is_task_routing_config():
    assert TaskRoutingConfig.class_key() == "task_routing_config"
