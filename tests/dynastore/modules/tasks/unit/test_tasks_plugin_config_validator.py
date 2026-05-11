"""#527 — refresh<=ttl/2 invariant enforced at config-load time."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.tasks.tasks_config import TasksPluginConfig


def test_default_pair_is_valid():
    cfg = TasksPluginConfig()
    assert cfg.capability_publisher_ttl_seconds == 60.0
    assert cfg.capability_publisher_refresh_seconds == 30.0


def test_refresh_equal_to_half_ttl_is_allowed():
    cfg = TasksPluginConfig(
        capability_publisher_ttl_seconds=100.0,
        capability_publisher_refresh_seconds=50.0,
    )
    assert cfg.capability_publisher_refresh_seconds == 50.0


def test_refresh_greater_than_half_ttl_is_rejected():
    with pytest.raises(ValidationError, match="refresh_seconds"):
        TasksPluginConfig(
            capability_publisher_ttl_seconds=60.0,
            capability_publisher_refresh_seconds=40.0,
        )
