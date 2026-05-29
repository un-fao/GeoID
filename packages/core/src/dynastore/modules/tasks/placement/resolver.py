"""Single function the claim path + execution engine consult.

Fail-open: any failure to load the placement config returns None ("no opinion"),
and the caller falls back to today's capable-set derivation. A fresh/degraded
registry can never make a pod refuse to claim.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from dynastore.modules.tasks.placement.model import PlacementEntry, TaskPlacementConfig
from dynastore.modules.tasks.placement.presets import PLACEMENT_PRESET_RUNNER

logger = logging.getLogger(__name__)


async def _load_config() -> TaskPlacementConfig:
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from dynastore.tools.discovery import get_protocol

    config_mgr = get_protocol(PlatformConfigsProtocol)
    if config_mgr is None:
        return TaskPlacementConfig()
    cfg = await config_mgr.get_config(TaskPlacementConfig)
    return cfg if isinstance(cfg, TaskPlacementConfig) else TaskPlacementConfig()


async def resolved_entry(task_key: str) -> Optional[PlacementEntry]:
    try:
        cfg = await _load_config()
        return cfg.resolved_entry(task_key)
    except Exception:
        logger.warning("placement: resolver failed for %r — failing open", task_key, exc_info=True)
        return None


async def resolved_consumers(task_key: str) -> Optional[List[str]]:
    entry = await resolved_entry(task_key)
    return list(entry.consumers) if entry is not None else None


async def off_load_runner_type() -> str:
    """Which runner_type serves off_load under the active preset (cloud default)."""
    try:
        cfg = await _load_config()
        return PLACEMENT_PRESET_RUNNER.get(cfg.placement_preset, "gcp_cloud_run")
    except Exception:
        return "gcp_cloud_run"
