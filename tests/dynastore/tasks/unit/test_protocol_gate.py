"""Unit tests for the service-affinity routing gate in CapabilityMap.refresh.

Replaces the previous ``@requires(Protocol)`` / ``are_protocols_satisfied``
gate (removed in favour of operator-controlled ``TaskRoutingConfig``).
Covers:

- ``CapabilityMap.refresh()`` narrowing claimable types when routing config
  pins a task to a different service.
- Empty / missing routing entry preserves legacy "any capable service may
  claim" behaviour.
- ``routing_disabled=True`` collapses the filter to no-op.
- ``service_name=None`` (no instance.json) preserves legacy behaviour.
- Multi-target arrays (``["catalog", "worker"]``) admit either service.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.runners import CapabilityMap
from dynastore.modules.tasks.tasks_config import TaskRoutingConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _runner(task_types):
    r = MagicMock()
    r.can_handle = lambda t: t in task_types
    return r


def _patch_refresh_deps(*, async_runners, sync_runners, loaded_types,
                       routing_cfg, service_name):
    """Patch every external dep CapabilityMap.refresh touches."""
    config_mgr = AsyncMock()
    config_mgr.get_config = AsyncMock(return_value=routing_cfg)

    return [
        patch("dynastore.tasks.get_loaded_task_types", return_value=list(loaded_types)),
        patch("dynastore.modules.tasks.runners.get_runners",
              side_effect=lambda mode: async_runners
              if str(mode).endswith("ASYNCHRONOUS") else sync_runners),
        patch("dynastore.tools.discovery.get_protocol",
              return_value=config_mgr if routing_cfg is not None else None),
        patch("dynastore.modules.tasks.dispatcher._SERVICE_NAME", service_name),
    ]


async def _refresh_with(routing_cfg, service_name, loaded_types,
                       async_handles, sync_handles):
    cap = CapabilityMap()
    patches = _patch_refresh_deps(
        async_runners=[_runner(set(async_handles))],
        sync_runners=[_runner(set(sync_handles))],
        loaded_types=loaded_types,
        routing_cfg=routing_cfg,
        service_name=service_name,
    )
    for p in patches:
        p.start()
    try:
        await cap.refresh()
        return cap
    finally:
        for p in reversed(patches):
            p.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_routing_config_legacy_behaviour():
    """When no TaskRoutingConfig is registered, all capable types are claimed."""
    cap = await _refresh_with(
        routing_cfg=None,
        service_name="catalog",
        loaded_types=["t_a", "t_b"],
        async_handles={"t_a", "t_b"},
        sync_handles=set(),
    )
    assert sorted(cap.async_types) == ["t_a", "t_b"]


@pytest.mark.asyncio
async def test_service_name_unset_skips_routing():
    """Without a service_name (no instance.json), routing is bypassed."""
    cfg = TaskRoutingConfig(routing={"t_a": ["catalog"]})
    cap = await _refresh_with(
        routing_cfg=cfg,
        service_name=None,
        loaded_types=["t_a", "t_b"],
        async_handles={"t_a", "t_b"},
        sync_handles=set(),
    )
    assert sorted(cap.async_types) == ["t_a", "t_b"]


@pytest.mark.asyncio
async def test_routing_disabled_kill_switch():
    """``routing_disabled=true`` returns to legacy behaviour."""
    cfg = TaskRoutingConfig(
        routing={"t_a": ["maps"]}, routing_disabled=True,
    )
    cap = await _refresh_with(
        routing_cfg=cfg,
        service_name="catalog",
        loaded_types=["t_a", "t_b"],
        async_handles={"t_a", "t_b"},
        sync_handles=set(),
    )
    assert sorted(cap.async_types) == ["t_a", "t_b"]


@pytest.mark.asyncio
async def test_routing_filters_out_other_service():
    """A task pinned to ``maps`` is not claimable on ``catalog``."""
    cfg = TaskRoutingConfig(
        routing={"t_a": ["maps"], "t_b": ["catalog"]},
    )
    cap = await _refresh_with(
        routing_cfg=cfg,
        service_name="catalog",
        loaded_types=["t_a", "t_b"],
        async_handles={"t_a", "t_b"},
        sync_handles=set(),
    )
    assert cap.async_types == ["t_b"]


@pytest.mark.asyncio
async def test_routing_admits_multi_target():
    """A task with multiple targets is claimable from any of them."""
    cfg = TaskRoutingConfig(
        routing={"t_a": ["catalog", "worker"]},
    )
    for svc in ("catalog", "worker"):
        cap = await _refresh_with(
            routing_cfg=cfg,
            service_name=svc,
            loaded_types=["t_a"],
            async_handles={"t_a"},
            sync_handles=set(),
        )
        assert cap.async_types == ["t_a"], f"failed for service={svc!r}"


@pytest.mark.asyncio
async def test_missing_routing_entry_treated_as_open():
    """Task types absent from the routing map remain open to any service."""
    cfg = TaskRoutingConfig(
        routing={"t_a": ["maps"]},  # only t_a is pinned
    )
    cap = await _refresh_with(
        routing_cfg=cfg,
        service_name="catalog",
        loaded_types=["t_a", "t_b"],
        async_handles={"t_a", "t_b"},
        sync_handles=set(),
    )
    # t_a is filtered out (pinned to maps); t_b is open
    assert cap.async_types == ["t_b"]


@pytest.mark.asyncio
async def test_empty_target_list_treated_as_open():
    """An explicit empty list is the same as a missing key — open to all."""
    cfg = TaskRoutingConfig(
        routing={"t_a": []},
    )
    cap = await _refresh_with(
        routing_cfg=cfg,
        service_name="catalog",
        loaded_types=["t_a"],
        async_handles={"t_a"},
        sync_handles=set(),
    )
    assert cap.async_types == ["t_a"]


# ---------------------------------------------------------------------------
# Routing-config apply-handler validation — same diagnostic logic the
# in-lifespan handler runs after every PUT /configs/classes/TaskRoutingConfig.
# We can't import the closure directly (it's defined inside lifespan), so
# we exercise the same shape: cross-reference routing keys against
# get_loaded_task_types() and emit a WARN for unknown ones.
# ---------------------------------------------------------------------------


def _validate_routing(cfg, my_service, loaded):
    """Mirror of the in-lifespan diagnostic. Returns the list of unknown keys
    that the apply-handler would WARN about."""
    return sorted(t for t in (getattr(cfg, "routing", {}) or {}) if t not in loaded)


def test_validator_flags_unknown_task_type_keys():
    """Routing keys not present in get_loaded_task_types are flagged."""
    cfg = TaskRoutingConfig(routing={
        "elasticsearch_index": ["catalog"],   # known
        "Elasticsearch_index": ["catalog"],   # typo (capital E)
        "tile_preseed": ["maps"],             # known
        "made_up_type": ["worker"],           # only in another deployment
    })
    loaded = {"elasticsearch_index", "tile_preseed"}
    unknown = _validate_routing(cfg, "catalog", loaded)
    assert unknown == ["Elasticsearch_index", "made_up_type"]


def test_validator_silent_when_all_keys_known():
    cfg = TaskRoutingConfig(routing={
        "elasticsearch_index": ["catalog"],
        "tile_preseed": ["maps"],
    })
    loaded = {"elasticsearch_index", "tile_preseed", "extra_one"}
    assert _validate_routing(cfg, "catalog", loaded) == []


def test_validator_handles_empty_routing():
    cfg = TaskRoutingConfig(routing={})
    assert _validate_routing(cfg, "catalog", {"any"}) == []
