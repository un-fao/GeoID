"""Deployment-profile presets for task routing.

``CloudTaskRoutingPreset`` and ``OnpremTaskRoutingPreset`` materialize a
``TaskRoutingConfig`` from the live task registry using
``build_routing_matrix``, matching the two deployment topologies.

The preset objects are module-level singletons ready to be registered.
Registration is intentionally NOT called at import time in this unit:
the legacy ``placement/presets.py`` still owns platform-tier preset
registration; wiring happens once placement is removed (Unit 2).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class _TaskRoutingPreset:
    """Minimal RoutingPreset: materializes a TaskRoutingConfig at platform tier.

    Mirrors the shape of ``_PlacementRoutingPreset`` in
    ``placement/presets.py``: a thin factory that emits a validated
    ``PresetBundle`` whose single entry carries the ``TaskRoutingConfig``
    for the active deployment profile.
    """

    catalog_scopable = False

    def __init__(self, name: str) -> None:
        self.name = name
        self.description = f"Task routing profile: {name}"
        self.tier = None  # set lazily on first build() to avoid import-time cycle

    def _get_tier(self):
        from dynastore.modules.storage.presets.protocol import PresetTier
        return PresetTier.PLATFORM

    def build(self, **_scope):
        from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry
        from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
        from dynastore.modules.tasks.routing.model import TaskRoutingConfig
        from dynastore.tasks import _DYNASTORE_TASKS, task_kind

        inventory = [
            InventoryItem(
                task_key=key,
                kind=task_kind(cfg),
                affinity_tier=getattr(cfg.cls, "affinity_tier", None),
            )
            for key, cfg in _DYNASTORE_TASKS.items()
        ]
        tasks_map, processes_map = build_routing_matrix(inventory, preset=self.name)
        instance = TaskRoutingConfig(tasks=tasks_map, processes=processes_map)
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="task_routing",
                    config_cls=TaskRoutingConfig,
                    instance=instance,
                ),
            )
        )


CloudTaskRoutingPreset = _TaskRoutingPreset("cloud")
OnpremTaskRoutingPreset = _TaskRoutingPreset("onprem")


def _register() -> None:
    # Registration wired in the wire-in step once the legacy placement presets are removed.
    try:
        from dynastore.modules.storage.presets import register_preset
        register_preset(CloudTaskRoutingPreset)
        register_preset(OnpremTaskRoutingPreset)
    except Exception:
        logger.warning(
            "task routing presets not registered in storage registry",
            exc_info=True,
        )
