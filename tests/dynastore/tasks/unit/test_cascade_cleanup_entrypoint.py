"""Regression: cascade_cleanup must be a discoverable, pinned worker task.

``CascadeCleanupTask`` is enqueued (schema=``system``) by the catalog
cascade orchestrator after a hard-delete to drain each resource owner's
``cleanup_one``. It is a fully-formed ``TaskProtocol`` with a ``run()``,
but it was never declared in the ``[project.entry-points."dynastore.tasks"]``
table, so ``discover_tasks()`` never imported the module, ``__init_subclass__``
never registered the class, and no service's ``CapabilityMap`` ever included
``cascade_cleanup``. Result: the row could never be claimed by any pod and
starved PENDING forever (retry_count=0, no DLQ path).

These pure-static checks (no app boot, no editable reinstall needed) pin the
two knobs that make the task claimable by exactly one capable consumer:

1. the entry-point declaration (so the class is discovered + registered), and
2. the routing pin to ``catalog`` — the SAME tier that enqueues the row, so
   the registered cleanup owners match the snapshotted refs. A wrong-tier
   claim raises ``KeyError`` per ref → marks them DEAD → permanent loss.
"""
from __future__ import annotations

import json
import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CORE_PYPROJECT = _REPO_ROOT / "packages" / "core" / "pyproject.toml"
_CONFIG_ROOT = (
    _REPO_ROOT / "packages" / "core" / "src" / "dynastore" / "docker" / "config"
)

_EXPECTED_TARGET = "dynastore.tasks.cascade_cleanup.task:CascadeCleanupTask"
_EXPECTED_CONSUMER = "catalog"


def test_cascade_cleanup_entrypoint_declared() -> None:
    """cascade_cleanup is declared in the dynastore.tasks entry-point group.

    Without this line ``discover_tasks()`` never loads the class on any
    service and the task is unclaimable by construction.
    """
    assert _CORE_PYPROJECT.is_file(), f"missing {_CORE_PYPROJECT}"
    data = tomllib.loads(_CORE_PYPROJECT.read_text())
    entry_points = (
        data.get("project", {})
        .get("entry-points", {})
        .get("dynastore.tasks", {})
    )
    assert "cascade_cleanup" in entry_points, (
        "cascade_cleanup is missing from [project.entry-points.\"dynastore.tasks\"] "
        "in packages/core/pyproject.toml. discover_tasks() loads task classes only "
        "from this group; without it the task is never registered and starves "
        "PENDING forever."
    )
    assert entry_points["cascade_cleanup"] == _EXPECTED_TARGET, (
        f"cascade_cleanup entry-point points at {entry_points['cascade_cleanup']!r}; "
        f"expected {_EXPECTED_TARGET!r}."
    )


def _full_routing_configs() -> list[Path]:
    """task-routing.json defaults that carry the full pin table.

    The minimal ``example`` service config is intentionally a reduced sample
    and is excluded.
    """
    return sorted(
        p
        for p in _CONFIG_ROOT.glob("*/defaults/task-routing.json")
        if p.parent.parent.name != "example"
    )


def test_cascade_cleanup_routing_pinned_to_catalog() -> None:
    """Every full routing default pins cascade_cleanup to exactly the catalog tier.

    catalog is the tier that runs the cascade orchestrator (enqueue side) and
    therefore holds the matching cleanup-owner registry. Pinning the consumer
    to the same tier guarantees the snapshotted refs have registered owners;
    routing it elsewhere would mark every ref DEAD and lose the cleanup.
    """
    configs = _full_routing_configs()
    assert configs, f"no full task-routing.json defaults found under {_CONFIG_ROOT}"
    for cfg_path in configs:
        routing = json.loads(cfg_path.read_text())["value"]["routing"]
        assert routing.get("cascade_cleanup") == [_EXPECTED_CONSUMER], (
            f"{cfg_path} must pin cascade_cleanup to [{_EXPECTED_CONSUMER!r}]; "
            f"got {routing.get('cascade_cleanup')!r}. The consumer must equal the "
            "enqueuing tier (catalog) so cleanup-owner sets match."
        )
