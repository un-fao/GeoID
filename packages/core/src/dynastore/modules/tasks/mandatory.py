#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Mandatory-task ownership guarantee + capability-less unclaimable detection.

Reads the capability registry. A mandatory task is satisfied iff a LIVE service
of its declared affinity_tier advertises it. Absence is a violation (loud) and
its PENDING rows are dead-lettered by the backstop branch.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _mandatory_specs() -> List[Tuple[str, Optional[str]]]:
    """(task_key, affinity_tier) for every loaded mandatory task."""
    from dynastore.tasks import _DYNASTORE_TASKS
    specs: List[Tuple[str, Optional[str]]] = []
    for task_key, cfg in _DYNASTORE_TASKS.items():
        if getattr(cfg.cls, "mandatory", False):
            specs.append((task_key, getattr(cfg.cls, "affinity_tier", None)))
    return specs


def _has_correct_tier_owner(owners, affinity_tier: Optional[str]) -> bool:
    if affinity_tier is None:
        return bool(owners)  # tier-agnostic mandatory task: any live owner suffices
    return any(o.get("affinity_tier") == affinity_tier for o in owners)


async def _fetch_live_owners_map(
    engine, conn: Optional[Any], ttl_grace_seconds: float
) -> Dict[str, List[dict]]:
    """Return all live registry rows grouped by task_key.

    Uses ``conn`` directly when supplied (avoids a pool round-trip); falls
    back to opening its own engine connection otherwise.
    """
    if conn is not None:
        from dynastore.modules.tasks.registry.repository import live_owners_all_conn
        return await live_owners_all_conn(conn, ttl_grace_seconds)
    from dynastore.modules.tasks.registry.repository import live_owners_all
    return await live_owners_all(engine, ttl_grace_seconds)


async def check_mandatory_ownership(
    engine, *, ttl_grace_seconds: float, conn: Optional[Any] = None
) -> List[str]:
    """Return task_keys of mandatory tasks lacking a live correct-tier owner.

    When ``conn`` is supplied the registry SELECT runs on that connection
    (avoids an extra pool acquire); callers without one still work via the
    ``conn=None`` default.
    """
    live_map = await _fetch_live_owners_map(engine, conn, ttl_grace_seconds)
    violations: List[str] = []
    for task_key, tier in _mandatory_specs():
        owners = live_map.get(task_key, [])
        if not _has_correct_tier_owner(owners, tier):
            logger.error(
                "MANDATORY TASK UNOWNED: %r has no live %s-tier owner "
                "(owners=%s) — its PENDING rows will be dead-lettered; "
                "restore a correct-tier consumer and requeue.",
                task_key, tier or "any", [o.get("service") for o in owners],
            )
            violations.append(task_key)
    return violations


async def find_unclaimable_task_types(
    engine, *, ttl_grace_seconds: float, conn: Optional[Any] = None
) -> List[str]:
    """Loaded task types with no live correct-tier owner — backstop DLQ targets.

    Issues a single batch SELECT for all live registry rows, then filters in
    Python — one round-trip regardless of how many task types are registered.

    Independent of required_capability: this is exactly the escape the
    capability-keyed reaper skips for required_capability = None rows.

    When ``conn`` is supplied the SELECT runs on that connection (avoids an
    extra pool acquire); callers without one still work via the ``conn=None``
    default.
    """
    from dynastore.tasks import _DYNASTORE_TASKS
    live_map = await _fetch_live_owners_map(engine, conn, ttl_grace_seconds)
    unclaimable: List[str] = []
    for task_key, cfg in _DYNASTORE_TASKS.items():
        tier = getattr(cfg.cls, "affinity_tier", None)
        owners = live_map.get(task_key, [])
        if not _has_correct_tier_owner(owners, tier):
            unclaimable.append(task_key)
    return unclaimable
