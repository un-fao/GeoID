"""Version-gated self-publish of this pod's task inventory.

Structural PG writes happen only when the build-keyed digest changes
(~once per deploy). The version gate is the shared ``@cached`` decorator
(tools/cache.py): the UPSERT is wrapped in a function keyed on
``(service, digest)``, so a cache hit (this build already published
cluster-wide) skips the write while a miss runs it. A single cheap last_seen
heartbeat refreshes liveness every tick. Cadence piggybacks the
capability-publisher refresh interval.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Tuple

from dynastore._version import get_git_commit, get_version
from dynastore.modules.db_config.instance import get_service_name
from dynastore.modules.tasks.registry import repository
from dynastore.modules.tasks.registry.model import CapabilityRow, compute_publish_digest
from dynastore.tasks import _DYNASTORE_TASKS, task_kind
from dynastore.tools.cache import CacheIgnore, cached

logger = logging.getLogger(__name__)

# Re-assert the same build's rows at most this often per service (self-heals a
# row that was deleted out-of-band while the build digest is unchanged). A new
# build always changes the digest -> new key -> immediate re-publish regardless.
_PUBLISH_DIGEST_TTL_SECONDS = 3600.0


def _observed_modes(task_key: str) -> List[str]:
    """Modes this pod can currently serve the task in.

    This records the conservative in-process capability (async) so the table
    is populated and inspectable. The full {off_load, async, sync} taxonomy is
    supplied later by the placement resolver, which overrides this from config.
    """
    from dynastore.modules.tasks.runners import capability_map  # local import: avoid cycle
    modes: List[str] = []
    if task_key in capability_map.async_types:
        modes.append("async")
    if task_key in capability_map.sync_types:
        modes.append("sync")
    return modes or ["async"]


def collect_local_inventory() -> Tuple[str, str, str, List[CapabilityRow]]:
    """Return (service, commit, version, rows) for this pod, or skip if no identity."""
    service = get_service_name()
    commit = get_git_commit()
    version = get_version()
    rows: List[CapabilityRow] = []
    if not service:
        return ("", commit, version, rows)
    for task_key, cfg in _DYNASTORE_TASKS.items():
        cls = cfg.cls
        rows.append(
            CapabilityRow(
                service=service,
                task_key=task_key,
                kind=task_kind(cfg),
                modes=_observed_modes(task_key),
                required_capability=None,  # payload-dependent; not summarizable per row
                mandatory=bool(getattr(cls, "mandatory", False)),
                affinity_tier=getattr(cls, "affinity_tier", None),
                service_version=version,
                service_commit=commit,
                version=commit,  # generic version == build commit for now
            )
        )
    return (service, commit, version, rows)


@cached(ttl=_PUBLISH_DIGEST_TTL_SECONDS, namespace="task_registry")
async def _publish_if_new(
    service: str,
    digest: str,
    *,
    engine: CacheIgnore[Any],
    rows: CacheIgnore[List[CapabilityRow]],
) -> bool:
    """UPSERT this pod's inventory, gated by the shared cache on (service, digest).

    A cache HIT means this exact build inventory was already published
    cluster-wide for the service, so the body is skipped. A MISS (new build, or
    the gate entry expired) runs the structural UPSERT. ``engine`` and ``rows``
    are CacheIgnore-annotated, so the cache key is only (service, digest); the
    annotation stays correct across parameter renames where a string ignore
    list would silently break the gate.
    """
    await repository.upsert_rows(engine, rows)
    logger.info(
        "task-registry: published %d rows for service=%r (digest=%s)",
        len(rows), service, digest[:12],
    )
    return True


async def publish_inventory(engine) -> None:
    """Publish this pod's inventory (cache-gated UPSERT) and heartbeat liveness.

    Fail-soft: never raises into the caller's loop. The structural publish and
    the liveness heartbeat fail independently — a transient UPSERT error must
    not also drop the heartbeat, because the mandatory-ownership check reads
    last_seen to find live owners. A failed UPSERT is not cached, so the next
    tick retries it.
    """
    try:
        service, commit, version, rows = collect_local_inventory()
    except Exception:  # fail-soft; registry must never brick startup
        logger.warning("task-registry: inventory collection failed (non-fatal)", exc_info=True)
        return
    if not service or not rows:
        return
    digest = compute_publish_digest(commit, version, rows)
    try:
        await _publish_if_new(service, digest, engine=engine, rows=rows)
    except Exception:
        logger.warning("task-registry: publish (upsert) failed (non-fatal)", exc_info=True)
    try:
        await repository.heartbeat(engine, service)
    except Exception:
        logger.warning("task-registry: heartbeat failed (non-fatal)", exc_info=True)


async def run_registry_heartbeat(
    engine,
    shutdown_event: asyncio.Event,
    *,
    refresh_seconds: float = 30.0,
) -> None:
    """Publish once immediately, then heartbeat on the given cadence until shutdown."""
    while True:
        await publish_inventory(engine)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=refresh_seconds)
            return  # shutdown signaled
        except asyncio.TimeoutError:
            continue
