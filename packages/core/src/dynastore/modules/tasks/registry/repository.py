"""SQL IO for the task-capability registry.

UPSERT keyed on the (service, task_key) PK. The only per-tick write in steady
state is the cheap last_seen heartbeat. The build-keyed publish digest is NOT
stored here — the publisher gates the structural UPSERT with the shared
``@cached`` decorator (tools/cache.py), so no bespoke digest store exists.
"""
from __future__ import annotations

from typing import List

from sqlalchemy import text

from dynastore.modules.tasks.registry.model import (
    TASK_CAPABILITY_REGISTRY_TABLE,
    CapabilityRow,
)

_UPSERT_SQL = f"""
INSERT INTO {TASK_CAPABILITY_REGISTRY_TABLE}
    (service, task_key, kind, modes, required_capability, mandatory,
     affinity_tier, service_version, service_commit, version, last_seen, updated_at)
VALUES
    (:service, :task_key, :kind, :modes, :required_capability, :mandatory,
     :affinity_tier, :service_version, :service_commit, :version, now(), now())
ON CONFLICT (service, task_key) DO UPDATE SET
    kind = EXCLUDED.kind,
    modes = EXCLUDED.modes,
    required_capability = EXCLUDED.required_capability,
    mandatory = EXCLUDED.mandatory,
    affinity_tier = EXCLUDED.affinity_tier,
    service_version = EXCLUDED.service_version,
    service_commit = EXCLUDED.service_commit,
    version = EXCLUDED.version,
    last_seen = now(),
    updated_at = now()
"""

_HEARTBEAT_SQL = f"""
UPDATE {TASK_CAPABILITY_REGISTRY_TABLE}
SET last_seen = now()
WHERE service = :service
"""

_LIST_SQL = f"""
SELECT service, task_key, kind, modes, required_capability, mandatory,
       affinity_tier, service_version, service_commit, version,
       last_seen, updated_at
FROM {TASK_CAPABILITY_REGISTRY_TABLE}
ORDER BY service, task_key
"""

# Live correct-tier owners of a task_key (consumed by the mandatory-ownership check).
_LIVE_OWNERS_SQL = f"""
SELECT service, affinity_tier, last_seen
FROM {TASK_CAPABILITY_REGISTRY_TABLE}
WHERE task_key = :task_key
  AND last_seen > now() - make_interval(secs => :ttl_grace_seconds)
"""


async def upsert_rows(engine, rows: List[CapabilityRow]) -> int:
    if not rows:
        return 0
    params = [r.model_dump() for r in rows]
    async with engine.begin() as conn:
        await conn.execute(text(_UPSERT_SQL), params)
    return len(rows)


async def heartbeat(engine, service: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(_HEARTBEAT_SQL), {"service": service})


async def list_all(engine) -> List[dict]:
    async with engine.connect() as conn:
        result = await conn.execute(text(_LIST_SQL))
        return [dict(row._mapping) for row in result]


async def live_owners_for(engine, task_key: str, ttl_grace_seconds: float) -> List[dict]:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(_LIVE_OWNERS_SQL),
            {"task_key": task_key, "ttl_grace_seconds": ttl_grace_seconds},
        )
        return [dict(row._mapping) for row in result]
