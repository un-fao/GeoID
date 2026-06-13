#    Copyright 2026 FAO
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

"""Valkey-backed per-(capability, task_type) observability counters.

Closes Signal A of #524: SCOPE-drift trip-wire dashboard. The dispatcher
already emits structured log lines at the two interesting moments —
``task_claim_rejected`` (a worker rejected a row) and
``dispatcher_dlq_bulk_total`` (a sibling batch was bulk-DLQed). Log-based
metrics work but require the log shipper round-trip; these Valkey
counters give the admin UI a sub-second view of the same signal.

Fail-open: every counter mutation is wrapped — Valkey down ⇒ silent
no-op so the counter layer can never block the dispatcher's hot path or
the proactive sweeper. Read side returns ``None`` for unavailable
counters; the admin endpoint surfaces that as "unknown" rather than 0.

Key layout
----------
``dynastore:cap_stats:{counter}:{capability_id}:{task_type}``
TTL = 24h on first write, refreshed implicitly by activity. Idle counters
expire so a long-decommissioned capability does not occupy memory.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


_PREFIX = "dynastore:cap_stats:"
_DEFAULT_TTL_S = 24 * 3600.0  # 24h


# Counter names. Keep stable — referenced by the admin endpoint response
# schema and by any dashboard query.
COUNTER_CLAIM_REJECTED = "claim_rejected"
COUNTER_DLQ_REACTIVE = "dlq_reactive"
COUNTER_DLQ_PROACTIVE = "dlq_proactive"

_ALL_COUNTERS = (
    COUNTER_CLAIM_REJECTED,
    COUNTER_DLQ_REACTIVE,
    COUNTER_DLQ_PROACTIVE,
)


def _key(counter: str, capability_id: str, task_type: str) -> str:
    return f"{_PREFIX}{counter}:{capability_id}:{task_type}"


async def _bump(counter: str, capability_id: str, task_type: str, amount: int) -> None:
    if amount <= 0 or not capability_id or not task_type:
        return
    try:
        from dynastore.tools.cache import get_cache_manager

        backend = get_cache_manager().get_async_backend()
        incr = getattr(backend, "incr", None)
        if incr is None:
            return  # backend lacks counter primitives (e.g. in-memory test stub)
        await incr(_key(counter, capability_id, task_type), amount, ttl=_DEFAULT_TTL_S)
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.debug(
            "capability_stats: %s bump failed (cap=%s task_type=%s): %s",
            counter, capability_id, task_type, exc,
        )


async def bump_claim_rejected(capability_id: str, task_type: str) -> None:
    """Increment after a worker emits ``task_claim_rejected``."""
    await _bump(COUNTER_CLAIM_REJECTED, capability_id, task_type, 1)


async def bump_dlq(source: str, capability_id: str, task_type: str, amount: int) -> None:
    """Increment after a bulk-DLQ batch lands.

    ``source`` is the same value embedded in the log line: ``"reactive"``
    or ``"proactive"``. Unknown values are ignored.
    """
    if source == "reactive":
        await _bump(COUNTER_DLQ_REACTIVE, capability_id, task_type, amount)
    elif source == "proactive":
        await _bump(COUNTER_DLQ_PROACTIVE, capability_id, task_type, amount)


async def read_counters(
    capability_id: str, task_type: str,
) -> Dict[str, Optional[int]]:
    """Return current counter values for one ``(capability_id, task_type)``.

    Missing keys map to ``0`` (counter never incremented in this TTL
    window). Returns ``{counter: None}`` for the whole row when the
    backend is unreachable so the admin endpoint can distinguish "zero"
    from "unknown".
    """
    try:
        from dynastore.tools.cache import get_cache_manager

        backend = get_cache_manager().get_async_backend()
        get_count = getattr(backend, "get_count", None)
        if get_count is None:
            return {c: None for c in _ALL_COUNTERS}
        out: Dict[str, Optional[int]] = {}
        for counter in _ALL_COUNTERS:
            try:
                val = await get_count(_key(counter, capability_id, task_type))
                out[counter] = int(val) if val is not None else 0
            except Exception:  # noqa: BLE001 — fail-open per counter
                out[counter] = None
        return out
    except Exception as exc:  # noqa: BLE001 — fail-open globally
        logger.debug(
            "capability_stats: read_counters failed (cap=%s task_type=%s): %s",
            capability_id, task_type, exc,
        )
        return {c: None for c in _ALL_COUNTERS}
