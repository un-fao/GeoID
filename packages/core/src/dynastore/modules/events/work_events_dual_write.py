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

# dynastore/modules/events/work_events_dual_write.py
"""Event-plane dual-write seam for the WorkClass durable-work unification.

``dispatch_event_dual_write`` is the single point of control for the event
emit path during the WorkClass cutover (#1807).  It sits between the public
``EventsModule.publish`` API and the two physical tables:

* **Legacy path** — ``events.events`` (the existing global outbox).  Active
  whenever ``WorkClassConfig.emit_events_to_legacy`` is True (the default).
  The legacy ``_publish_query`` write and the new ``tasks.work_events`` write
  share the same ``conn`` so they participate in the same transaction and roll
  back together on any failure.

* **New path** — ``tasks.work_events`` (the WorkClass global hot plane).
  Active whenever ``WorkClassConfig.emit_events_to_new`` is True.

The default ``WorkClassConfig`` has ``emit_target_events = EmitTarget.LEGACY``,
so ``emit_events_to_legacy=True`` and ``emit_events_to_new=False``.  This is
the fail-safe state: the legacy write runs unchanged, the new table is not
touched.

Fail-safe guarantee
-------------------
Any error resolving ``WorkClassConfig`` (missing ``ConfigsProtocol``, DB
error, import error) collapses to ``(write_legacy=True, write_new=False)``
inside a bare ``except``.  A config-resolution failure MUST NOT prevent the
legacy event from being emitted.

Scope normalisation
-------------------
``scope`` is stored as-is in the legacy ``events.events`` table (e.g.
``"PLATFORM"``).  The new ``tasks.work_events`` table has a PG CHECK
constraint requiring ``scope = lower(scope)``, so the value is lowercased
**only** for the work_events INSERT.

Phase-C deletion
----------------
This module is the entire event-plane dual-write seam.  When the migration
reaches the ``new``-only steady state and the legacy table is decommissioned,
remove this module and revert ``EventsModule.publish`` to call
``_publish_query`` directly.  There are no other callers.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.models.protocols.configs import ConfigsProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# work_events INSERT query — built lazily to avoid module-level import of
# tasks schema name (which is itself env-driven and read at call time).
# ---------------------------------------------------------------------------

_WORK_EVENTS_INSERT_SQL = """
INSERT INTO {task_schema}.work_events (
    event_id,
    day,
    shard,
    schema_name,
    scope,
    payload
) VALUES (
    CAST(:event_id AS uuid),
    CURRENT_DATE,
    :shard,
    :schema_name,
    :scope,
    CAST(:payload AS jsonb)
)
"""


async def dispatch_event_dual_write(
    conn: Any,
    *,
    event_type: str,
    scope: str,
    schema_name: Optional[str],
    catalog_id: Optional[str],
    collection_id: Optional[str],
    identity_id: Optional[str],
    payload_str: str,
    shard: int,
    configs: Optional["ConfigsProtocol"],
) -> str:
    """Emit a domain event to the legacy and/or new work_events table.

    Both writes execute on ``conn`` so they are co-transactional with the
    caller's outer ``managed_transaction`` context.  Any exception propagates
    unmodified so the caller's transaction rolls back atomically.

    Parameters
    ----------
    conn:
        An open SQLAlchemy ``AsyncConnection`` from
        ``managed_transaction(engine)``.
    event_type:
        The event type label (e.g. ``"catalog_creation"``).
    scope:
        The event scope as the caller provides it (e.g. ``"PLATFORM"``).
        Stored as-is in ``events.events``; lowercased for ``work_events``.
    schema_name:
        Tenant schema name; ``None`` means platform-wide.
    catalog_id:
        Catalog identifier, or ``None``.
    collection_id:
        Collection identifier, or ``None``.
    identity_id:
        Identity identifier, or ``None``.
    payload_str:
        JSON-serialised event payload (already serialised by the caller).
    shard:
        Pre-computed shard value (``abs(hash(catalog_id or "PLATFORM")) % 16``).
    configs:
        Resolved ``ConfigsProtocol`` instance, or ``None``.  When ``None``
        or when resolution raises, the legacy write runs and the new write
        is skipped (fail-safe).

    Returns
    -------
    str
        The ``event_id`` returned by the legacy INSERT (a UUID string), or
        the Python-generated UUID when only the new table was written to.
    """
    # --- Resolve emit targets (fail-safe to legacy-only) ------------------
    write_legacy = True
    write_new = False

    try:
        if configs is not None:
            # Lazy import to avoid cross-module import cycles at load time.
            from dynastore.modules.tasks.workclass_config import WorkClassConfig  # noqa: PLC0415

            cfg: WorkClassConfig = await configs.get_config(WorkClassConfig)
            write_legacy = cfg.emit_events_to_legacy
            write_new = cfg.emit_events_to_new
    except Exception:
        # Any failure — missing protocol, DB error, config not found — falls
        # back to legacy-only.  This must NEVER prevent the legacy write.
        logger.debug(
            "work_events_dual_write: could not resolve WorkClassConfig; "
            "falling back to legacy-only emit.",
            exc_info=True,
        )
        write_legacy = True
        write_new = False

    # --- Legacy write: events.events --------------------------------------
    legacy_event_id: Optional[str] = None

    if write_legacy:
        from dynastore.modules.events.events_module import _publish_query  # noqa: PLC0415

        legacy_event_id = await _publish_query.execute(
            conn,
            event_type=event_type,
            scope=scope,
            schema_name=schema_name,
            catalog_id=catalog_id,
            collection_id=collection_id,
            identity_id=identity_id,
            payload=payload_str,
            shard=shard,
        )

    # --- New write: tasks.work_events -------------------------------------
    new_event_id: Optional[str] = None

    if write_new:
        from dynastore.modules.tasks.tasks_module import get_task_schema  # noqa: PLC0415
        from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415
        from dynastore.modules.db_config.query_executor import (  # noqa: PLC0415
            DQLQuery,
            ResultHandler,
        )

        task_schema = get_task_schema()
        new_event_id = str(generate_uuidv7())
        scope_lower = (scope or "platform").lower()

        sql = _WORK_EVENTS_INSERT_SQL.format(task_schema=task_schema)
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            event_id=new_event_id,
            shard=shard,
            schema_name=schema_name,
            scope=scope_lower,
            payload=payload_str,
        )

    # Return the canonical event_id: legacy if present, otherwise new.
    if legacy_event_id is not None:
        return legacy_event_id
    if new_event_id is not None:
        return new_event_id
    # Both paths off is not a reachable steady-state config, but guard
    # defensively so callers always get a string back.
    from dynastore.tools.identifiers import generate_uuidv7  # noqa: PLC0415

    return str(generate_uuidv7())
