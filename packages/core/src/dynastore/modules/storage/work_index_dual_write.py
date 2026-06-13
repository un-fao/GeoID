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

# dynastore/modules/storage/work_index_dual_write.py

"""Index-plane dual-write into the global ``tasks.work_index`` table.

The legacy ES-index outbox is a per-tenant ``{schema}.storage_outbox`` table
written co-transactionally with the item write — ``pg_outbox.enqueue_bulk`` on
the caller's open transaction (see ``ItemService.upsert_bulk`` and the delete
twin ``ItemService._enqueue_index_deletes``). The WorkClass migration (#1807)
introduces a single GLOBAL ``tasks.work_index`` table where tenancy is carried
by a ``schema_name`` column instead of the table's physical location.

During cutover the same outbox records are written to both tables, gated
per-plane by :class:`WorkClassConfig.emit_target_index`:

* ``legacy`` (default) — only ``{schema}.storage_outbox`` (today's behaviour).
* ``both``   — both tables, on the same transaction.
* ``new``    — only ``tasks.work_index``.

This module owns the new-table writer and the emit-target resolution so the
seam stays a thin one-call dispatch and the legacy/new split lives in one
place that Phase C can collapse. The new-table INSERT is schema-qualified to
the global tasks schema (``get_task_schema()``) so it lands correctly
regardless of the caller connection's tenant-pinned ``search_path``; it rides
the caller's transaction, so a failed enqueue rolls back the primary write —
the same atomicity guarantee the legacy outbox provides.
"""
import json
import logging
from typing import Any, Sequence, Tuple

from dynastore.models.protocols.indexing import OutboxRecord
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


async def _resolve_index_emit_targets(configs: Any) -> Tuple[bool, bool]:
    """Return ``(write_legacy, write_new)`` for the index plane.

    Fail-safe to ``(True, False)`` — legacy-only, exactly today's behaviour —
    on a missing ConfigsProtocol or ANY resolution error. The index plane is
    the atomicity-critical seam in the item-write path: a config lookup must
    never break a write nor silently drop the legacy outbox row.
    """
    if configs is None:
        return True, False
    try:
        from dynastore.modules.tasks.workclass_config import WorkClassConfig

        cfg = await configs.get_config(WorkClassConfig)
        if isinstance(cfg, WorkClassConfig):
            return cfg.emit_index_to_legacy, cfg.emit_index_to_new
    except Exception:  # noqa: BLE001 — degrade to legacy-only, never fail the write
        logger.debug(
            "WorkClassConfig resolution failed; index dual-write stays "
            "legacy-only for this write.",
            exc_info=True,
        )
    return True, False


async def _enqueue_work_index(
    conn: Any,
    *,
    schema_name: str,
    rows: Sequence[OutboxRecord],
) -> None:
    """Insert outbox records into the global ``tasks.work_index`` on ``conn``.

    Runs a schema-qualified parameterised ``INSERT`` per row on the caller's
    open SQLAlchemy transaction (the co-transactional path), mirroring the
    legacy ``pg_outbox.enqueue_bulk`` caller-conn branch. ``schema_name`` is
    the tenant's physical schema (the value the legacy outbox uses both as the
    table namespace and as the catalog identity); it is bound as a column
    VALUE, never interpolated into SQL. ``day`` is ``CURRENT_DATE`` so the row
    lands in today's daily leaf (or the DEFAULT partition on a gap day).
    """
    if not rows:
        return
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.modules.tasks.tasks_module import get_task_schema

    task_schema = get_task_schema()
    # Defence-in-depth: the schema name comes from a trusted env default but is
    # placed in identifier position, so validate it like every other qualifier.
    validate_sql_identifier(task_schema)
    insert_sql = (
        f"INSERT INTO {task_schema}.work_index ("
        "    op_id, day, schema_name, driver_id, catalog_id, collection_id,"
        "    op, item_id, op_payload, idempotency_key"
        ") VALUES ("
        "    :op_id, CURRENT_DATE, :schema_name, :driver_id, :catalog_id,"
        "    :collection_id, :op, :item_id, CAST(:op_payload AS jsonb),"
        "    :idempotency_key"
        ")"
    )
    query = DQLQuery(insert_sql, result_handler=ResultHandler.NONE)
    for r in rows:
        await query.execute(
            conn,
            op_id=str(r.op_id),
            schema_name=schema_name,
            driver_id=r.driver_id,
            # In the outbox model the tenant's physical schema name is the
            # catalog identity; the legacy store carries no separate value.
            catalog_id=schema_name,
            collection_id=r.collection_id,
            op=r.op,
            item_id=r.item_id,
            op_payload=json.dumps(r.payload),
            idempotency_key=r.idempotency_key,
        )


async def dispatch_index_dual_write(
    conn: Any,
    *,
    outbox: Any,
    catalog_id: str,
    rows: Sequence[OutboxRecord],
    configs: Any,
) -> None:
    """Write index-outbox ``rows`` to the legacy and/or new table per config.

    The single dispatch point for the index-plane dual-write, shared by the
    upsert seam (``ItemService.upsert_bulk``) and the delete twin
    (``ItemService._enqueue_index_deletes``). Both writes ride ``conn`` (the
    caller's open item-write transaction), so either failing rolls back the
    primary write. With the default config this calls only
    ``outbox.enqueue_bulk`` — byte-for-byte today's path.

    ``catalog_id`` is the tenant's physical schema (the outbox convention);
    it is the legacy table's namespace AND the new table's ``schema_name``.
    """
    if not rows:
        return
    write_legacy, write_new = await _resolve_index_emit_targets(configs)
    if write_legacy:
        await outbox.enqueue_bulk(conn, catalog_id=catalog_id, rows=rows)
    if write_new:
        await _enqueue_work_index(conn, schema_name=catalog_id, rows=rows)
