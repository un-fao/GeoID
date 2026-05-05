"""``PgOutboxStore`` — :class:`OutboxStore` Protocol backed by PG.

Caller responsibility: connections passed to :meth:`enqueue_bulk` must
already have ``search_path`` set to the catalog's physical schema —
that method runs inside the caller's transaction context and we
deliberately do not mutate it. Methods that acquire their own
connection (:meth:`claim_batch`, ``mark_*``, :meth:`listen`) issue a
``SET LOCAL search_path TO "<schema>"`` separately via
:func:`dynastore.tools.db.validate_sql_identifier` before the
parameterised query so a fresh pool connection lands on the correct
per-tenant schema. asyncpg's extended-query protocol is single-
statement-only, which is why this is split into two ``execute`` calls
rather than concatenated into a single string.

* :meth:`enqueue_bulk` uses ``asyncpg.copy_records_to_table`` for
  high-throughput ingest (100k/min target).
* :meth:`claim_batch` uses ``FOR UPDATE SKIP LOCKED`` so multiple
  drainers can claim disjoint subsets in parallel.
* :meth:`listen` yields :class:`Notification` per ``pg_notify`` fired
  by the AFTER INSERT trigger so drainers wake on commit instead of
  polling.
"""
from __future__ import annotations

import json
from typing import Any, AsyncIterator, List, Sequence
from uuid import UUID

from dynastore.models.protocols.indexing import (
    Notification,
    OutboxRecord,
    OutboxRow,
)
from dynastore.tools.db import validate_sql_identifier


# Per-attempt retry backoff in seconds. Index by ``attempts`` (0-based);
# the last entry is reused for any attempt at or beyond its position so
# the backoff caps at ~30min instead of growing unbounded.
_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]


class PgOutboxStore:
    """Postgres-backed :class:`OutboxStore` implementation."""

    indexer_id: str = "pg_outbox"

    def __init__(
        self,
        *,
        pool: Any = None,
        single_conn: Any = None,
    ) -> None:
        if pool is None and single_conn is None:
            raise ValueError("PgOutboxStore: provide pool or single_conn")
        self._pool = pool
        self._single = single_conn

    async def _conn(self) -> Any:
        """Return the connection used for non-enqueue queries.

        ``single_conn`` mode wins — used by tests so the caller's
        ``search_path`` is preserved on the same physical session.
        """
        if self._single is not None:
            return self._single
        if self._pool is None:
            raise RuntimeError("PgOutboxStore: no connection source")
        return await self._pool.acquire()

    async def _ensure_search_path(self, conn: Any, catalog_id: str) -> None:
        """Pin ``search_path`` to ``catalog_id`` on a pool-acquired conn.

        No-op in ``single_conn`` mode — the test fixture has already set
        ``search_path`` on that session and we deliberately leave it alone.

        Issued as a standalone ``execute`` (not concatenated onto the
        next parameterised query) because asyncpg's extended-query
        protocol rejects multi-statement strings when ``$N`` placeholders
        are present.
        """
        if self._single is not None:
            return
        validate_sql_identifier(catalog_id)
        await conn.execute(f'SET LOCAL search_path TO "{catalog_id}"')

    async def enqueue_bulk(
        self,
        conn: Any,
        *,
        catalog_id: str,
        rows: Sequence[OutboxRecord],
    ) -> None:
        """Bulk-insert outbox rows via binary COPY.

        Runs on the caller's ``conn`` inside the caller's transaction;
        ``search_path`` must already be set by the caller.
        """
        if not rows:
            return
        records = [
            (
                r.op_id,
                r.driver_id,
                r.driver_instance_id,
                r.collection_id,
                r.op,
                r.item_id,
                json.dumps(r.payload),
                r.idempotency_key,
            )
            for r in rows
        ]
        await conn.copy_records_to_table(
            "storage_outbox",
            records=records,
            columns=[
                "op_id",
                "driver_id",
                "driver_instance_id",
                "collection_id",
                "op",
                "item_id",
                "payload",
                "idempotency_key",
            ],
        )

    async def claim_batch(
        self,
        *,
        driver_id: str,
        catalog_id: str,
        batch_size: int,
        claimed_by: str,
    ) -> List[OutboxRow]:
        """Atomically claim up to ``batch_size`` ready rows.

        ``FOR UPDATE SKIP LOCKED`` makes concurrent claimers see disjoint
        subsets; the row lock is released at TX commit so the caller is
        responsible for opening a transaction around the claim/process
        cycle.
        """
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        rows = await conn.fetch(
            """
            WITH claimed AS (
                SELECT op_id FROM storage_outbox
                WHERE driver_id = $1 AND status = 'ready' AND ready_at <= now()
                ORDER BY ready_at, op_id
                LIMIT $2 FOR UPDATE SKIP LOCKED
            )
            UPDATE storage_outbox
            SET status='in_flight', claimed_at=now(), claimed_by=$3
            FROM claimed
            WHERE storage_outbox.op_id = claimed.op_id
            RETURNING storage_outbox.op_id, storage_outbox.driver_id,
                      storage_outbox.driver_instance_id,
                      storage_outbox.collection_id, storage_outbox.op,
                      storage_outbox.item_id, storage_outbox.payload,
                      storage_outbox.idempotency_key,
                      storage_outbox.attempts
            """,
            driver_id,
            batch_size,
            claimed_by,
        )
        return [
            OutboxRow(
                op_id=r["op_id"],
                driver_id=r["driver_id"],
                driver_instance_id=r["driver_instance_id"],
                catalog_id=catalog_id,
                collection_id=r["collection_id"],
                op=r["op"],
                item_id=r["item_id"],
                payload=(
                    json.loads(r["payload"])
                    if isinstance(r["payload"], str)
                    else r["payload"]
                ),
                idempotency_key=r["idempotency_key"],
                attempts=r["attempts"],
            )
            for r in rows
        ]

    async def mark_done(
        self, *, catalog_id: str, op_ids: Sequence[UUID],
    ) -> None:
        """Mark the listed ops ``done`` and stamp ``finished_at``."""
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            "UPDATE storage_outbox SET status='done', finished_at=now() "
            "WHERE op_id = ANY($1::uuid[])",
            list(op_ids),
        )

    async def mark_retry(
        self,
        *,
        catalog_id: str,
        op_ids: Sequence[UUID],
        error: str,
        attempts_seen: int,
    ) -> None:
        """Bump ``attempts``, schedule retry per backoff curve.

        ``attempts_seen`` is informational — the row's own ``attempts``
        column drives the backoff lookup so concurrent retries can't
        de-sync the curve.
        """
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            """
            UPDATE storage_outbox
            SET status='ready', attempts=attempts+1, last_error=$2,
                ready_at = now() + (
                    CASE WHEN attempts >= array_length($3::int[], 1) - 1
                         THEN ($3::int[])[array_length($3::int[], 1)]
                         ELSE ($3::int[])[attempts + 1]
                    END
                ) * INTERVAL '1 second'
            WHERE op_id = ANY($1::uuid[])
            """,
            list(op_ids),
            error,
            _BACKOFF_SECONDS,
        )

    async def mark_failed(
        self, *, catalog_id: str, op_ids: Sequence[UUID], error: str,
    ) -> None:
        """Mark the listed ops ``failed`` (terminal) with ``last_error``."""
        if not op_ids:
            return
        conn = await self._conn()
        await self._ensure_search_path(conn, catalog_id)
        await conn.execute(
            "UPDATE storage_outbox SET status='failed', last_error=$2, "
            "finished_at=now() WHERE op_id = ANY($1::uuid[])",
            list(op_ids),
            error,
        )

    def listen(
        self, *, driver_id: str, catalog_id: str,
    ) -> AsyncIterator[Notification]:
        """Yield a :class:`Notification` per ``pg_notify`` on the
        ``outbox_<driver_id>_<catalog_id>`` channel.

        The connection used for ``LISTEN`` must be dedicated for the
        lifetime of the iterator. The caller controls iteration
        lifecycle — break out of the ``async for`` to release the
        listener and (in pool mode) drop the connection back to the
        pool.

        Outer ``def`` returns an inner async generator directly so the
        signature matches :class:`OutboxStore.listen` (the streaming-
        Protocol convention is plain ``def`` returning ``AsyncIterator``,
        not ``async def`` with ``yield``).
        """

        async def _gen() -> AsyncIterator[Notification]:
            import asyncio

            conn = await self._conn()
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(driver_id)
            channel = f"outbox_{driver_id}_{catalog_id}"
            queue: asyncio.Queue[Notification] = asyncio.Queue()

            async def _on_notify(
                _c: Any, _pid: int, _ch: str, payload: str,
            ) -> None:
                try:
                    doc = json.loads(payload)
                    await queue.put(
                        Notification(
                            driver_id=doc["driver_id"],
                            catalog_id=doc["catalog_id"],
                            op_id=UUID(doc["op_id"]),
                        )
                    )
                except Exception:  # noqa: BLE001 — malformed notify, drop
                    pass

            await conn.add_listener(channel, _on_notify)
            try:
                while True:
                    yield await queue.get()
            finally:
                await conn.remove_listener(channel, _on_notify)

        return _gen()
