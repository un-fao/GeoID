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

"""Index dispatcher — generic fan-out across configured Indexer drivers.

This is the production-readiness backbone for per-item index propagation.
It replaces (in subsequent phases) the event-driven listener pattern used
today by ``ItemsElasticsearchDriver`` and ``ElasticsearchModule`` with a
single, driver-agnostic fan-out site.

Design properties
-----------------

* **Generic** — knows nothing about ES.  Operates on
  :class:`~dynastore.models.protocols.indexer.Indexer` instances looked up
  by ``indexer_id`` from the protocol registry.  ES public, ES private,
  vector DB, audit log: all interchangeable.
* **Tier-uniform** — same code path for catalog / collection / item /
  asset entries; per-tier markers live on the implementations and on the
  routing-config auto-registration, not in the dispatcher.
* **In-process when possible** — when the dispatcher and the indexer run
  in the same pod the call is a direct ``await indexer.index(...)`` —
  no event/task hop.
* **Durable on failure** — the ``OUTBOX`` failure policy persists an
  ``_meta.index_outbox`` row in the *same* PG transaction as the upstream
  write.  PG TX commit guarantees neither the data nor the
  obligation-to-index can be lost.
* **Circuit-broken** — per-indexer-id breaker (Phase 3).  When open,
  sync attempts short-circuit; ``OUTBOX`` policy still enqueues so the
  worker drains when the breaker half-closes.

Phases
------

* Phase 1 (this module) — Protocol surface + dispatcher skeleton.  No
  consumer wiring yet; existing event-driven listeners remain in place.
* Phase 2 — ``_meta.index_outbox`` table + drain worker; replace the
  ES driver's event listeners with a direct dispatcher call from
  ``item_service.upsert``.
* Phase 3 — Circuit breaker (Valkey-backed shared state); migrate the
  private/per-catalog geoid-only indexer onto the same protocol.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence

from dynastore.models.protocols.indexer import (
    BulkResult,
    Indexer,
    IndexContext,
    IndexOp,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class IndexerFatal(Exception):
    """Raised by the dispatcher when an indexer with ``FailurePolicy.FATAL``
    fails.  Propagates out of the dispatcher; the caller's PG transaction
    rolls back, taking the upstream data write with it.
    """

    def __init__(self, indexer_id: str, op: IndexOp, original: BaseException) -> None:
        super().__init__(
            f"Fatal indexer failure: '{indexer_id}' on "
            f"{op.op_type}/{op.entity_type}/{op.entity_id}: {original}"
        )
        self.indexer_id = indexer_id
        self.op = op
        self.original = original


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class IndexDispatcher:
    """Fan-out point for index ops across all configured indexers.

    Instantiated once per process (typically a singleton resolved via
    protocol discovery).  Stateless except for the circuit-breaker
    handle (Phase 3).

    Parameters
    ----------
    routing_resolver
        Async callable ``(catalog, collection) -> CollectionRoutingConfig``
        used to look up ``operations[INDEX]``.  Pluggable so the dispatcher
        is testable without booting the full config service.
    indexer_registry
        Async callable ``(indexer_id) -> Indexer | None`` resolving the
        runtime instance for a routing entry's ``driver_id``.
    outbox
        Optional ``OutboxWriter`` (Phase 2) — when ``None``, ``OUTBOX``
        failure policy degrades to ``WARN`` with a one-time log.
    breaker
        Optional ``CircuitBreaker`` (Phase 3) — when ``None``, every
        sync attempt is tried unconditionally.
    """

    def __init__(
        self,
        *,
        routing_resolver,
        indexer_registry,
        outbox=None,
        breaker=None,
    ) -> None:
        self._resolve_routing = routing_resolver
        self._resolve_indexer = indexer_registry
        self._outbox = outbox
        self._breaker = breaker
        self._outbox_warning_emitted: set = set()

    # ------------------------------------------------------------------
    # Public surface — single-op and bulk
    # ------------------------------------------------------------------

    async def fan_out(self, ctx: IndexContext, op: IndexOp) -> None:
        """Dispatch a single index op across every configured indexer
        for ``(ctx.catalog, ctx.collection)``.

        Each indexer's outcome is governed by its routing entry's
        :attr:`OperationDriverEntry.on_failure` policy — the dispatcher
        does not stop on a single non-FATAL failure.
        """
        entries = await self._index_entries(ctx)
        for entry in entries:
            await self._dispatch_one(entry, ctx, op)

    async def fan_out_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> Dict[str, BulkResult]:
        """Dispatch a bulk of index ops across every configured indexer.

        Returns per-indexer ``BulkResult`` so callers can surface partial
        failures (a 207-style report).  Per-op failures inside an indexer
        are absorbed into ``BulkResult.failures``; an indexer raising
        applies ``FailurePolicy`` to the whole batch.
        """
        results: Dict[str, BulkResult] = {}
        entries = await self._index_entries(ctx)
        for entry in entries:
            indexer = await self._resolve_indexer(entry.driver_id)
            if indexer is None:
                continue
            results[entry.driver_id] = await self._dispatch_bulk(
                entry, indexer, ctx, ops,
            )
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _index_entries(
        self, ctx: IndexContext,
    ) -> List[OperationDriverEntry]:
        try:
            routing = await self._resolve_routing(ctx.catalog, ctx.collection)
        except Exception as exc:
            logger.warning(
                "IndexDispatcher: routing lookup failed for %s/%s: %s",
                ctx.catalog, ctx.collection, exc,
            )
            return []
        ops_map = getattr(routing, "operations", {}) or {}
        return list(ops_map.get(Operation.INDEX, []))

    async def _dispatch_one(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexOp,
    ) -> None:
        indexer = await self._resolve_indexer(entry.driver_id)
        if indexer is None:
            logger.debug(
                "IndexDispatcher: no runtime instance for indexer '%s' — skipping.",
                entry.driver_id,
            )
            return

        # OUTBOX_ONLY shortcut: never attempt sync, always enqueue.
        if entry.write_mode == WriteMode.ASYNC and entry.on_failure == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op)
            return

        # Circuit breaker check (Phase 3 — no-op when self._breaker is None).
        if self._breaker is not None and self._breaker.is_open(entry.driver_id):
            if entry.on_failure == FailurePolicy.OUTBOX:
                await self._enqueue_or_warn(entry, ctx, op)
            elif entry.on_failure == FailurePolicy.FATAL:
                raise IndexerFatal(
                    entry.driver_id, op,
                    RuntimeError("circuit breaker open"),
                )
            else:
                logger.debug(
                    "IndexDispatcher: breaker open for '%s' — skipping (%s).",
                    entry.driver_id, entry.on_failure,
                )
            return

        # Synchronous in-process attempt.
        try:
            await indexer.index(ctx, op)
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_id)
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_id)
            await self._handle_failure(entry, ctx, op, exc)

    async def _dispatch_bulk(
        self,
        entry: OperationDriverEntry,
        indexer: Indexer,
        ctx: IndexContext,
        ops: Sequence[IndexOp],
    ) -> BulkResult:
        if self._breaker is not None and self._breaker.is_open(entry.driver_id):
            return BulkResult(total=len(ops), failed=len(ops), failures=[
                {"reason": "circuit_breaker_open", "indexer": entry.driver_id},
            ])
        try:
            result = await indexer.index_bulk(ctx, ops)
            if self._breaker is not None:
                self._breaker.record_success(entry.driver_id)
            return result
        except Exception as exc:
            if self._breaker is not None:
                self._breaker.record_failure(entry.driver_id)
            # Bulk failure: apply policy to the whole batch.
            for op in ops:
                await self._handle_failure(entry, ctx, op, exc, bulk=True)
            return BulkResult(
                total=len(ops),
                failed=len(ops),
                failures=[{"reason": str(exc), "indexer": entry.driver_id}],
            )

    async def _handle_failure(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexOp,
        exc: BaseException,
        *,
        bulk: bool = False,
    ) -> None:
        policy = entry.on_failure
        if policy == FailurePolicy.FATAL:
            raise IndexerFatal(entry.driver_id, op, exc) from exc
        if policy == FailurePolicy.OUTBOX:
            await self._enqueue_or_warn(entry, ctx, op, original=exc)
            return
        if policy == FailurePolicy.WARN:
            logger.warning(
                "IndexDispatcher: indexer '%s' failed for %s/%s/%s "
                "(policy=warn%s): %s",
                entry.driver_id, op.op_type, op.entity_type, op.entity_id,
                ", bulk" if bulk else "", exc,
            )
            return
        # IGNORE — silent skip
        logger.debug(
            "IndexDispatcher: indexer '%s' failed (policy=ignore): %s",
            entry.driver_id, exc,
        )

    async def _enqueue_or_warn(
        self,
        entry: OperationDriverEntry,
        ctx: IndexContext,
        op: IndexOp,
        *,
        original: Optional[BaseException] = None,
    ) -> None:
        """Enqueue an outbox row when configured; otherwise degrade to WARN.

        Phase 1 ships without an outbox writer — this method emits a
        one-time warning per indexer and falls through to WARN-level
        log of the original exception.  Phase 2 wires the real writer.
        """
        if self._outbox is None:
            if entry.driver_id not in self._outbox_warning_emitted:
                self._outbox_warning_emitted.add(entry.driver_id)
                logger.warning(
                    "IndexDispatcher: indexer '%s' has on_failure=outbox but "
                    "no OutboxWriter is wired — failures degrade to WARN. "
                    "Phase 2 will activate durable retry.",
                    entry.driver_id,
                )
            if original is not None:
                logger.warning(
                    "IndexDispatcher: indexer '%s' failed for %s/%s/%s "
                    "(policy=outbox, degraded): %s",
                    entry.driver_id, op.op_type, op.entity_type,
                    op.entity_id, original,
                )
            return

        try:
            await self._outbox.enqueue(
                indexer_id=entry.driver_id,
                ctx=ctx,
                op=op,
                last_error=str(original) if original else None,
            )
        except Exception as enqueue_exc:
            # Outbox itself failed — last-resort WARN.  We do NOT escalate
            # to FATAL here because the upstream caller has already chosen
            # OUTBOX as a tolerant policy; surfacing this as fatal would
            # surprise them.
            logger.error(
                "IndexDispatcher: outbox enqueue failed for indexer '%s' "
                "on %s/%s/%s — original error: %s, enqueue error: %s",
                entry.driver_id, op.op_type, op.entity_type, op.entity_id,
                original, enqueue_exc,
            )
