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

"""
ReindexConsumer — M3.1b of the role-based-driver refactor.

Drives the durable outbox → :class:`ReindexWorker` dispatch pipeline.
Sits between the ``EventDriverProtocol`` (PG advisory lock + SKIP LOCKED
claim + LISTEN/NOTIFY wake-up) and the worker that translates one
``catalog_metadata_changed`` event into N Indexer calls.

Responsibilities (this file)
----------------------------

- **Leader election**: cluster-wide, via
  ``EventDriverProtocol.acquire_consumer_lock(key)``.  Exactly one
  replica fleet-wide runs the inner consume loop; the rest poll on
  5-second intervals waiting for the current leader to exit or die.
  The lock is held for the lifetime of an async context manager — on
  pod crash the backing PG session drops and the advisory lock
  releases automatically.
- **Batch claim**: calls ``driver.consume_batch(scope="PLATFORM",
  batch_size=N)`` to SKIP-LOCKED-claim a batch of PENDING events.
- **Batch dispatch**: hands the claimed events to
  :meth:`ReindexWorker.handle_batch`; translates the returned
  :class:`_DispatchResult` list into the ACK + NACK calls the outbox
  expects.
- **LISTEN/NOTIFY wake-up**: when the claim returns empty, waits on
  ``driver.wait_for_events(timeout=10.0)`` (PG ``LISTEN
  dynastore_events_channel``) rather than polling.  Producers
  ``NOTIFY`` after every INSERT so the consumer wakes
  sub-millisecond under load and consumes at idle-polling cost when
  quiet.

Not in this file (deferred)
---------------------------

- **Catch-up pass**: a sweep that walks ``catalog.catalogs`` + the
  split tables and emits synthetic ``catalog_metadata_changed``
  events for any catalog whose ``updated_at`` postdates the last
  Indexer-side mirror timestamp.  Needed for the first deployment
  (nothing in the outbox yet) and as a self-healing backstop for
  events silently dropped by an emission bug.  Will live in a
  separate module (``reindex_backfill.py``) with its own test suite.
- **Partial-batch retry semantics**: the current shape ACKs or NACKs
  events individually based on their ``_DispatchResult``.  A future
  enhancement could group-NACK an entire batch on a shared-error
  signal (e.g. "ES cluster unreachable") to avoid N individual NACK
  writes + N individual retry claims.

Failure policy
--------------

The consumer degrades, not fails.  A driver that raises during
``consume_batch`` / ``ack`` / ``nack`` logs at WARNING and the loop
continues after a backoff sleep.  A ``_DispatchResult`` with
``should_retry=True`` is NACKed (the outbox will re-deliver);
``should_retry=False, succeeded=True`` (degraded) is ACKed with the
degraded error recorded at WARNING level.  The worker never raises
into the consumer loop — exceptions inside ``_dispatch_one`` become
:class:`_DispatchResult` entries with ``fatal_errors``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional, Tuple

from dynastore.models.protocols.event_driver import EventDriverProtocol
from dynastore.modules.catalog.reindex_worker import (
    ReindexWorker,
    _DispatchResult,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


# Single canonical leader key across the catalog-metadata reindex
# fleet.  Separate from the platform-wide ``dynastore.events.consumer``
# lock (``event_service.py``) so both consumers can co-exist on the
# same deployment.  ``v1`` allows future-you to bump the key if the
# claim semantics change in a backwards-incompatible way and you need
# old + new consumers to NOT block each other on the same lock.
DEFAULT_LEADER_KEY = "dynastore.reindex.catalog.v1"

# Batch size for ``consume_batch`` claims.  100 is the existing
# shard-consumer default (see ``event_service.py::_consume_shard``);
# keeping the same number gives operators one mental model for
# "how much the consumer takes per round trip".
DEFAULT_BATCH_SIZE = 100

# Backoff used when:
# - No ``EventDriverProtocol`` is registered (deployment starting up
#   or event module disabled).
# - Leader-election lost — another replica holds the lock.
# - A recoverable error raised inside the consume loop.
#
# 5s matches ``event_service.py::_run_consume_loop`` and is short
# enough that a crashed leader's lock-release is picked up quickly
# without burning CPU on lock-poll thrash.
_LOOP_BACKOFF_S = 5.0

# LISTEN/NOTIFY wake-up timeout used when the outbox is empty.
# 10s is the existing ``_consume_shard`` value — long enough to avoid
# wake-up-and-find-nothing loops in quiet periods; short enough that
# a missed NOTIFY (theoretical — asyncpg's LISTEN is reliable in
# practice) is recovered within 10s rather than silently stalling.
_EMPTY_BATCH_WAIT_S = 10.0


class ReindexConsumer:
    """Leader-elected outbox → :class:`ReindexWorker` dispatch loop.

    One instance per replica; ``run_until_shutdown`` is idempotent
    with respect to the advisory lock — losing / never acquiring
    leadership is cheap (5s poll on a non-blocking lock).

    The consumer is deliberately stateless beyond the injected
    collaborators (worker + leader key + batch size).  All durable
    state — PENDING / PROCESSING / DEAD_LETTER transitions, retry
    counts, dedup keys — lives in the outbox driver.  That keeps the
    consumer restart-safe: a fresh instance on a new pod picks up
    exactly where the crashed leader left off.
    """

    def __init__(
        self,
        worker: Optional[ReindexWorker] = None,
        *,
        leader_key: str = DEFAULT_LEADER_KEY,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """
        Args:
            worker: ``ReindexWorker`` to dispatch each batch through.
                Default constructs a worker with the production
                router-based hydration — tests inject a canned worker
                to assert ACK/NACK translation without hitting the
                real Indexer fan-out.
            leader_key: Advisory-lock key for leader election.
                Defaults to :data:`DEFAULT_LEADER_KEY`; override for
                test isolation or parallel consumer fleets.
            batch_size: Max events pulled per ``consume_batch`` round-
                trip.  Tuning knob: larger values amortise the
                per-claim SQL cost but risk head-of-line blocking a
                slow Indexer behind a big chunk of fast events.
        """
        self._worker = worker if worker is not None else ReindexWorker()
        self._leader_key = leader_key
        self._batch_size = batch_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_until_shutdown(self, shutdown_event: Any) -> None:
        """Outer loop: acquire leadership, run consume-loop, repeat on loss.

        Blocks until ``shutdown_event.is_set()``.  Safe to launch with
        ``get_background_executor().submit(consumer.run_until_shutdown(evt))``
        or to ``await`` directly from an async entry-point.

        Loop shape (intentionally simple):

        - If no driver registered: sleep 5s, retry.
        - ``async with driver.acquire_consumer_lock(key)`` →
          - ``is_leader=False``: sleep 5s, retry.
          - ``is_leader=True``: run inner consume loop until it
            returns (shutdown) or raises (reconnect).
        - Any exception inside the ``async with`` scope: log + sleep
          5s + retry.  ``acquire_consumer_lock`` releases on exit so
          the lock doesn't leak across retries.
        """
        while not _is_shutdown(shutdown_event):
            driver = get_protocol(EventDriverProtocol)
            if driver is None:
                logger.warning(
                    "ReindexConsumer: EventDriverProtocol not registered; "
                    "retrying in %ss",
                    _LOOP_BACKOFF_S,
                )
                await asyncio.sleep(_LOOP_BACKOFF_S)
                continue

            try:
                async with driver.acquire_consumer_lock(
                    self._leader_key
                ) as is_leader:
                    if not is_leader:
                        # Another replica holds the lock — poll again
                        # shortly.  Non-blocking lock, so no risk of
                        # stacking waiters.
                        await asyncio.sleep(_LOOP_BACKOFF_S)
                        continue
                    logger.info(
                        "ReindexConsumer: leadership acquired "
                        "(key=%s, batch_size=%d)",
                        self._leader_key, self._batch_size,
                    )
                    await self._run_consume_loop(driver, shutdown_event)
                    logger.info(
                        "ReindexConsumer: leadership released "
                        "(key=%s, shutdown_requested=%s)",
                        self._leader_key,
                        _is_shutdown(shutdown_event),
                    )
            except asyncio.CancelledError:
                # Cooperative cancellation — propagate so the
                # background executor can unwind cleanly.
                raise
            except BaseException:  # noqa: BLE001 — recover + loop
                logger.exception(
                    "ReindexConsumer: outer loop error; reconnecting in %ss",
                    _LOOP_BACKOFF_S,
                )
                await asyncio.sleep(_LOOP_BACKOFF_S)

        logger.info(
            "ReindexConsumer: shutdown requested — outer loop exiting "
            "(key=%s)",
            self._leader_key,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _run_consume_loop(
        self,
        driver: EventDriverProtocol,
        shutdown_event: Any,
    ) -> None:
        """Inner loop: claim → dispatch → ACK/NACK while leader.

        Returns when ``shutdown_event.is_set()`` or when the driver's
        ``consume_batch`` / ``wait_for_events`` raises (the outer loop
        treats exceptions as leadership-loss and rebids for the lock).
        """
        while not _is_shutdown(shutdown_event):
            events = await driver.consume_batch(
                scope="PLATFORM", batch_size=self._batch_size,
            )
            if not events:
                # LISTEN/NOTIFY wake-up keeps the idle cost near zero
                # while still processing new events sub-millisecond
                # once a NOTIFY arrives.
                await driver.wait_for_events(timeout=_EMPTY_BATCH_WAIT_S)
                continue

            results = await self._worker.handle_batch(events)

            # Translate the worker's three-state outcome into the
            # outbox's two-state ACK/NACK shape:
            #
            # - ``succeeded=True``  → ACK (degraded or clean).  Any
            #   errors recorded on a succeeded result are logged but
            #   don't block the ACK — the SLA already said to degrade
            #   rather than retry (``on_timeout=degrade|skip``).
            # - ``succeeded=False`` → NACK.  The outbox increments
            #   retry_count and re-delivers; max-retries moves the
            #   event to DEAD_LETTER per the driver's own policy.
            ack_ids, nacks = self._partition_results(results)

            if ack_ids:
                try:
                    await driver.ack(event_ids=ack_ids)
                except Exception as exc:  # noqa: BLE001 — log, don't abort
                    logger.warning(
                        "ReindexConsumer: ACK batch failed (%d ids): %s — "
                        "events will re-deliver until individual NACK or "
                        "retry exhaustion",
                        len(ack_ids), exc,
                    )

            for event_id, error in nacks:
                try:
                    await driver.nack(event_id=event_id, error=error)
                except Exception as exc:  # noqa: BLE001 — log, don't abort
                    logger.warning(
                        "ReindexConsumer: NACK failed for %s: %s (original "
                        "error=%s) — retry-count not incremented, event "
                        "will re-appear on next claim",
                        event_id, exc, error,
                    )

    def _partition_results(
        self, results: List[_DispatchResult],
    ) -> Tuple[List[str], List[Tuple[str, str]]]:
        """Split a batch's ``_DispatchResult`` list into ACK ids + NACK pairs.

        Events with a missing ``event_id`` (malformed outbox row —
        shouldn't happen but defensive) are logged and dropped from
        both lists; they'll get re-claimed on the next round trip
        because the driver never saw an ACK/NACK for them.  A row
        without ``event_id`` is broken outbox state, not a consumer
        bug — surfacing a WARNING lets the operator go find the bad
        row without the consumer silently ignoring it.
        """
        ack_ids: List[str] = []
        nacks: List[Tuple[str, str]] = []
        for r in results:
            event_id = r.event_id
            if event_id is None:
                logger.warning(
                    "ReindexConsumer: _DispatchResult has no event_id "
                    "(errors=%r, succeeded=%s) — skipping ACK/NACK, "
                    "event will re-appear on next claim",
                    r.errors, r.succeeded,
                )
                continue
            if r.succeeded:
                ack_ids.append(event_id)
                if r.errors:
                    # Degraded ACK: log the per-driver errors the
                    # SLA-policy-skip / SLA-policy-degrade branches
                    # produced so operators can alert on them.
                    logger.warning(
                        "ReindexConsumer: ACKing degraded event %s "
                        "(errors=%r)",
                        event_id, r.errors,
                    )
            else:
                error_msg = "; ".join(r.errors) if r.errors else (
                    "dispatch failed with no error message"
                )
                nacks.append((event_id, error_msg))
        return ack_ids, nacks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_shutdown(shutdown_event: Any) -> bool:
    """Shape-tolerant ``is_set()`` probe.

    Historical sibling ``event_service.py::_consume_shard`` accepts a
    ``shutdown_event`` duck-typed on ``is_set``; we mirror that
    contract so callers can pass ``asyncio.Event``, ``threading.Event``,
    or any sentinel with a ``.is_set()`` attribute.
    """
    return bool(getattr(shutdown_event, "is_set", lambda: False)())
