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
ReindexWorker — M3.1 scaffold of the role-based-driver refactor.

Consumes ``catalog_metadata_changed`` events emitted by the catalog-tier
router (M3.0) and dispatches the mutation payload to registered INDEX
drivers (ES Indexers, vector-DB sinks, …).  The worker is the glue
between the authoritative store (catalog.catalog_metadata_core/_stac
written by the Primary drivers) and search-tier mirrors.

Scope of M3.1 (this file)
-------------------------

- **Batch dispatcher**: given a list of catalog_metadata_changed event
  dicts, hydrate each (via ``catalog_metadata_router.get_catalog_metadata``)
  and fan out to every INDEX-role driver configured in
  ``CatalogRoutingConfig.metadata.operations[INDEX]``.
- **Per-driver SLA**: honours ``DriverSla.timeout_ms`` via
  ``asyncio.wait_for``.  On timeout / raise, applies the entry's
  ``on_timeout`` ∈ {"fail", "degrade", "skip"} policy.
- **Best-effort**: worker-level exceptions in one entry don't poison
  the batch.  The caller (outbox consumer loop — M3.1b) decides
  whether a partial-success batch gets ACKed or NACKed via this
  function's return value.

Not in this file yet (follow-up M3.1b)
--------------------------------------

- LISTEN on ``dynastore_events_channel`` / leader-elected consumer
  loop: that path reuses the existing ``EventBusProtocol`` infrastructure
  (see ``modules/events/events_module.py``) and isn't reinvented here.
- Advisory-lock sharding across worker replicas.
- Catch-up / backfill pass that sweeps catalogs whose split-table
  ``updated_at`` ≥ some cursor.
- Per-Indexer TRANSFORM chain (when ``entry.transformed = True``).
  M3.1 only implements ``transformed = False`` — raw Primary envelope.

"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.metadata_driver import CatalogMetadataStore
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig, Operation, OperationDriverEntry,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result model (returned from ``handle_batch`` so the caller decides ACK/NACK)
# ---------------------------------------------------------------------------


class _IndexerResolution:
    """Cached result of :func:`_resolve_catalog_indexers` for one batch.

    Either the resolution succeeded and ``indexers`` holds the
    (entry, driver) pairs while ``error`` is ``None``, or it failed and
    ``indexers`` is empty while ``error`` carries the message that
    every in-scope event's ``_DispatchResult`` will inherit.

    Sharing the resolution across a batch avoids the N× rebuild of
    ``CatalogRoutingConfig`` + ``get_protocols(CatalogMetadataStore)``
    that the M3.1 scaffold paid per event; see
    :meth:`ReindexWorker.handle_batch` for the batch-caching rationale.
    """

    __slots__ = ("indexers", "error")

    def __init__(
        self,
        *,
        indexers: List[Tuple[OperationDriverEntry, CatalogMetadataStore]],
        error: Optional[str],
    ) -> None:
        self.indexers = indexers
        self.error = error


class _DispatchResult:
    """Per-event outcome returned by :meth:`ReindexWorker.handle_batch`.

    Caller semantics:

    - ``succeeded`` : every configured Indexer accepted the payload
      (or was skipped per ``on_timeout=skip``).  ACK the event.
    - ``should_retry`` : at least one Indexer failed with a
      ``on_timeout=fail`` policy or raised synchronously.  NACK the
      event — it'll be re-delivered by the outbox.
    - ``event_id`` : passthrough from the outbox row so the caller can
      build the ACK / NACK list without re-parsing.

    A three-state return (not just success/fail) keeps the interface
    explicit: a "degraded" entry is neither a success (the driver
    didn't actually see the data) nor a hard failure (retrying won't
    help — the SLA said to degrade).  It gets logged + ACKed.
    """

    __slots__ = ("event_id", "succeeded", "should_retry", "errors")

    def __init__(
        self,
        *,
        event_id: Optional[str],
        succeeded: bool,
        should_retry: bool,
        errors: List[str],
    ) -> None:
        self.event_id = event_id
        self.succeeded = succeeded
        self.should_retry = should_retry
        self.errors = errors

    def __repr__(self) -> str:
        return (
            f"_DispatchResult(event_id={self.event_id!r}, "
            f"succeeded={self.succeeded}, should_retry={self.should_retry}, "
            f"errors={self.errors!r})"
        )


# ---------------------------------------------------------------------------
# ReindexWorker
# ---------------------------------------------------------------------------


class ReindexWorker:
    """Dispatcher for ``catalog_metadata_changed`` events → INDEX drivers.

    Stateless by design — every method takes the inputs it needs.  The
    only instance state is the injected ``get_catalog_metadata``
    callable, which defaults to the production catalog-tier router's
    function but can be swapped for tests.

    Parallel shape for ``COLLECTION_METADATA_CHANGED`` will land with
    the collection-tier equivalent of M2.4 (deferred).  Today only the
    catalog-tier event flows through this worker.
    """

    def __init__(
        self,
        *,
        resolve_indexers: Optional[Callable[[], List[Tuple[OperationDriverEntry, CatalogMetadataStore]]]] = None,
        get_catalog_metadata: Optional[Callable[..., Any]] = None,
    ) -> None:
        """
        Args:
            resolve_indexers: Callable returning
                ``List[Tuple[OperationDriverEntry, CatalogMetadataStore]]``
                — the (entry, driver) pairs configured for
                ``CatalogRoutingConfig.metadata.operations[INDEX]``.
                Defaults to :func:`_resolve_catalog_indexers` which
                reads from the live CatalogRoutingConfig + registered
                drivers.  Tests inject a canned list to bypass
                discovery.
            get_catalog_metadata: Hydration callable used to fetch the
                current envelope for a mutated catalog.  Defaults to
                the catalog-tier router's ``get_catalog_metadata``.
        """
        self._resolve_indexers = (
            resolve_indexers or _resolve_catalog_indexers
        )
        if get_catalog_metadata is None:
            from dynastore.modules.catalog.catalog_metadata_router import (
                get_catalog_metadata as _default_hydrate,
            )
            self._get_catalog_metadata = _default_hydrate
        else:
            self._get_catalog_metadata = get_catalog_metadata

    # -- Public API ---------------------------------------------------------

    async def handle_batch(
        self,
        events: List[Dict[str, Any]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[_DispatchResult]:
        """Process a batch of ``catalog_metadata_changed`` events.

        Returns one :class:`_DispatchResult` per input event.  The
        caller (outbox consumer loop) decides which to ACK vs NACK
        based on ``succeeded`` / ``should_retry``.

        Events of other types are skipped with a log entry — they're
        not in this worker's scope.  A future per-event-type router
        can dispatch to different workers.

        Indexer-resolution caching
        --------------------------
        ``_resolve_indexers`` is called ONCE per batch rather than once
        per event.  The resolution walks ``CatalogRoutingConfig()`` +
        ``get_protocols(CatalogMetadataStore)``, both non-trivial under
        repeated invocation; a 100-event batch used to redo the work
        100 times.  Caching at the batch boundary keeps the config
        read close enough to realtime (next batch picks up any
        routing-config change) without paying per-event cost.

        A resolution failure is shared across the batch: every in-
        scope event returns ``succeeded=False, should_retry=True``
        with the same error.  The caller NACKs them all; a future
        delivery retries with a freshly-read config.
        """
        # Resolve once per batch — see docstring above.
        indexer_resolution: _IndexerResolution = self._resolve_indexers_once()

        results: List[_DispatchResult] = []
        for event in events:
            event_type = event.get("event_type")
            if event_type != "catalog_metadata_changed":
                logger.debug(
                    "ReindexWorker skipping non-catalog-metadata event "
                    "%r (event_id=%s)",
                    event_type, event.get("event_id"),
                )
                # Still ACK so the caller can prune the outbox — this
                # event is nobody's responsibility here.
                results.append(_DispatchResult(
                    event_id=event.get("event_id"),
                    succeeded=True,
                    should_retry=False,
                    errors=[],
                ))
                continue
            results.append(
                await self._handle_one(
                    event,
                    db_resource=db_resource,
                    indexer_resolution=indexer_resolution,
                )
            )
        return results

    def _resolve_indexers_once(self) -> "_IndexerResolution":
        """Invoke ``_resolve_indexers`` with shared-error semantics.

        Wraps the per-batch resolution call so a failure becomes a
        structured result that ``_handle_one`` can turn into per-event
        NACKs — rather than letting the exception kill the whole
        batch.  Success returns the ``indexers`` list and ``error=None``.
        """
        try:
            return _IndexerResolution(
                indexers=self._resolve_indexers(),
                error=None,
            )
        except Exception as exc:  # noqa: BLE001 — surface as per-event NACK
            logger.warning(
                "ReindexWorker: Indexer resolution failed for batch: %s",
                exc,
            )
            return _IndexerResolution(
                indexers=[],
                error=f"indexer resolution failed: {exc}",
            )

    # -- Internals ----------------------------------------------------------

    async def _handle_one(
        self,
        event: Dict[str, Any],
        *,
        db_resource: Optional[Any],
        indexer_resolution: Optional[_IndexerResolution] = None,
    ) -> _DispatchResult:
        """Process a single ``catalog_metadata_changed`` event.

        ``indexer_resolution`` is supplied by :meth:`handle_batch` so
        the resolution runs once per batch rather than once per event.
        When omitted (direct call from a test or a future single-event
        code path), the method falls back to resolving indexers
        itself — preserving the previous standalone contract.
        """
        event_id = event.get("event_id")
        payload = event.get("payload") or {}
        catalog_id = payload.get("catalog_id")
        operation = payload.get("operation", "upsert")

        if not catalog_id:
            logger.warning(
                "ReindexWorker: catalog_metadata_changed event %s has "
                "no catalog_id in payload; skipping", event_id,
            )
            return _DispatchResult(
                event_id=event_id, succeeded=True, should_retry=False,
                errors=["missing catalog_id"],
            )

        if indexer_resolution is None:
            indexer_resolution = self._resolve_indexers_once()

        if indexer_resolution.error is not None:
            # Shared resolution failure across the batch — every event
            # gets the same NACK + error.  A future delivery retries
            # with a freshly-read CatalogRoutingConfig.
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=[indexer_resolution.error],
            )

        indexers = indexer_resolution.indexers
        if not indexers:
            # No INDEX-role drivers configured — the event still
            # deserves an ACK so the outbox doesn't accumulate.  A
            # deploy that later registers Indexers will pick up new
            # events from that point forward; the catch-up backfill
            # (M3.1b) handles anything already ACKed.
            return _DispatchResult(
                event_id=event_id, succeeded=True, should_retry=False,
                errors=[],
            )

        # Hydrate envelope ONCE per event; every Indexer sees the same
        # merged CORE + STAC envelope at this snapshot in time.  A
        # subsequent mutation on the same catalog will produce a new
        # event and a fresh hydration — each Indexer eventually
        # converges to the latest state.
        try:
            envelope = await self._get_catalog_metadata(
                catalog_id, db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "ReindexWorker: hydrate failed for %s: %s — "
                "will retry on next delivery",
                catalog_id, exc,
            )
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=[f"hydrate failed: {exc}"],
            )

        fatal_errors: List[str] = []
        degraded_errors: List[str] = []
        for entry, driver in indexers:
            outcome = await self._dispatch_one(
                entry=entry,
                driver=driver,
                catalog_id=catalog_id,
                envelope=envelope,
                operation=operation,
                db_resource=db_resource,
            )
            if outcome is None:
                # succeeded (or skipped per SLA)
                continue
            is_fatal, message = outcome
            if is_fatal:
                fatal_errors.append(message)
            else:
                degraded_errors.append(message)

        if fatal_errors:
            return _DispatchResult(
                event_id=event_id, succeeded=False, should_retry=True,
                errors=fatal_errors + degraded_errors,
            )
        # No fatal errors — ACK even if some Indexers degraded.
        return _DispatchResult(
            event_id=event_id, succeeded=True, should_retry=False,
            errors=degraded_errors,
        )

    async def _dispatch_one(
        self,
        *,
        entry: OperationDriverEntry,
        driver: CatalogMetadataStore,
        catalog_id: str,
        envelope: Optional[Dict[str, Any]],
        operation: str,
        db_resource: Optional[Any],
    ) -> Optional[Tuple[bool, str]]:
        """Invoke ``driver.upsert_catalog_metadata`` honouring ``entry.sla``.

        Returns ``None`` on success or SLA-permitted skip.  Returns
        ``(is_fatal, message)`` on failure:

        - ``is_fatal=True``  → caller should NACK the event (retry).
        - ``is_fatal=False`` → caller should ACK with degraded errors
          recorded (retry won't help — the SLA said to degrade).
        """
        sla = _resolve_entry_sla(entry, driver)
        timeout_s = (sla.timeout_ms / 1000.0) if sla else None

        async def _invoke() -> None:
            if operation in ("delete", "soft_delete"):
                await driver.delete_catalog_metadata(
                    catalog_id,
                    soft=(operation == "soft_delete"),
                    db_resource=db_resource,
                )
            else:  # upsert / unknown-future → default to upsert
                if envelope is None:
                    # Upsert with no envelope is nonsensical — the
                    # Primary store has the catalog gone.  Treat as
                    # a delete so the Indexer stays in sync.
                    await driver.delete_catalog_metadata(
                        catalog_id, soft=False, db_resource=db_resource,
                    )
                else:
                    await driver.upsert_catalog_metadata(
                        catalog_id, envelope, db_resource=db_resource,
                    )

        try:
            if timeout_s is not None:
                await asyncio.wait_for(_invoke(), timeout=timeout_s)
            else:
                await _invoke()
            return None
        except asyncio.TimeoutError:
            return _apply_sla_policy(
                sla=sla,
                driver_id=entry.driver_id,
                catalog_id=catalog_id,
                reason=f"timeout after {sla.timeout_ms if sla else '?'}ms",
            )
        except Exception as exc:  # noqa: BLE001 — Indexer-side failure
            return _apply_sla_policy(
                sla=sla,
                driver_id=entry.driver_id,
                catalog_id=catalog_id,
                reason=f"driver exception: {exc}",
            )


# ---------------------------------------------------------------------------
# SLA / policy helpers
# ---------------------------------------------------------------------------


def _resolve_entry_sla(
    entry: OperationDriverEntry,
    driver: CatalogMetadataStore,
) -> Optional[DriverSla]:
    """Pick the effective SLA: per-entry override, then class default."""
    if entry.sla is not None:
        return entry.sla
    return getattr(driver, "sla", None)


def _apply_sla_policy(
    *,
    sla: Optional[DriverSla],
    driver_id: str,
    catalog_id: str,
    reason: str,
) -> Optional[Tuple[bool, str]]:
    """Translate SLA ``on_timeout`` into (is_fatal, message) or None.

    - ``skip``    → log debug, return None (treated as success).
    - ``degrade`` → log warning, return ``(False, message)``.  Caller
      ACKs the event but records the degradation.
    - ``fail``    → log error, return ``(True, message)``.  Caller
      NACKs so the event gets re-delivered.
    - No SLA on entry or class → default to ``fail`` (safe default —
      a silent drop would mask consumer-level failures).
    """
    policy = sla.on_timeout if sla else "fail"
    message = f"Indexer {driver_id}@{catalog_id}: {reason}"
    if policy == "skip":
        logger.debug("%s — SLA says skip", message)
        return None
    if policy == "degrade":
        logger.warning("%s — SLA says degrade; ACKing anyway", message)
        return (False, message)
    logger.error("%s — SLA says fail; NACKing for retry", message)
    return (True, message)


# ---------------------------------------------------------------------------
# Indexer resolution — reads from CatalogRoutingConfig's INDEX operation
# ---------------------------------------------------------------------------


def _resolve_catalog_indexers() -> List[
    Tuple[OperationDriverEntry, CatalogMetadataStore]
]:
    """Return the (entry, driver) pairs configured under INDEX for catalogs.

    Reads the live ``CatalogRoutingConfig`` default (no platform
    override support in M3.1 — that's straightforward to add later by
    threading ``catalog_id`` + ``ConfigsProtocol.get_config`` here).
    Filters to driver_ids that are actually registered via
    ``get_protocols(CatalogMetadataStore)``; unregistered entries are
    logged and dropped.
    """
    from dynastore.tools.discovery import get_protocols

    cfg = CatalogRoutingConfig()
    entries: List[OperationDriverEntry] = list(
        cfg.operations.get(Operation.INDEX, [])
    )
    if not entries:
        return []

    driver_index = {
        type(d).__name__: d for d in get_protocols(CatalogMetadataStore)
    }
    resolved: List[Tuple[OperationDriverEntry, CatalogMetadataStore]] = []
    for entry in entries:
        driver = driver_index.get(entry.driver_id)
        if driver is None:
            logger.warning(
                "CatalogRoutingConfig INDEX entry %r is not a registered "
                "CatalogMetadataStore — skipping",
                entry.driver_id,
            )
            continue
        resolved.append((entry, driver))
    return resolved
