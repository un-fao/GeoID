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
Catalog-tier metadata router (M2.3b of the role-based driver refactor).

Parallels :mod:`dynastore.modules.catalog.metadata_router` (collection
tier) but scoped to :class:`CatalogMetadataStore` implementations.
Three operations:

- :func:`get_catalog_metadata`      — fan-out READ that merges CORE +
  STAC envelopes into a single dict; later domains slot in here
  without changes at the call site.
- :func:`upsert_catalog_metadata`   — fan-out WRITE that hands the same
  payload to every configured driver (each driver ``_filter_payload``s
  down to its own domain's columns).
- :func:`delete_catalog_metadata`   — fan-out DELETE.

Resolution policy
-----------------

Resolves drivers by scanning every registered
:class:`CatalogMetadataStore` via ``get_protocols()``.  Routing-
config-driven resolution (per-catalog overrides) will land when the
``CatalogRoutingConfig`` apply-handler tests settle in M2.4+; for now
the router uses the discovered driver set directly, which matches the
M2.3a backfill's assumption that both CORE and STAC tables are always
populated for catalogs that need them.

Failure policy
--------------

Driver-level failures bubble up on WRITE / DELETE (data correctness
trumps availability).  On READ, a single domain's missing envelope is
treated as "this catalog has no data in that domain" — the merged dict
simply skips the absent keys.  A domain driver that raises on READ is
logged at WARNING and omitted from the merge; the caller sees a
partial envelope rather than a 5xx.  This matches the pre-refactor
behaviour where a legacy catalog with NULL STAC columns produced an
envelope missing those keys.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from dynastore.models.protocols.metadata_driver import CatalogMetadataStore

logger = logging.getLogger(__name__)


def _resolve_catalog_metadata_drivers() -> List[CatalogMetadataStore]:
    """Return the live :class:`CatalogMetadataStore` drivers registered via entry-points.

    Delegates to :func:`dynastore.tools.discovery.get_protocols` so the
    router picks up any driver module loaded by the current scope —
    PG Primaries in the default deployment, an ES indexer if the
    ``elasticsearch`` scope is loaded, etc.  Drivers the entry-point
    layer hasn't loaded are silently absent — no import-time coupling
    between this router and the backends it orchestrates.
    """
    from dynastore.tools.discovery import get_protocols

    drivers = list(get_protocols(CatalogMetadataStore))
    if not drivers:
        logger.warning(
            "No CatalogMetadataStore drivers registered; catalog-metadata "
            "router will no-op all operations.  Did the scope include "
            "module_metadata_postgresql?"
        )
    return drivers


async def get_catalog_metadata(
    catalog_id: str,
    *,
    context: Optional[Dict[str, Any]] = None,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogMetadataStore]] = None,
) -> Optional[Dict[str, Any]]:
    """Merged catalog metadata across all registered domain drivers.

    Each driver owns a slice of the envelope (CORE → title/description/…,
    STAC → stac_version/conforms_to/…).  This function awaits every
    driver concurrently via :func:`asyncio.gather` and merges the
    returned dicts.  Last-domain-wins on duplicate keys — not expected
    for domain-scoped drivers, but defined for safety.

    Returns ``None`` only when every driver returned ``None`` (i.e. the
    catalog has no metadata in any domain).  Any non-``None`` driver
    response yields a dict (possibly with just a subset of keys).

    ``drivers`` is injectable so callers with a pinned routing config
    can pass a filtered subset; default resolution discovers every
    registered driver.
    """
    drivers = drivers if drivers is not None else _resolve_catalog_metadata_drivers()
    if not drivers:
        return None

    async def _safe_get(d: CatalogMetadataStore) -> Optional[Dict[str, Any]]:
        try:
            return await d.get_catalog_metadata(
                catalog_id, context=context, db_resource=db_resource,
            )
        except Exception as exc:  # noqa: BLE001 — degrade, don't fail
            logger.warning(
                "Catalog-metadata READ failed for %s on %s: %s — "
                "omitting from merged envelope",
                type(d).__name__, catalog_id, exc,
            )
            return None

    results = await asyncio.gather(*[_safe_get(d) for d in drivers])

    merged: Dict[str, Any] = {}
    any_found = False
    for result in results:
        if result is None:
            continue
        any_found = True
        merged.update(result)
    return merged if any_found else None


async def upsert_catalog_metadata(
    catalog_id: str,
    metadata: Dict[str, Any],
    *,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogMetadataStore]] = None,
) -> None:
    """Fan-out WRITE across every registered catalog-metadata driver.

    Each driver receives the same ``metadata`` dict; the driver's
    internal ``_filter_payload`` strips keys outside its own domain.
    A caller therefore supplies the full envelope once — no need to
    pre-partition by domain.

    Sequential execution (not ``asyncio.gather``) to keep the driver
    order predictable inside the outer transaction.  The PG CORE and
    STAC drivers share a connection (``db_resource``) so running them
    in parallel over the same connection would cause race conditions
    on the underlying asyncpg cursor.  If a future backend genuinely
    supports per-driver connections, parallelisation can be opt-in.

    M3.0 — ``catalog_metadata_changed`` event emission
    --------------------------------------------------

    After the driver fan-out completes, emit one
    ``catalog_metadata_changed`` event per driver-domain touched.  The
    event is the single signal INDEX / BACKUP consumers
    (``ReindexWorker`` + future export endpoint) listen for to pick
    up mutations on the canonical store.  Emission shares the caller's
    ``db_resource`` so the event write lives in the same transaction
    as the metadata write — transactional-outbox semantics: if the
    enclosing transaction rolls back, the event disappears with it,
    and no consumer wastes work on a never-committed mutation.

    Best-effort: an emission failure is logged but does NOT roll back
    the successful driver writes (the authoritative store already
    has the update; a missing event just means slower propagation
    until the consumer's backfill catches up).
    """
    drivers = drivers if drivers is not None else _resolve_catalog_metadata_drivers()
    for driver in drivers:
        await driver.upsert_catalog_metadata(
            catalog_id, metadata, db_resource=db_resource,
        )
    await _emit_catalog_metadata_changed(
        catalog_id=catalog_id,
        drivers=drivers,
        operation="upsert",
        db_resource=db_resource,
    )


async def delete_catalog_metadata(
    catalog_id: str,
    *,
    soft: bool = False,
    db_resource: Optional[Any] = None,
    drivers: Optional[List[CatalogMetadataStore]] = None,
) -> None:
    """Fan-out DELETE across every registered catalog-metadata driver.

    ``soft=True`` pushes a tombstone via ``extra_metadata._deleted_at``
    on drivers that advertise ``SOFT_DELETE``; drivers that don't
    support soft-delete semantics (STAC driver has no extra_metadata
    column) quietly fall through to hard-delete via the base-class
    implementation.  That asymmetry is acceptable for M2.3b — M2.4's
    read flip will pull STAC via the same router, and a hard-deleted
    STAC row is indistinguishable from "never had STAC".
    """
    drivers = drivers if drivers is not None else _resolve_catalog_metadata_drivers()
    for driver in drivers:
        await driver.delete_catalog_metadata(
            catalog_id, soft=soft, db_resource=db_resource,
        )
    await _emit_catalog_metadata_changed(
        catalog_id=catalog_id,
        drivers=drivers,
        operation="soft_delete" if soft else "delete",
        db_resource=db_resource,
    )


# ---------------------------------------------------------------------------
# Event-emission helper (M3.0)
# ---------------------------------------------------------------------------


async def _emit_catalog_metadata_changed(
    *,
    catalog_id: str,
    drivers: List[CatalogMetadataStore],
    operation: str,
    db_resource: Optional[Any],
) -> None:
    """Emit one ``catalog_metadata_changed`` event per driver-domain touched.

    One event per domain rather than one per mutation because the
    ReindexWorker shards on ``(catalog_id, domain)`` — issuing a
    dedicated row per domain keeps each consumer's claim surface
    tight (no N-way contention over a single PLATFORM event).

    Domain is read from the driver's ``domain`` ClassVar
    (``MetadataDomain.CORE`` / ``STAC``).  Drivers without a
    ``domain`` attribute (future 3rd-party) default to ``CORE`` so
    the event still lands.

    Errors here are logged, not raised — the metadata write already
    succeeded and the caller's mutation intent is satisfied.  A
    missing event means INDEX/BACKUP propagation lags until the
    consumer runs its own catch-up pass.
    """
    from dynastore.modules.catalog.event_service import (
        CatalogEventType, emit_event,
    )

    emitted_domains: set[str] = set()
    for driver in drivers:
        domain_value = getattr(
            getattr(driver, "domain", None), "value", "CORE",
        )
        if domain_value in emitted_domains:
            continue  # de-dup within this call
        emitted_domains.add(domain_value)
        try:
            await emit_event(
                CatalogEventType.CATALOG_METADATA_CHANGED,
                catalog_id=catalog_id,
                db_resource=db_resource,
                payload={
                    "catalog_id": catalog_id,
                    "domain": domain_value,
                    "operation": operation,
                },
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.warning(
                "catalog_metadata_changed event emission failed for "
                "%s / %s: %s — INDEX / BACKUP consumers will pick "
                "this up via their backfill pass",
                catalog_id, domain_value, exc,
            )
