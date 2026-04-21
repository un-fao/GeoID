"""Unit tests for the M2.3b catalog-metadata router.

Covers:

- Read fan-out merges CORE + STAC dicts.
- Read returns ``None`` only when every driver returned ``None``.
- Read degrades: a driver that raises is logged at WARNING and omitted
  from the merged envelope (other drivers still contribute).
- Write fan-out calls every driver in order, sharing the same
  ``db_resource``.
- Delete fan-out honours ``soft``.
- Default driver resolution pulls from ``get_protocols(CatalogMetadataStore)``.
- Absence of registered drivers → router no-ops everything (no raise).
- CatalogRoutingConfig defaults (the other half of M2.3b) point at the
  canonical M2.1 driver names under both WRITE and READ.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# READ fan-out + merge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_metadata_merges_core_and_stac():
    """Router awaits both drivers concurrently and merges their dicts."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(return_value={"title": {"en": "T"}})
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value={"stac_version": "1.1.0"})

    result = await get_catalog_metadata("cat-42", drivers=[core, stac])

    assert result == {"title": {"en": "T"}, "stac_version": "1.1.0"}
    core.get_catalog_metadata.assert_awaited_once_with(
        "cat-42", context=None, db_resource=None,
    )
    stac.get_catalog_metadata.assert_awaited_once_with(
        "cat-42", context=None, db_resource=None,
    )


@pytest.mark.asyncio
async def test_get_catalog_metadata_returns_none_when_all_drivers_return_none():
    """Envelope absence is a domain-wide signal — not a partial dict."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        get_catalog_metadata,
    )

    d1 = MagicMock()
    d1.get_catalog_metadata = AsyncMock(return_value=None)
    d2 = MagicMock()
    d2.get_catalog_metadata = AsyncMock(return_value=None)

    assert await get_catalog_metadata("cat", drivers=[d1, d2]) is None


@pytest.mark.asyncio
async def test_get_catalog_metadata_returns_partial_when_one_driver_has_data():
    """One domain populated + one empty → dict of the populated domain."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(return_value={"title": "T"})
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value=None)

    assert await get_catalog_metadata("cat", drivers=[core, stac]) == {"title": "T"}


@pytest.mark.asyncio
async def test_get_catalog_metadata_degrades_on_driver_exception(caplog):
    """Driver raising on READ is logged at WARNING; merge keeps the rest."""
    from dynastore.modules.catalog import catalog_metadata_router as mod
    from dynastore.modules.catalog.catalog_metadata_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("pg connection reset"),
    )
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value={"stac_version": "1.1.0"})

    with caplog.at_level(logging.WARNING, logger=mod.__name__):
        result = await get_catalog_metadata("cat-42", drivers=[core, stac])

    assert result == {"stac_version": "1.1.0"}
    assert any(
        "Catalog-metadata READ failed" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_get_catalog_metadata_noop_without_registered_drivers():
    """Empty driver list → None with no crash and no warning per call."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        get_catalog_metadata,
    )

    # Explicit empty list — caller opted in to "no drivers".
    assert await get_catalog_metadata("cat", drivers=[]) is None


# ---------------------------------------------------------------------------
# WRITE fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_calls_every_driver_sequentially():
    """Sequential upsert — shared db_resource, deterministic order."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        upsert_catalog_metadata,
    )

    order: list[str] = []

    def _make(name):
        m = MagicMock()
        async def _upsert(catalog_id, metadata, *, db_resource=None):
            order.append(name)
        m.upsert_catalog_metadata = AsyncMock(side_effect=_upsert)
        return m

    core = _make("core")
    stac = _make("stac")
    payload = {"title": {"en": "T"}, "stac_version": "1.1.0"}
    fake_conn = MagicMock()

    await upsert_catalog_metadata(
        "cat-42", payload, db_resource=fake_conn, drivers=[core, stac],
    )

    assert order == ["core", "stac"]
    core.upsert_catalog_metadata.assert_awaited_once_with(
        "cat-42", payload, db_resource=fake_conn,
    )
    stac.upsert_catalog_metadata.assert_awaited_once_with(
        "cat-42", payload, db_resource=fake_conn,
    )


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_bubbles_driver_exceptions():
    """WRITE failures MUST propagate — dual-write needs all-or-nothing."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        upsert_catalog_metadata,
    )

    core = MagicMock()
    core.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("FK violation"),
    )

    with pytest.raises(RuntimeError, match="FK violation"):
        await upsert_catalog_metadata("cat", {}, drivers=[core])


# ---------------------------------------------------------------------------
# DELETE fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_catalog_metadata_forwards_soft_flag():
    """``soft=True`` reaches every driver."""
    from dynastore.modules.catalog.catalog_metadata_router import (
        delete_catalog_metadata,
    )

    core = MagicMock()
    core.delete_catalog_metadata = AsyncMock()
    stac = MagicMock()
    stac.delete_catalog_metadata = AsyncMock()

    await delete_catalog_metadata("cat", soft=True, drivers=[core, stac])

    core.delete_catalog_metadata.assert_awaited_once_with(
        "cat", soft=True, db_resource=None,
    )
    stac.delete_catalog_metadata.assert_awaited_once_with(
        "cat", soft=True, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Default driver resolution
# ---------------------------------------------------------------------------


def test_resolve_catalog_metadata_drivers_goes_through_get_protocols():
    """Default path resolves every registered CatalogMetadataStore."""
    from dynastore.modules.catalog import catalog_metadata_router as mod

    fake_driver = MagicMock()
    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[fake_driver],
    ) as gp:
        result = mod._resolve_catalog_metadata_drivers()
    assert result == [fake_driver]
    gp.assert_called_once()
    # Called with the CatalogMetadataStore protocol — the one arg to get_protocols.
    from dynastore.models.protocols.metadata_driver import CatalogMetadataStore
    assert gp.call_args.args[0] is CatalogMetadataStore


# ---------------------------------------------------------------------------
# CatalogRoutingConfig defaults (partner commit)
# ---------------------------------------------------------------------------


def test_catalog_routing_config_defaults_use_canonical_names():
    """After M2.1/M2.3b the defaults must reference the canonical drivers.

    Drift here silently breaks ``_validate_routing_entries`` on any
    deployment that loads ``CatalogRoutingConfig``'s defaults without
    an explicit platform override.
    """
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )

    cfg = CatalogRoutingConfig()
    # Both WRITE and READ fan out across the two domain-scoped primaries.
    write_ids = {e.driver_id for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_id for e in cfg.operations[Operation.READ]}
    assert write_ids == {"CatalogCorePostgresqlDriver", "CatalogStacPostgresqlDriver"}
    assert read_ids == {"CatalogCorePostgresqlDriver", "CatalogStacPostgresqlDriver"}
