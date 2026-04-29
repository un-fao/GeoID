"""Regression test — router falls back to model defaults when stored config has no entries.

Phase H deploy 4 (image :860, 2026-04-29) surfaced a regression where
both `ingestion` (collection routing READ) and `gdal` (asset routing
READ) failed with `ValueError: No collection/asset driver found for
operation='READ'`. The catalog API service had stored CollectionRoutingConfig /
AssetRoutingConfig rows with empty `operations: {}` (left over from a
parallel-session config-refactor PR that rewrote stored config shape).

Pre-fix behaviour: `_resolve_driver_ids_cached` reads
`routing_config.operations.get(operation, [])` → empty → caller raises
`ValueError`. Worse than having no stored row at all (which would have
let `configs.get_config` return `cls()` and fire `default_factory`).

Post-fix behaviour: when entries are empty, fall back to
`routing_plugin_cls().operations.get(operation, [])` — same defaults
that fire when no row exists. This restores the canonical PostgreSQL
driver as a safety net.
"""
from unittest.mock import patch

import pytest

from dynastore.modules.storage.router import _resolve_driver_ids_cached
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CollectionRoutingConfig,
    Operation,
    OperationDriverEntry,
)


class _MockConfigsProtocol:
    """Returns whatever stored_config we hand it, ignoring scope kwargs."""

    def __init__(self, stored_config):
        self._cfg = stored_config

    async def get_config(self, cls, **kwargs):
        return self._cfg


def _patch_configs(stored_config):
    """Patch get_protocol(ConfigsProtocol) to return our mock."""
    return patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=_MockConfigsProtocol(stored_config),
    )


@pytest.mark.asyncio
async def test_collection_routing_falls_back_to_defaults_when_empty() -> None:
    """Stored CollectionRoutingConfig with empty operations should fall
    back to default_factory entries (READ → items_postgresql_driver)."""
    empty_config = CollectionRoutingConfig(operations={})

    # Cache is keyed on (cls, catalog_id, collection_id, op, hint) —
    # use a unique id so each test gets a fresh resolution.
    with _patch_configs(empty_config):
        result = await _resolve_driver_ids_cached(
            CollectionRoutingConfig,
            "test_cat_empty_collection_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, (
        "Expected fallback to default_factory entries (READ → "
        "items_postgresql_driver) when stored config has empty operations. "
        "Got: empty list."
    )
    assert result[0][0] == "items_postgresql_driver", (
        f"Expected first driver_id to be items_postgresql_driver from "
        f"CollectionRoutingConfig.default_factory, got {result[0][0]!r}."
    )


@pytest.mark.asyncio
async def test_asset_routing_falls_back_to_defaults_when_empty() -> None:
    """Same fallback for AssetRoutingConfig — defaults to
    asset_postgresql_driver. Unblocks gdal asset READ when the stored
    AssetRoutingConfig row has empty operations."""
    empty_config = AssetRoutingConfig(operations={})

    with _patch_configs(empty_config):
        result = await _resolve_driver_ids_cached(
            AssetRoutingConfig,
            "test_cat_empty_asset_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, "Expected fallback for AssetRoutingConfig empty operations."
    assert result[0][0] == "asset_postgresql_driver", (
        f"Expected first driver_id to be asset_postgresql_driver, "
        f"got {result[0][0]!r}."
    )


@pytest.mark.asyncio
async def test_explicit_entries_are_not_overridden() -> None:
    """Negative case: when stored config HAS explicit entries, the
    fallback must not fire — we must respect operator-set routing.
    The fallback only fires for the NO-entries case."""
    explicit_config = CollectionRoutingConfig(
        operations={
            Operation.READ: [OperationDriverEntry(driver_id="explicitly_chosen_driver")],
        }
    )

    with _patch_configs(explicit_config):
        result = await _resolve_driver_ids_cached(
            CollectionRoutingConfig,
            "test_cat_explicit_routing",
            "test_col",
            Operation.READ,
            None,
        )

    assert result, "Expected explicit entries to be returned."
    assert result[0][0] == "explicitly_chosen_driver", (
        f"Fallback fired even when explicit entries were present. "
        f"Got {result[0][0]!r} instead of explicitly_chosen_driver."
    )
