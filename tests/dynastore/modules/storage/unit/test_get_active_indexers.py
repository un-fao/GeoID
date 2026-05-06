"""get_active_indexers — multi-driver fan-out driver_ref resolution.

Mocks ``_resolve_entity_operations`` so the test exercises the helper's
projection logic in isolation, without bringing up ConfigsProtocol.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.modules.storage.routing_config import (
    OperationDriverEntry,
    get_active_indexers,
)


def _ops(**by_op):
    """Build a fake operations dict from keyword pairs of OP -> [driver_ids]."""
    return {
        op: [OperationDriverEntry(driver_ref=did) for did in dids]
        for op, dids in by_op.items()
    }


@pytest.mark.asyncio
async def test_returns_all_INDEX_driver_ids():
    fake = _ops(INDEX=["es_items_driver", "pg_items_driver"], SEARCH=["es_items_driver"])
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == {"es_items_driver", "pg_items_driver"}


@pytest.mark.asyncio
async def test_returns_empty_set_when_no_INDEX_entries():
    fake = _ops(SEARCH=["es_items_driver"])  # No INDEX entry
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == set()


@pytest.mark.asyncio
async def test_works_for_each_entity_kind():
    """Helper is entity-agnostic — same shape for item / collection / catalog / asset."""
    fake = _ops(INDEX=["es_driver"])
    for entity in ("item", "collection", "catalog", "asset"):
        with patch(
            "dynastore.modules.storage.routing_config._resolve_entity_operations",
            return_value=fake,
        ):
            result = await get_active_indexers("c", entity=entity, collection_id="col")
        assert result == {"es_driver"}, f"failed for entity={entity}"


@pytest.mark.asyncio
async def test_dedupes_via_set_semantics():
    """Same driver_ref listed twice in INDEX collapses to one entry."""
    fake = _ops(INDEX=["es_items_driver", "es_items_driver"])
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == {"es_items_driver"}
