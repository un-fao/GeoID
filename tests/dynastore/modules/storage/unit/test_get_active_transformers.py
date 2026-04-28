"""get_active_transformers — ordered chain resolution by class name.

Routing-config driver_id strings under operations[TRANSFORM] are resolved
to live EntityTransformProtocol instances by class name, matching the
convention used by ``_self_register_indexers_into`` /
``_self_register_searchers_into``.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from dynastore.modules.storage.routing_config import (
    OperationDriverEntry,
    get_active_transformers,
)


# Distinct class names so routing entries can address them individually.
class TransformerA:
    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


class TransformerB:
    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


def _ops_with_transform(*driver_ids):
    return {
        "TRANSFORM": [OperationDriverEntry(driver_id=did) for did in driver_ids],
    }


@pytest.mark.asyncio
async def test_resolves_class_names_to_instances_in_order():
    a, b = TransformerA(), TransformerB()
    fake_ops = _ops_with_transform("TransformerA", "TransformerB")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake_ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[a, b],
    ):
        chain = await get_active_transformers("c", entity="item", collection_id="col")
    assert chain == [a, b]


@pytest.mark.asyncio
async def test_preserves_routing_order_not_discovery_order():
    """routing-config order wins, even if discovery returns a different order."""
    a, b = TransformerA(), TransformerB()
    fake_ops = _ops_with_transform("TransformerB", "TransformerA")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake_ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[a, b],
    ):
        chain = await get_active_transformers("c", entity="item", collection_id="col")
    assert chain == [b, a]


@pytest.mark.asyncio
async def test_skips_unknown_class_name_with_debug_log(caplog):
    a = TransformerA()
    fake_ops = _ops_with_transform("TransformerA", "TransformerMissing")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake_ops,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[a],
    ), caplog.at_level(logging.DEBUG, logger="dynastore.modules.storage.routing_config"):
        chain = await get_active_transformers("c", entity="item", collection_id="col")
    assert chain == [a]
    assert any("TransformerMissing" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_returns_empty_when_no_TRANSFORM_entries():
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value={},
    ):
        chain = await get_active_transformers("c", entity="item", collection_id="col")
    assert chain == []


@pytest.mark.asyncio
async def test_works_for_each_entity_kind():
    a = TransformerA()
    fake_ops = _ops_with_transform("TransformerA")
    for entity in ("item", "collection", "catalog", "asset"):
        with patch(
            "dynastore.modules.storage.routing_config._resolve_entity_operations",
            return_value=fake_ops,
        ), patch(
            "dynastore.tools.discovery.get_protocols",
            return_value=[a],
        ):
            chain = await get_active_transformers("c", entity=entity, collection_id="col")
        assert chain == [a], f"failed for entity={entity}"
