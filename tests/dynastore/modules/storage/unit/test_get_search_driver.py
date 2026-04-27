"""get_search_driver — single-driver semantics with hint override.

Default = first entry in operations[SEARCH]. ``driver_hint`` overrides
when present in the list; otherwise warning + fallback to default.
``None`` returned when no SEARCH entries exist.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from dynastore.modules.storage.routing_config import (
    OperationDriverEntry,
    get_search_driver,
)


def _ops_with_search(*driver_ids):
    return {
        "SEARCH": [OperationDriverEntry(driver_id=did) for did in driver_ids],
    }


@pytest.mark.asyncio
async def test_returns_first_driver_when_no_hint():
    fake = _ops_with_search("EsItemsDriver", "PgItemsDriver")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_search_driver("c", entity="item", collection_id="col")
    assert result == "EsItemsDriver"


@pytest.mark.asyncio
async def test_hint_overrides_default_when_in_list():
    fake = _ops_with_search("EsItemsDriver", "PgItemsDriver")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_search_driver(
            "c", entity="item", collection_id="col", driver_hint="PgItemsDriver",
        )
    assert result == "PgItemsDriver"


@pytest.mark.asyncio
async def test_hint_not_in_list_falls_back_to_default_with_warning(caplog):
    fake = _ops_with_search("EsItemsDriver", "PgItemsDriver")
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.storage.routing_config"):
        result = await get_search_driver(
            "c", entity="item", collection_id="col", driver_hint="UnknownDriver",
        )
    assert result == "EsItemsDriver"
    assert any("UnknownDriver" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_returns_none_when_no_SEARCH_entries():
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value={},
    ):
        result = await get_search_driver("c", entity="item", collection_id="col")
    assert result is None


@pytest.mark.asyncio
async def test_works_for_each_entity_kind():
    fake = _ops_with_search("EsDriver")
    for entity in ("item", "collection", "catalog", "asset"):
        with patch(
            "dynastore.modules.storage.routing_config._resolve_entity_operations",
            return_value=fake,
        ):
            result = await get_search_driver("c", entity=entity, collection_id="col")
        assert result == "EsDriver", f"failed for entity={entity}"
