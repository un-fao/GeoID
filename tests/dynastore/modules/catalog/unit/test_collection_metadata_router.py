"""Unit tests for the collection-metadata router (M2.5 hard cut).

Covers the same shape as ``test_catalog_metadata_router.py`` but for
the collection tier:

- Read fan-out merges CORE + STAC slices.
- Read returns ``None`` only when every driver returned ``None``.
- A driver raising on READ is logged + omitted; other drivers still
  contribute.
- Write fan-out calls every driver sequentially, sharing ``db_resource``.
- Delete fan-out honours ``soft``.
- Search picks a capability-matching driver.
- Absence of registered drivers → router no-ops (no raise).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.collection_metadata_router import (
    delete_collection_metadata,
    get_collection_metadata,
    search_collection_metadata,
    upsert_collection_metadata,
)


# ---------------------------------------------------------------------------
# Read fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_merges_core_and_stac_slices():
    core = MagicMock()
    core.get_metadata = AsyncMock(return_value={"title": {"en": "T"}, "description": None})
    stac = MagicMock()
    stac.get_metadata = AsyncMock(return_value={"stac_version": "1.1.0", "links": []})

    merged = await get_collection_metadata(
        "cat", "col", drivers=[core, stac],
    )

    assert merged is not None
    assert merged["title"] == {"en": "T"}
    assert merged["stac_version"] == "1.1.0"
    assert merged["links"] == []


@pytest.mark.asyncio
async def test_read_returns_none_when_every_driver_returns_none():
    a = MagicMock()
    a.get_metadata = AsyncMock(return_value=None)
    b = MagicMock()
    b.get_metadata = AsyncMock(return_value=None)

    merged = await get_collection_metadata("cat", "col", drivers=[a, b])
    assert merged is None


@pytest.mark.asyncio
async def test_read_degrades_on_driver_exception(caplog):
    good = MagicMock()
    good.get_metadata = AsyncMock(return_value={"title": {"en": "OK"}})
    bad = MagicMock()
    bad.get_metadata = AsyncMock(side_effect=RuntimeError("boom"))

    with caplog.at_level("WARNING"):
        merged = await get_collection_metadata("cat", "col", drivers=[good, bad])

    assert merged == {"title": {"en": "OK"}}
    assert any("Collection-metadata READ failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Write fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_calls_every_driver_with_shared_db_resource():
    a = MagicMock()
    a.upsert_metadata = AsyncMock(return_value=None)
    b = MagicMock()
    b.upsert_metadata = AsyncMock(return_value=None)
    conn = MagicMock(name="shared_conn")

    await upsert_collection_metadata(
        "cat", "col", {"title": {"en": "T"}}, db_resource=conn, drivers=[a, b],
    )

    a.upsert_metadata.assert_awaited_once_with(
        "cat", "col", {"title": {"en": "T"}}, db_resource=conn,
    )
    b.upsert_metadata.assert_awaited_once_with(
        "cat", "col", {"title": {"en": "T"}}, db_resource=conn,
    )


# ---------------------------------------------------------------------------
# Delete fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_soft_flag_propagates_to_every_driver():
    a = MagicMock()
    a.delete_metadata = AsyncMock(return_value=None)
    b = MagicMock()
    b.delete_metadata = AsyncMock(return_value=None)

    await delete_collection_metadata(
        "cat", "col", soft=True, drivers=[a, b],
    )

    a.delete_metadata.assert_awaited_once_with(
        "cat", "col", soft=True, db_resource=None,
    )
    b.delete_metadata.assert_awaited_once_with(
        "cat", "col", soft=True, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Search capability routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_picks_driver_with_matching_capability():
    from dynastore.models.protocols.metadata_driver import MetadataCapability

    no_search = MagicMock()
    no_search.capabilities = frozenset()
    no_search.search_metadata = AsyncMock(return_value=([], 0))
    yes_search = MagicMock()
    yes_search.capabilities = frozenset({MetadataCapability.SEARCH})
    yes_search.search_metadata = AsyncMock(return_value=([{"id": "c1"}], 1))

    rows, total = await search_collection_metadata(
        "cat", q="hello", drivers=[no_search, yes_search],
    )

    assert rows == [{"id": "c1"}]
    assert total == 1
    # The first driver lacks SEARCH so must NOT have been called.
    no_search.search_metadata.assert_not_awaited()
    yes_search.search_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_search_without_required_capability_falls_back_to_first_driver():
    a = MagicMock()
    a.capabilities = frozenset()
    a.search_metadata = AsyncMock(return_value=([], 0))

    rows, total = await search_collection_metadata("cat", drivers=[a])

    # No query shape imposes required capabilities, so router calls
    # the first (and only) driver directly.
    a.search_metadata.assert_awaited_once()
    assert rows == []
    assert total == 0


# ---------------------------------------------------------------------------
# No registered drivers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_no_drivers_registered_returns_none(caplog):
    with caplog.at_level("ERROR"):
        merged = await get_collection_metadata("cat", "col", drivers=[])
    assert merged is None


@pytest.mark.asyncio
async def test_write_no_drivers_registered_is_noop():
    # No drivers → no-op.  Should not raise.
    await upsert_collection_metadata("cat", "col", {"title": "T"}, drivers=[])


@pytest.mark.asyncio
async def test_delete_no_drivers_registered_is_noop():
    await delete_collection_metadata("cat", "col", drivers=[])


@pytest.mark.asyncio
async def test_search_no_drivers_registered_returns_empty():
    rows, total = await search_collection_metadata("cat", q="foo", drivers=[])
    assert rows == []
    assert total == 0
