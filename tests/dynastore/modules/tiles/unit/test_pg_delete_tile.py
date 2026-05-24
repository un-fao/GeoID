"""Unit tests for ``TilePGPreseedStorage.delete_tile`` (#1292 mark-stale).

Mock-based (mirrors ``tests/dynastore/extensions/tiles/test_tiles_db_unit.py``):
verifies the per-tile delete issues a DELETE, is idempotent when the preseed
table is absent, and reports failure (rather than raising) on a real error.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_storage():
    from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

    storage = TilePGPreseedStorage.__new__(TilePGPreseedStorage)
    storage.engine = MagicMock()
    storage._get_schema = AsyncMock(return_value="s_cat")  # type: ignore[attr-defined]
    return storage


@pytest.mark.asyncio
async def test_delete_tile_issues_delete():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL, patch(
        "dynastore.modules.tiles.tiles_module.cache_invalidate"
    ):
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(return_value=1)

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")

        assert ok is True
        # A DELETE statement was constructed.
        sql = MockDQL.call_args.args[0]
        assert "DELETE FROM" in sql and "preseeded_tiles" in sql


@pytest.mark.asyncio
async def test_delete_tile_idempotent_when_table_absent():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL:
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        # Simulate "relation does not exist" (42P01).
        MockDQL.return_value.execute = AsyncMock(
            side_effect=Exception("relation does not exist (42P01)")
        )

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")
        # Missing table → nothing to invalidate → idempotent success.
        assert ok is True


@pytest.mark.asyncio
async def test_delete_tile_reports_failure_without_raising():
    storage = _make_storage()
    with patch(
        "dynastore.modules.tiles.tiles_module.managed_transaction"
    ) as mtx, patch(
        "dynastore.modules.tiles.tiles_module.DQLQuery"
    ) as MockDQL:
        mtx.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        mtx.return_value.__aexit__ = AsyncMock(return_value=False)
        MockDQL.return_value.execute = AsyncMock(
            side_effect=Exception("connection reset")
        )

        ok = await storage.delete_tile("cat", "col", "WebMercatorQuad", 5, 1, 2, "mvt")
        # A genuine error → False (let the drain retry), never raises.
        assert ok is False
