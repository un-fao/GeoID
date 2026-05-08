"""Tile metadata resolution must route via Hint.TILES.

Without the hint, default READ routing returns the first matching driver —
typically Elasticsearch in the FAO config — and ES has no schema/table
identifiers, so tile rendering silently falls back to "No valid collections
found". The fix: tiles_module.get_tile_resolution_params passes
hint=Hint.TILES so routing returns a tile-capable driver (PG today).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.modules.tiles import tiles_module


def test_postgresql_driver_advertises_tiles_hint():
    """PG must declare Hint.TILES so routing prefers it for tile reads."""
    assert Hint.TILES in ItemsPostgresqlDriver.supported_hints


@pytest.mark.asyncio
async def test_get_tile_resolution_params_routes_with_tiles_hint():
    """get_tile_resolution_params must call get_driver with hint=Hint.TILES."""
    tiles_module.get_tile_resolution_params.cache_clear()

    fake_driver = AsyncMock()
    fake_driver.location = AsyncMock(
        return_value=SimpleNamespace(
            identifiers={"schema": "cat1_data", "table": "col1"}
        )
    )

    fake_catalogs = SimpleNamespace(
        configs=AsyncMock(get_config=AsyncMock(return_value=None),
                          set_config=AsyncMock(return_value=None)),
        get_collection_config=AsyncMock(return_value=None),
    )

    get_driver_mock = AsyncMock(return_value=fake_driver)

    with (
        patch.object(tiles_module, "_get_engine", return_value=AsyncMock()),
        patch("dynastore.modules.tiles.tiles_module.managed_transaction") as mt,
        patch("dynastore.modules.tiles.tiles_module.get_protocol",
              return_value=fake_catalogs),
        patch("dynastore.modules.storage.router.get_driver", new=get_driver_mock),
        patch.object(tiles_module, "get_collection_source_srid",
                     new=AsyncMock(return_value=4326)),
    ):
        # async context manager stub for managed_transaction
        mt.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mt.return_value.__aexit__ = AsyncMock(return_value=None)

        await tiles_module.get_tile_resolution_params("cat1", "col1")

    # Assert the load-bearing call signature: hint=Hint.TILES
    assert get_driver_mock.await_count == 1
    _args, kwargs = get_driver_mock.await_args
    assert kwargs.get("hint") == Hint.TILES, (
        f"get_tile_resolution_params must pass hint=Hint.TILES; "
        f"got kwargs={kwargs}"
    )
