from unittest.mock import AsyncMock, MagicMock
import pytest

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix


@pytest.mark.asyncio
async def test_loads_platform_enabled_flags():
    configs_svc = MagicMock()
    configs_svc.get_config = AsyncMock(side_effect=lambda cls, **kw:
        MagicMock(enabled=False) if "disabled" in cls.__name__.lower() else MagicMock(enabled=True))
    configs_svc.list_catalog_overrides = AsyncMock(return_value=[])

    class TilesCfg:
        __name__ = "TilesConfig"

    class FeaturesDisabledCfg:
        __name__ = "FeaturesDisabledCfg"

    matrix = ExposureMatrix(
        configs_svc,
        togglable_extensions=frozenset({"tiles", "features"}),
        plugin_class_by_extension={"tiles": TilesCfg, "features": FeaturesDisabledCfg},
        ttl_seconds=30,
    )
    snap = await matrix.get()
    assert snap.platform["tiles"] is True
    assert snap.platform["features"] is False


@pytest.mark.asyncio
async def test_cache_ttl_reuses_snapshot():
    configs_svc = MagicMock()
    configs_svc.get_config = AsyncMock(return_value=MagicMock(enabled=True))
    configs_svc.list_catalog_overrides = AsyncMock(return_value=[])

    class TilesCfg:
        __name__ = "TilesConfig"

    matrix = ExposureMatrix(
        configs_svc,
        togglable_extensions=frozenset({"tiles"}),
        plugin_class_by_extension={"tiles": TilesCfg},
        ttl_seconds=60,
    )
    s1 = await matrix.get()
    s2 = await matrix.get()
    assert s1 is s2
    assert configs_svc.get_config.call_count == 1


@pytest.mark.asyncio
async def test_invalidate_forces_reload():
    configs_svc = MagicMock()
    configs_svc.get_config = AsyncMock(return_value=MagicMock(enabled=True))
    configs_svc.list_catalog_overrides = AsyncMock(return_value=[])

    class TilesCfg:
        __name__ = "TilesConfig"

    matrix = ExposureMatrix(
        configs_svc,
        togglable_extensions=frozenset({"tiles"}),
        plugin_class_by_extension={"tiles": TilesCfg},
        ttl_seconds=60,
    )
    await matrix.get()
    matrix.invalidate()
    await matrix.get()
    assert configs_svc.get_config.call_count == 2
