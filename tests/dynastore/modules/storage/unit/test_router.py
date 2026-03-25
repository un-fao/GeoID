import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.router import _resolve_driver_id, _build_driver_index
from dynastore.modules.storage.config import StorageRoutingConfig


class TestResolveDriverId:
    """Test the pure function that picks driver_id from routing config."""

    def _make_routing(self, primary="postgresql", read_drivers=None, secondary=None):
        return StorageRoutingConfig(
            primary_driver=primary,
            read_drivers=read_drivers or {},
            secondary_drivers=secondary or [],
        )

    def test_write_always_returns_primary(self):
        routing = self._make_routing(
            primary="postgresql",
            read_drivers={"search": "elasticsearch"},
        )
        assert _resolve_driver_id(routing, hint="search", write=True) == "postgresql"

    def test_write_ignores_hint(self):
        routing = self._make_routing(
            primary="postgresql",
            read_drivers={"search": "elasticsearch", "default": "duckdb"},
        )
        assert _resolve_driver_id(routing, hint="analytics", write=True) == "postgresql"

    def test_read_uses_hint(self):
        routing = self._make_routing(
            primary="postgresql",
            read_drivers={"search": "elasticsearch"},
        )
        assert _resolve_driver_id(routing, hint="search", write=False) == "elasticsearch"

    def test_read_falls_back_to_default_hint(self):
        routing = self._make_routing(
            primary="postgresql",
            read_drivers={"default": "duckdb"},
        )
        assert _resolve_driver_id(routing, hint="analytics", write=False) == "duckdb"

    def test_read_falls_back_to_primary(self):
        routing = self._make_routing(primary="postgresql")
        assert _resolve_driver_id(routing, hint="search", write=False) == "postgresql"

    def test_read_hint_match_beats_default(self):
        routing = self._make_routing(
            primary="postgresql",
            read_drivers={"search": "elasticsearch", "default": "duckdb"},
        )
        assert _resolve_driver_id(routing, hint="search", write=False) == "elasticsearch"

    def test_read_no_match_uses_default_then_primary(self):
        routing = self._make_routing(
            primary="iceberg",
            read_drivers={"search": "elasticsearch"},
        )
        assert _resolve_driver_id(routing, hint="analytics", write=False) == "iceberg"

    def test_empty_read_drivers_uses_primary(self):
        routing = self._make_routing(primary="neo4j")
        assert _resolve_driver_id(routing, hint="default", write=False) == "neo4j"

    def test_all_documented_hints(self):
        hints = ["default", "search", "features", "graph", "analytics", "cache"]
        read_drivers = {h: f"driver_{h}" for h in hints}
        routing = self._make_routing(primary="postgresql", read_drivers=read_drivers)
        for hint in hints:
            assert _resolve_driver_id(routing, hint=hint, write=False) == f"driver_{hint}"


class TestBuildDriverIndex:
    def test_returns_dict(self):
        with patch("dynastore.tools.discovery.get_protocols") as mock_gp:
            driver1 = MagicMock()
            driver1.driver_id = "postgresql"
            driver2 = MagicMock()
            driver2.driver_id = "elasticsearch"
            mock_gp.return_value = [driver1, driver2]

            index = _build_driver_index()
            assert "postgresql" in index
            assert "elasticsearch" in index
            assert index["postgresql"] is driver1
            assert index["elasticsearch"] is driver2

    def test_empty_registry(self):
        with patch("dynastore.tools.discovery.get_protocols") as mock_gp:
            mock_gp.return_value = []
            index = _build_driver_index()
            assert index == {}

    def test_duplicate_driver_id_last_wins(self):
        with patch("dynastore.tools.discovery.get_protocols") as mock_gp:
            driver1 = MagicMock()
            driver1.driver_id = "postgresql"
            driver2 = MagicMock()
            driver2.driver_id = "postgresql"
            mock_gp.return_value = [driver1, driver2]

            index = _build_driver_index()
            assert index["postgresql"] is driver2


class TestGetDriver:
    @pytest.mark.asyncio
    async def test_get_driver_returns_matched_driver(self):
        from dynastore.modules.storage.router import get_driver

        mock_driver = MagicMock()
        mock_driver.driver_id = "postgresql"
        mock_driver.capabilities = frozenset()

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_cached",
                new_callable=AsyncMock,
                return_value="postgresql",
            ),
            patch(
                "dynastore.tools.discovery.get_protocols",
                return_value=[mock_driver],
            ),
        ):
            result = await get_driver("cat1", "col1")
            assert result is mock_driver

    @pytest.mark.asyncio
    async def test_get_driver_raises_on_missing_driver(self):
        from dynastore.modules.storage.router import get_driver

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_cached",
                new_callable=AsyncMock,
                return_value="nonexistent",
            ),
            patch(
                "dynastore.tools.discovery.get_protocols",
                return_value=[MagicMock(driver_id="postgresql")],
            ),
        ):
            with pytest.raises(ValueError, match="nonexistent"):
                await get_driver("cat1", "col1")

    @pytest.mark.asyncio
    async def test_get_driver_raises_on_read_only_write(self):
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.errors import ReadOnlyDriverError
        from dynastore.models.protocols.storage_driver import Capability

        mock_driver = MagicMock()
        mock_driver.driver_id = "duckdb"
        mock_driver.capabilities = frozenset({Capability.READ_ONLY})

        with (
            patch(
                "dynastore.modules.storage.router._resolve_driver_cached",
                new_callable=AsyncMock,
                return_value="duckdb",
            ),
            patch(
                "dynastore.tools.discovery.get_protocols",
                return_value=[mock_driver],
            ),
        ):
            with pytest.raises(ReadOnlyDriverError):
                await get_driver("cat1", "col1", write=True)
