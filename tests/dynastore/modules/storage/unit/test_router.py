import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.router import (
    ResolvedDriver,
    get_asset_driver,
    get_driver,
    resolve_drivers,
    _build_asset_driver_index,
    _build_collection_driver_index,
)
from dynastore.modules.storage.routing_config import (
    AssetRoutingPluginConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    RoutingPluginConfig,
    WriteMode,
)


def _make_routing(operations: dict) -> RoutingPluginConfig:
    """Build a RoutingPluginConfig from {operation: [(driver_id, hints, policy), ...]}."""
    ops = {}
    for op, entries in operations.items():
        ops[op] = [
            OperationDriverEntry(
                driver_id=e[0],
                hints=e[1] if len(e) > 1 else set(),
                on_failure=e[2] if len(e) > 2 else FailurePolicy.FATAL,
            )
            for e in entries
        ]
    return RoutingPluginConfig(operations=ops)


def _mock_configs_protocol(routing_config):
    """Return a mock ConfigsProtocol that returns the given routing config."""
    mock = MagicMock()
    mock.get_config = AsyncMock(return_value=routing_config)
    return mock


def _mock_driver(driver_id: str):
    """Create a mock driver whose class name equals ``driver_id``.

    The router builds the driver index via ``type(driver).__name__`` after the
    ``driver_id`` field was removed in favour of class-name routing keys, so
    mocks must carry that name in their type.
    """
    cls = type(driver_id, (MagicMock,), {})
    return cls()


# ---------------------------------------------------------------------------
# _build_collection_driver_index / _build_asset_driver_index
# ---------------------------------------------------------------------------


class TestBuildDriverIndex:
    def test_collection_driver_index(self):
        d1 = _mock_driver("postgresql")
        d2 = _mock_driver("elasticsearch")
        with patch(
            "dynastore.modules.storage.router._build_collection_driver_index",
            wraps=_build_collection_driver_index,
        ):
            with patch("dynastore.tools.discovery.get_protocols", return_value=[d1, d2]):
                index = _build_collection_driver_index()
                assert index == {"postgresql": d1, "elasticsearch": d2}

    def test_asset_driver_index(self):
        d1 = _mock_driver("postgresql")
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1]):
            index = _build_asset_driver_index()
            assert index == {"postgresql": d1}

    def test_empty_registry(self):
        with patch("dynastore.tools.discovery.get_protocols", return_value=[]):
            assert _build_collection_driver_index() == {}
            assert _build_asset_driver_index() == {}

    def test_duplicate_driver_id_last_wins(self):
        d1 = _mock_driver("postgresql")
        d2 = _mock_driver("postgresql")
        with patch("dynastore.tools.discovery.get_protocols", return_value=[d1, d2]):
            index = _build_collection_driver_index()
            assert index["postgresql"] is d2


# ---------------------------------------------------------------------------
# resolve_drivers — cached resolution via ConfigsProtocol
# ---------------------------------------------------------------------------


class TestResolveDrivers:
    @pytest.mark.asyncio
    async def test_write_returns_all_drivers(self):
        routing = _make_routing({
            Operation.WRITE: [("postgresql", set()), ("elasticsearch", set())],
        })
        mock_configs = _mock_configs_protocol(routing)
        pg = _mock_driver("postgresql")
        es = _mock_driver("elasticsearch")

        with (
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached.__wrapped__",
                  new=AsyncMock(return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC), ("elasticsearch", FailurePolicy.FATAL, WriteMode.SYNC)])) if False else
            patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs),
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg, "elasticsearch": es}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC), ("elasticsearch", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers("WRITE", "cat1", "col1")
            assert len(result) == 2
            assert result[0].driver is pg
            assert result[1].driver is es
            assert result[0].on_failure == FailurePolicy.FATAL

    @pytest.mark.asyncio
    async def test_read_returns_single_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers("READ", "cat1", "col1")
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_missing_driver_is_skipped(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("nonexistent", FailurePolicy.FATAL, WriteMode.SYNC), ("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers("READ", "cat1")
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_empty_resolution(self):
        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            result = await resolve_drivers("READ", "cat1")
            assert result == []

    @pytest.mark.asyncio
    async def test_asset_routing_uses_asset_driver_index(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_asset_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers(
                "READ", "cat1", routing_plugin_cls=AssetRoutingPluginConfig,
            )
            assert len(result) == 1
            assert result[0].driver is pg

    @pytest.mark.asyncio
    async def test_failure_policy_preserved(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.WARN, WriteMode.SYNC)])),
        ):
            result = await resolve_drivers("WRITE", "cat1")
            assert result[0].on_failure == FailurePolicy.WARN


# ---------------------------------------------------------------------------
# get_driver / get_asset_driver — convenience wrappers
# ---------------------------------------------------------------------------


class TestGetDriver:
    @pytest.mark.asyncio
    async def test_returns_first_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_driver("READ", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_raises_on_empty_resolution(self):
        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ValueError, match="No collection driver found"):
                await get_driver("READ", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_write_operation(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_collection_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_driver("WRITE", "cat1", "col1")
            assert result is pg


class TestGetAssetDriver:
    @pytest.mark.asyncio
    async def test_returns_first_asset_driver(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_asset_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver("READ", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_raises_on_empty_resolution(self):
        with (
            patch("dynastore.modules.storage.router._build_asset_driver_index", return_value={}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(return_value=[])),
        ):
            with pytest.raises(ValueError, match="No asset driver found"):
                await get_asset_driver("READ", "cat1", "col1")

    @pytest.mark.asyncio
    async def test_write_operation(self):
        pg = _mock_driver("postgresql")

        with (
            patch("dynastore.modules.storage.router._build_asset_driver_index", return_value={"postgresql": pg}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("postgresql", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver("WRITE", "cat1", "col1")
            assert result is pg

    @pytest.mark.asyncio
    async def test_search_with_hint(self):
        es = _mock_driver("elasticsearch")

        with (
            patch("dynastore.modules.storage.router._build_asset_driver_index", return_value={"elasticsearch": es}),
            patch("dynastore.modules.storage.router._resolve_driver_ids_cached", new=AsyncMock(
                return_value=[("elasticsearch", FailurePolicy.FATAL, WriteMode.SYNC)])),
        ):
            result = await get_asset_driver("SEARCH", "cat1", "col1", hint="search")
            assert result is es


# ---------------------------------------------------------------------------
# ResolvedDriver
# ---------------------------------------------------------------------------


class TestResolvedDriver:
    def test_driver_id_property(self):
        d = _mock_driver("postgresql")
        rd = ResolvedDriver(driver=d)
        assert rd.driver_id == "postgresql"

    def test_default_failure_policy(self):
        rd = ResolvedDriver(driver=_mock_driver("pg"))
        assert rd.on_failure == FailurePolicy.FATAL

    def test_custom_failure_policy(self):
        rd = ResolvedDriver(driver=_mock_driver("es"), on_failure=FailurePolicy.WARN)
        assert rd.on_failure == FailurePolicy.WARN

    def test_driver_id_falls_back_to_class_name(self):
        rd = ResolvedDriver(driver=object())
        assert rd.driver_id == "object"
