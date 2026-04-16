"""Unit tests for metadata router READ+WRITE resolution and ops defaults."""

import pytest
from unittest.mock import patch

from dynastore.models.protocols.metadata_driver import MetadataCapability
from dynastore.modules.catalog import metadata_router
from dynastore.modules.catalog.metadata_router import (
    _DEFAULT_WRITE_DRIVERS,
    _PREFERRED_DRIVER_ORDER,
    resolve_metadata_drivers,
)
from dynastore.modules.db_config.exceptions import ConfigResolutionError
from dynastore.modules.storage.routing_config import Operation


class _FakeDriver:
    """Minimal CollectionMetadataStore stand-in for unit tests."""

    capabilities = frozenset({MetadataCapability.READ, MetadataCapability.WRITE})

    def __init__(self, name: str, available: bool = True):
        self._name = name
        self._available = available
        self.__class__ = type(name, (_FakeDriver,), {})

    async def is_available(self) -> bool:
        return self._available


def _patch_driver_index(drivers: list):
    return patch.object(
        metadata_router,
        "_build_metadata_driver_index",
        return_value={type(d).__name__: d for d in drivers},
    )


def _patch_ids_exact(driver_ids_by_op: dict):
    """Replace ``_resolve_metadata_driver_ids`` with a fixed mapping."""

    async def _fake(catalog_id, operation):
        return list(driver_ids_by_op.get(operation, []))

    return patch.object(
        metadata_router, "_resolve_metadata_driver_ids", side_effect=_fake
    )


class TestResolveMetadataDriverIdsDefaults:
    """Exercise the real ``_resolve_metadata_driver_ids`` with no config."""

    @pytest.mark.asyncio
    async def test_read_falls_back_to_preferred_order(self):
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=None
        ):
            ids = await metadata_router._resolve_metadata_driver_ids(
                "cat", Operation.READ
            )
        assert ids == list(_PREFERRED_DRIVER_ORDER)

    @pytest.mark.asyncio
    async def test_write_falls_back_to_pg_primary(self):
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=None
        ):
            ids = await metadata_router._resolve_metadata_driver_ids(
                "cat", Operation.WRITE
            )
        assert ids == list(_DEFAULT_WRITE_DRIVERS)
        assert ids == ["MetadataPostgresqlDriver"]


class TestResolveMetadataDriversRead:
    @pytest.mark.asyncio
    async def test_returns_in_configured_order(self):
        pg = _FakeDriver("MetadataPostgresqlDriver")
        es = _FakeDriver("MetadataElasticsearchDriver")

        with _patch_driver_index([pg, es]), _patch_ids_exact(
            {Operation.READ: ["MetadataElasticsearchDriver", "MetadataPostgresqlDriver"]}
        ):
            result = await resolve_metadata_drivers("cat", Operation.READ)

        names = [type(d).__name__ for d in result]
        assert names == ["MetadataElasticsearchDriver", "MetadataPostgresqlDriver"]

    @pytest.mark.asyncio
    async def test_skips_unavailable_driver(self):
        pg = _FakeDriver("MetadataPostgresqlDriver", available=True)
        es = _FakeDriver("MetadataElasticsearchDriver", available=False)

        with _patch_driver_index([pg, es]), _patch_ids_exact(
            {Operation.READ: ["MetadataElasticsearchDriver", "MetadataPostgresqlDriver"]}
        ):
            result = await resolve_metadata_drivers("cat", Operation.READ)

        names = [type(d).__name__ for d in result]
        assert names == ["MetadataPostgresqlDriver"]

    @pytest.mark.asyncio
    async def test_empty_read_returns_empty_list_not_raises(self):
        """READ with no available driver must NOT raise — it emits a warning
        and returns ``[]``. The 404 is the caller's decision on the resource."""
        with _patch_driver_index([]), _patch_ids_exact(
            {Operation.READ: ["MetadataPostgresqlDriver"]}
        ):
            result = await resolve_metadata_drivers("cat", Operation.READ)
        assert result == []


class TestResolveMetadataDriversWrite:
    @pytest.mark.asyncio
    async def test_returns_pg_by_default(self):
        pg = _FakeDriver("MetadataPostgresqlDriver")
        es = _FakeDriver("MetadataElasticsearchDriver")

        with _patch_driver_index([pg, es]), _patch_ids_exact(
            {Operation.WRITE: ["MetadataPostgresqlDriver"]}
        ):
            result = await resolve_metadata_drivers("cat", Operation.WRITE)

        names = [type(d).__name__ for d in result]
        assert names == ["MetadataPostgresqlDriver"]

    @pytest.mark.asyncio
    async def test_honours_explicit_write_config(self):
        pg = _FakeDriver("MetadataPostgresqlDriver")
        es = _FakeDriver("MetadataElasticsearchDriver")

        with _patch_driver_index([pg, es]), _patch_ids_exact(
            {Operation.WRITE: ["MetadataPostgresqlDriver", "MetadataElasticsearchDriver"]}
        ):
            result = await resolve_metadata_drivers("cat", Operation.WRITE)

        names = [type(d).__name__ for d in result]
        assert names == ["MetadataPostgresqlDriver", "MetadataElasticsearchDriver"]

    @pytest.mark.asyncio
    async def test_empty_write_raises_config_resolution_error(self):
        """WRITE must never resolve empty — surface a ConfigResolutionError."""
        with _patch_driver_index([]), _patch_ids_exact(
            {Operation.WRITE: ["MetadataPostgresqlDriver"]}
        ):
            with pytest.raises(ConfigResolutionError) as excinfo:
                await resolve_metadata_drivers("cat", Operation.WRITE)

        err = excinfo.value
        assert err.missing_key.endswith("operations[WRITE]")
        assert "platform" in err.scope_tried
        assert err.hint

    @pytest.mark.asyncio
    async def test_write_raises_when_configured_driver_missing(self):
        """If config names a driver but it is not registered, WRITE must raise."""
        with _patch_driver_index([]), _patch_ids_exact(
            {Operation.WRITE: ["NotInstalledDriver"]}
        ):
            with pytest.raises(ConfigResolutionError) as excinfo:
                await resolve_metadata_drivers("cat", Operation.WRITE)

        assert "NotInstalledDriver" in str(excinfo.value)
