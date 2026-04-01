import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.errors import (
    ReadOnlyDriverError,
    SoftDeleteNotSupportedError,
)
from dynastore.modules.storage.driver_config import DuckDbCollectionDriverConfig


class TestDuckDBDriverMeta:
    def test_driver_id(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver.driver_id == "duckdb"

    def test_priority(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver.priority == 30

    def test_capabilities(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert Capability.READ in driver.capabilities
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SPATIAL_FILTER in driver.capabilities
        assert Capability.EXPORT in driver.capabilities

    def test_is_available_without_duckdb(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        with patch("dynastore.modules.storage.drivers.duckdb._duckdb_available", return_value=False):
            driver = DuckDBStorageDriver()
            assert driver.is_available() is False


class TestDuckDBFormatReaders:
    def test_reader_func_parquet(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver._reader_func("parquet") == "read_parquet"

    def test_reader_func_csv(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver._reader_func("csv") == "read_csv_auto"

    def test_reader_func_json(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver._reader_func("json") == "read_json_auto"

    def test_reader_func_unknown_defaults_to_parquet(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        assert driver._reader_func("unknown") == "read_parquet"


class TestDuckDBWritability:
    def test_is_writable_false_by_default(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(format="parquet", path="/data/f.parquet")
        assert driver._is_writable(loc) is False

    def test_is_writable_true_with_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(
            format="parquet",
            path="/data/f.parquet",
            write_path="/data/w.db",
            write_format="sqlite",
        )
        assert driver._is_writable(loc) is True


class TestDuckDBWriteEntities:
    @pytest.mark.asyncio
    async def test_write_raises_read_only_without_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(format="parquet", path="/data/f.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(ReadOnlyDriverError):
                await driver.write_entities("cat1", "col1", {"id": "1"})

    @pytest.mark.asyncio
    async def test_write_raises_read_only_without_location(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            with pytest.raises(ReadOnlyDriverError):
                await driver.write_entities("cat1", "col1", {"id": "1"})


class TestDuckDBDeleteEntities:
    @pytest.mark.asyncio
    async def test_delete_raises_read_only_without_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(format="parquet", path="/data/f.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(ReadOnlyDriverError):
                await driver.delete_entities("cat1", "col1", ["id1"])

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(
            format="parquet", path="/data/f.parquet",
            write_path="/data/w.db", write_format="sqlite",
        )
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(SoftDeleteNotSupportedError):
                await driver.delete_entities("cat1", "col1", ["id1"], soft=True)


class TestDuckDBDropStorage:
    @pytest.mark.asyncio
    async def test_soft_drop_raises(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.drop_storage("cat1", "col1", soft=True)


class TestDuckDBResolveLocation:
    @pytest.mark.asyncio
    async def test_resolve_returns_config(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(format="csv", path="/data/f.csv")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            result = await driver.resolve_storage_location("cat1", "col1")
            assert isinstance(result, DuckDbCollectionDriverConfig)
            assert result.format == "csv"

    @pytest.mark.asyncio
    async def test_resolve_returns_default_when_missing(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            result = await driver.resolve_storage_location("cat1")
            assert isinstance(result, DuckDbCollectionDriverConfig)
            assert result.format == "parquet"


class TestDuckDBEnsureStorage:
    @pytest.mark.asyncio
    async def test_ensure_storage_no_location(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            await driver.ensure_storage("cat1", "col1")  # should not raise

    @pytest.mark.asyncio
    async def test_ensure_storage_file_not_found(self):
        from dynastore.modules.storage.drivers.duckdb import DuckDBStorageDriver
        driver = DuckDBStorageDriver()
        loc = DuckDbCollectionDriverConfig(format="parquet", path="/nonexistent/file.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with patch.object(driver, "_get_conn", return_value=MagicMock()):
                with pytest.raises(FileNotFoundError):
                    await driver.ensure_storage("cat1", "col1")
