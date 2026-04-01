import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.drivers.postgresql import PostgresStorageDriver
from dynastore.models.ogc import Feature
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import PostgresCollectionDriverConfig


class TestPostgresStorageDriverMeta:
    def test_driver_id(self):
        driver = PostgresStorageDriver()
        assert driver.driver_id == "postgresql"

    def test_priority(self):
        driver = PostgresStorageDriver()
        assert driver.priority == 10

    def test_capabilities(self):
        driver = PostgresStorageDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SPATIAL_FILTER in driver.capabilities
        assert Capability.SOFT_DELETE in driver.capabilities
        assert Capability.EXPORT in driver.capabilities
        assert Capability.READ_ONLY not in driver.capabilities

    def test_is_available_with_items_protocol(self):
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_gp.return_value = MagicMock()
            driver = PostgresStorageDriver()
            assert driver.is_available() is True

    def test_is_available_without_items_protocol(self):
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_gp.return_value = None
            driver = PostgresStorageDriver()
            assert driver.is_available() is False


class TestWriteEntities:
    @pytest.mark.asyncio
    async def test_write_single_feature(self):
        driver = PostgresStorageDriver()
        mock_crud = AsyncMock()
        mock_crud.upsert = AsyncMock(return_value=[MagicMock(spec=Feature)])

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            feature = MagicMock(spec=Feature)
            result = await driver.write_entities("cat1", "col1", feature)
            mock_crud.upsert.assert_called_once()
            assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_write_returns_list_from_single(self):
        driver = PostgresStorageDriver()
        mock_crud = AsyncMock()
        single_result = MagicMock(spec=Feature)
        mock_crud.upsert = AsyncMock(return_value=single_result)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            result = await driver.write_entities("cat1", "col1", MagicMock(spec=Feature))
            assert isinstance(result, list)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_write_list_returns_list(self):
        driver = PostgresStorageDriver()
        mock_crud = AsyncMock()
        items = [MagicMock(spec=Feature), MagicMock(spec=Feature)]
        mock_crud.upsert = AsyncMock(return_value=items)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            result = await driver.write_entities("cat1", "col1", items)
            assert isinstance(result, list)
            assert len(result) == 2


class TestReadEntities:
    @pytest.mark.asyncio
    async def test_read_with_default_query_request(self):
        driver = PostgresStorageDriver()
        mock_query = AsyncMock()
        mock_feature = MagicMock(spec=Feature)

        async def mock_items():
            yield mock_feature

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities("cat1", "col1"):
                results.append(f)

            assert len(results) == 1
            assert results[0] is mock_feature
            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert isinstance(request_arg, QueryRequest)
            assert request_arg.limit == 100
            assert request_arg.offset == 0

    @pytest.mark.asyncio
    async def test_read_with_entity_ids(self):
        driver = PostgresStorageDriver()
        mock_query = AsyncMock()

        async def mock_items():
            yield MagicMock(spec=Feature)

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities(
                "cat1", "col1", entity_ids=["id1", "id2"]
            ):
                results.append(f)

            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert request_arg.item_ids == ["id1", "id2"]

    @pytest.mark.asyncio
    async def test_read_with_custom_request(self):
        driver = PostgresStorageDriver()
        mock_query = AsyncMock()

        async def mock_items():
            yield MagicMock(spec=Feature)

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        custom_request = QueryRequest(limit=50, offset=10)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities(
                "cat1", "col1", request=custom_request
            ):
                results.append(f)

            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert request_arg.limit == 50
            assert request_arg.offset == 10


class TestDeleteEntities:
    @pytest.mark.asyncio
    async def test_delete_entities(self):
        driver = PostgresStorageDriver()
        mock_crud = AsyncMock()
        mock_crud.delete_item = AsyncMock(return_value=1)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", ["id1", "id2", "id3"])
            assert count == 3
            assert mock_crud.delete_item.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_empty_list(self):
        driver = PostgresStorageDriver()
        mock_crud = AsyncMock()

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", [])
            assert count == 0

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        driver = PostgresStorageDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)


class TestLifecycleMethods:
    @pytest.mark.asyncio
    async def test_ensure_storage_noop_without_collection(self):
        """ensure_storage with no collection_id is a no-op."""
        driver = PostgresStorageDriver()
        # Should return immediately without touching the DB
        with patch.object(driver, "_resolve_schema", new_callable=AsyncMock) as mock_resolve:
            await driver.ensure_storage("cat1")
            mock_resolve.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_storage_raises_without_protocol(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_gp.return_value = None
            with pytest.raises(RuntimeError, match="CatalogsProtocol not available"):
                await driver.ensure_storage("cat1", "col1")

    @pytest.mark.asyncio
    async def test_drop_storage_collection(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_gp.return_value = mock_catalogs
            await driver.drop_storage("cat1", "col1")
            mock_catalogs.delete_collection.assert_called_once_with("cat1", "col1")

    @pytest.mark.asyncio
    async def test_drop_storage_catalog(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_gp.return_value = mock_catalogs
            await driver.drop_storage("cat1")
            mock_catalogs.delete_catalog.assert_called_once_with("cat1")

    @pytest.mark.asyncio
    async def test_drop_storage_soft(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_gp.return_value = mock_catalogs
            await driver.drop_storage("cat1", "col1", soft=True)
            mock_catalogs.delete_collection.assert_called_once_with("cat1", "col1")

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = PostgresStorageDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")


class TestResolveStorageLocation:
    @pytest.mark.asyncio
    async def test_resolve_with_collection(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")
            mock_collections = AsyncMock()
            mock_collections.resolve_physical_table = AsyncMock(return_value="my_table")

            def side_effect(proto):
                name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                if "Catalogs" in name:
                    return mock_catalogs
                if "Collections" in name:
                    return mock_collections
                return None

            mock_gp.side_effect = side_effect
            loc = await driver.resolve_storage_location("cat1", "col1")
            assert isinstance(loc, PostgresCollectionDriverConfig)
            assert loc.physical_schema == "my_schema"
            assert loc.physical_table == "my_table"

    @pytest.mark.asyncio
    async def test_resolve_without_collection(self):
        driver = PostgresStorageDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")

            def side_effect(proto):
                name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                if "Catalogs" in name:
                    return mock_catalogs
                return None

            mock_gp.side_effect = side_effect
            loc = await driver.resolve_storage_location("cat1")
            assert isinstance(loc, PostgresCollectionDriverConfig)
            assert loc.physical_schema == "my_schema"
            assert loc.physical_table is None
