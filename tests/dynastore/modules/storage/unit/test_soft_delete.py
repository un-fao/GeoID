import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.drivers.postgresql import CollectionPostgresqlDriver


class TestUnifiedSoftDeletePg:
    @pytest.mark.asyncio
    async def test_hard_delete_entities(self):
        driver = CollectionPostgresqlDriver()
        mock_crud = AsyncMock()
        mock_crud.delete_item = AsyncMock(return_value=1)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", ["id1"], soft=False)
            assert count == 1

    @pytest.mark.asyncio
    async def test_soft_delete_entities_raises(self):
        driver = CollectionPostgresqlDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)

    @pytest.mark.asyncio
    async def test_hard_drop_storage(self):
        driver = CollectionPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_gp.return_value = mock_catalogs
            await driver.drop_storage("cat1", "col1", soft=False)
            mock_catalogs.delete_collection.assert_called_once()

    @pytest.mark.asyncio
    async def test_soft_drop_storage_logs_and_proceeds(self):
        driver = CollectionPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_gp.return_value = mock_catalogs
            await driver.drop_storage("cat1", "col1", soft=True)
            mock_catalogs.delete_collection.assert_called_once()


class TestSoftDeleteNotSupportedError:
    def test_inherits_from_exception(self):
        assert issubclass(SoftDeleteNotSupportedError, Exception)

    def test_message(self):
        err = SoftDeleteNotSupportedError("test message")
        assert str(err) == "test message"
