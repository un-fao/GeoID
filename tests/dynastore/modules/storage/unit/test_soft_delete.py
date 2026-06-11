#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import pytest
from unittest.mock import AsyncMock, patch

from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver


class TestUnifiedSoftDeletePg:
    @pytest.mark.asyncio
    async def test_hard_delete_entities(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        mock_crud.delete_item = AsyncMock(return_value=1)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", ["id1"], soft=False)
            assert count == 1

    @pytest.mark.asyncio
    async def test_soft_delete_entities_raises(self):
        driver = ItemsPostgresqlDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)

    @pytest.mark.asyncio
    async def test_hard_drop_storage(self):
        """Hard drop with db_resource supplied: issues DDL for sidecars + hub."""
        import dynastore.modules.db_config.shared_queries as sq

        driver = ItemsPostgresqlDriver()
        mock_conn = AsyncMock()
        mock_execute = AsyncMock()

        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock,
                return_value="items_hub"
            ),
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock,
                return_value="cat1_schema"
            ),
            patch(
                "dynastore.modules.storage.drivers.pg_sidecars.registry"
                ".SidecarRegistry.get_available_types",
                return_value=["attributes"],
            ),
            patch.object(sq.delete_table_query, "execute", mock_execute),
        ):
            await driver.drop_storage("cat1", "col1", soft=False, db_resource=mock_conn)

        # 1 sidecar + 1 hub
        assert mock_execute.call_count == 2

    @pytest.mark.asyncio
    async def test_soft_drop_storage_logs_and_proceeds(self):
        """soft=True: no DDL issued, no catalog service calls."""
        driver = ItemsPostgresqlDriver()
        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock
            ) as mock_rpt,
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock
            ) as mock_rs,
        ):
            await driver.drop_storage("cat1", "col1", soft=True)
            mock_rpt.assert_not_called()
            mock_rs.assert_not_called()


class TestSoftDeleteNotSupportedError:
    def test_inherits_from_exception(self):
        assert issubclass(SoftDeleteNotSupportedError, Exception)

    def test_message(self):
        err = SoftDeleteNotSupportedError("test message")
        assert str(err) == "test message"
