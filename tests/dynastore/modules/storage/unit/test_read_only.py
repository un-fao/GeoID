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

from dynastore.models.protocols.storage_driver import Capability, ReadOnlyDriverMixin
from dynastore.modules.storage.errors import ReadOnlyDriverError


class ReadOnlyDriver(ReadOnlyDriverMixin):
    driver_ref = "test_readonly"
    capabilities = frozenset({Capability.READ_ONLY, Capability.STREAMING})


class TestReadOnlyDriverMixin:
    @pytest.mark.asyncio
    async def test_write_entities_raises(self):
        driver = ReadOnlyDriver()
        with pytest.raises(ReadOnlyDriverError):
            await driver.write_entities("cat1", "col1", [])

    @pytest.mark.asyncio
    async def test_delete_entities_raises(self):
        driver = ReadOnlyDriver()
        with pytest.raises(ReadOnlyDriverError):
            await driver.delete_entities("cat1", "col1", ["id1"])

    @pytest.mark.asyncio
    async def test_drop_storage_raises(self):
        driver = ReadOnlyDriver()
        with pytest.raises(ReadOnlyDriverError):
            await driver.drop_storage("cat1", "col1")

    def test_capabilities_include_read_only(self):
        driver = ReadOnlyDriver()
        assert Capability.READ_ONLY in driver.capabilities
