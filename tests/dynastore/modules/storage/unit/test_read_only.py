import pytest

from dynastore.models.protocols.storage_driver import Capability, ReadOnlyDriverMixin
from dynastore.modules.storage.errors import ReadOnlyDriverError


class ReadOnlyDriver(ReadOnlyDriverMixin):
    driver_id = "test_readonly"
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
