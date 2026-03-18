
import pytest
import asyncio
from unittest.mock import MagicMock, patch
from dynastore.extensions.assets.assets_service import AssetService
from dynastore.modules.catalog.asset_tasks_spi import AssetTasksSPI
from dynastore.modules.catalog.asset_service import Asset, AssetTypeEnum
from dynastore.tasks import TaskConfig
from dynastore.modules.processes.models import Process

class DummyAssetTask(AssetTasksSPI):
    @staticmethod
    def get_process_definition() -> Process:
        return Process(
            id="dummy", 
            title="Dummy", 
            description="Dummy", 
            version="1.0.0",
            inputs={},
            outputs={}
        )
    
    def __init__(self, app_state=None):
        self.app_state = app_state
    
    async def can_run_on_asset(self, asset: Asset) -> bool:
        return asset.asset_type == AssetTypeEnum.VECTORIAL

    async def run(self, payload):
        return "ok"

@pytest.mark.asyncio
async def test_spi_resolution_non_instantiated():
    # Mock config where instance is None but cls is DummyAssetTask
    dummy_config = TaskConfig(
        cls=DummyAssetTask,
        module_name="dummy",
        name="dummy",
        definition=DummyAssetTask.get_process_definition(),
        instance=None
    )
    
    with patch("dynastore.tasks.get_all_task_configs", return_value={"dummy": dummy_config}):
        asset = MagicMock(spec=Asset)
        asset.asset_id = "123"
        asset.asset_type = AssetTypeEnum.VECTORIAL
        
        # Use AssetService static method with app_state
        available = await AssetService._get_available_tasks_for_asset(asset, app_state=MagicMock())
        
        assert len(available) == 1
        assert available[0].id == "dummy"
        print("\nSUCCESS: Non-instantiated task implementing SPI was correctly identified.")

@pytest.mark.asyncio
async def test_spi_resolution_instantiated():
    instance = DummyAssetTask()
    dummy_config = TaskConfig(
        cls=DummyAssetTask,
        module_name="dummy",
        name="dummy",
        definition=DummyAssetTask.get_process_definition(),
        instance=instance
    )
    
    with patch("dynastore.tasks.get_all_task_configs", return_value={"dummy": dummy_config}):
        asset = MagicMock(spec=Asset)
        asset.asset_id = "123"
        asset.asset_type = AssetTypeEnum.VECTORIAL
        
        # Use AssetService static method with app_state
        available = await AssetService._get_available_tasks_for_asset(asset, app_state=MagicMock())
        
        assert len(available) == 1
        assert available[0].id == "dummy"
        print("SUCCESS: Instantiated task implementing SPI was correctly identified.")

if __name__ == "__main__":
    asyncio.run(test_spi_resolution_non_instantiated())
    asyncio.run(test_spi_resolution_instantiated())
