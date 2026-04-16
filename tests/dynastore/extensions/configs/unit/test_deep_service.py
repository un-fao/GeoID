from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs.deep_dto import ConfigViewEntry
from dynastore.extensions.configs.deep_service import DeepConfigService


def _default_config_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
    if isinstance(config_cls, str):
        from dynastore.modules.db_config.platform_config_service import (
            resolve_config_class,
        )

        config_cls = resolve_config_class(config_cls)
    return config_cls() if config_cls else None


@pytest.fixture()
def mock_config_service():
    svc = MagicMock()
    svc.list_configs = AsyncMock(return_value={"items": [], "total": 0})
    svc.get_config = AsyncMock(side_effect=_default_config_side_effect)
    return svc


@pytest.mark.asyncio
async def test_get_effective_configs_source_default(mock_config_service):
    svc = DeepConfigService(config_service=mock_config_service)
    configs = await svc._get_effective_configs(catalog_id=None, collection_id=None)
    assert isinstance(configs, dict)
    assert len(configs) > 0
    for entry in configs.values():
        assert isinstance(entry, ConfigViewEntry)
        assert entry.source == "default"


@pytest.mark.asyncio
async def test_get_effective_configs_catalog_source(mock_config_service):
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig

    async def catalog_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
        if isinstance(config_cls, str):
            from dynastore.modules.db_config.platform_config_service import (
                resolve_config_class,
            )

            config_cls = resolve_config_class(config_cls)
        if config_cls == CollectionRoutingConfig and catalog_id and not collection_id:
            return CollectionRoutingConfig(enabled=False)
        return config_cls() if config_cls else None

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and not collection_id:
            return {
                "items": [
                    {"plugin_id": "CollectionRoutingConfig", "config_data": {"enabled": False}}
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    mock_config_service.get_config.side_effect = catalog_side_effect
    mock_config_service.list_configs.side_effect = list_side_effect

    svc = DeepConfigService(config_service=mock_config_service)
    configs = await svc._get_effective_configs(catalog_id="my-catalog", collection_id=None)

    routing_entry = configs.get("CollectionRoutingConfig")
    assert routing_entry is not None
    assert routing_entry.source == "catalog"


@pytest.mark.asyncio
async def test_get_effective_configs_collection_source(mock_config_service):
    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and collection_id:
            return {
                "items": [
                    {"plugin_id": "CollectionRoutingConfig", "config_data": {"enabled": True}}
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    mock_config_service.list_configs.side_effect = list_side_effect

    svc = DeepConfigService(config_service=mock_config_service)
    configs = await svc._get_effective_configs(
        catalog_id="my-catalog", collection_id="landuse"
    )

    routing_entry = configs.get("CollectionRoutingConfig")
    assert routing_entry is not None
    assert routing_entry.source == "collection"


@pytest.mark.asyncio
async def test_resolve_driver_configs_embeds_driver_config(mock_config_service):
    routing_value = {
        "enabled": True,
        "operations": {
            "WRITE": [
                {
                    "driver_id": "CollectionPostgresqlDriver",
                    "on_failure": "fatal",
                    "write_mode": "sync",
                    "hints": [],
                }
            ],
            "READ": [
                {
                    "driver_id": "CollectionPostgresqlDriver",
                    "on_failure": "fatal",
                    "write_mode": "sync",
                    "hints": [],
                }
            ],
        },
        "metadata": {"operations": {}},
    }

    class _FakeDriver:
        pass

    _FakeDriver.__name__ = "CollectionPostgresqlDriver"
    fake_instance = _FakeDriver()

    class _FakeCfg:
        @classmethod
        def class_key(cls):
            return "CollectionPostgresqlDriverConfig"

        def model_dump(self):
            return {"enabled": True}

    with patch(
        "dynastore.extensions.configs.deep_service.DriverRegistry"
    ) as mock_registry, patch(
        "dynastore.extensions.configs.deep_service.resolve_config_class",
        return_value=_FakeCfg,
    ):
        mock_registry.collection_index.return_value = {
            "CollectionPostgresqlDriver": fake_instance
        }
        mock_config_service.get_config = AsyncMock(return_value=_FakeCfg())

        svc = DeepConfigService(config_service=mock_config_service)
        result = await svc._resolve_driver_configs(
            routing_value=routing_value,
            catalog_id="my-catalog",
            collection_id="landuse",
        )

    assert "WRITE" in result
    assert result["WRITE"][0].driver_id == "CollectionPostgresqlDriver"
    assert result["WRITE"][0].on_failure == "fatal"
    assert result["WRITE"][0].config == {"enabled": True}
    assert result["WRITE"][0].config_class_key == "CollectionPostgresqlDriverConfig"


@pytest.mark.asyncio
async def test_resolve_driver_configs_unknown_driver(mock_config_service):
    routing_value = {
        "enabled": True,
        "operations": {
            "WRITE": [
                {
                    "driver_id": "UnknownDriver",
                    "on_failure": "warn",
                    "write_mode": "sync",
                    "hints": [],
                }
            ],
        },
        "metadata": {"operations": {}},
    }

    with patch(
        "dynastore.extensions.configs.deep_service.DriverRegistry"
    ) as mock_registry:
        mock_registry.collection_index.return_value = {}

        svc = DeepConfigService(config_service=mock_config_service)
        result = await svc._resolve_driver_configs(
            routing_value=routing_value,
            catalog_id="my-catalog",
            collection_id=None,
        )

    assert result["WRITE"][0].driver_id == "UnknownDriver"
    assert result["WRITE"][0].config is None
    assert result["WRITE"][0].config_class_key is None
