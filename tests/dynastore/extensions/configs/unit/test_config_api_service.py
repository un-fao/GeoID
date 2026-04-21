from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs.config_api_dto import ConfigEntry
from dynastore.extensions.configs.config_api_service import ConfigApiService


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
    svc = ConfigApiService(config_service=mock_config_service)
    configs = await svc._get_effective_configs(catalog_id=None, collection_id=None)
    assert isinstance(configs, dict)
    assert len(configs) > 0
    for entry in configs.values():
        assert isinstance(entry, ConfigEntry)
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

    svc = ConfigApiService(config_service=mock_config_service)
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

    svc = ConfigApiService(config_service=mock_config_service)
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
                    "driver_id": "ItemsPostgresqlDriver",
                    "on_failure": "fatal",
                    "write_mode": "sync",
                    "hints": [],
                }
            ],
            "READ": [
                {
                    "driver_id": "ItemsPostgresqlDriver",
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

    _FakeDriver.__name__ = "ItemsPostgresqlDriver"
    fake_instance = _FakeDriver()

    class _FakeCfg:
        @classmethod
        def class_key(cls):
            return "ItemsPostgresqlDriverConfig"

        def model_dump(self):
            return {"enabled": True}

    with patch(
        "dynastore.extensions.configs.config_api_service.DriverRegistry"
    ) as mock_registry, patch(
        "dynastore.extensions.configs.config_api_service.resolve_config_class",
        return_value=_FakeCfg,
    ):
        mock_registry.collection_index.return_value = {
            "ItemsPostgresqlDriver": fake_instance
        }
        mock_config_service.get_config = AsyncMock(return_value=_FakeCfg())

        svc = ConfigApiService(config_service=mock_config_service)
        result = await svc._resolve_driver_configs(
            routing_value=routing_value,
            catalog_id="my-catalog",
            collection_id="landuse",
        )

    assert "WRITE" in result
    assert result["WRITE"][0].driver_id == "ItemsPostgresqlDriver"
    assert result["WRITE"][0].on_failure == "fatal"
    assert result["WRITE"][0].config == {"enabled": True}
    assert result["WRITE"][0].config_class_key == "ItemsPostgresqlDriverConfig"


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
        "dynastore.extensions.configs.config_api_service.DriverRegistry"
    ) as mock_registry:
        mock_registry.collection_index.return_value = {}

        svc = ConfigApiService(config_service=mock_config_service)
        result = await svc._resolve_driver_configs(
            routing_value=routing_value,
            catalog_id="my-catalog",
            collection_id=None,
        )

    assert result["WRITE"][0].driver_id == "UnknownDriver"
    assert result["WRITE"][0].config is None
    assert result["WRITE"][0].config_class_key is None


def test_next_link_on_first_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page(
        "http://test/config", "collections", 30, 1, 15, {"depth": 1}
    )
    assert any(link["rel"] == "next" for link in page.links)
    assert not any(link["rel"] == "prev" for link in page.links)
    next_href = next(link["href"] for link in page.links if link["rel"] == "next")
    assert "collections_page=2" in next_href
    assert "depth=1" in next_href


def test_prev_link_on_page_3():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 100, 3, 15, {})
    assert any(link["rel"] == "prev" for link in page.links)
    prev_href = next(link["href"] for link in page.links if link["rel"] == "prev")
    assert "collections_page=2" in prev_href


def test_no_next_on_last_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 10, 1, 15, {})
    assert not any(link["rel"] == "next" for link in page.links)


@pytest.mark.asyncio
async def test_collection_config_depth0_no_categories(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_enrich_routing_configs", new=AsyncMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col", depth=0
        )
    assert response.categories is None


@pytest.mark.asyncio
async def test_collection_config_depth1_has_assets_category(mock_config_service):
    mock_assets = MagicMock()
    mock_assets.list_assets = AsyncMock(return_value=[])
    mock_assets.count_assets = AsyncMock(return_value=0)
    svc = ConfigApiService(
        config_service=mock_config_service, assets_service=mock_assets
    )
    with patch.object(svc, "_get_effective_configs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_enrich_routing_configs", new=AsyncMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col", depth=1
        )
    assert response.categories is not None
    assert "assets" in response.categories
    assert response.categories["assets"].total == 0
    assert response.categories["assets"].items == []


@pytest.mark.asyncio
async def test_catalog_config_depth0_no_categories(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_enrich_routing_configs", new=AsyncMock()):
        response = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c", depth=0
        )
    assert response.categories is None


@pytest.mark.asyncio
async def test_catalog_config_depth1_has_collections(mock_config_service):
    mock_cats = MagicMock()
    mock_cats.list_collections = AsyncMock(return_value=[])
    mock_cats.count_collections = AsyncMock(return_value=0)
    mock_assets = MagicMock()
    mock_assets.list_assets = AsyncMock(return_value=[])
    mock_assets.count_assets = AsyncMock(return_value=0)
    svc = ConfigApiService(
        config_service=mock_config_service,
        catalogs_service=mock_cats,
        assets_service=mock_assets,
    )
    with patch.object(svc, "_get_effective_configs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_enrich_routing_configs", new=AsyncMock()):
        response = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c", depth=1
        )
    assert response.categories is not None
    assert "collections" in response.categories
    assert "assets" in response.categories


@pytest.mark.asyncio
async def test_platform_config_depth0_no_categories(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_enrich_routing_configs", new=AsyncMock()):
        response = await svc.compose_platform_config(base_url="http://test", depth=0)
    assert response.categories is None
    assert response.scope == "platform"
