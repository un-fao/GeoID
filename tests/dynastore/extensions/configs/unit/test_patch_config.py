"""Unit tests for PATCH /configs/config — ConfigApiService.patch_config."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict


def _make_config_service(stored: Dict[str, Any]):
    """Build a mock ConfigsProtocol that returns stored config values."""
    svc = MagicMock()

    async def _get_config(cls, catalog_id=None, collection_id=None, **_):
        return cls()  # always return defaults

    async def _set_config(cls, value, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored[key] = value
        return value

    async def _delete_config(cls, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored.pop(key, None)

    async def _list_configs(catalog_id=None, collection_id=None, limit=10, offset=0, **_):
        items = [
            {"plugin_id": k[0], "config_data": v.model_dump() if hasattr(v, "model_dump") else {}}
            for k, v in stored.items()
            if k[1] == catalog_id and k[2] == collection_id
        ]
        return {"items": items, "total": len(items)}

    svc.get_config = AsyncMock(side_effect=_get_config)
    svc.set_config = AsyncMock(side_effect=_set_config)
    svc.delete_config = AsyncMock(side_effect=_delete_config)
    svc.list_configs = AsyncMock(side_effect=_list_configs)
    return svc


@pytest.mark.asyncio
async def test_patch_platform_updates_multiple_configs():
    """PATCH with multiple keys updates each config independently."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    result = await api.patch_config(
        catalog_id=None,
        body={
            "TilesConfig": {"enabled": False},
            "FeaturesPluginConfig": {"enabled": True},
        },
    )

    assert "updated" in result
    assert "TilesConfig" in result["updated"]
    assert "FeaturesPluginConfig" in result["updated"]

    # Verify set_config was called twice
    assert config_svc.set_config.call_count == 2


@pytest.mark.asyncio
async def test_patch_null_deletes_config():
    """PATCH with null value calls delete_config for that key."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    result = await api.patch_config(
        catalog_id="my-catalog",
        body={"TilesConfig": None},
    )

    assert "TilesConfig" in result["updated"]
    config_svc.delete_config.assert_called_once()


@pytest.mark.asyncio
async def test_patch_unknown_plugin_raises_value_error():
    """PATCH with unknown plugin_id raises ValueError before any write."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    with pytest.raises(ValueError, match="Unknown plugin_id"):
        await api.patch_config(
            catalog_id=None,
            body={"NonExistentConfig": {"enabled": False}},
        )

    # No writes should have occurred
    config_svc.set_config.assert_not_called()
    config_svc.delete_config.assert_not_called()


@pytest.mark.asyncio
async def test_patch_validation_failure_prevents_writes():
    """PATCH with invalid value raises ValidationError before any write occurs."""
    import pydantic
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    # "not-a-bool" is not a valid bool for `enabled`
    with pytest.raises((pydantic.ValidationError, ValueError)):
        await api.patch_config(
            catalog_id=None,
            body={"TilesConfig": {"enabled": "not-a-bool"}},
        )

    # Because we validate before writing, no writes should have occurred
    config_svc.set_config.assert_not_called()


@pytest.mark.asyncio
async def test_patch_catalog_scope():
    """PATCH at catalog scope passes catalog_id to set_config."""
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    stored: Dict = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    await api.patch_config(
        catalog_id="test-catalog",
        body={"TilesConfig": {"enabled": False}},
    )

    # Verify catalog_id was passed to set_config
    call_kwargs = config_svc.set_config.call_args
    assert call_kwargs.kwargs.get("catalog_id") == "test-catalog"
