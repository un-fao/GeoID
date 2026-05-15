"""Unit tests for the core-IAM ``CatalogLookupAudienceHandler``.

The handler is the ``catalog_lookup_public_allowed`` ConditionHandler —
gates anonymous lookup traffic (typically ``/search`` paths under the
``lookup_only_search`` body filter) on the per-catalog
``CatalogLookupAudience.is_public`` opt-in. Migrated to core IAM (#286).
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

_GET_PROTOCOL_PATH = "dynastore.modules.iam.audience_handlers.get_protocol"


@pytest.mark.asyncio
async def test_handler_type_is_catalog_lookup_public_allowed():
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    assert handler.type == "catalog_lookup_public_allowed"


@pytest.mark.asyncio
async def test_handler_returns_true_when_catalog_is_public(monkeypatch):
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CatalogLookupAudience(is_public=True),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CatalogLookupAudience, catalog_id="cat",
    )


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_is_not_public(monkeypatch):
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CatalogLookupAudience(is_public=False),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_id_missing(monkeypatch):
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: MagicMock())
    ctx = MagicMock()
    ctx.catalog_id = None
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_configs_protocol_missing(monkeypatch):
    """``get_protocol(ConfigsProtocol)`` returns None → fail closed."""
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: None)
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_fails_closed_on_get_config_raises(monkeypatch):
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_is_registered_in_default_registry():
    """Auto-registration into the default ConditionRegistry — core IAM
    no longer relies on the geoid extension's startup hook."""
    from dynastore.modules.iam.audience_handlers import CatalogLookupAudienceHandler
    from dynastore.modules.iam.conditions import condition_registry

    h = condition_registry._handlers.get("catalog_lookup_public_allowed")
    assert isinstance(h, CatalogLookupAudienceHandler)


@pytest.mark.asyncio
async def test_collection_write_handler_is_registered_in_default_registry():
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler
    from dynastore.modules.iam.conditions import condition_registry

    h = condition_registry._handlers.get("collection_write_anonymous_allowed")
    assert isinstance(h, CollectionWriteAudienceHandler)
