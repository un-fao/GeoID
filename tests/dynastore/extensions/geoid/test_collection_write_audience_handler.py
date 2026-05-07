"""Unit tests for CollectionWriteAudienceHandler."""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_handler_type_is_collection_write_anonymous_allowed():
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    assert handler.type == "collection_write_anonymous_allowed"


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_allows_anonymous_create(monkeypatch):
    from dynastore.extensions.geoid.configs import CollectionWriteAudience
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    # The handler reads collection_id from the URL path via ctx.extras or path parse.
    # We pass it via ctx.extras["collection_id"] — handler must read it from there.
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_does_not_allow(monkeypatch):
    from dynastore.extensions.geoid.configs import CollectionWriteAudience
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=False),
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_id_missing(monkeypatch):
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: MagicMock(),
    )
    ctx = MagicMock()
    ctx.catalog_id = None
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_id_missing(monkeypatch):
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: MagicMock(),
    )
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_fails_closed_on_get_config_raises(monkeypatch):
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False
