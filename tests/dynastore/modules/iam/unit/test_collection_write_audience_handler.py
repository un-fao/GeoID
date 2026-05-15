"""Unit tests for the core-IAM ``CollectionWriteAudienceHandler``.

Migrated from the geoid extension's test tree (#286). The handler is the
``collection_write_anonymous_allowed`` ConditionHandler — gates anonymous
item-creation POSTs (STAC + OGC-Features) on the per-collection
``CollectionWriteAudience.allow_anonymous_create`` opt-in.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

_GET_PROTOCOL_PATH = "dynastore.modules.iam.audience_handlers.get_protocol"


@pytest.mark.asyncio
async def test_handler_type_is_collection_write_anonymous_allowed():
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    assert handler.type == "collection_write_anonymous_allowed"


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_allows_via_extras(monkeypatch):
    from dynastore.modules.iam.audience_configs import CollectionWriteAudience
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_id_parsed_from_stac_path(monkeypatch):
    """When extras has no collection_id, the handler parses ctx.path —
    STAC items-POST shape."""
    from dynastore.modules.iam.audience_configs import CollectionWriteAudience
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    ctx.path = "/stac/catalogs/cat/collections/intake_col/items"
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_id_parsed_from_features_path(monkeypatch):
    """The OGC-Features items-POST shape is path-shape-identical to the STAC
    one (``.../catalogs/{cat}/collections/{col}/items``); a single handler
    covers both surfaces — the B4 "extend to Features" requirement."""
    from dynastore.modules.iam.audience_configs import CollectionWriteAudience
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    ctx.path = "/features/catalogs/cat/collections/intake_col/items"
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_does_not_allow(monkeypatch):
    from dynastore.modules.iam.audience_configs import CollectionWriteAudience
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=False),
    )
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_id_missing(monkeypatch):
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: MagicMock())
    ctx = MagicMock()
    ctx.catalog_id = None
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_id_missing(monkeypatch):
    """No collection_id in extras AND path doesn't match → fail closed."""
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: MagicMock())
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    ctx.path = "/some/unrelated/path"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_fails_closed_on_get_config_raises(monkeypatch):
    from dynastore.modules.iam.audience_handlers import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(_GET_PROTOCOL_PATH, lambda _proto: fake_configs)
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


# ---------------------------------------------------------------------------
# Re-export shim (back-compat) — the same classes resolve via the legacy
# geoid-extension import path until the extension is fully removed (#287).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_resolvable_via_legacy_geoid_extension_import():
    """``from dynastore.extensions.geoid.conditions import ...`` still works."""
    from dynastore.extensions.geoid.conditions import (
        CollectionWriteAudienceHandler as LegacyHandler,
    )
    from dynastore.modules.iam.audience_handlers import (
        CollectionWriteAudienceHandler as NewHandler,
    )

    assert LegacyHandler is NewHandler


@pytest.mark.asyncio
async def test_config_resolvable_via_legacy_geoid_extension_import():
    """``from dynastore.extensions.geoid.configs import ...`` still works."""
    from dynastore.extensions.geoid.configs import (
        CollectionWriteAudience as LegacyConfig,
    )
    from dynastore.modules.iam.audience_configs import (
        CollectionWriteAudience as NewConfig,
    )

    assert LegacyConfig is NewConfig
