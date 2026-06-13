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

"""Unit tests for the CatalogLookupAudienceHandler condition."""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_handler_type_is_catalog_lookup_public_allowed():
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    assert handler.type == "catalog_lookup_public_allowed"


@pytest.mark.asyncio
async def test_handler_returns_true_when_catalog_is_public(monkeypatch):
    from dynastore.extensions.geoid.configs import CatalogLookupAudience
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=CatalogLookupAudience(is_public=True))
    monkeypatch.setattr(
        "dynastore.modules.iam.audience_handlers.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "customer_cat"
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CatalogLookupAudience, catalog_id="customer_cat",
    )


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_is_private(monkeypatch):
    from dynastore.extensions.geoid.configs import CatalogLookupAudience
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=CatalogLookupAudience(is_public=False))
    monkeypatch.setattr(
        "dynastore.modules.iam.audience_handlers.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "other_cat"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_id_missing(monkeypatch):
    """No catalog_id in context → fail closed."""
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    monkeypatch.setattr(
        "dynastore.modules.iam.audience_handlers.get_protocol",
        lambda _proto: MagicMock(),
    )
    ctx = MagicMock()
    ctx.catalog_id = None
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_configs_protocol_unavailable(monkeypatch):
    """ConfigsProtocol not loaded → fail closed."""
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    monkeypatch.setattr(
        "dynastore.modules.iam.audience_handlers.get_protocol",
        lambda _proto: None,
    )
    ctx = MagicMock()
    ctx.catalog_id = "any_cat"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_get_config_raises(monkeypatch):
    """ConfigsProtocol error → fail closed."""
    from dynastore.extensions.geoid.conditions import CatalogLookupAudienceHandler

    handler = CatalogLookupAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=RuntimeError("config store down"))
    monkeypatch.setattr(
        "dynastore.modules.iam.audience_handlers.get_protocol",
        lambda _proto: fake_configs,
    )
    ctx = MagicMock()
    ctx.catalog_id = "any_cat"
    assert await handler.evaluate({}, ctx) is False
