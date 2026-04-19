from unittest.mock import AsyncMock

import pytest

from dynastore.extensions.joins.joins_service import (
    _resolve_primary_driver, _stream_primary_features,
)
from dynastore.models.ogc import Feature


class _FakeDriver:
    def __init__(self, items):
        self._items = items

    async def read_entities(self, catalog_id, collection_id, **kwargs):
        for f in self._items:
            yield f


@pytest.mark.asyncio
async def test_resolve_primary_driver_returns_first(monkeypatch):
    import dynastore.extensions.joins.joins_service as mod

    fake_driver = object()
    fake_resolved = type("R", (), {"driver": fake_driver})()
    monkeypatch.setattr(
        mod, "resolve_drivers",
        AsyncMock(return_value=[fake_resolved]),
    )
    out = await _resolve_primary_driver("c", "l")
    assert out is fake_driver


@pytest.mark.asyncio
async def test_resolve_primary_driver_returns_none_when_no_driver(monkeypatch):
    import dynastore.extensions.joins.joins_service as mod

    monkeypatch.setattr(mod, "resolve_drivers", AsyncMock(return_value=[]))
    assert await _resolve_primary_driver("c", "l") is None


@pytest.mark.asyncio
async def test_stream_primary_features_passes_id_column():
    items = [
        Feature(type="Feature", id="1", geometry=None, properties={"uid": "a"}),
        Feature(type="Feature", id="2", geometry=None, properties={"uid": "b"}),
    ]
    driver = _FakeDriver(items)
    feats = [
        f async for f in _stream_primary_features(
            driver, catalog_id="c", collection_id="l",
            primary_column="uid", limit=10,
        )
    ]
    assert [f.id for f in feats] == ["1", "2"]
