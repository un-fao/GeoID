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


@pytest.mark.asyncio
async def test_stream_primary_features_forwards_request():
    """When query_request is supplied, it reaches read_entities."""
    from dynastore.extensions.joins.joins_service import _stream_primary_features
    from dynastore.models.query_builder import QueryRequest

    seen = {}

    class _Recording:
        async def read_entities(self, cat_id, col_id, **kwargs):
            seen.update(kwargs)
            yield Feature(type="Feature", id="1", geometry=None, properties={"uid": "a"})

    qr = QueryRequest(limit=10, cql_filter="x='y'")
    _ = [
        f async for f in _stream_primary_features(
            _Recording(), catalog_id="c", collection_id="l",
            primary_column="uid", limit=10, query_request=qr,
        )
    ]
    assert seen["request"] is qr
    assert seen["context"] == {"id_column": "uid"}
