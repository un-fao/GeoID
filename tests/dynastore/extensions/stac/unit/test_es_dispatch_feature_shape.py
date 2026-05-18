"""Unit tests pinning the ES-dispatch return contract.

``_maybe_dispatch_to_es_search`` is the ES fast path on STAC ``/search``.
The downstream serializer (``stac_generator.create_search_results_collection``)
reads ``feature.properties.get("_catalog_id" | "_collection_id")`` on each
hit, so the dispatch must return ``Feature`` pydantic instances with those
cross-collection markers injected — exactly the shape the PG path emits at
``packages/extensions/stac/.../search.py:882-883``.

Closes the regression that surfaces once #914 is fixed: with
``catalog_id`` now present on indexed docs the ES path actually returns
hits, exposing a ``'dict' object has no attribute 'properties'``
AttributeError in the serializer when the dispatch returns raw
``_source`` dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.stac.search import _maybe_dispatch_to_es_search
from dynastore.models.shared_models import Feature


@dataclass
class _StubItemSearchResult:
    features: List[Dict[str, Any]]
    total: int = 0


class _StubSearchProtocol:
    def __init__(self, features: List[Dict[str, Any]], total: int):
        self._result = _StubItemSearchResult(features=features, total=total)

    async def search_items_struct(self, **_kwargs):
        return self._result


class _StubDriver:
    """Class name must snake-case to ``items_elasticsearch_driver`` to
    pass the ES-routing gate in :func:`_maybe_dispatch_to_es_search`."""


# Rename via class-attribute trick so isinstance/repr don't matter; the
# function only reads ``type(driver).__name__``.
_StubDriver.__name__ = "ItemsElasticsearchDriver"


@dataclass
class _SearchRequest:
    filter: Optional[Any] = None
    collections: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[List[float]] = None
    intersects: Optional[Dict[str, Any]] = None
    datetime: Optional[str] = None
    limit: int = 10


def _patch_dispatch(monkeypatch, *, hits, total, driver_cls=_StubDriver):
    """Wire the four ``get_*`` lookups the dispatcher performs."""
    import dynastore.modules.storage.router as _router
    import dynastore.modules.storage.routing_config as _routing
    import dynastore.tools.discovery as _discovery
    from dynastore.models.protocols.item_search import ItemSearchProtocol

    async def _fake_get_driver(_op, _cat, _cid):
        return driver_cls()

    monkeypatch.setattr(_router, "get_driver", _fake_get_driver)
    # _to_snake is imported inside the function — pinned via module attr.
    monkeypatch.setattr(
        _routing, "_to_snake", lambda name: "items_elasticsearch_driver"
    )

    stub = _StubSearchProtocol(features=hits, total=total)

    def _fake_get_protocol(proto):
        if proto is ItemSearchProtocol:
            return stub
        return None

    # Imported inside the function body of search.py — patch the module
    # that gets imported there.
    monkeypatch.setattr(_discovery, "get_protocol", _fake_get_protocol)
    # search.py top-level also has ``get_protocol`` bound — patch both.
    import dynastore.extensions.stac.search as _search_mod

    monkeypatch.setattr(_search_mod, "get_protocol", _fake_get_protocol)


@pytest.mark.asyncio
async def test_es_dispatch_returns_feature_instances(monkeypatch):
    """Raw _source dicts must be parsed into Feature pydantic models so
    the cross-collection serializer's ``feature.properties.get(...)``
    calls don't AttributeError."""
    hit = {
        "type": "Feature",
        "id": "item-1",
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "bbox": [10.0, 20.0, 10.0, 20.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "collection": "col-a",
        "catalog_id": "cat-x",
    }
    _patch_dispatch(monkeypatch, hits=[hit], total=1)

    req = _SearchRequest(collections=["col-a"])
    out = await _maybe_dispatch_to_es_search("cat-x", req)

    assert out is not None
    features, total, aggs = out
    assert total == 1
    assert aggs is None
    assert len(features) == 1
    assert isinstance(features[0], Feature)


@pytest.mark.asyncio
async def test_es_dispatch_injects_cross_collection_markers(monkeypatch):
    """Mirror the PG path (search.py:882-883) — inject
    ``_catalog_id`` + ``_collection_id`` into ``feature.properties`` so
    ``stac_generator.create_search_results_collection`` can dispatch
    per-item rendering."""
    hit = {
        "type": "Feature",
        "id": "item-2",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [0.0, 0.0, 0.0, 0.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "collection": "col-b",
        "catalog_id": "cat-y",
    }
    _patch_dispatch(monkeypatch, hits=[hit], total=1)

    req = _SearchRequest(collections=["col-b"])
    out = await _maybe_dispatch_to_es_search("cat-y", req)
    assert out is not None
    features, _total, _aggs = out
    assert features[0].properties["_catalog_id"] == "cat-y"
    assert features[0].properties["_collection_id"] == "col-b"


@pytest.mark.asyncio
async def test_es_dispatch_falls_back_to_request_collection_when_missing(
    monkeypatch,
):
    """If the indexed doc lacks ``collection`` (legacy pre-#959 docs)
    and exactly one collection is in scope, fall back to the requested
    cid so the serializer still has a non-empty key."""
    hit = {
        "type": "Feature",
        "id": "legacy-item",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        # no `collection`, no `catalog_id`
    }
    _patch_dispatch(monkeypatch, hits=[hit], total=1)

    req = _SearchRequest(collections=["only-one"])
    out = await _maybe_dispatch_to_es_search("cat-z", req)
    assert out is not None
    features, _, _ = out
    assert features[0].properties["_catalog_id"] == "cat-z"
    assert features[0].properties["_collection_id"] == "only-one"


@pytest.mark.asyncio
async def test_es_dispatch_skips_malformed_hits(monkeypatch):
    """A doc missing required ``id`` (Feature is strict on it) must be
    skipped with a warning, not crash the whole search."""
    bad = {"type": "Feature", "properties": {}}  # no id
    good = {
        "type": "Feature",
        "id": "ok",
        "geometry": None,
        "properties": {},
        "collection": "c1",
    }
    _patch_dispatch(monkeypatch, hits=[bad, good], total=2)

    req = _SearchRequest(collections=["c1"])
    out = await _maybe_dispatch_to_es_search("cat-t", req)
    assert out is not None
    features, total, _ = out
    # total preserved (operator-visible numberMatched); 1 hit dropped.
    assert total == 2
    assert len(features) == 1
    assert features[0].id == "ok"
