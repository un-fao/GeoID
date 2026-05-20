"""Unit tests pinning ItemsReadPolicy threading through the OGC generators.

The read-side wire-shape contract (``ItemsReadPolicy.feature_type`` —
``expose`` computed-value merge and ``external_id_as_feature_id``) is applied
inside ``ItemsProtocol.map_row_to_feature``. The OGC Records / Features
generators map raw DB rows via that protocol as a fallback (when an item is
not already a mapped ``Feature``); these tests pin that the resolved
``read_policy`` is forwarded into that call so the contract is honoured on the
records / features read paths — not silently dropped (issue #1076).

``resolve_items_read_policy`` is the shared async resolver used by the OGC
service layer to fetch the policy once per request.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.extensions.records import records_generator as rgen
from dynastore.extensions.features import ogc_generator as fgen
from dynastore.extensions.tools.query import resolve_items_read_policy
from dynastore.modules.storage.read_policy import ItemsReadPolicy
from dynastore.modules.storage.computed_fields import FeatureType


class _CapturingItems:
    """Fake ``ItemsProtocol`` that records the ``read_policy`` it receives and
    returns a minimal valid GeoJSON ``Feature`` so generator post-processing
    runs without error."""

    def __init__(self) -> None:
        self.captured: dict = {}

    def map_row_to_feature(
        self,
        row: Any,
        col_config: Any,
        lang: str = "en",
        context: Any = None,
        read_policy: Optional[Any] = None,
    ) -> _GeoJSONFeature:
        self.captured["read_policy"] = read_policy
        return _GeoJSONFeature(
            type="Feature", geometry=None, properties={}, id="x"
        )


def test_db_row_to_record_threads_read_policy(monkeypatch) -> None:
    """``db_row_to_record`` must forward ``read_policy`` into the sidecar
    pipeline when mapping a raw row (the fallback branch)."""
    fake = _CapturingItems()
    monkeypatch.setattr(rgen, "get_protocol", lambda _proto: fake)

    policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
    # A raw dict (not a Feature) forces the map_row_to_feature fallback.
    rgen.db_row_to_record(
        {"geoid": "g1"},
        "cat",
        "col",
        "http://host",
        layer_config=object(),
        read_policy=policy,
    )
    assert fake.captured["read_policy"] is policy


def test_db_row_to_record_already_mapped_skips_mapping(monkeypatch) -> None:
    """An item that is already a Feature must NOT be re-mapped — the canonical
    read path already applied the read policy."""
    fake = _CapturingItems()
    monkeypatch.setattr(rgen, "get_protocol", lambda _proto: fake)

    already = _GeoJSONFeature(type="Feature", geometry=None, properties={}, id="y")
    rgen.db_row_to_record(
        already, "cat", "col", "http://host", layer_config=object(),
        read_policy=ItemsReadPolicy(),
    )
    assert "read_policy" not in fake.captured  # mapping fallback never ran


def test_db_row_to_ogc_feature_threads_read_policy(monkeypatch) -> None:
    """``_db_row_to_ogc_feature`` must forward ``read_policy`` into the sidecar
    pipeline when mapping a raw row (the fallback branch)."""
    fake = _CapturingItems()
    monkeypatch.setattr(fgen, "get_protocol", lambda _proto: fake)

    policy = ItemsReadPolicy(
        feature_type=FeatureType(external_id_as_feature_id=False)
    )
    fgen._db_row_to_ogc_feature(
        {"geoid": "g1"},
        "cat",
        "col",
        "http://host",
        layer_config=object(),
        read_policy=policy,
    )
    assert fake.captured["read_policy"] is policy


def test_db_row_to_ogc_feature_already_mapped_skips_mapping(monkeypatch) -> None:
    fake = _CapturingItems()
    monkeypatch.setattr(fgen, "get_protocol", lambda _proto: fake)

    already = _GeoJSONFeature(type="Feature", geometry=None, properties={}, id="z")
    fgen._db_row_to_ogc_feature(
        already, "cat", "col", "http://host", layer_config=object(),
        read_policy=ItemsReadPolicy(),
    )
    assert "read_policy" not in fake.captured


@pytest.mark.asyncio
async def test_resolve_items_read_policy_returns_config(monkeypatch) -> None:
    """The shared resolver returns the configs-protocol result for the
    collection."""
    sentinel = ItemsReadPolicy(feature_type=FeatureType(expose=["x"]))

    class _Configs:
        async def get_config(self, model, catalog_id=None, collection_id=None):
            assert model is ItemsReadPolicy
            assert (catalog_id, collection_id) == ("cat", "col")
            return sentinel

    # ``resolve_items_read_policy`` imports get_protocol inside the function
    # body; patch the discovery module it pulls from.
    import dynastore.tools.discovery as _discovery

    monkeypatch.setattr(_discovery, "get_protocol", lambda _proto: _Configs())

    out = await resolve_items_read_policy("cat", "col")
    assert out is sentinel


@pytest.mark.asyncio
async def test_resolve_items_read_policy_none_when_unavailable(monkeypatch) -> None:
    """When the configs protocol is unavailable the resolver returns ``None``
    so callers fall back to the default wire shape."""
    import dynastore.tools.discovery as _discovery

    monkeypatch.setattr(_discovery, "get_protocol", lambda _proto: None)
    out = await resolve_items_read_policy("cat", "col")
    assert out is None


@pytest.mark.asyncio
async def test_resolve_items_read_policy_swallows_errors(monkeypatch) -> None:
    """A config-resolution error must not break read assembly — the resolver
    degrades to ``None``."""
    import dynastore.tools.discovery as _discovery

    class _Boom:
        async def get_config(self, *a, **k):
            raise RuntimeError("config backend down")

    monkeypatch.setattr(_discovery, "get_protocol", lambda _proto: _Boom())
    out = await resolve_items_read_policy("cat", "col")
    assert out is None
