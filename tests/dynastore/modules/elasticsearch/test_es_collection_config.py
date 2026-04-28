"""Unit tests for ElasticsearchCollectionConfig + is_collection_private.

Covers the override-then-catalog waterfall, cache hit/miss, apply-handler
invalidation, and graceful degradation when ConfigsProtocol is unavailable.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.elasticsearch.es_catalog_config import (
    ElasticsearchCatalogConfig,
)
from dynastore.modules.elasticsearch.es_collection_config import (
    ElasticsearchCollectionConfig,
    _on_apply_es_collection_config,
    is_collection_private,
)


@pytest.fixture(autouse=True)
def _clear_resolver_cache():
    """Reset the @cached layer on the module-level resolver between tests."""
    is_collection_private.cache_clear()
    yield
    is_collection_private.cache_clear()


def _fake_configs(
    *, collection_cfg=None, catalog_cfg=None,
    collection_raises=False, catalog_raises=False,
):
    """Build a fake ConfigsProtocol that returns the supplied configs.

    Either argument may be ``None`` (returns ``None`` from get_config) or
    set to a Config instance.  ``*_raises=True`` makes the corresponding
    fetch raise to exercise the warning-and-fall-through paths.
    """
    proto = MagicMock()

    async def _get(cls, *, catalog_id=None, collection_id=None):
        if cls is ElasticsearchCollectionConfig:
            if collection_raises:
                raise RuntimeError("boom-collection")
            return collection_cfg
        if cls is ElasticsearchCatalogConfig:
            if catalog_raises:
                raise RuntimeError("boom-catalog")
            return catalog_cfg
        return None

    proto.get_config = AsyncMock(side_effect=_get)
    return proto


# ---------------------------------------------------------------------------
# Resolution waterfall
# ---------------------------------------------------------------------------


class TestResolverWaterfall:
    @pytest.mark.asyncio
    async def test_returns_false_when_no_configs_protocol(self, monkeypatch):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        monkeypatch.setattr(mod, "get_protocol", lambda p: None)
        assert await is_collection_private("cat", "col") is False

    @pytest.mark.asyncio
    async def test_collection_override_true_wins(self, monkeypatch):
        """Collection override True must beat catalog False."""
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=True),
            catalog_cfg=ElasticsearchCatalogConfig(private=False),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col1") is True

    @pytest.mark.asyncio
    async def test_collection_override_false_wins(self, monkeypatch):
        """Collection override False must beat catalog True (opt-out)."""
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=False),
            catalog_cfg=ElasticsearchCatalogConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col2") is False

    @pytest.mark.asyncio
    async def test_inherits_catalog_when_collection_private_is_none(
        self, monkeypatch,
    ):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=None),
            catalog_cfg=ElasticsearchCatalogConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col3") is True

    @pytest.mark.asyncio
    async def test_inherits_catalog_when_collection_cfg_absent(
        self, monkeypatch,
    ):
        """No collection-tier config registered → fall through to catalog."""
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=None,
            catalog_cfg=ElasticsearchCatalogConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col4") is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_configs_at_either_tier(
        self, monkeypatch,
    ):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(collection_cfg=None, catalog_cfg=None)
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col5") is False

    @pytest.mark.asyncio
    async def test_collection_fetch_exception_falls_through_to_catalog(
        self, monkeypatch,
    ):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_raises=True,
            catalog_cfg=ElasticsearchCatalogConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col6") is True

    @pytest.mark.asyncio
    async def test_catalog_fetch_exception_returns_false(self, monkeypatch):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(collection_cfg=None, catalog_raises=True)
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)
        assert await is_collection_private("cat", "col7") is False


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------


class TestResolverCache:
    @pytest.mark.asyncio
    async def test_repeated_call_hits_cache(self, monkeypatch):
        """Second call with identical args must NOT re-invoke get_config."""
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)

        await is_collection_private("cat_cache_a", "col_cache_a")
        await is_collection_private("cat_cache_a", "col_cache_a")
        assert proto.get_config.call_count == 1

    @pytest.mark.asyncio
    async def test_distinct_collection_keys_dont_collide(self, monkeypatch):
        import dynastore.modules.elasticsearch.es_collection_config as mod
        proto = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto)

        await is_collection_private("cat_cache_b", "col_b1")
        await is_collection_private("cat_cache_b", "col_b2")
        await is_collection_private("cat_cache_b", "col_b1")  # cached

        assert proto.get_config.call_count == 2

    @pytest.mark.asyncio
    async def test_apply_handler_invalidates_cached_entry(self, monkeypatch):
        """Writing a collection-tier config must drop the cached resolver
        entry so the next call sees the new override."""
        import dynastore.modules.elasticsearch.es_collection_config as mod

        # Round 1: collection_cfg has private=False — cache misses, then hits.
        proto1 = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=False),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto1)
        assert await is_collection_private("cat_inval", "col_inval") is False
        assert await is_collection_private("cat_inval", "col_inval") is False
        assert proto1.get_config.call_count == 1

        # Apply-handler fires for the same (cat, col) — invalidates cache.
        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(private=True),
            catalog_id="cat_inval",
            collection_id="col_inval",
            db_resource=None,
        )

        # Round 2: same proto returns the new private=True value;
        # because the cache was invalidated, a fresh fetch happens.
        proto2 = _fake_configs(
            collection_cfg=ElasticsearchCollectionConfig(private=True),
        )
        monkeypatch.setattr(mod, "get_protocol", lambda p: proto2)
        assert await is_collection_private("cat_inval", "col_inval") is True
        assert proto2.get_config.call_count == 1

    @pytest.mark.asyncio
    async def test_apply_handler_noop_when_catalog_or_collection_missing(self):
        """Platform-tier writes (catalog_id/collection_id None) are no-ops."""
        # Should not raise, should not crash; nothing to assert beyond that.
        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(private=True),
            catalog_id=None, collection_id="col", db_resource=None,
        )
        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(private=True),
            catalog_id="cat", collection_id=None, db_resource=None,
        )
