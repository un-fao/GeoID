"""Verify _on_apply_es_collection_config dispatches a single-collection
reindex with the right mode after an explicit override is written.

Item D from the per-collection obfuscation spec — the apply-handler
follow-up to PR #45 (config + resolver) and PR #47 (dispatch wiring).
"""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_resolver_cache():
    from dynastore.modules.elasticsearch.es_collection_config import (
        is_collection_obfuscated,
    )
    is_collection_obfuscated.cache_clear()
    yield
    is_collection_obfuscated.cache_clear()


def _stub_es_module(monkeypatch, *, available: bool = True, raises: bool = False):
    """Replace get_protocol(ElasticsearchModule) with a fake whose
    bulk_reindex is observable.  Returns the fake (or None when
    ``available=False``)."""
    import dynastore.modules.elasticsearch.es_collection_config as mod

    if not available:
        monkeypatch.setattr(mod, "get_protocol", lambda p: None)
        return None

    fake = MagicMock()
    if raises:
        fake.bulk_reindex = AsyncMock(side_effect=RuntimeError("dispatch boom"))
    else:
        fake.bulk_reindex = AsyncMock(return_value={"status": "dispatched"})
    monkeypatch.setattr(mod, "get_protocol", lambda p: fake)
    return fake


# ---------------------------------------------------------------------------
# Reindex dispatch on explicit override
# ---------------------------------------------------------------------------


class TestApplyDispatchesReindex:
    @pytest.mark.asyncio
    async def test_explicit_true_dispatches_obfuscated_reindex(self, monkeypatch):
        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        fake = _stub_es_module(monkeypatch)

        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=True),
            catalog_id="cat", collection_id="col", db_resource=None,
        )

        fake.bulk_reindex.assert_awaited_once()
        kwargs = fake.bulk_reindex.await_args.kwargs
        assert kwargs["catalog_id"] == "cat"
        assert kwargs["collection_id"] == "col"
        assert kwargs["mode"] == "obfuscated"

    @pytest.mark.asyncio
    async def test_explicit_false_dispatches_catalog_mode_reindex(self, monkeypatch):
        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        fake = _stub_es_module(monkeypatch)

        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=False),
            catalog_id="cat", collection_id="col2", db_resource=None,
        )

        fake.bulk_reindex.assert_awaited_once()
        kwargs = fake.bulk_reindex.await_args.kwargs
        assert kwargs["mode"] == "catalog"

    @pytest.mark.asyncio
    async def test_revert_to_inherit_does_not_dispatch_reindex(self, monkeypatch):
        """`obfuscated=None` is a revert-to-inherit; the catalog default is
        unchanged so existing index contents already match — no reindex
        dispatched.  Only the cache invalidation happens."""
        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        fake = _stub_es_module(monkeypatch)

        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=None),
            catalog_id="cat", collection_id="col3", db_resource=None,
        )

        fake.bulk_reindex.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_es_module_unavailable_swallowed(self, monkeypatch):
        """No ElasticsearchModule registered → log debug, no crash."""
        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        _stub_es_module(monkeypatch, available=False)

        # Must not raise — apply-handlers run inside config writes; a
        # crashing handler would block the write.
        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=True),
            catalog_id="cat", collection_id="col4", db_resource=None,
        )

    @pytest.mark.asyncio
    async def test_dispatch_failure_swallowed_with_warning(
        self, monkeypatch, caplog,
    ):
        """If bulk_reindex raises, the apply handler logs a warning and
        returns.  The operator can re-trigger via the dedicated task; the
        config write itself is unaffected."""
        import logging

        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        _stub_es_module(monkeypatch, raises=True)

        with caplog.at_level(logging.WARNING):
            await _on_apply_es_collection_config(
                ElasticsearchCollectionConfig(obfuscated=True),
                catalog_id="cat", collection_id="col5", db_resource=None,
            )

        assert any(
            "failed to dispatch reindex" in r.message for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_missing_catalog_or_collection_id_short_circuits(
        self, monkeypatch,
    ):
        """Platform-tier writes (catalog_id/collection_id None) are no-ops
        — no dispatch, no cache invalidation, no crash."""
        from dynastore.modules.elasticsearch.es_collection_config import (
            ElasticsearchCollectionConfig,
            _on_apply_es_collection_config,
        )

        fake = _stub_es_module(monkeypatch)

        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=True),
            catalog_id=None, collection_id="col", db_resource=None,
        )
        await _on_apply_es_collection_config(
            ElasticsearchCollectionConfig(obfuscated=True),
            catalog_id="cat", collection_id=None, db_resource=None,
        )

        fake.bulk_reindex.assert_not_awaited()
