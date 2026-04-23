"""Verify ElasticsearchModule._on_item_upsert / _on_item_bulk_upsert dispatch
to the obfuscated indexer task IFF is_collection_obfuscated() returns True.

These tests target item C from the per-collection obfuscation spec — the
indexer dispatch refactor that makes a per-collection override actually
take effect at write time (rather than only being readable via the
resolver).
"""
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def _clear_resolver_cache():
    """Reset the @cached resolver between tests so a per-test stub isn't
    masked by an earlier test's cached value."""
    from dynastore.modules.elasticsearch.es_collection_config import (
        is_collection_obfuscated,
    )
    is_collection_obfuscated.cache_clear()
    yield
    is_collection_obfuscated.cache_clear()


def _stub_resolver(monkeypatch, return_value: bool):
    """Replace is_collection_obfuscated at its import site inside module.py
    with an AsyncMock that returns ``return_value``.  Returns the mock so
    tests can assert call args."""
    import dynastore.modules.elasticsearch.es_collection_config as cfg_mod
    fake = AsyncMock(return_value=return_value)
    monkeypatch.setattr(cfg_mod, "is_collection_obfuscated", fake)
    return fake


def _new_module():
    """Build an ElasticsearchModule with _dispatch_task and a few helpers
    stubbed so we can exercise dispatch without touching ES, the tasks
    module, or PG."""
    from dynastore.modules.elasticsearch.module import ElasticsearchModule

    mod = ElasticsearchModule()
    mod._dispatch_task = AsyncMock()
    return mod


# ---------------------------------------------------------------------------
# _on_item_upsert
# ---------------------------------------------------------------------------


class TestOnItemUpsertDispatch:
    @pytest.mark.asyncio
    async def test_resolver_true_dispatches_obfuscated_task(self, monkeypatch):
        fake = _stub_resolver(monkeypatch, return_value=True)
        mod = _new_module()

        await mod._on_item_upsert(
            catalog_id="cat", collection_id="col", item_id="i1",
        )

        fake.assert_awaited_once_with("cat", "col")
        assert mod._dispatch_task.await_count == 1
        kwargs = mod._dispatch_task.await_args.kwargs
        assert kwargs["task_type"] == "elasticsearch_obfuscated_index"
        assert kwargs["inputs"] == {
            "geoid": "i1", "catalog_id": "cat", "collection_id": "col",
        }

    @pytest.mark.asyncio
    async def test_resolver_false_skips_obfuscated_path(self, monkeypatch):
        """When the resolver says False, the obfuscated task is NOT
        dispatched — control flow falls through to the classic path
        (which then exits early because the test stubs out _is_es_active)."""
        fake = _stub_resolver(monkeypatch, return_value=False)

        # Stub _is_es_active so the classic path also early-exits cleanly.
        import dynastore.modules.elasticsearch.module as es_mod
        monkeypatch.setattr(
            es_mod, "_is_es_active", AsyncMock(return_value=False),
        )

        mod = _new_module()
        await mod._on_item_upsert(
            catalog_id="cat", collection_id="col", item_id="i2",
        )

        fake.assert_awaited_once_with("cat", "col")
        # No dispatch happened — neither obfuscated nor classic.
        assert mod._dispatch_task.await_count == 0

    @pytest.mark.asyncio
    async def test_missing_required_arg_short_circuits_before_resolver(
        self, monkeypatch,
    ):
        """Early-return guard at the top of the handler must run BEFORE the
        resolver — otherwise a None catalog_id would hit the resolver and
        cache a meaningless entry."""
        fake = _stub_resolver(monkeypatch, return_value=True)
        mod = _new_module()

        await mod._on_item_upsert(
            catalog_id=None, collection_id="col", item_id="i3",
        )
        await mod._on_item_upsert(
            catalog_id="cat", collection_id=None, item_id="i3",
        )
        await mod._on_item_upsert(
            catalog_id="cat", collection_id="col", item_id=None,
        )

        fake.assert_not_awaited()
        assert mod._dispatch_task.await_count == 0


# ---------------------------------------------------------------------------
# _on_item_bulk_upsert
# ---------------------------------------------------------------------------


class TestOnItemBulkUpsertDispatch:
    @pytest.mark.asyncio
    async def test_bulk_resolver_called_once_for_whole_batch(
        self, monkeypatch,
    ):
        """All items in one bulk event share the same (catalog, collection),
        so the resolver should be queried ONCE per call — not per item.
        Cache would mask repeats anyway, but the single-call discipline
        keeps the dispatch loop free of avoidable async hops."""
        fake = _stub_resolver(monkeypatch, return_value=True)
        mod = _new_module()

        await mod._on_item_bulk_upsert(
            catalog_id="cat",
            collection_id="col",
            payload={
                "items_subset": [
                    {"id": f"i{i}"} for i in range(5)
                ],
            },
        )

        fake.assert_awaited_once_with("cat", "col")
        assert mod._dispatch_task.await_count == 5
        for call in mod._dispatch_task.await_args_list:
            assert call.kwargs["task_type"] == "elasticsearch_obfuscated_index"
        dispatched_ids = [
            c.kwargs["inputs"]["geoid"]
            for c in mod._dispatch_task.await_args_list
        ]
        assert dispatched_ids == ["i0", "i1", "i2", "i3", "i4"]

    @pytest.mark.asyncio
    async def test_bulk_resolver_false_falls_through_to_classic(
        self, monkeypatch,
    ):
        fake = _stub_resolver(monkeypatch, return_value=False)
        import dynastore.modules.elasticsearch.module as es_mod
        monkeypatch.setattr(
            es_mod, "_is_es_active", AsyncMock(return_value=False),
        )

        mod = _new_module()
        await mod._on_item_bulk_upsert(
            catalog_id="cat",
            collection_id="col",
            payload={"items_subset": [{"id": "i1"}, {"id": "i2"}]},
        )

        fake.assert_awaited_once_with("cat", "col")
        # Resolver said False AND _is_es_active returns False, so no dispatch.
        assert mod._dispatch_task.await_count == 0

    @pytest.mark.asyncio
    async def test_bulk_skips_items_without_id(self, monkeypatch):
        """Items missing an 'id' field are skipped silently — guards against
        partially-formed bulk payloads."""
        _stub_resolver(monkeypatch, return_value=True)
        mod = _new_module()

        await mod._on_item_bulk_upsert(
            catalog_id="cat",
            collection_id="col",
            payload={"items_subset": [
                {"id": "good1"},
                {"id": ""},  # falsy → skipped
                {},          # no key → skipped
                {"id": "good2"},
            ]},
        )

        assert mod._dispatch_task.await_count == 2
        ids = [c.kwargs["inputs"]["geoid"] for c in mod._dispatch_task.await_args_list]
        assert ids == ["good1", "good2"]
