"""Unit tests for config_cache.py — M6.

Covers:
- L4 per-request context var lifecycle (init / get / clear)
- L4 isolation across independent async task contexts
- publish_router_invalidation: local-only path (no VALKEY_URL)
- RouterCacheInvalidator.start/stop: no-op when VALKEY_URL absent
"""
import asyncio
import os

import pytest

from dynastore.modules.storage.config_cache import (
    RouterCacheInvalidator,
    clear_request_driver_cache,
    get_request_driver_cache,
    init_request_driver_cache,
    publish_router_invalidation,
)


# ---------------------------------------------------------------------------
# L4 context-var tests
# ---------------------------------------------------------------------------


class TestRequestDriverCache:
    def test_get_before_init_returns_empty_dict(self):
        """Outside a request context the cache is an ephemeral empty dict."""
        cache = get_request_driver_cache()
        assert cache == {}
        # Writes to the sentinel dict do not leak across calls
        cache["foo"] = "bar"
        assert get_request_driver_cache() == {}

    def test_init_and_get_returns_same_dict(self):
        token = init_request_driver_cache()
        try:
            cache1 = get_request_driver_cache()
            cache2 = get_request_driver_cache()
            assert cache1 is cache2
        finally:
            clear_request_driver_cache(token)

    def test_clear_resets_to_unset(self):
        token = init_request_driver_cache()
        cache = get_request_driver_cache()
        cache["key"] = "value"
        clear_request_driver_cache(token)
        # After reset the sentinel is returned again (empty, not the same dict)
        after = get_request_driver_cache()
        assert after == {}
        assert after is not cache

    def test_writes_are_visible_within_same_context(self):
        token = init_request_driver_cache()
        try:
            cache = get_request_driver_cache()
            cache[("READ", "cat1", "col1", None)] = ["driver_a"]
            result = get_request_driver_cache()
            assert result[("READ", "cat1", "col1", None)] == ["driver_a"]
        finally:
            clear_request_driver_cache(token)

    @pytest.mark.asyncio
    async def test_context_var_isolation_across_tasks(self):
        """Two concurrent async tasks must not share their L4 caches."""

        async def task_a() -> dict:
            token = init_request_driver_cache()
            try:
                cache = get_request_driver_cache()
                cache["owner"] = "task_a"
                await asyncio.sleep(0)  # yield so task_b can run
                return dict(get_request_driver_cache())
            finally:
                clear_request_driver_cache(token)

        async def task_b() -> dict:
            token = init_request_driver_cache()
            try:
                cache = get_request_driver_cache()
                cache["owner"] = "task_b"
                await asyncio.sleep(0)
                return dict(get_request_driver_cache())
            finally:
                clear_request_driver_cache(token)

        result_a, result_b = await asyncio.gather(task_a(), task_b())
        assert result_a["owner"] == "task_a"
        assert result_b["owner"] == "task_b"

    @pytest.mark.asyncio
    async def test_nested_init_restores_previous_state(self):
        """init inside an already-init'd context restores on clear."""
        outer_token = init_request_driver_cache()
        outer_cache = get_request_driver_cache()
        outer_cache["level"] = "outer"

        inner_token = init_request_driver_cache()
        inner_cache = get_request_driver_cache()
        inner_cache["level"] = "inner"
        assert get_request_driver_cache()["level"] == "inner"

        clear_request_driver_cache(inner_token)
        # Outer context restored
        assert get_request_driver_cache()["level"] == "outer"

        clear_request_driver_cache(outer_token)
        assert get_request_driver_cache() == {}


# ---------------------------------------------------------------------------
# publish_router_invalidation — local-only (no Valkey)
# ---------------------------------------------------------------------------


class TestPublishRouterInvalidation:
    def test_local_invalidation_does_not_raise(self, monkeypatch):
        """publish_router_invalidation must not raise even with no Valkey."""
        monkeypatch.delenv("VALKEY_URL", raising=False)
        # Should complete without exception
        publish_router_invalidation("cat1", "col1")

    def test_local_invalidation_with_none_ids(self, monkeypatch):
        monkeypatch.delenv("VALKEY_URL", raising=False)
        publish_router_invalidation(None, None)

    @pytest.mark.asyncio
    async def test_local_invalidation_from_async_context(self, monkeypatch):
        monkeypatch.delenv("VALKEY_URL", raising=False)
        publish_router_invalidation("cat2", "col2")
        # Allow any tasks to settle
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# RouterCacheInvalidator — no-op without Valkey
# ---------------------------------------------------------------------------


class TestRouterCacheInvalidator:
    @pytest.mark.asyncio
    async def test_start_noop_without_valkey_url(self, monkeypatch):
        monkeypatch.delenv("VALKEY_URL", raising=False)
        await RouterCacheInvalidator.start()
        assert RouterCacheInvalidator._task is None

    @pytest.mark.asyncio
    async def test_stop_noop_when_never_started(self, monkeypatch):
        monkeypatch.delenv("VALKEY_URL", raising=False)
        RouterCacheInvalidator._task = None
        await RouterCacheInvalidator.stop()  # must not raise
