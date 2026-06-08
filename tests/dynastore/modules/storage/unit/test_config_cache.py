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

"""Unit tests for config_cache.py — L4 per-request context var.

Covers:
- L4 per-request context var lifecycle (init / get / clear)
- L4 isolation across independent async task contexts
- Sentinel behavior when L4 is not initialized
"""
import asyncio

import pytest

from dynastore.modules.storage.config_cache import (
    clear_request_driver_cache,
    get_request_driver_cache,
    init_request_driver_cache,
)


class TestRequestDriverCache:
    """L4 per-request cache via contextvars.ContextVar."""

    def test_get_before_init_returns_empty_dict(self):
        """Outside a request context the cache is an ephemeral empty dict."""
        cache = get_request_driver_cache()
        assert cache == {}
        # Writes to the sentinel dict do not leak across calls
        cache["foo"] = "bar"
        assert get_request_driver_cache() == {}

    def test_init_and_get_returns_same_dict(self):
        """After init, subsequent get calls return the same dict instance."""
        token = init_request_driver_cache()
        try:
            cache1 = get_request_driver_cache()
            cache2 = get_request_driver_cache()
            assert cache1 is cache2
        finally:
            clear_request_driver_cache(token)

    def test_clear_resets_to_unset(self):
        """After clear, context var returns to None state (sentinel dict)."""
        token = init_request_driver_cache()
        cache = get_request_driver_cache()
        cache["key"] = "value"
        clear_request_driver_cache(token)
        # After reset the sentinel is returned again (empty, not the same dict)
        after = get_request_driver_cache()
        assert after == {}
        assert after is not cache

    def test_writes_are_visible_within_same_context(self):
        """Writes to the context dict are immediately visible."""
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
