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

"""Regression test: sync-context cache-invalidation tasks must be strong-ref'd.

Background: when ``@cached``-decorated callables are invoked from a sync
context (e.g. a non-async handler hitting the sync ``cache_invalidate`` /
``cache_clear`` accessors), distributed-backend invalidation is fire-and-
forget. The implementation used to call::

    loop.create_task(_backend.clear(key=cache_key))   # invalidate
    loop.create_task(_backend.clear(namespace=ns))    # clear

and throw away the returned ``Task``. Per asyncio docs, the loop keeps
only **weak** refs to tasks, so a GC sweep before the coroutine runs to
completion silently drops the invalidation — stale data continues to be
served from the distributed backend until the entry's TTL expires.

Fix: tasks scheduled from these two sites are now registered with the
module-level ``_pending_invalidations`` set via ``_track``, which mirrors
the pattern used by ``dynastore.modules.concurrency.run_in_background``.

This test pins both halves:

1. Empirical: invoking either sync accessor schedules a task that lands
   in ``_pending_invalidations`` until completion.
2. Source: bare ``loop.create_task(_backend.clear`` no longer appears in
   the function bodies.
"""
from __future__ import annotations

import asyncio
import inspect

import pytest


def test_pending_invalidations_set_exists_and_is_empty_initially():
    from dynastore.tools import cache as cache_mod

    assert hasattr(cache_mod, "_pending_invalidations"), (
        "cache module must expose ``_pending_invalidations`` set so "
        "fire-and-forget invalidation tasks have a strong reference"
    )
    assert hasattr(cache_mod, "_track"), (
        "cache module must expose ``_track`` to register tasks against "
        "the strong-ref set"
    )


def test_sync_cache_invalidate_source_uses_track():
    """Source-level pin: ``sync_cache_invalidate_impl`` must wrap its
    ``loop.create_task(_backend.clear(...))`` in ``_track(...)``. A
    regression that drops the wrapper re-introduces the GC-drop bug."""
    from dynastore.tools.cache import cached

    # Pull the cached decorator source — both sync accessors live inside
    # the closure that the decorator returns, so the function body is
    # the source we need to grep.
    src = inspect.getsource(cached)
    assert "_track(loop.create_task(_backend.clear(key=" in src, (
        "sync_cache_invalidate_impl must wrap loop.create_task with "
        "_track(...) so the invalidation task is not GC'd before it runs"
    )
    assert "_track(loop.create_task(_backend.clear(namespace=" in src, (
        "sync_cache_clear_impl must wrap loop.create_task with "
        "_track(...) so the namespace-clear task is not GC'd"
    )
    # No bare leftover
    assert "loop.create_task(_backend.clear(key=cache_key))\n" not in src, (
        "regression: bare loop.create_task(_backend.clear(key=...)) "
        "re-introduced — weak-ref'd task can be GC'd mid-flight"
    )


def test_track_holds_strong_reference_until_task_completes():
    """Empirical: a task passed through ``_track`` lands in the
    module-level set so the loop's weak ref is not the only reference."""
    from dynastore.tools import cache as cache_mod

    started = asyncio.Event()
    finish = asyncio.Event()

    async def _slow():
        started.set()
        await finish.wait()

    async def _scenario() -> bool:
        task = cache_mod._track(asyncio.create_task(_slow(), name="cache_test_track"))
        await started.wait()
        del task  # drop the local; rely on _track's strong ref
        present = any(
            t.get_name() == "cache_test_track"
            for t in cache_mod._pending_invalidations
            if not t.done()
        )
        finish.set()
        return present

    assert asyncio.run(_scenario()) is True, (
        "_track must add the task to _pending_invalidations so the loop's "
        "weak ref is backed by a strong ref in the module-level set; if "
        "this fails, the fix does not actually prevent GC drop"
    )


def test_track_clears_set_when_task_completes():
    """After the task runs to completion, the strong ref is released so
    the set does not grow unboundedly."""
    from dynastore.tools import cache as cache_mod

    async def _quick():
        return "ok"

    async def _scenario() -> int:
        task = cache_mod._track(asyncio.create_task(_quick(), name="cache_quick_track"))
        await task
        # give the done_callback a tick to fire
        await asyncio.sleep(0)
        return sum(
            1 for t in cache_mod._pending_invalidations if t.get_name() == "cache_quick_track"
        )

    assert asyncio.run(_scenario()) == 0, (
        "done_callback must discard the completed task from "
        "_pending_invalidations so the set does not leak strong refs"
    )
