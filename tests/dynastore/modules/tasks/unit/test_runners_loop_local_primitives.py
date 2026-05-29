"""Regression: the import-time task-runner singletons hold loop-local primitives.

``capability_map`` (module-level singleton) and ``BackgroundRunner`` (registered
at import via ``register_default_runners()``) used to hold a raw
``asyncio.Lock()`` / ``asyncio.Semaphore()`` created in ``__init__`` — bound to
the first loop that awaited them and unusable from another loop. They now use
``LoopLocalLock`` / ``LoopLocalSemaphore`` (#1640). These tests exercise the
singletons' primitives across *distinct* event loops; the old code could raise
``RuntimeError: ... bound to a different event loop``.
"""
from __future__ import annotations

import asyncio

from dynastore.modules.tasks.runners import BackgroundRunner, capability_map
from dynastore.tools.async_utils import LoopLocalLock, LoopLocalSemaphore


def test_capability_map_singleton_lock_is_loop_local() -> None:
    assert isinstance(capability_map._lock, LoopLocalLock)

    async def _use_lock() -> bool:
        async with capability_map._lock:
            return True

    # Two fresh loops reusing the same module-level singleton's lock.
    assert asyncio.run(_use_lock()) is True
    assert asyncio.run(_use_lock()) is True


def test_background_runner_singleton_semaphore_is_loop_local() -> None:
    runner = BackgroundRunner()
    assert isinstance(runner._semaphore, LoopLocalSemaphore)

    async def _use_semaphore() -> bool:
        async with runner._semaphore:
            return True

    # Reuse the same instance's semaphore across two distinct loops.
    assert asyncio.run(_use_semaphore()) is True
    assert asyncio.run(_use_semaphore()) is True
