"""Regression: task runners must not hold import-time loop-bound primitives.

``capability_map`` (module-level singleton) and ``BackgroundRunner`` (registered
at import via ``register_default_runners()``) are both constructed before any
event loop exists. Their internal synchronisation primitives therefore must be
per-running-loop proxies (``LoopLocalLock`` / ``LoopLocalSemaphore``), not raw
``asyncio.Lock``/``Semaphore`` — otherwise the primitive binds to the first loop
that awaits it and raises ``RuntimeError: ... bound to a different event loop``
when reused from another loop (a fresh loop per test, or any multi-loop runtime).

See issue #1640 / #1628 (import-time singleton antipattern sweep).
"""
from __future__ import annotations

import asyncio

from dynastore.modules.tasks.runners import BackgroundRunner, capability_map
from dynastore.tools.async_utils import LoopLocalLock, LoopLocalSemaphore

# Instantiated outside any running loop — the exact import-time scenario the
# real ``register_default_runners()`` creates. Reused across the fresh loops
# spun up by each ``asyncio.run`` below.
_RUNNER = BackgroundRunner()


def test_capability_map_lock_is_loop_local() -> None:
    assert isinstance(capability_map._lock, LoopLocalLock)


def test_background_runner_semaphore_is_loop_local() -> None:
    assert isinstance(_RUNNER._semaphore, LoopLocalSemaphore)


def test_capability_map_lock_reusable_across_loops() -> None:
    async def _use() -> bool:
        async with capability_map._lock:
            return True

    # A raw asyncio.Lock() on the singleton would raise on the second loop.
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_background_runner_semaphore_reusable_across_loops() -> None:
    async def _use() -> bool:
        async with _RUNNER._semaphore:
            return True

    # A raw asyncio.Semaphore() on the import-time runner would raise on the
    # second loop ("bound to a different event loop").
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_background_runner_semaphore_still_caps_concurrency() -> None:
    # The cap must survive the move to LoopLocalSemaphore: a runner built with
    # max_concurrency=2 admits at most 2 concurrent critical sections per loop.
    runner = BackgroundRunner()
    runner._max_concurrency = 2
    runner._semaphore = LoopLocalSemaphore(2)

    async def _run() -> int:
        current = 0
        peak = 0

        async def worker() -> None:
            nonlocal current, peak
            async with runner._semaphore:
                current += 1
                peak = max(peak, current)
                await asyncio.sleep(0.01)
                current -= 1

        await asyncio.gather(*(worker() for _ in range(6)))
        return peak

    assert asyncio.run(_run()) == 2
