"""Unit tests for ``dynastore.tools.async_utils.LoopLocalSemaphore``.

Like the ``LoopLocalLock`` tests, these deliberately use ``asyncio.run(...)``
(a fresh event loop each call) because the whole point of
``LoopLocalSemaphore`` is correctness across *distinct* loops — the scenario a
raw module-level / singleton ``asyncio.Semaphore()`` fails.
"""
from __future__ import annotations

import asyncio

from dynastore.tools.async_utils import LoopLocalSemaphore

# Constructed at module scope on purpose: must not raise (no running loop at
# import) and must be safely reusable from every test's fresh loop below.
_SHARED = LoopLocalSemaphore(2)


def test_construct_without_running_loop_is_safe() -> None:
    # No running loop, no first use -> construction alone must not raise.
    sem = LoopLocalSemaphore(3)
    assert sem._value == 3


def test_caps_concurrency_within_one_loop() -> None:
    async def _run() -> int:
        sem = LoopLocalSemaphore(2)
        peak = 0
        live = 0

        async def worker() -> None:
            nonlocal peak, live
            async with sem:
                live += 1
                peak = max(peak, live)
                await asyncio.sleep(0.01)
                live -= 1

        await asyncio.gather(*(worker() for _ in range(6)))
        return peak

    # value=2 → never more than 2 concurrent holders.
    assert asyncio.run(_run()) == 2


def test_reusable_across_separate_event_loops() -> None:
    async def _use() -> bool:
        async with _SHARED:
            return True

    # A raw module-level asyncio.Semaphore() would bind to the first loop and
    # could raise "bound to a different event loop" on the second asyncio.run().
    # LoopLocalSemaphore keeps a per-loop semaphore, so both fresh loops succeed.
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_distinct_loops_get_distinct_semaphores() -> None:
    seen: list[int] = []

    async def _capture() -> None:
        async with _SHARED:
            seen.append(id(_SHARED._for_running_loop()))

    asyncio.run(_capture())
    asyncio.run(_capture())
    # Each fresh loop lazily created its own underlying semaphore object.
    assert len(seen) == 2 and seen[0] != seen[1]
