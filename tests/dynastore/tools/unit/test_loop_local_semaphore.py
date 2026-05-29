"""Unit tests for ``dynastore.tools.async_utils.LoopLocalSemaphore``.

Like the ``LoopLocalLock`` tests, these deliberately use ``asyncio.run(...)``
(a fresh event loop each call) rather than a single pytest-asyncio loop: the
whole point of ``LoopLocalSemaphore`` is correctness across *distinct* loops —
the scenario a raw module-level ``asyncio.Semaphore()`` fails with
"bound to a different event loop".
"""
from __future__ import annotations

import asyncio

from dynastore.tools.async_utils import LoopLocalSemaphore

# Constructed at module scope on purpose: this must not raise (no running loop
# at import) and must be safely reusable from every test's fresh loop below.
_SHARED = LoopLocalSemaphore(2)


def test_construct_without_running_loop_is_safe() -> None:
    # No running loop at construction — must not raise.
    sem = LoopLocalSemaphore(5)
    assert sem._maxsize == 5


def test_caps_concurrency_within_one_loop() -> None:
    async def _run() -> int:
        sem = LoopLocalSemaphore(2)
        current = 0
        peak = 0

        async def worker() -> None:
            nonlocal current, peak
            async with sem:
                current += 1
                peak = max(peak, current)
                await asyncio.sleep(0.01)
                current -= 1

        await asyncio.gather(*(worker() for _ in range(6)))
        return peak

    # Six workers, maxsize 2 -> never more than 2 in the critical section.
    assert asyncio.run(_run()) == 2


def test_reusable_across_separate_event_loops() -> None:
    async def _use() -> bool:
        async with _SHARED:
            return True

    # A raw module-level asyncio.Semaphore() would bind to the first loop and
    # raise "bound to a different event loop" on the second asyncio.run().
    # LoopLocalSemaphore keeps a per-loop semaphore, so both fresh loops succeed.
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_each_loop_gets_its_own_full_count() -> None:
    # Exhaust all slots on one loop, release, then a brand-new loop must start
    # with a fresh full-count semaphore (no leaked permits across loops).
    async def _exhaust_and_release() -> bool:
        async with _SHARED:  # 1 of 2
            async with _SHARED:  # 2 of 2
                return True

    assert asyncio.run(_exhaust_and_release()) is True
    # Fresh loop — must not raise and must again allow 2 concurrent acquires.
    assert asyncio.run(_exhaust_and_release()) is True


def test_release_returns_slot_for_reuse_within_loop() -> None:
    async def _run() -> int:
        sem = LoopLocalSemaphore(1)
        order: list[int] = []

        async def worker(tag: int) -> None:
            async with sem:
                order.append(tag)
                await asyncio.sleep(0.001)

        # maxsize 1 serialises: each must release before the next acquires.
        await asyncio.gather(worker(1), worker(2), worker(3))
        return len(order)

    assert asyncio.run(_run()) == 3
