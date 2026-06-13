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

"""Unit tests for ``dynastore.tools.async_utils.LoopLocalLock``.

The tests deliberately use ``asyncio.run(...)`` (a fresh event loop each call)
rather than a single pytest-asyncio loop, because the whole point of
``LoopLocalLock`` is correctness across *distinct* loops — the scenario a raw
module-level ``asyncio.Lock()`` fails.
"""
from __future__ import annotations

import asyncio

from dynastore.tools.async_utils import LoopLocalLock

# Constructed at module scope on purpose: this must not raise (no running loop
# at import) and must be safely reusable from every test's fresh loop below.
_SHARED = LoopLocalLock()


def test_construct_without_running_loop_is_safe() -> None:
    lock = LoopLocalLock()
    # No running loop -> locked() reports False instead of raising.
    assert lock.locked() is False


def test_serialises_concurrent_tasks_within_one_loop() -> None:
    async def _run() -> list[str]:
        lock = LoopLocalLock()
        events: list[str] = []

        async def worker(tag: int) -> None:
            async with lock:
                events.append(f"enter{tag}")
                await asyncio.sleep(0.01)
                events.append(f"exit{tag}")

        await asyncio.gather(worker(1), worker(2))
        return events

    events = asyncio.run(_run())
    # Critical sections must not interleave: each enter is immediately
    # followed by its own exit.
    assert len(events) == 4
    for i in (0, 2):
        assert events[i].startswith("enter")
        assert events[i + 1] == events[i].replace("enter", "exit")


def test_reusable_across_separate_event_loops() -> None:
    async def _use() -> bool:
        async with _SHARED:
            return True

    # A raw module-level asyncio.Lock() would bind to the first loop and raise
    # "bound to a different event loop" on the second asyncio.run(). LoopLocalLock
    # keeps a per-loop lock, so both fresh loops succeed.
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_locked_reflects_held_state() -> None:
    async def _run() -> None:
        lock = LoopLocalLock()
        assert lock.locked() is False
        async with lock:
            assert lock.locked() is True
        assert lock.locked() is False

    asyncio.run(_run())


def test_isolation_between_distinct_loops_no_cross_binding() -> None:
    # Acquire/release on one loop, then acquire again on a brand-new loop.
    async def _acquire_release() -> None:
        async with _SHARED:
            await asyncio.sleep(0)

    asyncio.run(_acquire_release())
    # Fresh loop — must not raise RuntimeError about a different loop.
    asyncio.run(_acquire_release())
