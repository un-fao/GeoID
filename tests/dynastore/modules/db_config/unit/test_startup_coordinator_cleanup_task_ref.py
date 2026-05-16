"""Regression test: ``_StartupCoordinator`` cleanup task must be strong-ref'd.

Background: ``_StartupCoordinator.run_once`` schedules a fire-and-forget
``_cleanup`` task that sleeps 5s and then pops the cached future from
``_tasks`` (so a later retry of the same key actually re-runs the
coroutine instead of getting a "still in progress" hit on a stale
future). The old code stored the task in a local ``cleanup_task_ref``
that went out of scope on the next ``return result`` — only the loop's
weak ref remained, so a GC sweep between the schedule and the
``asyncio.sleep(5)`` wake-up could drop the cleanup. Result: ``_tasks``
slowly accumulates stale futures indistinguishable from in-flight ones.

Fix: tasks are now registered against the class-level
``_StartupCoordinator._cleanup_tasks`` set, with a ``done_callback``
that discards on completion.

Test pins both halves: the class-level set exists and is used, and
scheduling a cleanup adds the task to the set until completion.
"""
from __future__ import annotations

import asyncio
import inspect

import pytest


def test_cleanup_tasks_set_exists_on_class():
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator

    assert hasattr(_StartupCoordinator, "_cleanup_tasks"), (
        "_StartupCoordinator must expose a class-level ``_cleanup_tasks`` "
        "set so the fire-and-forget _cleanup task has a strong reference "
        "while it sleeps; without it the loop's weak ref can be GC'd "
        "before the cache entry is popped from ``_tasks``."
    )
    assert isinstance(_StartupCoordinator._cleanup_tasks, set)


def test_run_once_source_registers_cleanup_in_set():
    """Source-level pin: the cleanup task must be added to the
    class-level ``_cleanup_tasks`` set immediately after creation, and
    the prior ``cleanup_task_ref`` local-only pattern must be gone."""
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator

    src = inspect.getsource(_StartupCoordinator.run_once)

    assert "cls._cleanup_tasks.add(cleanup_task)" in src, (
        "run_once must register the cleanup task against the class-level "
        "_cleanup_tasks set so the loop's weak ref is backed by a strong "
        "reference"
    )
    assert "cls._cleanup_tasks.discard" in src, (
        "run_once must wire add_done_callback(_cleanup_tasks.discard) so "
        "the set does not leak completed tasks"
    )
    assert "cleanup_task_ref = asyncio.create_task" not in src, (
        "regression: bare ``cleanup_task_ref = asyncio.create_task(...)`` "
        "local-only assignment is back; the local goes out of scope at "
        "the next ``return`` and only the loop's weak ref remains"
    )


def test_cleanup_task_lands_in_class_set_after_run_once():
    """Empirical: drive ``run_once`` once and verify a task lands in the
    class-level set before the 5s cleanup-sleep elapses."""
    from dynastore.modules.db_config.locking_tools import _StartupCoordinator

    # Use a fresh subclass so we don't pollute the global ``_tasks``/
    # ``_cleanup_tasks`` state shared with other tests.
    class _Local(_StartupCoordinator):
        _tasks = {}
        _lock = asyncio.Lock()
        _cleanup_tasks = set()

    async def _payload() -> str:
        return "ok"

    async def _scenario() -> int:
        result = await _Local.run_once("k1", _payload)
        assert result == "ok"
        # cleanup task scheduled, still sleeping
        active = sum(
            1 for t in _Local._cleanup_tasks if not t.done()
        )
        # Drain so we don't leak pending tasks across the test boundary —
        # cancelling is fine because the function returned already and
        # nothing else depends on the cleanup popping ``_tasks``.
        for t in list(_Local._cleanup_tasks):
            t.cancel()
        await asyncio.sleep(0)
        return active

    assert asyncio.run(_scenario()) >= 1, (
        "run_once must add at least one active cleanup task to the "
        "class-level _cleanup_tasks set; if zero, the strong reference "
        "is missing and the cleanup can be GC'd mid-sleep"
    )
