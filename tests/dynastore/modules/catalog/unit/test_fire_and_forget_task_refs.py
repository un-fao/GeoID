"""Regression test: fire-and-forget tasks must hold a strong reference.

Background: four sites used to call ``asyncio.create_task(coro)`` and throw
the returned ``Task`` away — the event loop only keeps weak references, so
the asyncio docs explicitly warn the task can disappear mid-execution and
its work is silently dropped (cf. Python 3.12 ``asyncio.create_task``
documentation, "Important" note).

* ``event_service.emit``           — async event listener dispatch
* ``event_service.emit_detached``  — detached async listener dispatch
* ``asset_service._fan_out_asset_writes``  — async asset secondary write
* ``item_service._fan_out_secondary_writes`` — async item secondary write

In long-lived web workers under GC pressure, a dropped event listener
silently skips cache-invalidation/notification side effects, and a dropped
secondary write silently skips replication into ES/PG mirrors.

Fix: all four sites now go through ``dynastore.modules.concurrency
.run_in_background``, which adds the task to a module-level set
(``_background_tasks``) so the strong reference outlives the task.

Source-level guards below pin the corrected pattern so a future refactor
that re-introduces bare ``asyncio.create_task`` fails loudly.
"""
from __future__ import annotations

import inspect


def _read_source(qualname: str) -> str:
    """Import the function/method at ``qualname`` and return its source."""
    module_path, attr_path = qualname.split(":", 1)
    mod = __import__(module_path, fromlist=["*"])
    obj = mod
    for name in attr_path.split("."):
        obj = getattr(obj, name)
    return inspect.getsource(obj)


def test_event_service_emit_uses_run_in_background():
    src = _read_source(
        "dynastore.modules.catalog.event_service:EventService.emit"
    )
    assert "run_in_background(" in src, (
        "EventService.emit must dispatch async listeners via run_in_background "
        "so the task is strong-referenced. Bare asyncio.create_task can be "
        "GC'd mid-execution (asyncio docs §Important) and silently drop the "
        "listener's work."
    )
    assert "asyncio.create_task(listener" not in src, (
        "regression: bare asyncio.create_task(listener…) reintroduced — "
        "weak-ref'd tasks can vanish mid-flight"
    )


def test_event_service_emit_detached_uses_run_in_background():
    src = _read_source(
        "dynastore.modules.catalog.event_service:EventService.emit_detached"
    )
    assert "run_in_background(" in src, (
        "EventService.emit_detached must dispatch listeners via "
        "run_in_background; bare asyncio.create_task is weak-ref'd"
    )
    assert "asyncio.create_task(listener" not in src


def test_asset_service_fan_out_uses_run_in_background():
    src = _read_source(
        "dynastore.modules.catalog.asset_service:AssetService._fan_out_asset_writes"
    )
    assert "run_in_background(" in src, (
        "_fan_out_asset_writes must schedule async secondary writes via "
        "run_in_background so a GC pass cannot silently drop the secondary "
        "ES/PG replication"
    )
    assert "asyncio.create_task(" not in src, (
        "regression: bare asyncio.create_task reintroduced in async-write "
        "fan-out — weak-ref'd tasks can vanish mid-flight"
    )


def test_item_service_fan_out_uses_run_in_background():
    src = _read_source(
        "dynastore.modules.catalog.item_service:ItemService._fan_out_to_secondary_drivers"
    )
    assert "run_in_background(" in src, (
        "_fan_out_to_secondary_drivers must schedule async secondary writes "
        "via run_in_background"
    )
    assert "asyncio.create_task(" not in src


def test_run_in_background_keeps_strong_reference():
    """Empirical pin: ``run_in_background`` must keep a strong reference
    in ``_background_tasks`` until the task completes; otherwise the
    above fixes would not actually solve the GC-drop bug they target."""
    import asyncio

    from dynastore.modules import concurrency

    started = asyncio.Event()
    finish = asyncio.Event()

    async def _slow():
        started.set()
        await finish.wait()

    async def _scenario() -> bool:
        task = concurrency.run_in_background(_slow(), name="test_strong_ref")
        await started.wait()
        # Drop the local reference. If run_in_background were not keeping a
        # strong reference internally, only the loop's weak ref would remain
        # and a GC sweep could collect the task. We verify by membership in
        # the module-level tracking set.
        del task
        present = any(
            t.get_name() == "test_strong_ref"
            for t in concurrency._background_tasks
            if not t.done()
        )
        finish.set()
        return present

    assert asyncio.run(_scenario()) is True, (
        "run_in_background must hold a strong reference in "
        "concurrency._background_tasks; if this assertion fails, the "
        "fix at the four call sites does not actually prevent GC drop"
    )
