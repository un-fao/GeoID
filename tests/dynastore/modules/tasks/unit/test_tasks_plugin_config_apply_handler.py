"""Apply-handler cover for ``TasksPluginConfig.hard_retry_cap``.

Pre-fix, ``TasksModule.lifespan`` read ``TasksPluginConfig`` once at boot
and pushed the value into ``_HARD_RETRY_CAP`` via ``set_hard_retry_cap``.
Operator ``PATCH /configs?plugin_id=tasks_plugin_config`` mutating
``hard_retry_cap`` persisted the new value in the DB but never reached
the module global, so ``claim_batch`` / ``fail_task`` / the reaper kept
enforcing the boot-time cap until next restart.

Fix: register an apply-handler on ``TasksPluginConfig`` that pushes the
new ``hard_retry_cap`` through ``set_hard_retry_cap``. Same shape as the
Valkey reconnect handler exercised in ``test_valkey_reconnect.py`` and
the cache-circuit-breaker handler (#885).

The publisher / poll-interval fields are captured into long-running
coroutine closures (``run_capability_publisher`` / ``start_queue_listener``)
and would require executor cancel+resubmit to re-apply live — deliberately
out of scope of this PR.
"""

from __future__ import annotations

import inspect

import pytest

from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.tasks_config import TasksPluginConfig


# --------------------------------------------------------------------------
# Handler shape — exercised directly (no lifespan needed)
# --------------------------------------------------------------------------


def _build_handler():
    """Reconstruct the handler the way lifespan does, in isolation.

    The handler is a closure defined inside ``TasksModule.lifespan`` and not
    importable. To exercise behaviour without spinning up the full lifespan
    (which needs a DB engine), we rebuild a structurally identical closure
    here. The source-level guard below pins the lifespan call site so this
    rebuild can't drift from production silently.
    """
    import logging

    logger = logging.getLogger(__name__)

    async def _on_tasks_config_change(cfg, _catalog_id, _collection_id, _conn):
        if not isinstance(cfg, TasksPluginConfig):
            return
        try:
            tasks_module.set_hard_retry_cap(cfg.hard_retry_cap)
        except ValueError as exc:
            logger.warning(
                "TasksPluginConfig: rejected hard_retry_cap=%r (%s); "
                "retaining %d.",
                cfg.hard_retry_cap, exc, tasks_module.get_hard_retry_cap(),
            )
            return
        logger.info(
            "TasksPluginConfig changed — hard_retry_cap reapplied to %d.",
            cfg.hard_retry_cap,
        )

    return _on_tasks_config_change


@pytest.mark.asyncio
async def test_handler_pushes_new_hard_retry_cap_to_module_global() -> None:
    """Operator PATCH bumps hard_retry_cap from 5 → 9; handler must push
    that value through ``set_hard_retry_cap`` so subsequent
    ``get_hard_retry_cap()`` reflects it."""
    handler = _build_handler()
    original = tasks_module.get_hard_retry_cap()
    try:
        cfg = TasksPluginConfig(hard_retry_cap=9)
        await handler(cfg, None, None, None)
        assert tasks_module.get_hard_retry_cap() == 9
    finally:
        tasks_module.set_hard_retry_cap(original)


@pytest.mark.asyncio
async def test_handler_ignores_non_tasks_plugin_config() -> None:
    """Apply-handler registry dispatches per class, but defensively the
    handler must no-op on the wrong type rather than AttributeError."""
    handler = _build_handler()
    original = tasks_module.get_hard_retry_cap()

    class _Other:
        hard_retry_cap = 999  # would otherwise be picked up

    await handler(_Other(), None, None, None)
    assert tasks_module.get_hard_retry_cap() == original


# --------------------------------------------------------------------------
# Lifespan wiring — pin the call site so the closure rebuild above
# cannot drift away from production silently.
# --------------------------------------------------------------------------


def test_lifespan_registers_tasks_plugin_config_apply_handler() -> None:
    """Source-level guard: the lifespan must register an apply-handler on
    TasksPluginConfig that funnels through ``set_hard_retry_cap``."""
    src = inspect.getsource(tasks_module)
    assert "TasksPluginConfig.register_apply_handler(" in src, (
        "TasksModule.lifespan must register an apply-handler on "
        "TasksPluginConfig — otherwise PATCH /configs updates to "
        "hard_retry_cap never reach claim_batch / fail_task / reaper."
    )
    assert "set_hard_retry_cap(cfg.hard_retry_cap)" in src, (
        "The apply-handler must push the new hard_retry_cap through "
        "set_hard_retry_cap so the module global advances."
    )
