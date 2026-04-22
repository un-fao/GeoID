"""Dispatcher loop resilience under startup / DB-reset races.

Regression guard for production error stream (2026-04-22) where a
dev-compose db-reset race caused the dispatcher to log-spam:

    Dispatcher: Unexpected error: Database error (42P01)
    (Details: relation "tasks.tasks" does not exist)

The dispatcher now catches `TableNotFoundError` explicitly, logs at
WARNING (not ERROR), and backs off longer so the subsystem quietly
waits for TasksModule.lifespan to re-create the schema.
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.modules.tasks import dispatcher as dispatcher_mod


@pytest.mark.asyncio
async def test_dispatcher_downgrades_tasktable_missing_to_warning(caplog):
    """A TableNotFoundError from claim_batch must not crash nor ERROR-log."""
    shutdown = asyncio.Event()
    call_count = {"n": 0}

    async def fake_wait_for(*_args, **_kwargs):
        return True  # pretend we got a NEW_TASK_QUEUED signal

    async def fake_claim(*_args, **_kwargs):
        call_count["n"] += 1
        # Set shutdown on the 2nd call so the WARNING fires (the handler
        # checks shutdown_event BEFORE logging; we want the warning path
        # exercised, so the 1st call raises while shutdown is still unset).
        if call_count["n"] >= 2:
            shutdown.set()
        raise TableNotFoundError("relation \"tasks.tasks\" does not exist")

    heartbeat = AsyncMock()

    async def fast_sleep(_delay):
        # Collapse the 10s back-off so the test completes quickly.
        return

    with patch.object(dispatcher_mod, "BatchedHeartbeat", return_value=heartbeat), \
         patch.object(dispatcher_mod.signal_bus, "wait_for", side_effect=fake_wait_for), \
         patch("dynastore.modules.tasks.tasks_module.claim_batch", side_effect=fake_claim), \
         patch("dynastore.modules.tasks.runners.capability_map") as cap_map, \
         patch.object(dispatcher_mod.asyncio, "sleep", side_effect=fast_sleep):
        cap_map.refresh = AsyncMock()
        cap_map.async_types = ["elasticsearch_index"]
        cap_map.sync_types = ["elasticsearch_index"]

        caplog.set_level(logging.WARNING)

        await asyncio.wait_for(
            dispatcher_mod.run_dispatcher(
                engine=object(),  # sentinel — claim_batch is patched
                schema=None,
                shutdown_event=shutdown,
                signal_timeout=0.01,
            ),
            timeout=5.0,
        )

    # No ERROR-level records from the dispatcher for the missing-table case.
    errors = [
        r for r in caplog.records
        if r.levelno >= logging.ERROR and r.name == "dynastore.modules.tasks.dispatcher"
    ]
    assert errors == [], f"expected no ERROR logs, got: {[(r.levelname, r.message) for r in errors]}"

    # And there must be a WARNING from the dispatcher explaining the transient
    # table-missing state.
    warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "dynastore.modules.tasks.dispatcher"
        and "tasks table not yet available" in r.getMessage()
    ]
    assert warnings, (
        "expected WARNING explaining the TableNotFoundError; got records: "
        f"{[(r.levelname, r.name, r.getMessage()) for r in caplog.records]}"
    )
