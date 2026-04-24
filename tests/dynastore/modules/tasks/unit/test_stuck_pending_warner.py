"""Unit tests for ``tasks_module._warn_stuck_pending_tasks``.

The warner is a periodic read-only scan that logs WARN lines for tasks that
have been PENDING with retry_count=0 for too long — typically caused by a
TaskRoutingConfig typo or a service that should claim them but isn't deployed.

We mock the SQL execution so the test doesn't need a real PG. The contract:
- when ``shutdown_event`` fires immediately, the loop returns cleanly.
- when the query returns rows, each row produces one WARN log line.
- when the query raises, the warner logs and continues (doesn't crash).
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import _warn_stuck_pending_tasks


@pytest.mark.asyncio
async def test_warner_exits_immediately_on_shutdown(caplog):
    """If shutdown_event is already set, the loop returns without scanning."""
    shutdown = asyncio.Event()
    shutdown.set()
    caplog.set_level(logging.WARNING)
    with patch.object(
        __import__("dynastore.modules.tasks.tasks_module", fromlist=["DQLQuery"]),
        "DQLQuery",
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01,
            ),
            timeout=1.0,
        )


@pytest.mark.asyncio
async def test_warner_logs_one_line_per_stuck_row(caplog):
    """Each stuck row from the query produces one WARN log line."""
    shutdown = asyncio.Event()
    fake_rows = [
        {"task_id": "abc-1", "task_type": "elasticsearch_index",
         "schema_name": "s_x", "age_s": 999.0},
        {"task_id": "abc-2", "task_type": "tile_preseed",
         "schema_name": "s_y", "age_s": 1234.5},
    ]

    async def _fake_execute(*args, **kwargs):
        # First call returns rows then sets shutdown so the loop exits next pass.
        shutdown.set()
        return fake_rows

    fake_query = AsyncMock()
    fake_query.execute = _fake_execute

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    caplog.set_level(logging.WARNING)
    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery",
        return_value=fake_query,
    ), patch(
        "dynastore.modules.tasks.tasks_module.managed_transaction",
        _fake_managed_transaction,
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01, min_age_s=10.0,
            ),
            timeout=2.0,
        )

    warn_lines = [r.message for r in caplog.records if "stuck-pending: task" in r.message]
    assert len(warn_lines) == 2
    assert any("abc-1" in m and "elasticsearch_index" in m for m in warn_lines)
    assert any("abc-2" in m and "tile_preseed" in m for m in warn_lines)


@pytest.mark.asyncio
async def test_warner_swallows_query_errors(caplog):
    """A failing query is logged but doesn't crash the loop."""
    shutdown = asyncio.Event()

    call_count = {"n": 0}

    async def _failing_execute(*args, **kwargs):
        call_count["n"] += 1
        shutdown.set()  # exit after this scan
        raise RuntimeError("PG died")

    fake_query = AsyncMock()
    fake_query.execute = _failing_execute

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    caplog.set_level(logging.WARNING)
    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery",
        return_value=fake_query,
    ), patch(
        "dynastore.modules.tasks.tasks_module.managed_transaction",
        _fake_managed_transaction,
    ):
        await asyncio.wait_for(
            _warn_stuck_pending_tasks(
                engine=object(), schema="tasks", shutdown_event=shutdown,
                interval_s=0.01,
            ),
            timeout=2.0,
        )

    assert call_count["n"] == 1
    # The error message is logged at WARNING level — the loop survived.
    assert any(
        "stuck-pending warner: scan failed" in r.message for r in caplog.records
    )
