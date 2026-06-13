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

"""Unit tests for ``tasks_module._run_proactive_capability_sweep`` (#524 PR B).

Covers:
- Clean shutdown when ``shutdown_event`` is set up-front.
- Distinct-pending query failure does not crash the loop.
- ``sweep_dead_capability_rows`` failure for one capability does not prevent
  subsequent capabilities from being swept.
- Nonzero sweep result emits the structured INFO line.
- Shutdown during inner iteration short-circuits cleanly.

The dispatcher's ``sweep_dead_capability_rows`` and the SQL DISTINCT query
are mocked — both have their own coverage upstream (reactive_reaper tests +
the new PG integration test, respectively).
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import ANY, AsyncMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import (
    _distinct_pending_capability_ids,
    _run_proactive_capability_sweep,
)


@pytest.mark.asyncio
async def test_sweep_exits_immediately_on_shutdown():
    """If shutdown_event is already set the loop returns without scanning."""
    shutdown = asyncio.Event()
    shutdown.set()
    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=AsyncMock(),
    ) as distinct_mock:
        await asyncio.wait_for(
            _run_proactive_capability_sweep(
                engine=object(),
                schema="tasks",
                shutdown_event=shutdown,
                interval_s=0.01,
            ),
            timeout=1.0,
        )
    distinct_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_logs_dlq_count_on_nonzero_result(caplog):
    """When sweep_dead_capability_rows returns N>0 the loop emits a
    structured INFO line so a log-based metric can pick it up."""
    shutdown = asyncio.Event()
    caplog.set_level(logging.INFO, logger="dynastore.modules.tasks.tasks_module")

    sweep = AsyncMock(return_value=4)
    distinct = AsyncMock(return_value=["dead_cap_1"])

    async def stop_after_first_pass():
        await asyncio.sleep(0.05)
        shutdown.set()

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.dispatcher.sweep_dead_capability_rows",
        new=sweep,
    ):
        await asyncio.gather(
            _run_proactive_capability_sweep(
                engine=object(),
                schema="tasks",
                shutdown_event=shutdown,
                interval_s=0.01,
            ),
            stop_after_first_pass(),
        )

    sweep.assert_awaited_with(ANY, "dead_cap_1", task_type="index_propagation")
    info_lines = [
        r.message for r in caplog.records if r.levelno == logging.INFO
    ]
    assert any(
        "proactive_sweep: DLQ'd 4 row(s)" in m and "dead_cap_1" in m
        for m in info_lines
    ), f"missing proactive_sweep info line in: {info_lines}"


@pytest.mark.asyncio
async def test_sweep_continues_when_distinct_query_fails(caplog):
    """A failing DISTINCT query for one task_type must be logged and the
    loop must keep going (does not crash the entire sweeper)."""
    shutdown = asyncio.Event()
    caplog.set_level(logging.WARNING, logger="dynastore.modules.tasks.tasks_module")

    distinct = AsyncMock(side_effect=RuntimeError("boom"))

    async def stop_after_first_pass():
        await asyncio.sleep(0.05)
        shutdown.set()

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ):
        await asyncio.gather(
            _run_proactive_capability_sweep(
                engine=object(),
                schema="tasks",
                shutdown_event=shutdown,
                interval_s=0.01,
            ),
            stop_after_first_pass(),
        )

    warn_lines = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "proactive_sweep: distinct query failed" in m and "boom" in m
        for m in warn_lines
    ), f"missing distinct-failure warning in: {warn_lines}"


@pytest.mark.asyncio
async def test_sweep_continues_when_one_capability_sweep_fails(caplog):
    """A sweep_dead_capability_rows raising for one (cap, task_type) pair
    must not abort the pass — subsequent cap_ids still get a chance."""
    shutdown = asyncio.Event()
    caplog.set_level(logging.WARNING, logger="dynastore.modules.tasks.tasks_module")

    distinct = AsyncMock(return_value=["bad_cap", "good_cap"])

    sweep_results = {"bad_cap": RuntimeError("ouch"), "good_cap": 2}

    async def sweep_side_effect(_engine, cap_id, *, task_type):
        result = sweep_results[cap_id]
        if isinstance(result, Exception):
            raise result
        return result

    sweep = AsyncMock(side_effect=sweep_side_effect)

    async def stop_after_first_pass():
        await asyncio.sleep(0.05)
        shutdown.set()

    with patch(
        "dynastore.modules.tasks.tasks_module._distinct_pending_capability_ids",
        new=distinct,
    ), patch(
        "dynastore.modules.tasks.dispatcher.sweep_dead_capability_rows",
        new=sweep,
    ):
        await asyncio.gather(
            _run_proactive_capability_sweep(
                engine=object(),
                schema="tasks",
                shutdown_event=shutdown,
                interval_s=0.01,
            ),
            stop_after_first_pass(),
        )

    # Both cap_ids must have been tried — bad_cap fails, good_cap succeeds.
    swept_caps = [call.args[1] for call in sweep.await_args_list]
    assert "bad_cap" in swept_caps and "good_cap" in swept_caps, (
        f"both caps should have been attempted; got {swept_caps}"
    )
    warn_lines = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "proactive_sweep: sweep failed" in m and "bad_cap" in m
        for m in warn_lines
    ), f"missing sweep-failure warning in: {warn_lines}"


@pytest.mark.asyncio
async def test_distinct_query_filters_pending_retry_zero_with_min_age():
    """The SQL DISTINCT query must filter on status=PENDING + retry_count=0
    + non-null inputs key + age threshold. Verified by inspecting the SQL
    text passed to DQLQuery."""
    captured_sqls = []

    class _FakeQuery:
        def __init__(self, sql, *args, **kwargs):
            captured_sqls.append(sql)
            self._sql = sql

        async def execute(self, conn, **kwargs):
            return [{"cap_id": "x"}, {"cap_id": "y"}, {"cap_id": None}]

    class _FakeTxn:
        async def __aenter__(self):
            return object()
        async def __aexit__(self, *_):
            return False

    with patch(
        "dynastore.modules.tasks.tasks_module.DQLQuery", _FakeQuery,
    ), patch(
        "dynastore.modules.tasks.tasks_module.managed_transaction",
        return_value=_FakeTxn(),
    ):
        out = await _distinct_pending_capability_ids(
            engine=object(),
            schema="tasks",
            task_type="index_propagation",
            inputs_key="indexer_id",
            min_age_s=300.0,
            sample_limit=50,
        )

    assert out == ["x", "y"], f"None must be filtered; got {out}"
    assert len(captured_sqls) == 1
    sql = captured_sqls[0]
    assert "status = 'PENDING'" in sql
    assert "retry_count = 0" in sql
    assert "inputs->>'indexer_id'" in sql
    assert "make_interval(secs => :min_age_s)" in sql
