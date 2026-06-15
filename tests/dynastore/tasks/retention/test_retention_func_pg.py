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

"""Integration tests: execute the tasks-partition retention function against
a real PostgreSQL instance.

These tests require a running PostgreSQL at the URL exported by DATABASE_URL
(default: postgresql://testuser:testpassword@localhost:54320/gis_dev).
They are skipped automatically when the DB is unreachable.

What is proven here:
- The retention function CREATEs without error (body compiles).
- The function EXECUTEs without error — specifically it does NOT raise
  "unit 'daily' not recognized" (regression for #1998).
- The function drops monthly partitions older than 1 month when called.
- The function deletes stale rows from tasks_default (the DEFAULT partition
  drain added in #1998).

Run in isolation:
    PYTHONPATH=packages/core/src \\
      .venv/bin/python -m pytest \\
      tests/dynastore/tasks/retention/test_retention_func_pg.py \\
      -q -p no:cacheprovider -n0
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _provision_retention_func(conn, schema: str) -> None:
    """CREATE OR REPLACE the retention function in *schema*."""
    from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_RETENTION_FUNC_DDL

    # Render exactly as production does: DDLQuery substitutes {schema} via
    # str.replace, NOT str.format. Using .format here would (a) collapse the
    # regex's single braces \d{4} into positional fields and raise, and (b)
    # mask the doubled-brace bug fixed in the DDL — the literal "\d{{4}}" that
    # str.replace leaves intact silently disabled monthly retention.
    sql = GLOBAL_TASKS_RETENTION_FUNC_DDL.replace("{schema}", schema)
    await conn.execute(sql)  # type: ignore[attr-defined]


async def _call_retention_func(conn, schema: str) -> None:
    """CALL the retention function in *schema* — raises on any PL/pgSQL error."""
    await conn.execute(  # type: ignore[attr-defined]
        f'SELECT "{schema}"."maintain_partitions_{schema}_tasks"()'
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retention_func_creates_and_executes(retention_schema, async_conn):
    """Retention function must CREATE and EXECUTE without raising.

    Regression guard for #1998: the previous body contained
    date_trunc('daily', ...) which is invalid PostgreSQL and caused
    every invocation to fail at runtime with
    "unit 'daily' not recognized for type timestamp with time zone".
    """
    schema = retention_schema
    conn = async_conn

    # Must not raise on CREATE
    await _provision_retention_func(conn, schema)

    # Must not raise on EXECUTE — this is the key regression assertion.
    # With the 'daily' bug, this line raises asyncpg.exceptions.InvalidParameterValueError.
    await _call_retention_func(conn, schema)


@pytest.mark.asyncio
async def test_retention_func_drops_old_monthly_partition(retention_schema, async_conn):
    """Retention function must DROP a monthly partition older than 1 month."""
    schema = retention_schema
    conn = async_conn

    # Create a monthly partition for 3 months ago so it is older than cutoff.
    old_dt = datetime.now(tz=timezone.utc).replace(day=1)
    from dateutil.relativedelta import relativedelta  # type: ignore[import-untyped]

    three_months_ago = old_dt - relativedelta(months=3)
    part_name = f"tasks_{three_months_ago.strftime('%Y_%m')}"
    start = three_months_ago.replace(day=1)
    from dateutil.relativedelta import relativedelta as rd  # noqa: PLC0415
    end = start + rd(months=1)

    await conn.execute(  # type: ignore[attr-defined]
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{part_name}"
        PARTITION OF "{schema}".tasks
        FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');
        """
    )

    await _provision_retention_func(conn, schema)
    await _call_retention_func(conn, schema)

    # The partition should no longer exist after the retention run.
    exists = await conn.fetchval(  # type: ignore[attr-defined]
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
        )
        """,
        schema,
        part_name,
    )
    assert not exists, (
        f"Retention function should have dropped {schema}.{part_name}"
    )


@pytest.mark.asyncio
async def test_retention_func_drains_tasks_default(retention_schema, async_conn):
    """Retention function must DELETE stale rows from tasks_default.

    Rows with timestamps older than 1 month that ended up in the DEFAULT
    partition (e.g. due to clock-skew) must be removed.
    """
    schema = retention_schema
    conn = async_conn

    # Insert a row with a timestamp well outside the monthly partition range
    # so it lands in tasks_default.
    old_ts = datetime(2020, 1, 15, tzinfo=timezone.utc)
    task_id = uuid.uuid4()
    await conn.execute(  # type: ignore[attr-defined]
        f'INSERT INTO "{schema}".tasks (task_id, timestamp) VALUES ($1, $2)',
        task_id,
        old_ts,
    )

    # Verify it actually landed in tasks_default.
    count_before = await conn.fetchval(  # type: ignore[attr-defined]
        f'SELECT COUNT(*) FROM "{schema}".tasks_default'
    )
    assert count_before == 1, "Row should have landed in tasks_default"

    await _provision_retention_func(conn, schema)
    await _call_retention_func(conn, schema)

    count_after = await conn.fetchval(  # type: ignore[attr-defined]
        f'SELECT COUNT(*) FROM "{schema}".tasks_default'
    )
    assert count_after == 0, (
        "Retention function should have deleted the stale row from tasks_default"
    )


@pytest.mark.asyncio
async def test_retention_func_announces_prune_at_log_level(retention_schema, async_conn):
    """Retention must announce the prune at LOG level (#2106) so partition
    deletion is observable, not silent.

    The per-partition message in the function is NOTICE, which the server log
    suppresses at the default log_min_messages=WARNING.  The pre-flight summary
    is RAISE LOG (written to the server log at the default).  We raise this
    connection's client_min_messages to 'log' so asyncpg surfaces it to the
    log listener and we can assert on its content (count + partition name).
    """
    from dateutil.relativedelta import relativedelta  # type: ignore[import-untyped]

    schema = retention_schema
    conn = async_conn

    three_months_ago = datetime.now(tz=timezone.utc).replace(day=1) - relativedelta(months=3)
    part_name = f"tasks_{three_months_ago.strftime('%Y_%m')}"
    start = three_months_ago.replace(day=1)
    end = start + relativedelta(months=1)
    await conn.execute(  # type: ignore[attr-defined]
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{part_name}" '
        f'PARTITION OF "{schema}".tasks '
        f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');"
    )

    captured: list[str] = []
    conn.add_log_listener(lambda _c, m: captured.append(getattr(m, "message", str(m))))  # type: ignore[attr-defined]
    # RAISE LOG reaches the client only when client_min_messages <= log.
    await conn.execute("SET client_min_messages TO 'log'")  # type: ignore[attr-defined]

    await _provision_retention_func(conn, schema)
    await _call_retention_func(conn, schema)

    preflight = [m for m in captured if "partition retention" in m and part_name in m]
    assert preflight, (
        f"expected a pre-flight LOG line naming {part_name}; captured={captured!r}"
    )
    assert "1 monthly partition" in preflight[0], preflight[0]


@pytest.mark.asyncio
async def test_retention_func_quiet_when_nothing_to_prune(retention_schema, async_conn):
    """No pre-flight LOG when there is nothing older than the cutoff — the
    summary must not become per-tick noise on a healthy queue."""
    schema = retention_schema
    conn = async_conn

    captured: list[str] = []
    conn.add_log_listener(lambda _c, m: captured.append(getattr(m, "message", str(m))))  # type: ignore[attr-defined]
    await conn.execute("SET client_min_messages TO 'log'")  # type: ignore[attr-defined]

    # Fresh schema: only the parent + DEFAULT partition exist, nothing aged out.
    await _provision_retention_func(conn, schema)
    await _call_retention_func(conn, schema)

    assert not [m for m in captured if "partition retention" in m], (
        f"expected no pre-flight LOG when nothing to prune; captured={captured!r}"
    )
