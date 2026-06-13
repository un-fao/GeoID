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

"""Maintenance supervisor pg_cron resilience — startup must not abort when pg_cron is absent.

Regression guard for the dev outage where a database without the optional
``pg_cron`` extension made the maintenance flow crash at startup.

After #1927 all cron-registration logic was removed from ``EventsModule`` and
replaced by the leader-elected ``MaintenanceSupervisor``.  The entry point for
pg_cron interaction is now
``dynastore.modules.catalog.maintenance_supervisor.unschedule_superseded_cron_jobs``,
which:

- Returns 0 immediately when pg_cron is absent (no query executed).
- Unschedules legacy cron jobs when pg_cron is present.
- Propagates any DB error to the caller (``CatalogModule.lifespan`` wraps the
  call in ``try/except Exception`` so failures never abort startup).

These tests exercise ``unschedule_superseded_cron_jobs`` directly with a mocked
transaction; no DB required.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _patch_managed_txn(conn):
    @asynccontextmanager
    async def _fake_txn(_engine):
        yield conn

    return patch(
        "dynastore.modules.catalog.maintenance_supervisor.managed_transaction",
        side_effect=_fake_txn,
    )


@pytest.mark.asyncio
async def test_skips_and_does_not_raise_when_pg_cron_absent():
    """pg_cron missing → returns 0 immediately, no unschedule query executed."""
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")

    with _patch_managed_txn(conn), patch(
        "dynastore.modules.catalog.maintenance_supervisor.check_extension_exists",
        new=AsyncMock(return_value=False),
    ):
        result = await unschedule_superseded_cron_jobs(engine)

    assert result == 0


@pytest.mark.asyncio
async def test_unschedules_jobs_when_pg_cron_present():
    """pg_cron installed → unschedule query is executed and row count returned."""
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")

    # Simulate 3 superseded cron jobs being unscheduled.
    mock_dql_execute = AsyncMock(return_value=3)

    with _patch_managed_txn(conn), patch(
        "dynastore.modules.catalog.maintenance_supervisor.check_extension_exists",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery.execute",
        new=mock_dql_execute,
    ):
        result = await unschedule_superseded_cron_jobs(engine)

    assert result == 3


@pytest.mark.asyncio
async def test_propagates_db_error_when_unschedule_query_fails():
    """pg_cron present but DB query raises → exception propagates to caller.

    ``CatalogModule.lifespan`` wraps the call in ``try/except Exception`` so
    no startup abort occurs in practice; the propagation contract here ensures
    the caller's guard is what prevents the abort, not silent swallowing inside
    the function.
    """
    from dynastore.modules.catalog.maintenance_supervisor import (
        unschedule_superseded_cron_jobs,
    )

    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")

    boom = AsyncMock(side_effect=RuntimeError("cron.unschedule blew up"))

    with _patch_managed_txn(conn), patch(
        "dynastore.modules.catalog.maintenance_supervisor.check_extension_exists",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.db_config.query_executor.DQLQuery.execute",
        new=boom,
    ):
        with pytest.raises(RuntimeError, match="cron.unschedule blew up"):
            await unschedule_superseded_cron_jobs(engine)
