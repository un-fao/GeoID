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

"""EventsModule pg_cron resilience — startup must not abort when pg_cron is absent.

Regression guard for the dev outage where a database without the optional
``pg_cron`` extension made ``EventsModule`` (a *foundational* module) crash at
startup — ``SELECT 1 FROM cron.job`` raised ``UndefinedTableError (42P01)``,
which propagated and aborted the whole container, so every deploy failed the
Cloud Run startup probe.

Retention + reaper are best-effort maintenance, NOT a hard dependency. When
pg_cron is missing — or any cron-registration call fails — the module must log
a WARNING and continue booting. These tests exercise
``_register_cron_maintenance`` directly with a mocked transaction; no DB.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_module():
    from dynastore.modules.events.events_module import EventsModule

    mod = EventsModule(app_state=MagicMock())  # type: ignore[abstract]
    mod._engine = MagicMock(name="engine")
    return mod


def _patch_txn(conn):
    @asynccontextmanager
    async def _fake_txn(_engine):
        yield conn

    return patch(
        "dynastore.modules.events.events_module.managed_transaction",
        side_effect=_fake_txn,
    )


@pytest.mark.asyncio
async def test_skips_and_does_not_raise_when_pg_cron_absent():
    """pg_cron missing → no registration calls, no exception, WARNING logged."""
    mod = _make_module()
    conn = MagicMock(name="conn")

    with _patch_txn(conn), patch(
        "dynastore.modules.db_config.locking_tools.check_extension_exists",
        new=AsyncMock(return_value=False),
    ), patch(
        "dynastore.modules.events.events_module._register_events_retention",
        new=AsyncMock(),
    ) as retention, patch(
        "dynastore.modules.events.events_module._register_events_reaper",
        new=AsyncMock(),
    ) as reaper:
        # Must not raise.
        await mod._register_cron_maintenance()

    retention.assert_not_called()
    reaper.assert_not_called()


@pytest.mark.asyncio
async def test_registers_jobs_when_pg_cron_present():
    """pg_cron installed → both retention and reaper registered exactly once."""
    mod = _make_module()
    conn = MagicMock(name="conn")

    with _patch_txn(conn), patch(
        "dynastore.modules.db_config.locking_tools.check_extension_exists",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.events.events_module._register_events_retention",
        new=AsyncMock(),
    ) as retention, patch(
        "dynastore.modules.events.events_module._register_events_reaper",
        new=AsyncMock(),
    ) as reaper:
        await mod._register_cron_maintenance()

    retention.assert_awaited_once()
    reaper.assert_awaited_once()
    # First positional arg of each must be the yielded connection.
    assert retention.await_args is not None and retention.await_args.args[0] is conn
    assert reaper.await_args is not None and reaper.await_args.args[0] is conn


@pytest.mark.asyncio
async def test_swallows_cron_registration_failure():
    """pg_cron present but a cron call raises → swallowed, startup continues."""
    mod = _make_module()
    conn = MagicMock(name="conn")

    boom = AsyncMock(side_effect=RuntimeError("cron.schedule blew up"))

    with _patch_txn(conn), patch(
        "dynastore.modules.db_config.locking_tools.check_extension_exists",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.events.events_module._register_events_retention",
        new=boom,
    ), patch(
        "dynastore.modules.events.events_module._register_events_reaper",
        new=AsyncMock(),
    ):
        # Must not propagate the RuntimeError.
        await mod._register_cron_maintenance()

    boom.assert_awaited_once()
