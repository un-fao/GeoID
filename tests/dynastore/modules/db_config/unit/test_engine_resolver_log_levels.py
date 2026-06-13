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

"""``build_engine_snapshot`` log-level discrimination — #845.

The first-pass per-engine fetch always raises ``ValueError("Cannot start
managed_transaction: db_resource is None.")`` because DBConfigModule
(priority 0) starts before DBService (priority 10) installs the pool.
That's a benign race recovered by ``refresh_snapshot_until_ready``; it
must NOT surface at WARNING (which made the operationally-meaningful
``engine snapshot ready`` INFO impossible to find during the dynastore
#264 Valkey cutover).

Real load failures (anything other than the pool-not-ready
``ValueError``) must remain at WARNING.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.db_config import engine_resolver as er
from dynastore.modules.db_config.engine_config import (
    EngineConfig,
    PostgresqlEngineConfig,
    ValkeyEngineConfig,
)


@pytest.fixture
def pcfg_stub():
    stub = MagicMock()
    stub.get_config = AsyncMock()
    return stub


def test_is_pool_not_ready_matches_managed_transaction_value_error():
    """The exact message ``managed_transaction`` raises must match."""
    exc = ValueError("Cannot start managed_transaction: db_resource is None.")
    assert er._is_pool_not_ready(exc) is True


def test_is_pool_not_ready_rejects_runtime_error_with_same_substring():
    """Match is type-narrow: same substring on a different exception is NOT the race."""
    exc = RuntimeError("db_resource is None")
    assert er._is_pool_not_ready(exc) is False


def test_is_pool_not_ready_rejects_unrelated_value_error():
    """A ``ValueError`` without the marker substring is not the race."""
    exc = ValueError("bad config field")
    assert er._is_pool_not_ready(exc) is False


@pytest.mark.asyncio
async def test_pool_not_ready_logs_at_debug_not_warning(pcfg_stub, caplog):
    """The benign race: DEBUG line emitted, WARNING channel silent."""
    pcfg_stub.get_config.side_effect = ValueError(
        "Cannot start managed_transaction: db_resource is None."
    )

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("DEBUG", logger="dynastore.modules.db_config.engine_resolver"):
        await er.build_engine_snapshot(pcfg_stub, into=snapshot)

    debug_lines = [
        rec for rec in caplog.records
        if rec.levelname == "DEBUG"
        and "deferred" in rec.getMessage()
    ]
    warning_lines = [
        rec for rec in caplog.records
        if rec.levelname == "WARNING"
        and rec.name == "dynastore.modules.db_config.engine_resolver"
    ]

    assert debug_lines, "pool-not-ready race must emit a DEBUG line"
    assert not warning_lines, (
        "pool-not-ready race must NOT emit a WARNING (the line operators read "
        "as a real engine-cache failure during the #264 cutover triage)"
    )


@pytest.mark.asyncio
async def test_real_load_failure_still_logs_at_warning(pcfg_stub, caplog):
    """Anything other than the pool-not-ready race stays loud."""

    def _by_type(cls):
        if cls is ValkeyEngineConfig:
            # A genuine config-row corruption / mis-shape — operators need
            # to see this.
            raise RuntimeError("ValkeyEngineConfig: invalid stored payload")
        return cls()

    pcfg_stub.get_config.side_effect = _by_type

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("WARNING", logger="dynastore.modules.db_config.engine_resolver"):
        await er.build_engine_snapshot(pcfg_stub, into=snapshot)

    warning_lines = [
        rec for rec in caplog.records
        if rec.levelname == "WARNING"
        and "failed to load valkey_engine_config" in rec.getMessage()
    ]
    assert warning_lines, "real load failure must stay at WARNING"

    # And the postgresql side still loaded — best-effort property preserved.
    assert "postgresql_engine_config" in snapshot
    assert "valkey_engine_config" not in snapshot


@pytest.mark.asyncio
async def test_mixed_failures_split_by_level(pcfg_stub, caplog):
    """Two engines failing for two different reasons get two different levels."""

    def _by_type(cls):
        if cls is ValkeyEngineConfig:
            raise ValueError(
                "Cannot start managed_transaction: db_resource is None."
            )
        if cls is PostgresqlEngineConfig:
            raise RuntimeError("pg dsn rejected")
        return cls()

    pcfg_stub.get_config.side_effect = _by_type

    snapshot: dict[str, EngineConfig] = {}
    with caplog.at_level("DEBUG", logger="dynastore.modules.db_config.engine_resolver"):
        await er.build_engine_snapshot(pcfg_stub, into=snapshot)

    valkey_debug = [
        rec for rec in caplog.records
        if rec.levelname == "DEBUG"
        and "valkey_engine_config" in rec.getMessage()
        and "deferred" in rec.getMessage()
    ]
    pg_warning = [
        rec for rec in caplog.records
        if rec.levelname == "WARNING"
        and "postgresql_engine_config" in rec.getMessage()
        and "failed to load" in rec.getMessage()
    ]

    assert valkey_debug, "pool-not-ready engine → DEBUG"
    assert pg_warning, "real-failure engine → WARNING"
