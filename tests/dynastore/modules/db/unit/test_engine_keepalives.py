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

"""Issue #655 — DB engine pool hygiene + TCP keepalives.

The async engine (``db_service.py``) and the sync engine
(``datastore_service.py``) must both:

* keep the pool hygienic so a NAT/AlloyDB-dropped idle connection is
  recycled rather than handed out dead-at-the-wire (``pool_pre_ping`` +
  ``pool_recycle``), and
* send TCP keepalive probes so Cloud NAT never silently drops the idle
  mapping in the first place.

Tests drive the real module lifespans with the SQLAlchemy engine
factories mocked, then assert the kwargs that reach the factory.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Warm ``models.protocols`` before importing the service modules.
#
# History: this guarded against the #686 circular import (markers + ``PluginConfig``
# co-located under ``models/protocols/``). PR #707 (2026-05-14) moved ``PluginConfig``
# out to ``modules/db_config/plugin_config.py`` and the original circular path is gone
# — but the warm-up is still load-bearing for a different reason: under xdist
# parallel collection (``pytest.ini`` configures ``-n auto --dist worksteal``), 12
# worker processes race to import ``datastore_service`` and the chain through
# ``models.protocols``. Without an explicit warm-up at least one worker errors down
# with ``INTERNALERROR ... KeyError: <WorkerController gw*>`` and the whole run
# yields 0 tests. Full-suite collection warms ``models.protocols`` implicitly via
# earlier imports; an isolated run of this file does not, so we do it explicitly.
#
# Verified 2026-05-15 in worktree ``710-item5-drop-protocols-warmup``: removing
# this line passes serial (``-n0``) but crashes xdist parallel — both with this
# file alone and with the surrounding ``modules/db/unit`` directory.
import dynastore.models.protocols  # noqa: F401

from dynastore.modules.datastore.datastore_service import DatastoreModule
from dynastore.modules.db.db_service import DBService
from dynastore.modules.db_config.db_config import DBConfig


def _async_engine_mock() -> MagicMock:
    """A create_async_engine return value whose ``dispose`` is awaitable."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    return engine


# --------------------------------------------------------------------------
# DBConfig — new keepalive tunables
# --------------------------------------------------------------------------


def test_db_config_exposes_tcp_keepalive_defaults():
    cfg = DBConfig()
    # Idle window must sit well under the egress path's ~1200s established-conn
    # timeout so the mapping is refreshed before it is dropped.
    assert cfg.tcp_keepalives_idle == 300
    assert cfg.tcp_keepalives_interval == 30
    assert cfg.tcp_keepalives_count == 5


def test_db_config_exposes_pool_recycle_default():
    # #729 — pool_recycle is env-driven so idle-prone environments can retire
    # connections before the egress path silently drops them.
    assert DBConfig().pool_recycle == 1800


# --------------------------------------------------------------------------
# Async engine (asyncpg) — server-side keepalive GUCs
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_engine_sets_tcp_keepalive_server_settings():
    app_state = SimpleNamespace(db_config=DBConfig())

    with patch(
        "dynastore.modules.db.db_service.create_async_engine",
        return_value=_async_engine_mock(),
    ) as mk:
        async with DBService(app_state).lifespan(app_state):
            pass

    kwargs = mk.call_args.kwargs
    server_settings = kwargs["connect_args"]["server_settings"]
    # asyncpg has no libpq client keepalive params; the server-side GUCs
    # must be passed as strings via server_settings.
    assert server_settings["tcp_keepalives_idle"] == "300"
    assert server_settings["tcp_keepalives_interval"] == "30"
    assert server_settings["tcp_keepalives_count"] == "5"
    # The #702 application_name tag must survive alongside the new keys.
    assert "application_name" in server_settings


@pytest.mark.asyncio
async def test_async_engine_pool_recycle_tracks_config():
    # #729 — the async engine recycles by DBConfig.pool_recycle, not a
    # hardcoded constant, so idle-prone environments can lower it.
    app_state = SimpleNamespace(db_config=DBConfig())

    with patch(
        "dynastore.modules.db.db_service.create_async_engine",
        return_value=_async_engine_mock(),
    ) as mk:
        async with DBService(app_state).lifespan(app_state):
            pass

    kwargs = mk.call_args.kwargs
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["pool_recycle"] == app_state.db_config.pool_recycle == 1800


# --------------------------------------------------------------------------
# Sync engine (psycopg2) — pool hygiene parity + libpq client keepalives
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_engine_has_pool_pre_ping_and_recycle():
    app_state = SimpleNamespace(db_config=DBConfig())

    with patch(
        "dynastore.modules.datastore.datastore_service.create_engine",
        return_value=MagicMock(),
    ) as mk, patch(
        "dynastore.modules.datastore.datastore_service.ensure_init_db",
        new=AsyncMock(),
    ):
        async with DatastoreModule(app_state).lifespan(app_state):
            pass

    kwargs = mk.call_args.kwargs
    # Parity with the async engine in db_service.py.
    assert kwargs["pool_pre_ping"] is True
    # pool_recycle tracks DBConfig (#729) rather than a hardcoded constant.
    assert kwargs["pool_recycle"] == app_state.db_config.pool_recycle == 1800


@pytest.mark.asyncio
async def test_sync_engine_sets_libpq_tcp_keepalives():
    app_state = SimpleNamespace(db_config=DBConfig())

    with patch(
        "dynastore.modules.datastore.datastore_service.create_engine",
        return_value=MagicMock(),
    ) as mk, patch(
        "dynastore.modules.datastore.datastore_service.ensure_init_db",
        new=AsyncMock(),
    ):
        async with DatastoreModule(app_state).lifespan(app_state):
            pass

    connect_args = mk.call_args.kwargs["connect_args"]
    # psycopg2 speaks libpq, so client-side keepalive params apply directly.
    assert connect_args["keepalives"] == 1
    assert connect_args["keepalives_idle"] == 300
    assert connect_args["keepalives_interval"] == 30
    assert connect_args["keepalives_count"] == 5
    # The #702 application_name tag must survive alongside the new keys.
    assert "application_name" in connect_args
