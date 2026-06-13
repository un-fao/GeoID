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

"""Tests for the event-plane dual-write seam (WorkClass #1807 PR-4).

These tests exercise ``dispatch_event_dual_write`` against real PostgreSQL to
prove atomicity, fail-safe behaviour, and scope normalisation.

The test schema is created fresh per-test and torn down afterwards.  The
``legacy`` table mirrors the structure of ``events.events`` that
``_publish_query`` writes to; the ``new`` table mirrors
``tasks.work_events``.  Both are created inside a unique test-scoped schema so
concurrent test runs on the same DB do not interfere.

The fixture skips automatically when PostgreSQL is unreachable.

Scenarios covered
-----------------
1. ``configs=None`` (fail-safe) → only the legacy table gets a row.
2. ``get_config`` raising (fail-safe) → only the legacy table gets a row.
3. ``EmitTarget.BOTH`` → both tables get a row; work_events scope is lowercase.
4. ``EmitTarget.NEW`` → only work_events gets a row.
5. Rollback atomicity: an exception inside the outer transaction leaves zero
   rows in either table.
"""

from __future__ import annotations

import os
from typing import AsyncIterator, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sa_db_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def sa_engine():
    """SQLAlchemy AsyncEngine with NullPool; skips if DB is unreachable."""
    sqlalchemy_async = pytest.importorskip(
        "sqlalchemy.ext.asyncio",
        reason="sqlalchemy[asyncio] not installed",
    )
    pytest.importorskip("asyncpg", reason="asyncpg not installed")

    from sqlalchemy.pool import NullPool

    engine = sqlalchemy_async.create_async_engine(_sa_db_url(), poolclass=NullPool)
    try:
        async with engine.connect() as probe:
            await probe.close()
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping dual-write tests.")

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def dual_write_schema(sa_engine) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test schema with a legacy-events-like table and a work_events-like table.

    Table structures match what ``_publish_query`` and
    ``dispatch_event_dual_write`` write to, but are isolated to a throwaway
    schema so nothing touches the real events / tasks schemas.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    schema = f"dw_test_{generate_id_hex()[:10]}"

    async with managed_transaction(sa_engine) as conn:
        # Create schema
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)

        # Legacy events-like table: mirrors events.events (enough columns for
        # _publish_query to INSERT into).
        await DQLQuery(
            f"""
            CREATE TABLE "{schema}".events (
                event_id      UUID          NOT NULL DEFAULT gen_random_uuid(),
                event_type    VARCHAR       NOT NULL,
                scope         VARCHAR(50)   NOT NULL DEFAULT 'PLATFORM',
                schema_name   VARCHAR(255),
                catalog_id    VARCHAR(255),
                collection_id VARCHAR(255),
                identity_id   VARCHAR(255),
                payload       JSONB         NOT NULL DEFAULT '{{}}',
                status        VARCHAR       NOT NULL DEFAULT 'PENDING',
                created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                processed_at  TIMESTAMPTZ,
                error_message TEXT,
                retry_count   INT           NOT NULL DEFAULT 0,
                shard         SMALLINT      NOT NULL,
                PRIMARY KEY (shard, event_id)
            )
            """,
            result_handler=ResultHandler.NONE,
        ).execute(conn)

        # work_events-like table: matches tasks.work_events DDL exactly
        # (non-partitioned for simplicity — the INSERT path is identical).
        await DQLQuery(
            f"""
            CREATE TABLE "{schema}".work_events (
                event_id        UUID            NOT NULL,
                day             DATE            NOT NULL DEFAULT CURRENT_DATE,
                shard           SMALLINT        NOT NULL,
                schema_name     TEXT,
                scope           TEXT            NOT NULL DEFAULT 'platform'
                                    CHECK (scope = lower(scope)),
                event_type      TEXT            NOT NULL,
                status          TEXT            NOT NULL DEFAULT 'PENDING',
                payload         JSONB           NOT NULL DEFAULT '{{}}'::jsonb,
                claim_version   INTEGER         NOT NULL DEFAULT 0,
                owner_id        TEXT,
                locked_until    TIMESTAMPTZ,
                retry_count     INTEGER         NOT NULL DEFAULT 0,
                max_retries     INTEGER,
                error_message   TEXT,
                created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
                processed_at    TIMESTAMPTZ,
                PRIMARY KEY (day, event_id)
            )
            """,
            result_handler=ResultHandler.NONE,
        ).execute(conn)

    try:
        yield schema
    finally:
        async with managed_transaction(sa_engine) as conn:
            try:
                await DQLQuery(
                    f'DROP SCHEMA IF EXISTS "{schema}" CASCADE',
                    result_handler=ResultHandler.NONE,
                ).execute(conn)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass


# ---------------------------------------------------------------------------
# Stub configs factories
# ---------------------------------------------------------------------------


def _make_configs(emit_target_str: Optional[str]) -> MagicMock:
    """Return a mock ``ConfigsProtocol`` whose ``get_config`` returns a
    ``WorkClassConfig`` with the given ``emit_target_events`` value, or raises
    when ``emit_target_str`` is the sentinel ``"RAISE"``.
    """
    from dynastore.modules.tasks.workclass_config import EmitTarget, WorkClassConfig

    configs = MagicMock()

    if emit_target_str == "RAISE":
        configs.get_config = AsyncMock(side_effect=RuntimeError("injected config error"))
    else:
        target = EmitTarget(emit_target_str) if emit_target_str else EmitTarget.LEGACY
        cfg = WorkClassConfig(emit_target_events=target)
        configs.get_config = AsyncMock(return_value=cfg)

    return configs


# ---------------------------------------------------------------------------
# Row-count helpers (read committed, fresh connection)
# ---------------------------------------------------------------------------


async def _count(engine, schema: str, table: str) -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        n = await DQLQuery(
            f'SELECT count(*) FROM "{schema}".{table}',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn)
        return n or 0


async def _fetch_scope(engine, schema: str, table: str) -> Optional[str]:
    """Return the ``scope`` of the first row in the table, or None."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery,
        ResultHandler,
        managed_transaction,
    )

    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            f'SELECT scope FROM "{schema}".{table} LIMIT 1',
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn)
        return row["scope"] if row else None


# ---------------------------------------------------------------------------
# _publish_query patcher
#
# ``_publish_query`` is a module-level DQLQuery that targets
# ``events.events``.  In tests we replace the entire module-level object with
# a new DQLQuery whose SQL points at the throwaway test-schema table so no
# writes hit the real events schema.
# ---------------------------------------------------------------------------


def _patch_publish_query(schema: str):
    """Context manager: redirect _publish_query to the test schema events table."""
    import contextlib
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    patched = DQLQuery(
        f"""
        INSERT INTO "{schema}".events
            (event_type, scope, schema_name, catalog_id, collection_id, identity_id,
             payload, shard)
        VALUES
            (:event_type, :scope, :schema_name, :catalog_id, :collection_id,
             :identity_id, :payload, :shard)
        RETURNING event_id::text;
        """,
        result_handler=ResultHandler.SCALAR_ONE,
    )

    @contextlib.asynccontextmanager
    async def _ctx():
        with mock.patch(
            "dynastore.modules.events.events_module._publish_query",
            patched,
        ):
            yield

    return _ctx()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failsafe_configs_none(sa_engine, dual_write_schema):
    """When configs=None, only the legacy table gets a row (fail-safe)."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    async with _patch_publish_query(dual_write_schema):
        async with managed_transaction(sa_engine) as conn:
            event_id = await dispatch_event_dual_write(
                conn,
                event_type="test_event",
                scope="PLATFORM",
                schema_name=None,
                catalog_id=None,
                collection_id=None,
                identity_id=None,
                payload_str='{"x": 1}',
                shard=3,
                configs=None,
            )

    assert event_id is not None and len(event_id) > 0
    assert await _count(sa_engine, dual_write_schema, "events") == 1
    assert await _count(sa_engine, dual_write_schema, "work_events") == 0


@pytest.mark.asyncio
async def test_failsafe_get_config_raises(sa_engine, dual_write_schema):
    """When get_config raises, only the legacy table gets a row (fail-safe)."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    configs = _make_configs("RAISE")

    async with _patch_publish_query(dual_write_schema):
        async with managed_transaction(sa_engine) as conn:
            event_id = await dispatch_event_dual_write(
                conn,
                event_type="test_event",
                scope="PLATFORM",
                schema_name=None,
                catalog_id="cat1",
                collection_id=None,
                identity_id=None,
                payload_str='{"y": 2}',
                shard=7,
                configs=configs,
            )

    assert event_id is not None
    assert await _count(sa_engine, dual_write_schema, "events") == 1
    assert await _count(sa_engine, dual_write_schema, "work_events") == 0


@pytest.mark.asyncio
async def test_both_mode_writes_to_both_tables(sa_engine, dual_write_schema):
    """EmitTarget.BOTH → row in both events and work_events."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    configs = _make_configs("both")

    async with _patch_publish_query(dual_write_schema):
        with mock.patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value=dual_write_schema,
        ):
            async with managed_transaction(sa_engine) as conn:
                event_id = await dispatch_event_dual_write(
                    conn,
                    event_type="collection_creation",
                    scope="PLATFORM",
                    schema_name="tenant_a",
                    catalog_id="mycat",
                    collection_id="mycol",
                    identity_id=None,
                    payload_str='{"z": 3}',
                    shard=5,
                    configs=configs,
                )

    assert event_id is not None
    assert await _count(sa_engine, dual_write_schema, "events") == 1
    assert await _count(sa_engine, dual_write_schema, "work_events") == 1

    # work_events scope must be lowercase even though we passed "PLATFORM"
    we_scope = await _fetch_scope(sa_engine, dual_write_schema, "work_events")
    assert we_scope == "platform", f"expected 'platform', got {we_scope!r}"

    # legacy events scope is unchanged (uppercase)
    ev_scope = await _fetch_scope(sa_engine, dual_write_schema, "events")
    assert ev_scope == "PLATFORM", f"expected 'PLATFORM', got {ev_scope!r}"


@pytest.mark.asyncio
async def test_new_mode_writes_only_to_work_events(sa_engine, dual_write_schema):
    """EmitTarget.NEW → only work_events gets a row, events stays empty."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    configs = _make_configs("new")

    async with _patch_publish_query(dual_write_schema):
        with mock.patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value=dual_write_schema,
        ):
            async with managed_transaction(sa_engine) as conn:
                event_id = await dispatch_event_dual_write(
                    conn,
                    event_type="catalog_deletion",
                    scope="PLATFORM",
                    schema_name=None,
                    catalog_id="gone_cat",
                    collection_id=None,
                    identity_id="user42",
                    payload_str='{"deleted": true}',
                    shard=2,
                    configs=configs,
                )

    assert event_id is not None
    assert await _count(sa_engine, dual_write_schema, "events") == 0
    assert await _count(sa_engine, dual_write_schema, "work_events") == 1

    # Scope must be lowercased in work_events
    we_scope = await _fetch_scope(sa_engine, dual_write_schema, "work_events")
    assert we_scope == "platform", f"expected 'platform', got {we_scope!r}"


@pytest.mark.asyncio
async def test_scope_lowercased_in_work_events(sa_engine, dual_write_schema):
    """Mixed-case scope value is lowercased for work_events, preserved for legacy."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    configs = _make_configs("both")

    async with _patch_publish_query(dual_write_schema):
        with mock.patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value=dual_write_schema,
        ):
            async with managed_transaction(sa_engine) as conn:
                await dispatch_event_dual_write(
                    conn,
                    event_type="scope_test",
                    scope="PLATFORM",
                    schema_name=None,
                    catalog_id=None,
                    collection_id=None,
                    identity_id=None,
                    payload_str="{}",
                    shard=0,
                    configs=configs,
                )

    ev_scope = await _fetch_scope(sa_engine, dual_write_schema, "events")
    we_scope = await _fetch_scope(sa_engine, dual_write_schema, "work_events")
    assert ev_scope == "PLATFORM"
    assert we_scope == "platform"


@pytest.mark.asyncio
async def test_rollback_leaves_no_rows_in_either_table(sa_engine, dual_write_schema):
    """When the outer transaction is rolled back, both tables stay empty."""
    import unittest.mock as mock

    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.events.work_events_dual_write import dispatch_event_dual_write

    configs = _make_configs("both")

    async with _patch_publish_query(dual_write_schema):
        with mock.patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value=dual_write_schema,
        ):
            with pytest.raises(RuntimeError, match="simulated failure"):
                async with managed_transaction(sa_engine) as conn:
                    # Write to both tables
                    await dispatch_event_dual_write(
                        conn,
                        event_type="rollback_test",
                        scope="PLATFORM",
                        schema_name=None,
                        catalog_id=None,
                        collection_id=None,
                        identity_id=None,
                        payload_str="{}",
                        shard=1,
                        configs=configs,
                    )
                    # Verify rows are visible mid-transaction before aborting
                    from dynastore.modules.db_config.query_executor import (
                        DQLQuery,
                        ResultHandler,
                    )

                    ev_mid = (
                        await DQLQuery(
                            f'SELECT count(*) FROM "{dual_write_schema}".events',
                            result_handler=ResultHandler.SCALAR,
                        ).execute(conn)
                        or 0
                    )
                    we_mid = (
                        await DQLQuery(
                            f'SELECT count(*) FROM "{dual_write_schema}".work_events',
                            result_handler=ResultHandler.SCALAR,
                        ).execute(conn)
                        or 0
                    )
                    assert ev_mid == 1, f"expected 1 in-tx events row, got {ev_mid}"
                    assert we_mid == 1, f"expected 1 in-tx work_events row, got {we_mid}"
                    raise RuntimeError("simulated failure")

    # After rollback both tables must be empty
    assert await _count(sa_engine, dual_write_schema, "events") == 0
    assert await _count(sa_engine, dual_write_schema, "work_events") == 0
