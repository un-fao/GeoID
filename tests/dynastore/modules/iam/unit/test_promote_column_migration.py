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

"""Unit tests for promote_envelope_attr_column migration.

Covers:
1. Sentinel idempotency — second call is a no-op (no DDL/DQL calls).
2. DDL shape — ALTER TABLE and CREATE INDEX SQL contain expected identifiers.
3. Backfill SQL pagination — loop exits when batch returns fewer than BATCH_SIZE rows.
4. Identifier validation — column names that fail _KEY_PATTERN are rejected early.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

import dynastore.modules.iam.migrations.promote_envelope_attr_column as m


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn_and_txn():
    conn = MagicMock()

    @asynccontextmanager
    async def _txn(_engine):
        yield conn

    return conn, _txn


# ---------------------------------------------------------------------------
# 1. Idempotency — already applied
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_is_noop_when_sentinel_exists():
    """If iam.applied_presets already has the sentinel, nothing else runs."""
    engine = MagicMock()
    conn, _txn = _make_conn_and_txn()

    ddl_calls: List[Any] = []
    dql_calls: List[Any] = []

    async def _select_applied(_res, **kw):
        return (1,)  # already applied

    async def _ddl_exec(self, _conn, **kw):
        ddl_calls.append(self)

    async def _dql_exec(self, _conn, **kw):
        dql_calls.append(self)

    from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m._SELECT_APPLIED, "execute", _select_applied), \
         patch.object(DDLQuery, "execute", _ddl_exec), \
         patch.object(DQLQuery, "execute", _dql_exec):
        await m.promote_envelope_attr_column(
            engine, "cat1", "col1", "dept", "TEXT", "s_abc"
        )

    assert ddl_calls == [], "No DDL should run when sentinel exists"
    # Only the SELECT_APPLIED DQLQuery should have been called.
    assert len(dql_calls) <= 1


# ---------------------------------------------------------------------------
# 2. DDL shape
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_ddl_contains_expected_identifiers():
    """ALTER TABLE and CREATE INDEX include schema, column, and index names."""
    engine = MagicMock()
    conn, _txn = _make_conn_and_txn()

    ddl_sqls: List[str] = []
    dql_calls: List[Any] = []

    async def _select_applied(_res, **kw):
        return None  # not applied

    async def _ddl_exec(self, _conn, **kw):
        ddl_sqls.append(self._executor.query_builder_strategy.sql_template
                        if hasattr(self._executor, "query_builder_strategy")
                        else str(self))

    async def _dql_exec(self, _conn, **kw):
        # Backfill returns 0 rows updated on first call → loop exits.
        if "UPDATE" in str(getattr(self, "_executor", "")):
            return 0
        dql_calls.append(kw)

    from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery

    captured_ddl: List[str] = []

    class _CapturingDDLQuery(DDLQuery):
        def __init__(self, sql, **kw):
            captured_ddl.append(sql)
            super().__init__(sql, **kw)

        async def execute(self, conn, **kwargs):  # type: ignore[override]
            return None

    class _CapturingDQLQuery(DQLQuery):
        async def execute(self, conn, **kwargs):  # type: ignore[override]
            # Simulate: SELECT sentinel → None (not applied), backfill UPDATE → 0
            return None

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m._SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._INSERT_APPLIED, "execute", AsyncMock(return_value=None)), \
         patch.object(m, "DDLQuery", _CapturingDDLQuery), \
         patch.object(m, "DQLQuery", _CapturingDQLQuery):
        await m.promote_envelope_attr_column(
            engine, "cat1", "col1", "dept", "TEXT", "s_abc"
        )

    # At least ALTER TABLE and CREATE INDEX DDL statements were issued.
    assert any("ALTER TABLE" in sql for sql in captured_ddl), (
        f"Expected ALTER TABLE in DDL; got: {captured_ddl}"
    )
    assert any("CREATE INDEX" in sql for sql in captured_ddl), (
        f"Expected CREATE INDEX in DDL; got: {captured_ddl}"
    )
    # Schema and column identifiers are present.
    full_ddl = " ".join(captured_ddl)
    assert "s_abc" in full_ddl
    assert "_attr_dept" in full_ddl


@pytest.mark.asyncio
async def test_migration_gin_index_for_array_type():
    """TEXT[] columns get a GIN index."""
    engine = MagicMock()
    conn, _txn = _make_conn_and_txn()

    captured_ddl: List[str] = []

    class _CapturingDDLQuery:
        def __init__(self, sql, **kw):
            captured_ddl.append(sql)

        async def execute(self, *a, **kw):
            return None

    class _CapturingDQLQuery:
        def __init__(self, sql, **kw):
            pass

        async def execute(self, *a, **kw):
            return None

    async def _select_applied(_res, **kw):
        return None

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m._SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._INSERT_APPLIED, "execute", AsyncMock(return_value=None)), \
         patch.object(m, "DDLQuery", _CapturingDDLQuery), \
         patch.object(m, "DQLQuery", _CapturingDQLQuery):
        await m.promote_envelope_attr_column(
            engine, "cat1", "col1", "tags", "TEXT[]", "s_abc"
        )

    assert any("USING GIN" in sql for sql in captured_ddl), (
        f"Expected GIN index for TEXT[]; got: {captured_ddl}"
    )


# ---------------------------------------------------------------------------
# 3. Backfill pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_backfill_exits_when_batch_under_limit():
    """Backfill loop stops when a batch updates fewer than BATCH_SIZE rows."""
    engine = MagicMock()
    conn, _txn = _make_conn_and_txn()

    batch_call_count = 0

    class _MockDQLQuery:
        def __init__(self, sql, **kw):
            self._sql = sql

        async def execute(self, _conn, **kw):
            nonlocal batch_call_count
            if "UPDATE" in self._sql:
                batch_call_count += 1
                if batch_call_count == 1:
                    return m._BATCH_SIZE  # full batch → continue
                return 0  # second batch → done
            return None  # sentinel SELECT

    class _MockDDLQuery:
        def __init__(self, sql, **kw):
            pass

        async def execute(self, *a, **kw):
            return None

    async def _select_applied(_res, **kw):
        return None

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m._SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._INSERT_APPLIED, "execute", AsyncMock(return_value=None)), \
         patch.object(m, "DDLQuery", _MockDDLQuery), \
         patch.object(m, "DQLQuery", _MockDQLQuery):
        await m.promote_envelope_attr_column(
            engine, "cat1", "col1", "dept", "TEXT", "s_abc"
        )

    assert batch_call_count == 2, (
        f"Expected 2 backfill batches (1 full + 1 empty-exit); got {batch_call_count}"
    )


# ---------------------------------------------------------------------------
# 4. Identifier validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_rejects_invalid_column_name():
    """column_name with dots/quotes/spaces raises ValueError before any DDL."""
    engine = MagicMock()

    for bad_name in ["bad.col", "bad col", "bad'col", '"quoted"', "123start"]:
        with pytest.raises(ValueError, match=r"must match"):
            await m.promote_envelope_attr_column(
                engine, "cat1", "col1", bad_name, "TEXT", "s_abc"
            )


@pytest.mark.asyncio
async def test_migration_rejects_invalid_pg_type():
    """Disallowed pg_type raises ValueError before any DDL."""
    engine = MagicMock()

    with pytest.raises(ValueError, match="not allowed"):
        await m.promote_envelope_attr_column(
            engine, "cat1", "col1", "dept", "INTEGER", "s_abc"
        )
