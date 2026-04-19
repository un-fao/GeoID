"""Global stale ``idx_*_attributes_ext_id`` rebuild — unit-level coverage.

The SQL is exercised by integration tests against a real Postgres; these
unit tests cover the Python orchestration: early return on empty engine,
no-op when the scan returns zero rows, count + per-table DDL when rows
are found, and continue-on-failure behavior.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from dynastore.modules.db_config import stale_index_migration as mod


class _FakeDDL:
    def __init__(self, raises: bool = False):
        self.raises = raises
        self.executed: List[str] = []

    async def execute(self, conn, **kwargs):
        self.executed.append(conn._last_ddl)
        if self.raises:
            raise RuntimeError("simulated DDL failure")


class _FakeConn:
    def __init__(self):
        self._last_ddl = ""


class _FakeTxCM:
    def __init__(self, conn: _FakeConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def patched(monkeypatch):
    """Stub ``managed_transaction``, ``DDLQuery``, and the find-stale DQL."""
    state = {
        "rows": [],
        "ddl_calls": [],
        "ddl_raises_on": set(),
    }

    def fake_managed(engine):
        return _FakeTxCM(_FakeConn())

    class _FakeDDLQuery:
        def __init__(self, sql: str, **_):
            self.sql = sql

        async def execute(self, conn, **kwargs):
            conn._last_ddl = self.sql
            state["ddl_calls"].append(self.sql)
            for marker in state["ddl_raises_on"]:
                if marker in self.sql:
                    raise RuntimeError("simulated DDL failure")

    async def fake_find(conn, **kwargs):
        return state["rows"]

    monkeypatch.setattr(mod, "managed_transaction", fake_managed, raising=True)
    monkeypatch.setattr(mod, "DDLQuery", _FakeDDLQuery, raising=True)
    monkeypatch.setattr(
        mod._find_stale_query, "execute", fake_find, raising=True,
    )
    return state


@pytest.mark.asyncio
async def test_no_engine_is_noop():
    assert await mod.migrate_stale_attributes_ext_id_indexes(None) == 0


@pytest.mark.asyncio
async def test_clean_database_is_noop(patched):
    patched["rows"] = []
    rebuilt = await mod.migrate_stale_attributes_ext_id_indexes(object())
    assert rebuilt == 0
    assert patched["ddl_calls"] == []


@pytest.mark.asyncio
async def test_rebuilds_every_stale_index(patched):
    patched["rows"] = [
        {"schema_name": "cat_a", "table_name": "foo_attributes",
         "index_name": "idx_foo_attributes_ext_id"},
        {"schema_name": "cat_b", "table_name": "bar_attributes",
         "index_name": "idx_bar_attributes_ext_id"},
    ]
    rebuilt = await mod.migrate_stale_attributes_ext_id_indexes(object())
    assert rebuilt == 2
    assert len(patched["ddl_calls"]) == 2
    assert '"cat_a"."foo_attributes"' in patched["ddl_calls"][0]
    assert '(geoid, validity, external_id)' in patched["ddl_calls"][0]
    assert '"cat_b"."bar_attributes"' in patched["ddl_calls"][1]


@pytest.mark.asyncio
async def test_one_failure_does_not_block_remaining(patched):
    patched["rows"] = [
        {"schema_name": "cat_a", "table_name": "foo_attributes",
         "index_name": "idx_foo_attributes_ext_id"},
        {"schema_name": "cat_b", "table_name": "bar_attributes",
         "index_name": "idx_bar_attributes_ext_id"},
    ]
    patched["ddl_raises_on"].add("cat_a")
    rebuilt = await mod.migrate_stale_attributes_ext_id_indexes(object())
    assert rebuilt == 1
    assert len(patched["ddl_calls"]) == 2  # both attempted
