"""Global ``idx_assets_metadata_gin_*`` backfill — unit-level coverage.

The SQL is exercised by integration tests against a real Postgres; these
unit tests cover the Python orchestration: early return on empty engine,
no-op when the scan returns zero rows, count + per-schema DDL when rows
are found, and continue-on-failure behavior.
"""

from __future__ import annotations

from typing import List

import pytest

from dynastore.modules.db_config import assets_metadata_gin_migration as mod


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
    """Stub ``managed_transaction``, ``DDLQuery``, and the find-missing DQL."""
    state: dict = {
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
        mod._find_missing_gin_query, "execute", fake_find, raising=True,
    )
    return state


@pytest.mark.asyncio
async def test_no_engine_is_noop():
    assert await mod.migrate_assets_metadata_gin_index(None) == 0


@pytest.mark.asyncio
async def test_clean_database_is_noop(patched):
    patched["rows"] = []
    created = await mod.migrate_assets_metadata_gin_index(object())
    assert created == 0
    assert patched["ddl_calls"] == []


@pytest.mark.asyncio
async def test_creates_index_on_every_missing_schema(patched):
    patched["rows"] = [
        {"schema_name": "cat_a"},
        {"schema_name": "cat_b"},
    ]
    created = await mod.migrate_assets_metadata_gin_index(object())
    assert created == 2
    assert len(patched["ddl_calls"]) == 2
    assert '"cat_a".assets' in patched["ddl_calls"][0]
    assert "idx_assets_metadata_gin_cat_a" in patched["ddl_calls"][0]
    assert "USING GIN (metadata jsonb_path_ops)" in patched["ddl_calls"][0]
    assert '"cat_b".assets' in patched["ddl_calls"][1]


@pytest.mark.asyncio
async def test_one_failure_does_not_block_remaining(patched):
    patched["rows"] = [
        {"schema_name": "cat_a"},
        {"schema_name": "cat_b"},
    ]
    patched["ddl_raises_on"].add("cat_a")
    created = await mod.migrate_assets_metadata_gin_index(object())
    assert created == 1
    assert len(patched["ddl_calls"]) == 2  # both attempted


@pytest.mark.asyncio
async def test_dotted_schema_name_is_sanitized_for_index_name(patched):
    """Schema names with dots get the dot replaced with underscore in the
    derived ``schema_tag`` so the generated index identifier is a single
    valid SQL identifier."""
    patched["rows"] = [{"schema_name": "tenant.foo"}]
    created = await mod.migrate_assets_metadata_gin_index(object())
    assert created == 1
    assert "idx_assets_metadata_gin_tenant_foo" in patched["ddl_calls"][0]
