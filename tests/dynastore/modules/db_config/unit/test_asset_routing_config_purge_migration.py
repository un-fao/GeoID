"""Unit-level coverage for the AssetRoutingConfig purge migration (#641)."""

from __future__ import annotations

import pytest

from dynastore.modules.db_config import asset_routing_config_purge_migration as mod


class _FakeConn:
    pass


class _FakeTxCM:
    def __init__(self, conn: _FakeConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def patched(monkeypatch):
    state: dict = {
        "tenant_tables": [],
        "ddl_calls": [],
        "ddl_raises_on": set(),
    }

    def fake_managed(engine):
        return _FakeTxCM(_FakeConn())

    class _FakeDDLQuery:
        def __init__(self, sql: str, **_):
            self.sql = sql

        async def execute(self, conn, **kwargs):
            state["ddl_calls"].append(self.sql)
            for marker in state["ddl_raises_on"]:
                if marker in self.sql:
                    raise RuntimeError("simulated DDL failure")

    async def fake_find(conn, **kwargs):
        return state["tenant_tables"]

    monkeypatch.setattr(mod, "managed_transaction", fake_managed, raising=True)
    monkeypatch.setattr(mod, "DDLQuery", _FakeDDLQuery, raising=True)
    monkeypatch.setattr(
        mod._find_tenant_config_tables_query, "execute", fake_find, raising=True,
    )
    return state


@pytest.mark.asyncio
async def test_no_engine_is_noop():
    assert await mod.purge_asset_routing_config_rows(None) == 0


@pytest.mark.asyncio
async def test_clean_tree_still_purges_platform_table(patched):
    # Even with no tenant schemas, the platform table is always purged
    # (it always exists; the DELETE is a no-op when there are no rows).
    purged = await mod.purge_asset_routing_config_rows(object())
    assert purged == 1
    assert len(patched["ddl_calls"]) == 1
    assert 'configs"."platform_configs' in patched["ddl_calls"][0]
    assert "asset_routing_config" in patched["ddl_calls"][0]


@pytest.mark.asyncio
async def test_purges_every_tenant_config_table(patched):
    patched["tenant_tables"] = [
        {"schema_name": "cat_a", "table_name": "catalog_configs"},
        {"schema_name": "cat_a", "table_name": "collection_configs"},
        {"schema_name": "cat_b", "table_name": "catalog_configs"},
    ]
    purged = await mod.purge_asset_routing_config_rows(object())
    assert purged == 4  # 3 tenant + 1 platform
    joined = "\n".join(patched["ddl_calls"])
    assert '"cat_a"."catalog_configs"' in joined
    assert '"cat_a"."collection_configs"' in joined
    assert '"cat_b"."catalog_configs"' in joined
    assert all("asset_routing_config" in sql for sql in patched["ddl_calls"])


@pytest.mark.asyncio
async def test_one_failure_does_not_block_remaining(patched):
    patched["tenant_tables"] = [
        {"schema_name": "cat_a", "table_name": "catalog_configs"},
        {"schema_name": "cat_b", "table_name": "catalog_configs"},
    ]
    patched["ddl_raises_on"].add('"cat_a"')
    purged = await mod.purge_asset_routing_config_rows(object())
    # platform_configs + cat_b succeed; cat_a fails
    assert purged == 2
    assert len(patched["ddl_calls"]) == 3
