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
    """Patch managed_transaction + DQLQuery so DELETE calls are recorded
    and the rowcount each one returns is scriptable per-table."""
    state: dict = {
        "tenant_tables": [],
        "delete_calls": [],
        "rowcount_by_target": {},  # ("schema", "table") -> int
        "raises_on": set(),  # substrings matched against the SQL
    }

    def fake_managed(engine):
        return _FakeTxCM(_FakeConn())

    class _FakeDQLQuery:
        def __init__(self, sql: str, *, result_handler=None, **_):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            state["delete_calls"].append((self.sql, kwargs))
            for marker in state["raises_on"]:
                if marker in self.sql:
                    raise RuntimeError("simulated DELETE failure")
            for (schema, table), rc in state["rowcount_by_target"].items():
                if f'"{schema}"."{table}"' in self.sql:
                    return rc
            return 0

    async def fake_find(conn, **kwargs):
        return state["tenant_tables"]

    monkeypatch.setattr(mod, "managed_transaction", fake_managed, raising=True)
    monkeypatch.setattr(mod, "DQLQuery", _FakeDQLQuery, raising=True)
    monkeypatch.setattr(
        mod._find_tenant_config_tables_query, "execute", fake_find, raising=True,
    )
    return state


@pytest.mark.asyncio
async def test_no_engine_is_noop():
    assert await mod.purge_asset_routing_config_rows(None) == 0


@pytest.mark.asyncio
async def test_clean_tree_returns_zero_but_still_issues_platform_delete(patched):
    # No tenant schemas; platform DELETE runs once and rowcount=0.
    deleted = await mod.purge_asset_routing_config_rows(object())
    assert deleted == 0
    assert len(patched["delete_calls"]) == 1
    sql, kwargs = patched["delete_calls"][0]
    assert 'configs"."platform_configs' in sql
    assert kwargs == {"class_key": "asset_routing_config"}


@pytest.mark.asyncio
async def test_returns_total_rowcount_across_all_tables(patched):
    patched["tenant_tables"] = [
        {"schema_name": "cat_a", "table_name": "catalog_configs"},
        {"schema_name": "cat_a", "table_name": "collection_configs"},
        {"schema_name": "cat_b", "table_name": "catalog_configs"},
    ]
    patched["rowcount_by_target"] = {
        ("configs", "platform_configs"): 1,
        ("cat_a", "catalog_configs"): 3,
        ("cat_a", "collection_configs"): 0,
        ("cat_b", "catalog_configs"): 2,
    }
    deleted = await mod.purge_asset_routing_config_rows(object())
    assert deleted == 6  # 1 + 3 + 0 + 2
    assert len(patched["delete_calls"]) == 4
    assert all(
        kwargs == {"class_key": "asset_routing_config"}
        for _, kwargs in patched["delete_calls"]
    )


@pytest.mark.asyncio
async def test_uses_named_param_binding_not_inline_literal(patched):
    """Regression guard against the original f-string-inline shape (#641 followup)."""
    await mod.purge_asset_routing_config_rows(object())
    sql, kwargs = patched["delete_calls"][0]
    assert ":class_key" in sql
    assert "'asset_routing_config'" not in sql
    assert kwargs == {"class_key": "asset_routing_config"}


@pytest.mark.asyncio
async def test_one_failure_does_not_block_remaining(patched):
    patched["tenant_tables"] = [
        {"schema_name": "cat_a", "table_name": "catalog_configs"},
        {"schema_name": "cat_b", "table_name": "catalog_configs"},
    ]
    patched["rowcount_by_target"] = {
        ("configs", "platform_configs"): 1,
        ("cat_b", "catalog_configs"): 5,
    }
    patched["raises_on"].add('"cat_a"')
    deleted = await mod.purge_asset_routing_config_rows(object())
    assert deleted == 6  # platform=1, cat_b=5; cat_a raises and contributes 0
    assert len(patched["delete_calls"]) == 3
