"""A PENDING row of an unclaimable task with required_capability=None IS
dead-lettered by the backstop (the #1647-class escape the capability reaper skips)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import dispatcher


@pytest.mark.asyncio
async def test_capabilityless_unclaimable_rows_are_dlqd(monkeypatch):
    executed = {"sql": None, "params": None}

    class _Result:
        rowcount = 3

    class _Conn:
        async def execute(self, sql, params=None):
            executed["sql"] = str(sql)
            executed["params"] = params
            return _Result()
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _Engine:
        def begin(self): return _Conn()

    async def _unclaimable(_engine, *, ttl_grace_seconds):
        return ["cascade_cleanup"]
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _unclaimable)

    n = await dispatcher.sweep_unclaimable_rows(
        _Engine(), schema="system", ttl_grace_seconds=90, min_age_s=300
    )
    assert n == 3
    assert "dead_letter" in executed["sql"].lower()
    assert "cascade_cleanup" in str(executed["params"])
    assert "min_age_s" in str(executed["params"])
    assert "make_interval" in executed["sql"]


@pytest.mark.asyncio
async def test_no_unclaimable_types_is_a_noop(monkeypatch):
    async def _none(_engine, *, ttl_grace_seconds):
        return []
    monkeypatch.setattr(dispatcher, "_find_unclaimable_task_types", _none)

    class _Engine:
        def begin(self):  # must NOT be called
            raise AssertionError("should not open a transaction when nothing is unclaimable")

    n = await dispatcher.sweep_unclaimable_rows(
        _Engine(), schema="system", ttl_grace_seconds=90, min_age_s=300
    )
    assert n == 0
