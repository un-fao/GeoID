"""Pool-hygiene guard for ``managed_transaction(engine)``.

A pooled connection may be handed back with a pending rollback left over from a
prior caller that exited without cleanly closing its transaction. Before
opening a new transaction the engine branch of ``managed_transaction`` issues a
best-effort ``rollback()`` to reset the checkout, and on
``PendingRollbackError``/``InvalidRequestError`` invalidates the connection
and retries with a fresh one from the pool.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest
from sqlalchemy.exc import PendingRollbackError


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, *, poisoned: bool, log: List[str]):
        self._poisoned = poisoned
        self._log = log
        self.invalidated_count = 0
        self.closed = False
        self.rollback_count = 0
        self.begin_count = 0
        self.sync_connection = SimpleNamespace()

    async def rollback(self):
        self.rollback_count += 1
        self._log.append("rollback")
        if self._poisoned:
            self._poisoned = False
            raise PendingRollbackError(
                "pending rollback on pooled connection", None, None
            )

    async def invalidate(self):
        self.invalidated_count += 1
        self._log.append("invalidate")

    async def close(self):
        self.closed = True
        self._log.append("close")

    def begin(self):
        self.begin_count += 1
        self._log.append("begin")
        return _FakeTx()


class _FakeEngine:
    def __init__(self, conns: List[_FakeConn]):
        self._conns = list(conns)
        self.connect_calls = 0

    async def connect(self):
        self.connect_calls += 1
        return self._conns.pop(0)


@pytest.mark.asyncio
async def test_pool_hygiene_invalidates_and_retries_on_pending_rollback(
    monkeypatch,
):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    poisoned = _FakeConn(poisoned=True, log=log)
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([poisoned, clean])

    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    assert engine.connect_calls == 2
    assert poisoned.invalidated_count == 1
    assert poisoned.closed is True
    assert clean.rollback_count == 1
    assert clean.begin_count == 1
    assert clean.closed is True
    assert log == [
        "rollback",
        "invalidate",
        "close",
        "rollback",
        "begin",
        "close",
    ]


@pytest.mark.asyncio
async def test_pool_hygiene_is_cheap_no_op_on_clean_connection(monkeypatch):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([clean])

    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    assert engine.connect_calls == 1
    assert clean.rollback_count == 1
    assert clean.invalidated_count == 0
    assert clean.begin_count == 1
    assert clean.closed is True
