"""``EventService._consume_shard`` must back off exponentially when the
DB pool can't be checked out, instead of retrying every 5 s and adding to
the pile-on. Generic exceptions keep the existing 5 s back-off so the
pool-pressure path stays differentiated from "everything else".
"""

from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.catalog.event_service import EventService


class _Shutdown:
    def __init__(self, after_iters: int) -> None:
        self._remaining = after_iters

    def is_set(self) -> bool:
        if self._remaining <= 0:
            return True
        self._remaining -= 1
        return False


def _service() -> EventService:
    svc = EventService.__new__(EventService)
    svc._async_listeners = {}
    svc._consumer_running = False
    svc._consumer_task = None
    return svc


@pytest.fixture
def sleep_recorder(monkeypatch):
    sleeps: List[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return sleeps


async def test_pool_timeout_backs_off_exponentially(sleep_recorder, monkeypatch):
    svc = _service()
    driver = AsyncMock()
    # First three polls timeout, fourth succeeds with empty batch.
    driver.consume_batch = AsyncMock(side_effect=[
        asyncio.TimeoutError(),
        asyncio.TimeoutError(),
        asyncio.TimeoutError(),
        [],
    ])
    driver.wait_for_events = AsyncMock(return_value=None)

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: driver,
    )

    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_Shutdown(after_iters=4),
        scope="PLATFORM",
    )

    # First entry is the shard-startup stagger (shard 0 → 0 s, no sleep
    # call — the function skips it). Backoffs after each TimeoutError:
    # 5, 10, 20 (capped doubling). The fourth iteration hits wait_for_events,
    # not asyncio.sleep.
    timeout_backoffs = [s for s in sleep_recorder if s in (5.0, 10.0, 20.0, 40.0, 60.0)]
    assert timeout_backoffs == [5.0, 10.0, 20.0]


async def test_pool_timeout_backoff_caps_at_60s(sleep_recorder, monkeypatch):
    svc = _service()
    driver = AsyncMock()
    driver.consume_batch = AsyncMock(side_effect=[
        asyncio.TimeoutError() for _ in range(8)
    ])
    driver.wait_for_events = AsyncMock(return_value=None)

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: driver,
    )

    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_Shutdown(after_iters=8),
        scope="PLATFORM",
    )

    timeout_backoffs = [s for s in sleep_recorder if s in (5.0, 10.0, 20.0, 40.0, 60.0)]
    # 8 iterations: 5, 10, 20, 40, 60, 60, 60, 60
    assert timeout_backoffs == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0, 60.0]


async def test_successful_consume_resets_timeout_backoff(sleep_recorder, monkeypatch):
    svc = _service()
    driver = AsyncMock()
    driver.consume_batch = AsyncMock(side_effect=[
        asyncio.TimeoutError(),
        asyncio.TimeoutError(),
        [],                       # success → reset
        asyncio.TimeoutError(),   # next failure should restart at 5, not 20
    ])
    driver.wait_for_events = AsyncMock(return_value=None)

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: driver,
    )

    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_Shutdown(after_iters=4),
        scope="PLATFORM",
    )

    timeout_backoffs = [s for s in sleep_recorder if s in (5.0, 10.0, 20.0, 40.0, 60.0)]
    # 5, 10, (success — no sleep here, wait_for_events instead), 5
    assert timeout_backoffs == [5.0, 10.0, 5.0]


async def test_generic_exception_keeps_constant_5s_backoff(sleep_recorder, monkeypatch):
    svc = _service()
    driver = AsyncMock()
    driver.consume_batch = AsyncMock(side_effect=[
        RuntimeError("boom"),
        RuntimeError("boom"),
        RuntimeError("boom"),
    ])

    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol",
        lambda _: driver,
    )

    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_Shutdown(after_iters=3),
        scope="PLATFORM",
    )

    # All three failures use the constant 5 s backoff for non-pool errors.
    generic_backoffs = [s for s in sleep_recorder if s == 5.0]
    assert generic_backoffs == [5.0, 5.0, 5.0]
