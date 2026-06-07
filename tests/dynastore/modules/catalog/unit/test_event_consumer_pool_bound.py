"""The durable event consumer must not exhaust the shared DB pool.

The 16 shard consumers exist to partition the outbox for lock-free SKIP LOCKED
draining, NOT to each pin a pooled connection. Two invariants are pinned here:

1. **Bounded checkout.** All 16 shards share one ``LoopLocalSemaphore``
   (``EventService._consume_semaphore``); only the active pull + listener fan-out
   + ack hold a slot, so concurrent checkouts can never exceed
   ``_EVENT_CONSUMER_MAX_CONCURRENCY`` regardless of shard count.

2. **Correct pool-timeout classification.** Pool exhaustion surfaces as
   ``sqlalchemy.exc.TimeoutError`` (subclass of ``SQLAlchemyError`` — NOT the
   builtin ``TimeoutError``). The dedicated exponential-backoff branch must
   catch it; a prior version listed only ``(asyncio.TimeoutError, TimeoutError)``
   so the real exception fell through to the generic constant-5 s handler and
   the backoff never engaged under the exact pressure it was written for.
"""

from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import AsyncMock

from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from dynastore.modules.catalog.event_service import EventService
from dynastore.tools.async_utils import LoopLocalSemaphore


class _Shutdown:
    """Allows ``after_iters`` loop-body executions, then signals shutdown."""

    def __init__(self, after_iters: int) -> None:
        self._remaining = after_iters

    def is_set(self) -> bool:
        if self._remaining <= 0:
            return True
        self._remaining -= 1
        return False


def _service(concurrency: int = 4) -> EventService:
    svc = EventService.__new__(EventService)
    svc._consumer_running = False
    svc._consumer_task = None
    svc._consume_semaphore = LoopLocalSemaphore(concurrency)
    return svc


def test_service_constructs_loop_local_consume_semaphore():
    """Real ``__init__`` wires a loop-local bounded semaphore (idiomatic; #1640)."""
    svc = EventService()
    assert isinstance(svc._consume_semaphore, LoopLocalSemaphore)


async def test_sqlalchemy_pool_timeout_backs_off_exponentially(monkeypatch):
    """The REAL pool-exhaustion exception triggers exponential backoff.

    Regression for: ``except (asyncio.TimeoutError, TimeoutError)`` did not
    catch ``sqlalchemy.exc.TimeoutError`` ("QueuePool limit ... reached").
    """
    sleeps: List[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    svc = _service()
    driver = AsyncMock()
    driver.consume_batch = AsyncMock(
        side_effect=[
            SQLAlchemyTimeoutError("QueuePool limit of size 5 overflow 5 reached"),
            SQLAlchemyTimeoutError("QueuePool limit of size 5 overflow 5 reached"),
            SQLAlchemyTimeoutError("QueuePool limit of size 5 overflow 5 reached"),
            [],
        ]
    )
    driver.wait_for_events = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol", lambda _: driver
    )

    await svc._consume_shard(
        shard_id=0,
        shutdown_event=_Shutdown(after_iters=4),
        scope="ALL",
    )

    # Exponential pool-timeout backoff — NOT the generic constant 5 s path.
    timeout_backoffs = [s for s in sleeps if s in (5.0, 10.0, 20.0, 40.0, 60.0)]
    assert timeout_backoffs == [5.0, 10.0, 20.0]


async def test_concurrent_db_checkouts_never_exceed_cap(monkeypatch):
    """With the shared semaphore, observed concurrent pulls stay within the cap."""
    cap = 2
    svc = _service(concurrency=cap)
    state = {"cur": 0, "max": 0}

    async def consume_batch(*, scope, batch_size=100, shard=0):
        state["cur"] += 1
        state["max"] = max(state["max"], state["cur"])
        # Hold the slot long enough for siblings to contend for it.
        await asyncio.sleep(0.05)
        state["cur"] -= 1
        return []

    driver = AsyncMock()
    driver.consume_batch = consume_batch
    driver.wait_for_events = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.get_protocol", lambda _: driver
    )

    # All shards use shard_id=0 so none pays the per-shard startup stagger and
    # they contend at once; the 0.05 s hold above exercises the semaphore bound.
    async def run_one() -> None:
        await svc._consume_shard(
            shard_id=0,
            shutdown_event=_Shutdown(after_iters=1),
            scope="ALL",
        )

    await asyncio.wait_for(
        asyncio.gather(*(run_one() for _ in range(8))), timeout=5.0
    )

    assert state["max"] <= cap
