"""Unit tests for :class:`ReindexConsumer` (M3.1b).

Covers the contract between the consumer loop and the underlying
``EventDriverProtocol`` / :class:`ReindexWorker`:

- Leader election — ``is_leader=False`` → sleep + retry, no consume.
- Empty batch — wait on ``driver.wait_for_events`` (LISTEN/NOTIFY).
- Mixed result batch — ACK successes, NACK failures individually.
- Degraded (ACK + errors) → ACK, WARNING log.
- ACK/NACK exceptions bubbled to WARNING but don't break the loop.
- Missing ``event_id`` on a result → logged, neither ACKed nor NACKed.
- No ``EventDriverProtocol`` registered → polite retry, no crash.
- Shutdown breaks the outer AND inner loops cleanly.

Driver + worker are both mocked; this file never touches asyncpg /
CatalogRoutingConfig.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _FakeShutdown:
    """Duck-typed ``shutdown_event`` — returns ``False`` for *N* reads, then ``True``.

    The outer loop checks ``is_set`` once per round; the inner loop
    checks once per claim.  For a single-batch test we need 2 Falses
    (outer-entry + inner-entry) before True, so tests pass
    ``fire_after=2``.  A ``_FakeShutdown`` never flips back to False
    once it's returned True — consistent with a real
    ``asyncio.Event`` / ``threading.Event``.
    """

    def __init__(self, fire_after: int = 2) -> None:
        self._remaining = fire_after

    def is_set(self) -> bool:
        if self._remaining > 0:
            self._remaining -= 1
            return False
        return True


def _fake_driver(
    *,
    is_leader: bool = True,
    batches: Optional[List[List[dict]]] = None,
) -> MagicMock:
    """Build a mock ``EventDriverProtocol`` with configurable behaviour.

    - ``is_leader`` drives ``acquire_consumer_lock``'s yielded value.
    - ``batches`` is a list of batches ``consume_batch`` will return
      in order; after the list is exhausted further calls return ``[]``
      (empty batch) so the inner loop exits via ``wait_for_events``.
    """
    driver = MagicMock()

    @asynccontextmanager
    async def _acquire(key):
        yield is_leader

    driver.acquire_consumer_lock = _acquire
    batch_iter = iter(batches or [])

    async def _consume_batch(scope, batch_size, shard=None):
        return next(batch_iter, [])

    driver.consume_batch = AsyncMock(side_effect=_consume_batch)
    driver.wait_for_events = AsyncMock()
    driver.ack = AsyncMock()
    driver.nack = AsyncMock()
    return driver


# ---------------------------------------------------------------------------
# Result-partitioning helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mixed_batch_acks_successes_and_nacks_failures():
    """ACK all succeeded results in one call; NACK each failure individually."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(event_id="e1", succeeded=True, should_retry=False, errors=[]),
        _DispatchResult(event_id="e2", succeeded=True, should_retry=False, errors=[]),
        _DispatchResult(event_id="e3", succeeded=False, should_retry=True,
                        errors=["ES 500"]),
        _DispatchResult(event_id="e4", succeeded=False, should_retry=True,
                        errors=["ES 500", "timeout"]),
    ])

    driver = _fake_driver(batches=[[{"event_id": f"e{i}"} for i in (1, 2, 3, 4)]])
    shutdown = _FakeShutdown(fire_after=2)

    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ):
        await consumer.run_until_shutdown(shutdown)

    # Exactly one ACK call with both success ids, in order.
    driver.ack.assert_awaited_once_with(event_ids=["e1", "e2"])
    # Two NACK calls, one per failure, carrying the joined error messages.
    assert driver.nack.await_count == 2
    assert driver.nack.await_args_list[0].kwargs == {
        "event_id": "e3", "error": "ES 500",
    }
    assert driver.nack.await_args_list[1].kwargs == {
        "event_id": "e4", "error": "ES 500; timeout",
    }


@pytest.mark.asyncio
async def test_degraded_result_is_acked_with_warning(caplog):
    """``succeeded=True`` + errors → ACK + WARNING log (SLA-degraded branch)."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id="e9", succeeded=True, should_retry=False,
            errors=["ES degraded: timeout on on_timeout=degrade"],
        ),
    ])
    driver = _fake_driver(batches=[[{"event_id": "e9"}]])
    shutdown = _FakeShutdown(fire_after=2)

    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ), caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_consumer",
    ):
        await consumer.run_until_shutdown(shutdown)

    driver.ack.assert_awaited_once_with(event_ids=["e9"])
    driver.nack.assert_not_awaited()
    assert any("degraded event e9" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_result_missing_event_id_is_logged_and_skipped(caplog):
    """``event_id=None`` from the worker is an outbox anomaly — log + skip."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(event_id=None, succeeded=False, should_retry=True,
                        errors=["missing event_id"]),
        _DispatchResult(event_id="ok", succeeded=True, should_retry=False,
                        errors=[]),
    ])
    driver = _fake_driver(batches=[[{"event_id": "anything"}, {"event_id": "ok"}]])
    shutdown = _FakeShutdown(fire_after=2)

    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ), caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_consumer",
    ):
        await consumer.run_until_shutdown(shutdown)

    # Only the id-ful result made it into the ACK list.
    driver.ack.assert_awaited_once_with(event_ids=["ok"])
    driver.nack.assert_not_awaited()
    assert any("has no event_id" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Leader election + empty-batch paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_leader_does_not_consume(monkeypatch):
    """``is_leader=False`` → poll sleep + retry, no consume_batch calls."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer

    # Skip the 5s backoff so the test runs instantly.
    sleep_calls: List[float] = []

    async def _fast_sleep(s):
        sleep_calls.append(s)

    monkeypatch.setattr(
        "dynastore.modules.catalog.reindex_consumer.asyncio.sleep", _fast_sleep,
    )

    driver = _fake_driver(is_leader=False)
    worker = MagicMock()
    worker.handle_batch = AsyncMock()

    shutdown = _FakeShutdown(fire_after=2)
    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ):
        await consumer.run_until_shutdown(shutdown)

    # Consumer saw is_leader=False → slept once on the backoff path.
    assert 5.0 in sleep_calls
    # No consume / worker invocations — exactly the leader-election
    # contract we wanted to pin.
    driver.consume_batch.assert_not_awaited()
    worker.handle_batch.assert_not_awaited()
    driver.ack.assert_not_awaited()
    driver.nack.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch_waits_for_events(monkeypatch):
    """Consume returning ``[]`` → call ``wait_for_events`` (LISTEN/NOTIFY)."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer

    driver = _fake_driver(batches=[[]])
    worker = MagicMock()
    worker.handle_batch = AsyncMock()

    # Flip shutdown after one wait_for_events call so the inner loop
    # exits right after the empty-batch branch.
    wait_count = [0]

    async def _wait(timeout):
        wait_count[0] += 1

    driver.wait_for_events = AsyncMock(side_effect=_wait)

    shutdown = _FakeShutdown(fire_after=3)  # generous — covers outer+inner

    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ):
        await consumer.run_until_shutdown(shutdown)

    driver.consume_batch.assert_awaited()
    driver.wait_for_events.assert_awaited()
    worker.handle_batch.assert_not_awaited()  # no batch → no dispatch


# ---------------------------------------------------------------------------
# Fault tolerance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_driver_registered_backs_off_without_crashing(
    monkeypatch, caplog,
):
    """Missing ``EventDriverProtocol`` → WARNING + backoff, not exception."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer

    sleep_count = [0]

    async def _fast_sleep(s):
        sleep_count[0] += 1

    monkeypatch.setattr(
        "dynastore.modules.catalog.reindex_consumer.asyncio.sleep", _fast_sleep,
    )

    worker = MagicMock()
    worker.handle_batch = AsyncMock()
    shutdown = _FakeShutdown(fire_after=2)

    consumer = ReindexConsumer(worker=worker)
    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=None,
    ), caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_consumer",
    ):
        await consumer.run_until_shutdown(shutdown)

    assert sleep_count[0] >= 1
    worker.handle_batch.assert_not_awaited()
    assert any(
        "EventDriverProtocol not registered" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_ack_exception_logged_but_loop_survives(caplog):
    """``driver.ack`` raising must not break the consume loop."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(event_id="e1", succeeded=True, should_retry=False, errors=[]),
    ])
    driver = _fake_driver(batches=[[{"event_id": "e1"}]])
    driver.ack = AsyncMock(side_effect=RuntimeError("pg down"))

    shutdown = _FakeShutdown(fire_after=2)
    consumer = ReindexConsumer(worker=worker)

    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ), caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_consumer",
    ):
        # Must not raise — the consumer absorbs the ACK failure.
        await consumer.run_until_shutdown(shutdown)

    assert any("ACK batch failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_nack_exception_logged_but_loop_survives(caplog):
    """``driver.nack`` raising on one event must not skip the rest."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(event_id="e1", succeeded=False, should_retry=True,
                        errors=["A"]),
        _DispatchResult(event_id="e2", succeeded=False, should_retry=True,
                        errors=["B"]),
    ])
    driver = _fake_driver(batches=[[{"event_id": "e1"}, {"event_id": "e2"}]])
    # First NACK blows up, second must still be attempted.
    driver.nack = AsyncMock(side_effect=[RuntimeError("pg down"), None])

    shutdown = _FakeShutdown(fire_after=2)
    consumer = ReindexConsumer(worker=worker)

    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ), caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_consumer",
    ):
        await consumer.run_until_shutdown(shutdown)

    assert driver.nack.await_count == 2
    assert any("NACK failed for e1" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_leader_loop_exception_triggers_outer_reconnect(monkeypatch):
    """Inner loop raising → outer loop logs + reconnects, doesn't crash."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer

    monkeypatch.setattr(
        "dynastore.modules.catalog.reindex_consumer.asyncio.sleep", AsyncMock(),
    )

    # First consume raises; we immediately shutdown on the next outer
    # iteration to keep the test bounded.
    driver = MagicMock()

    @asynccontextmanager
    async def _acquire(key):
        yield True

    driver.acquire_consumer_lock = _acquire
    driver.consume_batch = AsyncMock(side_effect=RuntimeError("pg dropped"))
    driver.wait_for_events = AsyncMock()
    driver.ack = AsyncMock()
    driver.nack = AsyncMock()

    worker = MagicMock()
    shutdown = _FakeShutdown(fire_after=2)
    consumer = ReindexConsumer(worker=worker)

    with patch(
        "dynastore.modules.catalog.reindex_consumer.get_protocol",
        return_value=driver,
    ):
        # The outer loop catches BaseException, logs, and moves on —
        # the test passes if the call returns at all.
        await consumer.run_until_shutdown(shutdown)

    driver.consume_batch.assert_awaited()


# ---------------------------------------------------------------------------
# Construction defaults
# ---------------------------------------------------------------------------


def test_default_worker_constructed_when_not_injected():
    """No ``worker`` → default ``ReindexWorker`` materialised."""
    from dynastore.modules.catalog.reindex_consumer import ReindexConsumer
    from dynastore.modules.catalog.reindex_worker import ReindexWorker

    consumer = ReindexConsumer()
    assert isinstance(consumer._worker, ReindexWorker)


def test_leader_key_and_batch_size_defaults():
    """Defaults align with the documented tuning knobs."""
    from dynastore.modules.catalog.reindex_consumer import (
        DEFAULT_BATCH_SIZE, DEFAULT_LEADER_KEY, ReindexConsumer,
    )

    consumer = ReindexConsumer()
    assert consumer._leader_key == DEFAULT_LEADER_KEY
    assert consumer._leader_key == "dynastore.reindex.catalog.v1"
    assert consumer._batch_size == DEFAULT_BATCH_SIZE == 100
