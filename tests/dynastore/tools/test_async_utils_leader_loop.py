"""run_leader_loop — leadership released on exception."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from dynastore.tools.async_utils import run_leader_loop


class _LeadershipTracker:
    """Records enter/exit pairs around the leader-held context."""

    def __init__(self, *, is_leader_sequence: list[bool]) -> None:
        self._is_leader = list(is_leader_sequence)
        self.acquired = 0
        self.released = 0
        self.held = False

    @asynccontextmanager
    async def acquire(self):
        is_leader = self._is_leader.pop(0) if self._is_leader else False
        self.acquired += 1
        if is_leader:
            self.held = True
        try:
            yield is_leader
        finally:
            if is_leader:
                self.held = False
                self.released += 1


@pytest.mark.asyncio
async def test_resigns_on_exception_inside_on_leader():
    """Body raising must NOT keep leadership — the lock must be released
    before the outer loop retries."""
    tracker = _LeadershipTracker(is_leader_sequence=[True, True])
    held_during_exception = False

    async def _raising_body():
        nonlocal held_during_exception
        held_during_exception = tracker.held
        raise RuntimeError("boom")

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 2  # let one full iteration run then stop

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_raising_body,
        name="test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
    )

    assert held_during_exception is True
    assert tracker.held is False
    assert tracker.acquired >= 1
    assert tracker.released == tracker.acquired


@pytest.mark.asyncio
async def test_non_leader_sleeps_and_retries():
    tracker = _LeadershipTracker(is_leader_sequence=[False, False, True])
    body_calls = {"n": 0}

    async def _body():
        body_calls["n"] += 1

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 3

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_body,
        name="test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
    )

    assert body_calls["n"] == 1
    assert tracker.acquired == 3


@pytest.mark.asyncio
async def test_cancelled_error_propagates():
    tracker = _LeadershipTracker(is_leader_sequence=[True])

    async def _cancel_body():
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await run_leader_loop(
            acquire_leadership=tracker.acquire,
            on_leader=_cancel_body,
            name="test",
            cadence_seconds=0.0,
        )

    assert tracker.held is False
    assert tracker.released == 1
