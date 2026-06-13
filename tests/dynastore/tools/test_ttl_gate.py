#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for ``dynastore.tools.ttl_gate.TTLGate``.

The gate is the reusable building block behind the OIDC role-sync
throttle (``IamService._reconcile_oidc_roles``). The same primitive is
intended for any future "rate-limit an idempotent async op per-key"
need (cache-warm, per-tenant index refresh, ...), so the tests here pin
the contract independently of any specific consumer.

Coverage:
  - First-call lets the work run; mark suppresses subsequent calls within
    TTL; expiry re-opens the gate.
  - Without ``mark()`` the gate stays open forever for that key — failure
    paths shouldn't lock callers out of recovery.
  - Concurrent callers for the same key serialize via the per-key lock;
    only the first observes ``should_run=True``, the rest see the
    just-marked timestamp and skip.
  - Concurrent callers for *different* keys do NOT serialize.
  - ``maxsize`` enforces an LRU bound; the least-recently-touched key
    falls out first.
  - ``maxsize=0`` disables the gate entirely.
  - Bad constructor args raise.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from dynastore.tools.ttl_gate import TTLGate


@pytest.mark.asyncio
async def test_first_call_runs_then_throttles() -> None:
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)

    async with gate.acquire("k") as h:
        assert h.should_run is True
        h.mark()

    async with gate.acquire("k") as h:
        assert h.should_run is False, "Subsequent call within TTL must be suppressed"


@pytest.mark.asyncio
async def test_unmarked_handle_does_not_throttle_next_call() -> None:
    """If the caller never calls mark() (e.g. the work failed), the next
    request must still be allowed to run. Otherwise transient failures
    would lock the key out for the full TTL."""
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)

    async with gate.acquire("k") as h:
        assert h.should_run is True
        # No mark — simulate a failure in the body.

    async with gate.acquire("k") as h:
        assert h.should_run is True, "Without mark(), the gate must stay open"


@pytest.mark.asyncio
async def test_ttl_expiry_reopens_gate() -> None:
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=0.05)

    async with gate.acquire("k") as h:
        assert h.should_run is True
        h.mark()

    async with gate.acquire("k") as h:
        assert h.should_run is False

    # Sleep past the TTL.
    await asyncio.sleep(0.07)

    async with gate.acquire("k") as h:
        assert h.should_run is True


@pytest.mark.asyncio
async def test_concurrent_same_key_serializes() -> None:
    """Two concurrent acquires for the same key MUST serialize, and the
    second caller must observe should_run=False (because the first marked
    the key from inside its critical section)."""
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)

    observed: list[bool] = []
    barrier = asyncio.Event()

    async def worker(label: str, hold: float) -> None:
        async with gate.acquire("shared") as h:
            observed.append(h.should_run)
            if h.should_run:
                # Hold the lock briefly to force the second worker to wait.
                await asyncio.sleep(hold)
                h.mark()
                barrier.set()

    await asyncio.gather(worker("a", 0.05), worker("b", 0.0))

    assert observed.count(True) == 1, (
        f"Exactly one of the concurrent callers must run, got {observed!r}"
    )
    assert observed.count(False) == 1


@pytest.mark.asyncio
async def test_concurrent_different_keys_do_not_serialize() -> None:
    """Different keys MUST NOT block each other."""
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)

    started_a = asyncio.Event()
    release_a = asyncio.Event()
    b_finished = False

    async def hold_a() -> None:
        async with gate.acquire("a") as h:
            assert h.should_run is True
            started_a.set()
            await release_a.wait()
            h.mark()

    async def quick_b() -> None:
        nonlocal b_finished
        await started_a.wait()
        async with gate.acquire("b") as h:
            assert h.should_run is True
            h.mark()
        b_finished = True

    a = asyncio.create_task(hold_a())
    b = asyncio.create_task(quick_b())

    # Give b a chance to complete while a is still holding its lock.
    await asyncio.sleep(0.05)
    assert b_finished is True, (
        "Worker B (different key) must not block on worker A's lock"
    )

    release_a.set()
    await asyncio.gather(a, b)


@pytest.mark.asyncio
async def test_lru_eviction_when_full() -> None:
    gate: TTLGate[str] = TTLGate(maxsize=2, ttl_seconds=60)

    async with gate.acquire("a") as h:
        h.mark()
    async with gate.acquire("b") as h:
        h.mark()
    # Touching 'a' moves it to the front of the LRU.
    async with gate.acquire("a") as h:
        assert h.should_run is False  # within TTL
    # Inserting 'c' must evict 'b' (the LRU).
    async with gate.acquire("c") as h:
        h.mark()

    assert len(gate) == 2

    # 'a' was NOT evicted (it was touched after 'b'), still throttled.
    # Check 'a' BEFORE 'b' — re-acquiring 'b' inserts a fresh entry which
    # would itself evict 'a' under maxsize=2, masking the assertion below.
    async with gate.acquire("a") as h:
        assert h.should_run is False

    # 'b' was evicted → fresh state, should_run=True.
    async with gate.acquire("b") as h:
        assert h.should_run is True


@pytest.mark.asyncio
async def test_maxsize_zero_disables_gate() -> None:
    gate: TTLGate[str] = TTLGate(maxsize=0, ttl_seconds=60)

    for _ in range(5):
        async with gate.acquire("k") as h:
            assert h.should_run is True
            h.mark()  # no-op when disabled

    assert len(gate) == 0


@pytest.mark.asyncio
async def test_clear_drops_all_entries() -> None:
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)
    async with gate.acquire("a") as h:
        h.mark()
    async with gate.acquire("b") as h:
        h.mark()
    assert len(gate) == 2

    gate.clear()
    assert len(gate) == 0

    async with gate.acquire("a") as h:
        assert h.should_run is True


def test_invalid_constructor_args() -> None:
    with pytest.raises(ValueError):
        TTLGate(maxsize=-1, ttl_seconds=60)
    with pytest.raises(ValueError):
        TTLGate(maxsize=10, ttl_seconds=-1.0)


@pytest.mark.asyncio
async def test_mark_is_idempotent_within_handle() -> None:
    """Calling mark() twice on the same handle is harmless."""
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)
    async with gate.acquire("k") as h:
        h.mark()
        h.mark()  # no-op second time

    async with gate.acquire("k") as h:
        assert h.should_run is False


@pytest.mark.asyncio
async def test_mark_on_should_run_false_handle_is_noop() -> None:
    """A handle yielded with should_run=False has nothing to advance —
    calling mark() must not crash and must not change anything."""
    gate: TTLGate[str] = TTLGate(maxsize=16, ttl_seconds=60)
    async with gate.acquire("k") as h:
        h.mark()
    t1 = time.monotonic()

    async with gate.acquire("k") as h:
        assert h.should_run is False
        h.mark()  # caller used the wrong shape — must not raise

    # Still throttled.
    async with gate.acquire("k") as h:
        assert h.should_run is False
        # Timestamp shouldn't have moved past t1+ttl prematurely; we can't
        # easily probe it directly but we can verify the gate is still
        # closed, which is the contract the consumer relies on.
    _ = t1  # silence linter
