"""TTL gate — bounded per-key throttle with single-flight semantics.

Use when you need to **rate-limit an idempotent async operation per-key**:

  - Reconcile OIDC roles for a given principal at most once per N seconds.
  - Refresh a per-tenant cache at most once per N seconds.
  - Serialize concurrent requests so only one runs the work, the others
    return a cached "already-fresh" signal.

The gate combines three concerns the call site otherwise has to wire by hand:

  1. **Bounded LRU**  — never grow without bound. Old keys evict when the
     gate is full so a long-lived process serving many distinct keys
     doesn't leak memory.
  2. **Per-key asyncio.Lock** — concurrent callers for the same key
     serialize. The second caller waits, sees the (now-fresh) timestamp,
     and skips. Idempotent ops then run exactly once per TTL window.
  3. **Explicit success marking** — caller decides whether to update the
     timestamp. On success → mark; on failure → leave the timestamp alone
     so the next request retries immediately (don't lock yourself out
     of recovery).

Usage:

    gate = TTLGate(maxsize=4096, ttl_seconds=60)

    async with gate.acquire(principal_id) as h:
        if not h.should_run:
            return False  # suppressed within TTL window
        ok = await do_expensive_idempotent_work()
        if ok:
            h.mark()  # success → suppress further runs for ttl_seconds
        return ok

Design notes:

  - Single-threaded by design (asyncio). Not safe to share across threads.
  - Per-key locks are stored in the same OrderedDict as timestamps so
    eviction stays consistent — when a key falls out of the LRU its lock
    goes with it. Concurrent waiters on an evicted-then-recreated key get
    a fresh lock; functionally fine for idempotent throttle (the rare
    extra run is the same code path as a normal first-call).
  - ``maxsize=0`` disables the gate (all calls report ``should_run=True``,
    nothing is tracked). Useful for tests that want to bypass throttling
    without conditionally importing.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)


@dataclass
class TTLGateHandle:
    """Yielded by ``TTLGate.acquire``. Carries the run decision and
    exposes ``mark()`` so the caller controls when the timestamp is
    advanced (typically only on successful work)."""

    should_run: bool
    _gate: "TTLGate" = None  # type: ignore[assignment]
    _key: Hashable = None
    _marked: bool = False

    def mark(self) -> None:
        """Advance the gate's timestamp for this key to "now". Subsequent
        ``acquire`` calls within ``ttl_seconds`` will report
        ``should_run=False``. No-op if the gate is disabled or the
        handle was opened with ``should_run=False`` (the timestamp is
        already fresh)."""
        if self._gate is None or self._marked:
            return
        self._gate._set_timestamp(self._key, time.monotonic())
        self._marked = True


class TTLGate(Generic[K]):
    """Bounded per-key TTL+lock throttle. See module docstring."""

    __slots__ = ("_ttl", "_maxsize", "_entries")

    def __init__(self, *, maxsize: int = 4096, ttl_seconds: float = 60.0) -> None:
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        self._ttl = float(ttl_seconds)
        self._maxsize = int(maxsize)
        # Each entry: [timestamp, lock]
        self._entries: "OrderedDict[K, list]" = OrderedDict()

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def __len__(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        """Drop all entries. Safe to call between TTL changes or in tests."""
        self._entries.clear()

    def _get_lock(self, key: K) -> asyncio.Lock:
        entry = self._entries.get(key)
        if entry is None:
            entry = [0.0, asyncio.Lock()]
            self._entries[key] = entry
            self._evict_if_needed()
        else:
            self._entries.move_to_end(key)
        return entry[1]

    def _set_timestamp(self, key: K, ts: float) -> None:
        entry = self._entries.get(key)
        if entry is None:
            return
        entry[0] = ts
        self._entries.move_to_end(key)

    def _evict_if_needed(self) -> None:
        while self._maxsize and len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    @asynccontextmanager
    async def acquire(self, key: K) -> AsyncIterator[TTLGateHandle]:
        """Acquire the gate for ``key``. Holds a per-key lock for the
        duration of the ``async with`` block, so concurrent callers for
        the same key serialize.

        Yields a :class:`TTLGateHandle`:
          - ``handle.should_run`` — True iff the TTL has elapsed (or
            this is the first call). The caller is expected to skip its
            work when False.
          - ``handle.mark()`` — call after successful work to advance
            the timestamp.

        When ``maxsize=0`` the gate is disabled: every call yields
        ``should_run=True`` with no locking and no tracking.
        """
        if self._maxsize == 0:
            yield TTLGateHandle(should_run=True)
            return

        lock = self._get_lock(key)
        async with lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            last = entry[0] if entry is not None else 0.0
            handle = TTLGateHandle(
                should_run=(now - last) >= self._ttl,
                _gate=self,
                _key=key,
            )
            yield handle
