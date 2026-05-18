"""Unit tests for ``capability_stats`` — Valkey-backed observability
counters added in PR B of #524.

Verifies:
- Bump is fail-open (Valkey absent ⇒ silent no-op).
- Bump no-ops on empty cap_id / task_type / zero amount.
- Bump uses the expected key shape and TTL semantics.
- Read returns 0 for never-incremented counters.
- Read returns None on backend failure (admin endpoint must distinguish).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import capability_stats as cs


class _FakeAsyncBackend:
    def __init__(self):
        self._store: dict[str, int] = {}
        self.incr_calls: list[tuple[str, int, float | None]] = []

    async def incr(self, key, amount=1, *, ttl=None):
        self.incr_calls.append((key, amount, ttl))
        self._store[key] = self._store.get(key, 0) + amount
        return self._store[key]

    async def get_count(self, key):
        return self._store.get(key)


def _patch_backend(backend):
    mgr = MagicMock()
    mgr.get_async_backend.return_value = backend
    return patch("dynastore.tools.cache.get_cache_manager", return_value=mgr)


@pytest.mark.asyncio
async def test_bump_dlq_reactive_writes_expected_key_and_ttl():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_dlq("reactive", "cap-1", "index_propagation", 4)
    assert backend.incr_calls == [
        ("dynastore:cap_stats:dlq_reactive:cap-1:index_propagation", 4, 24 * 3600.0),
    ]


@pytest.mark.asyncio
async def test_bump_dlq_proactive_uses_distinct_key():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_dlq("proactive", "cap-1", "index_propagation", 2)
        await cs.bump_dlq("reactive", "cap-1", "index_propagation", 1)
    keys = sorted({k for k, _, _ in backend.incr_calls})
    assert keys == [
        "dynastore:cap_stats:dlq_proactive:cap-1:index_propagation",
        "dynastore:cap_stats:dlq_reactive:cap-1:index_propagation",
    ], "reactive and proactive must not collide"


@pytest.mark.asyncio
async def test_bump_dlq_unknown_source_is_noop():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_dlq("mystery", "cap-1", "index_propagation", 7)
    assert backend.incr_calls == []


@pytest.mark.asyncio
async def test_bump_claim_rejected_uses_distinct_key():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_claim_rejected("cap-1", "index_propagation")
    assert backend.incr_calls == [
        ("dynastore:cap_stats:claim_rejected:cap-1:index_propagation", 1, 24 * 3600.0),
    ]


@pytest.mark.asyncio
async def test_bump_swallows_zero_or_empty_args():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_dlq("reactive", "", "index_propagation", 1)
        await cs.bump_dlq("reactive", "cap-1", "", 1)
        await cs.bump_dlq("reactive", "cap-1", "index_propagation", 0)
        await cs.bump_claim_rejected("", "index_propagation")
    assert backend.incr_calls == []


@pytest.mark.asyncio
async def test_bump_fail_open_when_backend_raises():
    backend = MagicMock()
    backend.incr = AsyncMock(side_effect=RuntimeError("valkey down"))
    with _patch_backend(backend):
        # Must not raise.
        await cs.bump_claim_rejected("cap-1", "index_propagation")


@pytest.mark.asyncio
async def test_bump_fail_open_when_no_backend():
    """If get_cache_manager raises (backend not registered) the bump must
    swallow silently — never block the dispatcher hot path."""
    with patch(
        "dynastore.tools.cache.get_cache_manager",
        side_effect=RuntimeError("no cache"),
    ):
        await cs.bump_claim_rejected("cap-1", "index_propagation")


@pytest.mark.asyncio
async def test_read_counters_returns_zero_for_unset_keys():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        out = await cs.read_counters("cap-1", "index_propagation")
    assert out == {
        "claim_rejected": 0,
        "dlq_reactive": 0,
        "dlq_proactive": 0,
    }


@pytest.mark.asyncio
async def test_read_counters_returns_recorded_values():
    backend = _FakeAsyncBackend()
    with _patch_backend(backend):
        await cs.bump_dlq("reactive", "cap-1", "index_propagation", 3)
        await cs.bump_dlq("proactive", "cap-1", "index_propagation", 5)
        await cs.bump_claim_rejected("cap-1", "index_propagation")
        await cs.bump_claim_rejected("cap-1", "index_propagation")
        out = await cs.read_counters("cap-1", "index_propagation")
    assert out == {
        "claim_rejected": 2,
        "dlq_reactive": 3,
        "dlq_proactive": 5,
    }


@pytest.mark.asyncio
async def test_read_counters_returns_none_on_backend_unavailable():
    """When the backend is unreachable, the read returns None per counter
    so the admin endpoint can distinguish 'unknown' from 'zero'."""
    with patch(
        "dynastore.tools.cache.get_cache_manager",
        side_effect=RuntimeError("no cache"),
    ):
        out = await cs.read_counters("cap-1", "index_propagation")
    assert out == {
        "claim_rejected": None,
        "dlq_reactive": None,
        "dlq_proactive": None,
    }
