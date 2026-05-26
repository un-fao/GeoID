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

"""TTL + version-keyed compiled-policy cache (#1343).

The per-process LRU on ``PolicyService.get_effective_policies`` is now
bounded by both a wall-clock TTL and the shared IAM rule_version counter
(shared with phantom_token). The matrix the tests pin:

* TTL expiry: an entry older than ``compiled_rule_cache_ttl_seconds``
  forces a re-compile on the next read.
* Rule-version invalidation: a bump on the binding-version counter (which
  every IAM CRUD writer already triggers via the storage layer) causes a
  cache MISS on the next read, regardless of how recent the entry is.
* Version stamping: a fresh entry carries the rule_version current at the
  moment of compute; after a bump, a NEW entry carries the higher version.
* Backend-down: with no distributed counter backend, the cache still works
  in-process (version stays at 0; TTL governs).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
from typing import List

import pytest

from dynastore.modules.iam import compiled_rule_cache
from dynastore.modules.iam.compiled_rule_cache import (
    CompiledRuleCache,
    get_compiled_rule_cache,
    get_ttl_seconds,
    iam_rule_version,
    iam_rule_version_async,
)
from dynastore.modules.iam.policies import PolicyService, Policy


@pytest.fixture(autouse=True)
def _reset_cache_state():
    """Each test starts on a fresh module-global cache + version snapshot."""
    compiled_rule_cache._reset_for_tests()
    yield
    compiled_rule_cache._reset_for_tests()


def _make_policy(pid: str) -> Policy:
    return Policy(
        id=pid,
        actions=["GET"],
        resources=["/x/.*"],
        effect="ALLOW",
        priority=0,
        partition_key="global",
        conditions=[],
    )


def _service_with_storage(storage: AsyncMock) -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc.storage = storage
    return svc


# --------------------------------------------------------------------------- #
# TTL semantics
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_cache_hit_within_ttl_skips_recompile() -> None:
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    # Fix the rule_version + ttl so the test is deterministic.
    with patch(
        "dynastore.modules.iam.policies.iam_rule_version_async",
        AsyncMock(return_value=0),
    ), patch(
        "dynastore.modules.iam.policies.get_ttl_seconds", return_value=60.0
    ):
        out1 = await svc.get_effective_policies("global", "iam")
        out2 = await svc.get_effective_policies("global", "iam")

    assert [p.id for p in out1] == ["p1"]
    assert [p.id for p in out2] == ["p1"]
    # One compile, two reads => the second was a HIT.
    assert storage.list_policies.await_count == 1


@pytest.mark.asyncio
async def test_cache_recomputes_after_ttl_expiry() -> None:
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    # Drive monotonic time manually so the test is not flaky on a busy box.
    clock = {"now": 1000.0}

    def _now() -> float:
        return clock["now"]

    cache = get_compiled_rule_cache()

    async def _compile() -> List[Policy]:
        return await storage.list_policies(
            partition_key="global", limit=1000, schema="iam"
        )

    await cache.get_or_compute(
        key=("global", "iam"),
        compute=_compile,
        ttl_seconds=10.0,
        rule_version=0,
        now=_now,
    )
    # Within TTL
    clock["now"] = 1009.0
    await cache.get_or_compute(
        key=("global", "iam"),
        compute=_compile,
        ttl_seconds=10.0,
        rule_version=0,
        now=_now,
    )
    assert storage.list_policies.await_count == 1

    # Past TTL — must re-compile.
    clock["now"] = 1011.0
    await cache.get_or_compute(
        key=("global", "iam"),
        compute=_compile,
        ttl_seconds=10.0,
        rule_version=0,
        now=_now,
    )
    assert storage.list_policies.await_count == 2


# --------------------------------------------------------------------------- #
# Rule-version invalidation
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_rule_version_bump_invalidates_within_ttl() -> None:
    """A version bump must invalidate the cache even when the TTL has not
    expired — that is the whole point of the version key. Otherwise a
    write on a sibling pod would leave this pod serving stale rules until
    the TTL window closes."""
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    version = {"v": 0}

    async def _async_v(schema: str) -> int:
        return version["v"]

    with patch(
        "dynastore.modules.iam.policies.iam_rule_version_async",
        side_effect=_async_v,
    ), patch(
        "dynastore.modules.iam.policies.get_ttl_seconds", return_value=3600.0
    ):
        await svc.get_effective_policies("global", "iam")
        await svc.get_effective_policies("global", "iam")
        assert storage.list_policies.await_count == 1  # HIT

        # Simulate a write on any pod: bump the version counter.
        version["v"] = 1
        await svc.get_effective_policies("global", "iam")
        assert storage.list_policies.await_count == 2  # MISS, re-compile


@pytest.mark.asyncio
async def test_fresh_entry_stamped_with_current_rule_version() -> None:
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    version = {"v": 7}

    async def _async_v(schema: str) -> int:
        return version["v"]

    with patch(
        "dynastore.modules.iam.policies.iam_rule_version_async",
        side_effect=_async_v,
    ), patch(
        "dynastore.modules.iam.policies.get_ttl_seconds", return_value=60.0
    ):
        await svc.get_effective_policies("global", "iam")

        snap = get_compiled_rule_cache().snapshot()
        assert ("global", "iam") in snap
        rule_v, _fetched_at = snap[("global", "iam")]
        assert rule_v == 7

        version["v"] = 8
        await svc.get_effective_policies("global", "iam")
        snap = get_compiled_rule_cache().snapshot()
        rule_v, _ = snap[("global", "iam")]
        assert rule_v == 8


# --------------------------------------------------------------------------- #
# Backend-down resilience
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_backend_down_falls_through_to_local_cache() -> None:
    """With no distributed counter backend, ``get_binding_version`` returns
    0 and the cache still works keyed on (partition, schema, 0). The hot
    path is never blocked by a Valkey outage."""
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    # Patch phantom_token to mimic "no distributed backend" — the function
    # logs and returns 0.
    with patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        AsyncMock(return_value=0),
    ), patch(
        "dynastore.modules.iam.policies.get_ttl_seconds", return_value=60.0
    ):
        out1 = await svc.get_effective_policies("global", "iam")
        out2 = await svc.get_effective_policies("global", "iam")

    assert [p.id for p in out1] == [p.id for p in out2] == ["p1"]
    assert storage.list_policies.await_count == 1  # HIT — local cache works


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #


def test_default_ttl_when_config_unreachable() -> None:
    """The hot path consumes ``get_ttl_seconds`` synchronously; it must
    return a sane default before the first config refresh has landed."""
    assert get_ttl_seconds() >= 1.0


def test_iam_rule_version_sync_default_zero() -> None:
    assert iam_rule_version() == 0


@pytest.mark.asyncio
async def test_iam_rule_version_async_updates_sync_snapshot() -> None:
    with patch(
        "dynastore.modules.iam.phantom_token.get_binding_version",
        AsyncMock(return_value=42),
    ):
        v = await iam_rule_version_async("iam")
    assert v == 42
    # Sync snapshot follows the async read for the platform schema.
    assert iam_rule_version() == 42


@pytest.mark.asyncio
async def test_invalidate_cache_drops_local_entries() -> None:
    storage = AsyncMock()
    storage.list_policies.return_value = [_make_policy("p1")]
    svc = _service_with_storage(storage)

    with patch(
        "dynastore.modules.iam.policies.iam_rule_version_async",
        AsyncMock(return_value=0),
    ), patch(
        "dynastore.modules.iam.policies.get_ttl_seconds", return_value=60.0
    ):
        await svc.get_effective_policies("global", "iam")
        svc.invalidate_cache()
        await svc.get_effective_policies("global", "iam")

    assert storage.list_policies.await_count == 2


# --------------------------------------------------------------------------- #
# Maxsize cap
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_maxsize_caps_entry_count() -> None:
    cache: CompiledRuleCache[str] = CompiledRuleCache(maxsize=2)

    async def _make(label: str):
        return label

    await cache.get_or_compute(
        key="a", compute=lambda: _make("A"), ttl_seconds=60.0, rule_version=0
    )
    await cache.get_or_compute(
        key="b", compute=lambda: _make("B"), ttl_seconds=60.0, rule_version=0
    )
    await cache.get_or_compute(
        key="c", compute=lambda: _make("C"), ttl_seconds=60.0, rule_version=0
    )

    assert len(cache.snapshot()) == 2
    # FIFO eviction — the just-inserted "c" must still be present.
    assert "c" in cache.snapshot()
