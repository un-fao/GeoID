"""Regression tests for the IAM seeding race fixed by #204.

The pre-fix `flush_pending_registrations` fanned ~30 policy persists +
4 role persists into ``asyncio.gather(*tasks, return_exceptions=True)``.
Each task opened its own ``managed_transaction(engine)``. Two compounding
bugs (issue #203):

1. **Deadlock window** between sibling services seeding the same
   ``iam.policies_global`` partition concurrently.
2. **Silent failures** — ``return_exceptions=True`` dropped the
   rolled-back exceptions on the floor.

These tests pin the post-fix invariants so the silent-failure mode
can't sneak back in:

  * Single-transaction guarantee — the entire flush opens exactly
    ONE ``managed_transaction(engine)`` regardless of how many
    policies / roles are pending.
  * No-swallow guarantee — a storage exception during any per-row
    upsert PROPAGATES out of ``flush_pending_registrations`` instead
    of being collected by ``asyncio.gather`` and dropped.
  * Empty-flush short-circuit — no DB call when nothing is pending.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers — minimal IamModule stub that exposes flush_pending_registrations
# ---------------------------------------------------------------------------

def _make_module_with_pending(policies: list, roles: list, *, fail_on_idx: int | None = None):
    """Build an ``IamModule`` instance pre-loaded with ``policies`` + ``roles``,
    a stubbed ``_policy_service`` whose ``storage.update_policy`` records
    every call (and optionally raises on the Nth call to simulate a failure
    inside the per-row upsert).
    """
    import asyncio

    from dynastore.modules.iam.module import IamModule

    mod = IamModule()
    mod._pending_policies = {p.id: p for p in policies}
    mod._pending_roles = {r.name: r for r in roles}
    mod._role_lock = asyncio.Lock()

    # Storage stub with call-counting + optional injected failure.
    storage = MagicMock()
    storage.get_role = AsyncMock(return_value=None)
    storage.create_role = AsyncMock(return_value=None)
    storage.update_role = AsyncMock(return_value=None)

    call_count = {"n": 0}

    async def _maybe_fail_update_policy(*args, **kwargs):
        call_count["n"] += 1
        if fail_on_idx is not None and call_count["n"] == fail_on_idx:
            raise RuntimeError(f"injected failure on policy upsert #{fail_on_idx}")
        return None

    storage.update_policy = _maybe_fail_update_policy
    mod.storage = storage

    policy_service = MagicMock()
    policy_service.storage = storage
    policy_service.invalidate_cache = MagicMock()
    mod._policy_service = policy_service

    return mod, call_count


def _mk_policy(pid: str):
    p = MagicMock()
    p.id = pid
    p.partition_key = "global"
    return p


def _mk_role(name: str, policies: list[str]):
    r = MagicMock()
    r.name = name
    r.policies = policies
    r.model_copy = lambda update: r  # passthrough — we don't assert on the merged payload
    return r


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_flush_short_circuits_without_db_call() -> None:
    """No pending policies/roles → no DB engine acquisition, no transaction."""
    mod, calls = _make_module_with_pending([], [])
    with patch("dynastore.modules.iam.module.get_protocol") as gp:
        await mod.flush_pending_registrations()
        # If the function tried to grab an engine, get_protocol would have been called.
        gp.assert_not_called()
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_flush_opens_single_transaction_for_all_pending() -> None:
    """Pin the load-bearing invariant of #204: ONE managed_transaction(engine)
    regardless of how many policies/roles are pending. Pre-fix this would
    have been ``len(policies) + len(roles)`` separate transactions.
    """
    policies = [_mk_policy(f"p{i}") for i in range(5)]
    roles = [_mk_role(f"r{i}", []) for i in range(2)]
    mod, calls = _make_module_with_pending(policies, roles)

    # Stub managed_transaction to count entries; yield a sentinel "conn".
    enter_count = {"n": 0}

    class _MTx:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            enter_count["n"] += 1
            return MagicMock()  # conn

        async def __aexit__(self, *_):
            return False

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        await mod.flush_pending_registrations()

    assert enter_count["n"] == 1, (
        f"Expected exactly 1 managed_transaction(engine) call, got {enter_count['n']}"
    )
    assert calls["n"] == 5, f"Expected 5 update_policy calls, got {calls['n']}"


@pytest.mark.asyncio
async def test_flush_propagates_storage_exception_instead_of_swallowing() -> None:
    """The pre-fix ``asyncio.gather(..., return_exceptions=True)`` swallowed
    rollbacks silently. Post-fix, an exception in any per-row upsert
    propagates out of flush_pending_registrations so the operator sees it
    in the lifespan startup logs (and on a CI smoke test).
    """
    policies = [_mk_policy(f"p{i}") for i in range(5)]
    mod, calls = _make_module_with_pending(policies, [], fail_on_idx=3)

    class _MTx:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return MagicMock()

        async def __aexit__(self, *_):
            return False

    fake_db = MagicMock()
    fake_db.engine = MagicMock()

    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        with pytest.raises(RuntimeError, match="injected failure on policy upsert #3"):
            await mod.flush_pending_registrations()

    # 3 calls before the failure (1, 2, 3) — the 4th and 5th never happened
    # because the raise short-circuits the loop. This is the desired
    # fail-fast behaviour: don't leave the partial seed half-committed.
    assert calls["n"] == 3, f"Expected exactly 3 update_policy calls (failed on #3), got {calls['n']}"


@pytest.mark.asyncio
async def test_flush_clears_pending_buffers_before_attempting_persist() -> None:
    """Whether or not the persist succeeds, the pending buffers must be
    drained — otherwise a retry path would re-attempt the same payload
    indefinitely. Pin the existing behaviour so a refactor doesn't drop it.
    """
    policies = [_mk_policy(f"p{i}") for i in range(3)]
    roles = [_mk_role("r0", [])]
    mod, _ = _make_module_with_pending(policies, roles)

    class _MTx:
        def __init__(self, *_a, **_kw):
            pass

        async def __aenter__(self):
            return MagicMock()

        async def __aexit__(self, *_):
            return False

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        await mod.flush_pending_registrations()

    assert mod._pending_policies == {}, "pending policies should be drained"
    assert mod._pending_roles == {}, "pending roles should be drained"
