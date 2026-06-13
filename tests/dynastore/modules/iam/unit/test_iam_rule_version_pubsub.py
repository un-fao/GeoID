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

"""Rule-version invalidation discipline across IAM writers (#1343).

The project's cache layer has no pub/sub channel (documented in
:mod:`dynastore.modules.iam.phantom_token`). Cross-pod invalidation rides
on the per-schema **binding-version counter**: every IAM mutation
increments the counter, every reader keys its cache on the counter value,
so a write on pod A is observed by pod B on its next read (key miss).

These tests pin the writer-side discipline that the compiled-rule cache
relies on: each storage-layer mutation must call ``_bump_binding_version``
(which delegates to :func:`phantom_token.bump_binding_version`) so the
shared counter advances. If a future commit adds a new write path that
forgets to bump, the read-side cache will silently serve stale rules until
the TTL backstop fires — that is a regression these tests catch.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.iam import compiled_rule_cache, phantom_token


@pytest.fixture(autouse=True)
def _reset_state():
    compiled_rule_cache._reset_for_tests()
    phantom_token._reset_caches()
    yield
    compiled_rule_cache._reset_for_tests()
    phantom_token._reset_caches()


# --------------------------------------------------------------------------- #
# Discipline 1: every existing CRUD storage method that ends a write must
# have ``_bump_binding_version`` somewhere in its body. The grep here is
# deliberately source-level (not behavioural) because the actual bump
# travels through ``await`` chains that are awkward to monkeypatch without
# spinning a Postgres engine — and the point of the test is to fail loudly
# the moment a new write method skips the bump.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "module_path,method_name",
    [
        ("dynastore.modules.iam.postgres_iam_storage", "create_role"),
        ("dynastore.modules.iam.postgres_iam_storage", "update_role"),
        ("dynastore.modules.iam.postgres_iam_storage", "delete_role"),
        ("dynastore.modules.iam.postgres_iam_storage", "add_role_hierarchy"),
        ("dynastore.modules.iam.postgres_iam_storage", "remove_role_hierarchy"),
        ("dynastore.modules.iam.postgres_iam_storage", "grant"),
        ("dynastore.modules.iam.postgres_iam_storage", "revoke"),
        ("dynastore.modules.iam.postgres_iam_storage", "revoke_by_match"),
        ("dynastore.modules.iam.postgres_iam_storage", "update_principal"),
        ("dynastore.modules.iam.postgres_policy_storage", "create_policy"),
        ("dynastore.modules.iam.postgres_policy_storage", "update_policy"),
        ("dynastore.modules.iam.postgres_policy_storage", "delete_policy"),
    ],
)
def test_writer_bumps_binding_version(module_path: str, method_name: str) -> None:
    import importlib

    mod = importlib.import_module(module_path)
    # The class is whichever attribute owns the method; find it.
    candidates = [
        v
        for v in vars(mod).values()
        if inspect.isclass(v) and getattr(v, method_name, None) is not None
    ]
    assert candidates, f"no class in {module_path} defines {method_name}"

    found = False
    for cls in candidates:
        m = getattr(cls, method_name, None)
        if m is None:
            continue
        src = inspect.getsource(m)
        if "_bump_binding_version" in src:
            found = True
            break

    assert found, (
        f"{module_path}.{method_name} writes IAM state but does not call "
        f"``_bump_binding_version`` — sibling pods will serve stale rules "
        f"until the compiled-rule TTL backstop fires."
    )


# --------------------------------------------------------------------------- #
# Discipline 2: a duplicate bump is idempotent — counter monotonicity must
# never go backwards. The counter is an INCR on a Valkey key so this is
# trivially true at the backend level; the test asserts the local memo
# behaves the same way (no double-decrement on a missed pop).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_double_bump_is_monotonic() -> None:
    """Receiving the same version-bump event twice must not break
    monotonicity. The backend ``incr`` is naturally monotonic; the local
    memo pop is idempotent (``dict.pop`` with default never raises)."""
    counts = {"n": 0}

    async def _incr(key):
        counts["n"] += 1
        return counts["n"]

    async def _get_count(key):
        return counts["n"]

    fake_backend = AsyncMock()
    fake_backend.incr = AsyncMock(side_effect=_incr)
    fake_backend.get_count = AsyncMock(side_effect=_get_count)

    with patch(
        "dynastore.modules.iam.phantom_token._distributed_backend",
        return_value=fake_backend,
    ):
        v0 = await phantom_token.get_binding_version("iam")
        await phantom_token.bump_binding_version("iam")
        v1 = await phantom_token.get_binding_version("iam")
        await phantom_token.bump_binding_version("iam")
        v2 = await phantom_token.get_binding_version("iam")

    assert v0 == 0
    assert v1 == 1
    assert v2 == 2


# --------------------------------------------------------------------------- #
# Discipline 3: the compiled-rule cache's ``iam_rule_version_async`` reads
# the same counter phantom_token writes — one signal, never two.
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_rule_version_shares_counter_with_phantom_token() -> None:
    counts = {"iam": 5}

    async def _get_count(key):
        # The key prefix is implementation detail; the version routes
        # through the public ``get_binding_version`` helper which knows
        # how to map ``schema -> backend key``.
        return counts["iam"]

    fake_backend = AsyncMock()
    fake_backend.get_count = AsyncMock(side_effect=_get_count)

    with patch(
        "dynastore.modules.iam.phantom_token._distributed_backend",
        return_value=fake_backend,
    ):
        v = await compiled_rule_cache.iam_rule_version_async("iam")

    assert v == 5
    # Sync snapshot tracks the async read (platform schema).
    assert compiled_rule_cache.iam_rule_version() == 5
