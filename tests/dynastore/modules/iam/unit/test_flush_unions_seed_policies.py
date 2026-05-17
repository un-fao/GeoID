"""Regression for geoid#902: flush_pending_registrations must UNION
``RoleSeed.policies`` for the role name into the persisted policy list.

Why this exists
---------------

The catalog-tier ``RoleSeed("unauthenticated", policies=["public_access"])``
at ``models/protocols/authorization.py:96`` declares the floor binding
that whitelists ``/health`` (and other anonymous endpoints).

``provision_default_policies(catalog_id=None)`` only writes
``platform_roles`` into the platform ``iam`` schema (per-catalog tier roles
are seeded by the per-catalog lifecycle hook). But every OGC extension
calls ``register_role(Role(name="unauthenticated",
policies=["<extension>_public_access"]))`` during its lifespan — and
``flush_pending_registrations`` persists ALL buffered roles to
``schema="iam"`` regardless of tier.

Pre-fix: the read-modify-write block at module.py:599 started from
``existing=None`` (no seed write ever landed for the catalog-tier role in
platform schema), so the row was written with only the contributor
policy list. The seed-declared ``public_access`` policy was silently
dropped → Cloud Run startup probe to ``/health`` returned 403 → no
revision could come up cold (geoid#902).

Post-fix: ``flush_pending_registrations`` builds a ``seed_policies_by_role``
lookup from ``IamRolesConfig.platform_roles + catalog_roles`` and unions
the seed policy list into the merge before writing — for both
update-existing and create-new paths.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _mk_role(name: str, policies: list[str]):
    """A simple stand-in for ``Role`` that records the post-``model_copy``
    payload so the test can assert on the policies the storage call was
    handed."""
    captured: dict[str, Any] = {"latest": None}

    class _Role:
        def __init__(self, n: str, ps: list[str]):
            self.name = n
            self.policies = ps

        def model_copy(self, update: dict):
            new = _Role(self.name, list(update.get("policies", self.policies)))
            captured["latest"] = new
            return new

    return _Role(name, policies), captured


def _mk_seed(name: str, policies: list[str]):
    seed = MagicMock()
    seed.name = name
    seed.policies = policies
    return seed


def _make_module(role: Any, *, existing_policies: list[str] | None,
                 platform_seeds: list, catalog_seeds: list):
    """Build an IamModule pre-loaded with one pending role, a stubbed
    storage whose ``get_role`` returns either ``None`` (no row yet) or a
    role with ``existing_policies`` already in the DB, and a stubbed
    ``_policy_service._role_config`` carrying the seed lists."""
    from dynastore.modules.iam.module import IamModule

    mod = IamModule()
    mod._pending_policies = {}
    mod._pending_roles = {role.name: role}
    mod._role_lock = asyncio.Lock()

    storage = MagicMock()
    if existing_policies is None:
        storage.get_role = AsyncMock(return_value=None)
    else:
        existing = MagicMock()
        existing.policies = list(existing_policies)
        storage.get_role = AsyncMock(return_value=existing)
    storage.create_role = AsyncMock(return_value=None)
    storage.update_role = AsyncMock(return_value=None)
    storage.update_policy = AsyncMock(return_value=None)
    mod.storage = storage

    role_config = MagicMock()
    role_config.platform_roles = platform_seeds
    role_config.catalog_roles = catalog_seeds

    policy_service = MagicMock()
    policy_service.storage = storage
    policy_service.invalidate_cache = MagicMock()
    policy_service._role_config = role_config
    mod._policy_service = policy_service

    return mod, storage


class _MTx:
    """Minimal ``managed_transaction`` async-context-manager stub."""

    def __init__(self, *_a, **_kw):
        pass

    async def __aenter__(self):
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)
        return conn

    async def __aexit__(self, *_):
        return False


def _captured_policies(storage_call: MagicMock) -> set[str]:
    """Extract the ``policies`` list from the role object handed to
    create_role/update_role and return it as a set."""
    args, kwargs = storage_call.call_args
    role = args[0] if args else kwargs.get("role")
    return set(role.policies)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_create_role_unions_seed_policies() -> None:
    """When the role does NOT exist in the platform ``iam`` schema yet —
    the cold-boot path on a freshly-cleared DB — ``flush_pending_registrations``
    MUST call ``create_role`` with the UNION of contributor policies AND
    the matching ``RoleSeed.policies``. This is the exact path that
    silently dropped ``public_access`` and caused geoid#902.
    """
    role, _ = _mk_role("unauthenticated", ["web_public_access", "stac_public_access"])
    seed = _mk_seed("unauthenticated", ["public_access"])

    mod, storage = _make_module(
        role,
        existing_policies=None,
        platform_seeds=[],
        catalog_seeds=[seed],
    )

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        await mod.flush_pending_registrations()

    assert storage.create_role.call_count == 1, (
        "Expected exactly one create_role call for the new 'unauthenticated' row."
    )
    persisted = _captured_policies(storage.create_role)
    assert "public_access" in persisted, (
        f"geoid#902 regression: seed-declared 'public_access' policy is "
        f"missing from the persisted policy list. Got: {sorted(persisted)}. "
        f"flush_pending_registrations must UNION RoleSeed.policies into the "
        f"create_role payload — otherwise the catalog-tier seed binding is "
        f"dropped on cold boot and /health returns 403."
    )
    assert "web_public_access" in persisted, (
        "Contributor policy must also be present — the fix UNIONS seed + "
        "contributor, it does not replace one with the other."
    )
    assert "stac_public_access" in persisted


@pytest.mark.asyncio
async def test_flush_update_role_unions_seed_policies() -> None:
    """When the role already exists in the DB (warm boot or repeated
    flush), the update path MUST still union the seed policies — otherwise
    a DB row written before this fix shipped would never self-heal to
    include ``public_access`` after restart on the fixed code.
    """
    role, _ = _mk_role("unauthenticated", ["web_public_access"])
    seed = _mk_seed("unauthenticated", ["public_access"])

    # Existing row in DB lacks public_access — the broken state.
    mod, storage = _make_module(
        role,
        existing_policies=["notebooks_public_access"],
        platform_seeds=[],
        catalog_seeds=[seed],
    )

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        await mod.flush_pending_registrations()

    assert storage.update_role.call_count == 1
    persisted = _captured_policies(storage.update_role)
    assert {"public_access", "notebooks_public_access", "web_public_access"} <= persisted, (
        f"Update path must UNION existing-DB + seed + contributor. "
        f"Got: {sorted(persisted)}."
    )


@pytest.mark.asyncio
async def test_flush_no_seed_for_role_leaves_contributor_only() -> None:
    """For a role with NO matching RoleSeed (e.g. an extension-only role
    like 'custom_extension_role'), the seed-union must be a no-op — the
    persisted list is just contributor policies. This pins that the fix
    doesn't accidentally inject phantom policies into unrelated roles.
    """
    role, _ = _mk_role("custom_extension_role", ["custom_policy"])
    # No seed matching this role name.
    seed = _mk_seed("unauthenticated", ["public_access"])

    mod, storage = _make_module(
        role,
        existing_policies=None,
        platform_seeds=[],
        catalog_seeds=[seed],
    )

    fake_db = MagicMock()
    fake_db.engine = MagicMock()
    with patch("dynastore.modules.iam.module.get_protocol", return_value=fake_db), \
         patch("dynastore.modules.db_config.query_executor.managed_transaction", _MTx):
        await mod.flush_pending_registrations()

    persisted = _captured_policies(storage.create_role)
    assert persisted == {"custom_policy"}, (
        f"Roles without a matching seed must be persisted with exactly "
        f"the contributor policy list — no phantom seed injection. "
        f"Got: {sorted(persisted)}."
    )
