"""Unit tests for the geoid extension's per-catalog anonymous policy
registration (``catalog_policies.register_geoid_policies_for_catalog``).

Policy registration moved from a global extension-startup hook to a
per-catalog hook fired when the geoid preset is applied to a catalog. The
preset defines four anonymous policies, created in the catalog's tenant
schema:

- ALLOW lookup search — POST /search/catalogs/{cat}/items-search
  (resolve one item) when the catalog opted in (``is_public``).
- DENY STAC enumeration in lookup-only mode.
- DENY OGC Features enumeration in lookup-only mode.
- ALLOW anonymous create per-collection (deny-wins still applies).

Role enrichment (binding these to the catalog's unauthenticated role) runs
against tenant-schema storage and is exercised by integration tests; here we
pin the policy *contents* deterministically with a mocked PermissionProtocol.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.geoid import catalog_policies


def _cond_type(cond) -> str:
    """Condition entries may be raw dicts or coerced PolicyCondition objects."""
    return cond.get("type") if isinstance(cond, dict) else cond.type


def _mock_pm() -> MagicMock:
    """A PermissionProtocol stand-in: every policy is new, creation records."""
    pm = MagicMock()
    pm.get_policy = AsyncMock(return_value=None)
    pm.create_policy = AsyncMock()
    return pm


async def _register_and_collect(monkeypatch, pm: MagicMock) -> dict:
    monkeypatch.setattr(
        "dynastore.extensions.geoid.catalog_policies.get_protocol",
        lambda _proto: pm,
    )
    await catalog_policies.register_geoid_policies_for_catalog("cat-x")
    return {call.args[0].id: call.args[0] for call in pm.create_policy.call_args_list}


@pytest.mark.asyncio
async def test_registers_anonymous_lookup_allow(monkeypatch):
    """ALLOW on POST /search/catalogs/.../items-search with the lookup-public
    condition (#1210). The ALLOW must not leak onto the broad catalog search."""
    created = await _register_and_collect(monkeypatch, _mock_pm())

    assert "geoid_anonymous_lookup" in created
    p = created["geoid_anonymous_lookup"]
    assert p.effect == "ALLOW"
    assert p.actions == ["POST"]
    assert p.resources
    assert all("items-search" in r for r in p.resources)
    assert any(_cond_type(c) == "catalog_lookup_public_allowed" for c in p.conditions)


@pytest.mark.asyncio
async def test_registers_stac_anon_deny(monkeypatch):
    """DENY anonymous STAC enumeration when in lookup-only mode."""
    created = await _register_and_collect(monkeypatch, _mock_pm())

    assert "geoid_anonymous_stac_deny_lookup_only" in created
    p = created["geoid_anonymous_stac_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/stac/catalogs/" in r for r in p.resources)
    assert any(_cond_type(c) == "catalog_lookup_public_allowed" for c in p.conditions)


@pytest.mark.asyncio
async def test_registers_features_anon_deny(monkeypatch):
    """DENY anonymous OGC Features enumeration when in lookup-only mode."""
    created = await _register_and_collect(monkeypatch, _mock_pm())

    assert "geoid_anonymous_features_deny_lookup_only" in created
    p = created["geoid_anonymous_features_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/features/catalogs/" in r for r in p.resources)
    assert any(_cond_type(c) == "catalog_lookup_public_allowed" for c in p.conditions)


@pytest.mark.asyncio
async def test_registers_anonymous_create_allow(monkeypatch):
    """ALLOW anonymous POST to a collection's items when the collection opted in
    via CollectionWriteAudience (deny-wins still applies)."""
    created = await _register_and_collect(monkeypatch, _mock_pm())

    assert "geoid_anonymous_create_per_collection" in created
    p = created["geoid_anonymous_create_per_collection"]
    assert p.effect == "ALLOW"
    assert "POST" in p.actions
    assert any(
        "/stac/catalogs/" in r and "/collections/" in r and "/items" in r
        for r in p.resources
    )
    assert any(_cond_type(c) == "collection_write_anonymous_allowed" for c in p.conditions)


@pytest.mark.asyncio
async def test_registers_all_four_anonymous_policies(monkeypatch):
    """The geoid profile defines exactly these four anonymous policies — the
    same set bound to the catalog's unauthenticated role on enrichment."""
    created = await _register_and_collect(monkeypatch, _mock_pm())

    assert {
        "geoid_anonymous_lookup",
        "geoid_anonymous_stac_deny_lookup_only",
        "geoid_anonymous_features_deny_lookup_only",
        "geoid_anonymous_create_per_collection",
    }.issubset(set(created))


@pytest.mark.asyncio
async def test_no_op_when_permission_protocol_missing(monkeypatch, caplog):
    """No PermissionProtocol → warn and register nothing (don't crash)."""
    monkeypatch.setattr(
        "dynastore.extensions.geoid.catalog_policies.get_protocol",
        lambda _proto: None,
    )

    with caplog.at_level("WARNING"):
        await catalog_policies.register_geoid_policies_for_catalog("cat-x")

    assert any("PermissionProtocol not available" in rec.message for rec in caplog.records)
