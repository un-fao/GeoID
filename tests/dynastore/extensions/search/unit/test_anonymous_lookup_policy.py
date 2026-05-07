"""Unit tests for the search extension's policy registration:

- ALLOW anonymous on /search/catalogs/{cat}/geoid/* when catalog opts in.
- DENY anonymous on /stac/catalogs/{cat}/* and /features/catalogs/{cat}/*
  when the same catalog opts in (lockdown to lookup-only for anonymous).
- Reindex policy (admin only) still registers.
"""
from unittest.mock import MagicMock

from dynastore.models.protocols.authorization import DefaultRole


def test_register_search_policies_registers_anonymous_lookup_allow(monkeypatch):
    """Registers ALLOW policy on /search/catalogs/.../geoid/* with the lookup-public condition."""
    from dynastore.extensions.search import policies as search_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: pm,
    )

    search_policies.register_search_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "search_anonymous_lookup_per_catalog" in registered
    p = registered["search_anonymous_lookup_per_catalog"]
    assert p.effect == "ALLOW"
    assert "GET" in p.actions and "POST" in p.actions
    assert any("/search/catalogs/" in r and "geoid" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_register_search_policies_registers_stac_anon_deny(monkeypatch):
    """Registers DENY policy on /stac/catalogs/{cat}/* anonymous when catalog opted in."""
    from dynastore.extensions.search import policies as search_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: pm,
    )
    search_policies.register_search_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "search_anonymous_stac_deny_lookup_only" in registered
    p = registered["search_anonymous_stac_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/stac/catalogs/" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_register_search_policies_registers_features_anon_deny(monkeypatch):
    """Registers DENY policy on /features/catalogs/{cat}/* anonymous when catalog opted in."""
    from dynastore.extensions.search import policies as search_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: pm,
    )
    search_policies.register_search_policies()

    registered = {call.args[0].id: call.args[0] for call in pm.register_policy.call_args_list}
    assert "search_anonymous_features_deny_lookup_only" in registered
    p = registered["search_anonymous_features_deny_lookup_only"]
    assert p.effect == "DENY"
    assert any("/features/catalogs/" in r for r in p.resources)
    assert any(c.type == "catalog_lookup_public_allowed" for c in p.conditions)


def test_anonymous_role_grants_all_three_lookup_policies(monkeypatch):
    """The ANONYMOUS role aggregates the ALLOW + 2 DENY policies."""
    from dynastore.extensions.search import policies as search_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: pm,
    )
    search_policies.register_search_policies()

    anon_grants = [
        call.args[0] for call in pm.register_role.call_args_list
        if call.args[0].name == DefaultRole.ANONYMOUS.value
    ]
    assert anon_grants, "ANONYMOUS role grant missing"
    granted_policies = set()
    for g in anon_grants:
        granted_policies.update(g.policies)
    assert "search_anonymous_lookup_per_catalog" in granted_policies
    assert "search_anonymous_stac_deny_lookup_only" in granted_policies
    assert "search_anonymous_features_deny_lookup_only" in granted_policies


def test_register_search_policies_keeps_reindex_admin_policy(monkeypatch):
    from dynastore.extensions.search import policies as search_policies

    pm = MagicMock()
    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: pm,
    )
    search_policies.register_search_policies()

    registered_ids = {call.args[0].id for call in pm.register_policy.call_args_list}
    assert "search_reindex_admin" in registered_ids


def test_register_search_policies_no_op_when_permission_protocol_missing(monkeypatch, caplog):
    from dynastore.extensions.search import policies as search_policies

    monkeypatch.setattr(
        "dynastore.extensions.search.policies.get_protocol",
        lambda _proto: None,
    )

    with caplog.at_level("WARNING"):
        search_policies.register_search_policies()

    assert any("PermissionProtocol not available" in rec.message for rec in caplog.records)
