"""Mandatory invariant: a live correct-tier owner satisfies; wrong tier or no
owner fails. Encodes 'has an owner != has the correct-tier owner'."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import mandatory


@pytest.mark.asyncio
async def test_satisfied_by_correct_tier_owner(monkeypatch):
    async def _owners_map(_engine, _conn, _grace):
        return {"cascade_cleanup": [{"service": "catalog", "affinity_tier": "catalog", "last_seen": "now"}]}
    monkeypatch.setattr(mandatory, "_fetch_live_owners_map", _owners_map)
    monkeypatch.setattr(mandatory, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    violations = await mandatory.check_mandatory_ownership(engine=object(), ttl_grace_seconds=90)
    assert violations == []


@pytest.mark.asyncio
async def test_violation_when_only_wrong_tier_owner(monkeypatch):
    async def _owners_map(_engine, _conn, _grace):
        return {"cascade_cleanup": [{"service": "worker", "affinity_tier": None, "last_seen": "now"}]}
    monkeypatch.setattr(mandatory, "_fetch_live_owners_map", _owners_map)
    monkeypatch.setattr(mandatory, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])

    violations = await mandatory.check_mandatory_ownership(engine=object(), ttl_grace_seconds=90)
    assert violations == ["cascade_cleanup"]


@pytest.mark.asyncio
async def test_no_owner_is_violation_and_unclaimable(monkeypatch):
    async def _none(_engine, _conn, _grace):
        return {}
    monkeypatch.setattr(mandatory, "_fetch_live_owners_map", _none)
    monkeypatch.setattr(mandatory, "_mandatory_specs", lambda: [("cascade_cleanup", "catalog")])
    violations = await mandatory.check_mandatory_ownership(engine=object(), ttl_grace_seconds=90)
    assert "cascade_cleanup" in violations
