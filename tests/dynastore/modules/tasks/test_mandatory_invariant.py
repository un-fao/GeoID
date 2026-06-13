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
