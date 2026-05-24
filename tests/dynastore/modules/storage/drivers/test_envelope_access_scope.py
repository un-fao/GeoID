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
"""Unit tests for the shared read-scope compile helper used by every search
entry point that dispatches to the access-controlled envelope driver."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.models.protocols.access_filter import AccessClause, AccessFilter, FieldPredicate
from dynastore.modules.storage.drivers.elasticsearch_envelope import access_scope


class _FakePerms:
    """Records the compile_read_filter call and returns a canned filter."""

    def __init__(self, result: AccessFilter) -> None:
        self.result = result
        self.calls: list = []

    async def compile_read_filter(
        self, principals, catalog_id=None, collection_id=None, *, principal=None
    ):
        self.calls.append(
            {
                "principals": principals,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "principal": principal,
            }
        )
        return self.result


def _patch_get_protocol(monkeypatch, value) -> None:
    # The helper imports get_protocol from dynastore.tools.discovery at call time.
    import dynastore.tools.discovery as discovery

    monkeypatch.setattr(discovery, "get_protocol", lambda _proto: value)


@pytest.mark.asyncio
async def test_no_permission_protocol_fails_closed(monkeypatch):
    _patch_get_protocol(monkeypatch, None)
    af = await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["c"], principals=["anon"], principal=None
    )
    assert isinstance(af, AccessFilter)
    assert af.deny_all is True


@pytest.mark.asyncio
async def test_single_collection_compiles_at_collection_scope(monkeypatch):
    perms = _FakePerms(
        AccessFilter(allow=(AccessClause((FieldPredicate("visibility", ("public",)),)),))
    )
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["only"], principals=["r"], principal=None
    )
    assert perms.calls[0]["catalog_id"] == "acme"
    assert perms.calls[0]["collection_id"] == "only"


@pytest.mark.asyncio
async def test_multi_collection_compiles_at_catalog_scope(monkeypatch):
    perms = _FakePerms(AccessFilter.allow_everything())
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=["a", "b"], principals=["r"], principal=None
    )
    # More than one collection → no single collection pin (catalog scope).
    assert perms.calls[0]["collection_id"] is None


@pytest.mark.asyncio
async def test_none_principals_normalize_to_empty_list(monkeypatch):
    perms = _FakePerms(AccessFilter.deny_everything())
    _patch_get_protocol(monkeypatch, perms)
    await access_scope.compile_read_access_filter(
        catalog_id="acme", collections=None, principals=None, principal=None
    )
    assert perms.calls[0]["principals"] == []


def test_principals_from_request_state_flat_list():
    request = SimpleNamespace(
        state=SimpleNamespace(
            principal="P", principal_id="user:alice", principal_role=["admin", "reader"]
        )
    )
    principals, principal = access_scope.principals_from_request_state(request)
    assert principal == "P"
    assert principals == ["user:alice", "admin", "reader"]


def test_principals_from_request_state_anonymous_scalar_role():
    request = SimpleNamespace(
        state=SimpleNamespace(
            principal=None, principal_id=None, principal_role="unauthenticated"
        )
    )
    principals, principal = access_scope.principals_from_request_state(request)
    assert principal is None
    assert principals == ["unauthenticated"]
