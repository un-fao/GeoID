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

"""Unit tests for ``IamListingVisibility``.

Exercises catalog and collection filter compilation via scripted stubs:
allow-all shortcut, mixed-visible catalogs, deny-everything paths, caching,
and collection-level compile isolation (no allow_all shortcut).

``_enumerate_catalog_ids`` / ``_enumerate_collection_ids`` call
``get_protocol(CatalogsProtocol)`` via a lazy local import inside the
implementation. We patch ``dynastore.tools.discovery.get_protocol`` to
return a stub that does not need to satisfy the full CatalogsProtocol
structural interface.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    extract_id_constraint,
    get_request_visibility,
)
from dynastore.modules.iam.listing_visibility import IamListingVisibility


# ---------------------------------------------------------------------------
# Stub: scripted PermissionProtocol
# ---------------------------------------------------------------------------


class _StubPermissions:
    """Scripted ``PermissionProtocol`` whose ``compile_read_filter`` is driven
    by a mapping of (catalog_id, collection_id) → AccessFilter.

    ``default`` is returned for any key not in the map. A call counter lets
    tests verify caching behaviour without subclassing.
    """

    def __init__(
        self,
        script: Dict[Tuple[Optional[str], Optional[str]], AccessFilter],
        default: Optional[AccessFilter] = None,
    ) -> None:
        self._script = script
        self._default = default or AccessFilter.deny_everything()
        self.call_count = 0

    async def compile_read_filter(
        self,
        principals: list,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        *,
        principal: Any = None,
        principal_id: Any = None,
    ) -> AccessFilter:
        self.call_count += 1
        return self._script.get(
            (catalog_id, collection_id), self._default
        )


# ---------------------------------------------------------------------------
# Stub: catalog/collection enumerator
# ---------------------------------------------------------------------------


class _StubCatalogs:
    """Lightweight stub for CatalogsProtocol enumeration.

    Also asserts the bypass contract: during enumeration, the visibility
    contextvar must be None (proving the provider ran under visibility_bypass).
    """

    def __init__(
        self,
        catalog_ids: List[str],
        collection_map: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        self._catalog_ids = catalog_ids
        self._collection_map = collection_map or {}

    async def list_catalogs(
        self, limit: int = 500, offset: int = 0, **kwargs: Any
    ):
        assert get_request_visibility() is None, (
            "Enumeration must run under visibility_bypass() — contextvar must be None"
        )
        rows = [SimpleNamespace(id=cid) for cid in self._catalog_ids]
        return rows[offset: offset + limit]

    async def list_collections(
        self, catalog_id: str, limit: int = 500, offset: int = 0, **kwargs: Any
    ):
        assert get_request_visibility() is None, (
            "Enumeration must run under visibility_bypass() — contextvar must be None"
        )
        col_ids = self._collection_map.get(catalog_id, [])
        rows = [SimpleNamespace(id=cid) for cid in col_ids]
        return rows[offset: offset + limit]


# ---------------------------------------------------------------------------
# Patch helper
# ---------------------------------------------------------------------------


@contextmanager
def _patch_catalogs(stub: _StubCatalogs):
    """Patch dynastore.tools.discovery.get_protocol so the implementation's
    lazy ``from dynastore.tools.discovery import get_protocol`` resolves to
    a function that returns the stub for CatalogsProtocol."""
    from dynastore.models.protocols import CatalogsProtocol

    original_get_protocol = None

    def _fake_get_protocol(proto):
        if proto is CatalogsProtocol:
            return stub
        if original_get_protocol is not None:
            return original_get_protocol(proto)
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_fake_get_protocol) as m:
        original_get_protocol = m.DEFAULT  # unused here; just for shape
        yield


def _vis(*principals: str) -> RequestVisibility:
    return RequestVisibility(principals=principals)


def _allow_ids(*ids: str) -> AccessFilter:
    return AccessFilter.from_clauses(
        [AccessClause((FieldPredicate("id", tuple(ids)),))]
    )


# ---------------------------------------------------------------------------
# catalog_listing_filter: allow-all shortcut
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_platform_allow_all_no_loop():
    """Platform compile returns allow_all with no deny/union → allow_everything,
    no enumeration (CatalogsProtocol not consulted)."""
    perms = _StubPermissions(
        {(None, None): AccessFilter.allow_everything()}
    )
    stub = _StubCatalogs(catalog_ids=["cat-a", "cat-b"])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("role:sysadmin")
        result = await svc.catalog_listing_filter(vis)
    assert result.is_unconditional
    # call_count == 1: the platform probe (catalog_id=None), no per-catalog loops
    assert perms.call_count == 1


@pytest.mark.asyncio
async def test_platform_allow_all_with_deny_still_loops():
    """allow_all + deny on the platform probe is NOT the shortcut path; loops run."""
    perms = _StubPermissions({
        # Platform scope: allow_all but with a deny clause present
        (None, None): AccessFilter(
            allow_all=True,
            deny=(AccessClause((FieldPredicate("id", ("x",)),)),),
        ),
        ("cat-a", None): AccessFilter.allow_everything(),
        ("cat-b", None): AccessFilter.deny_everything(),
    })
    stub = _StubCatalogs(catalog_ids=["cat-a", "cat-b"])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("role:user")
        result = await svc.catalog_listing_filter(vis)
    ids = extract_id_constraint(result)
    assert ids == frozenset({"cat-a"})
    # At least one per-catalog call happened beyond the platform probe
    assert perms.call_count >= 2


# ---------------------------------------------------------------------------
# catalog_listing_filter: mixed and all-deny
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mixed_catalogs_projects_visible_only():
    """Cat-A allows, cat-B deny_all → filter projects to {cat-a}."""
    perms = _StubPermissions({
        (None, None): AccessFilter(allow_all=False, allow=()),
        ("cat-a", None): AccessFilter.allow_everything(),
        ("cat-b", None): AccessFilter.deny_everything(),
    }, default=AccessFilter.deny_everything())
    stub = _StubCatalogs(catalog_ids=["cat-a", "cat-b"])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("user:alice")
        result = await svc.catalog_listing_filter(vis)
    ids = extract_id_constraint(result)
    assert ids == frozenset({"cat-a"})


@pytest.mark.asyncio
async def test_all_deny_returns_deny_everything():
    perms = _StubPermissions(
        {(None, None): AccessFilter.deny_everything()},
        default=AccessFilter.deny_everything(),
    )
    stub = _StubCatalogs(catalog_ids=["cat-a"])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("user:alice")
        result = await svc.catalog_listing_filter(vis)
    assert result.deny_all


# ---------------------------------------------------------------------------
# collection_listing_filter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_catalog_scope_deny_returns_deny_without_loop():
    """Catalog-scope compile → deny_all ⟹ deny_everything, no per-collection loop."""
    perms = _StubPermissions(
        {("cat-a", None): AccessFilter.deny_everything()},
    )
    stub = _StubCatalogs(
        catalog_ids=["cat-a"],
        collection_map={"cat-a": ["col-1", "col-2"]},
    )
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("user:anon")
        result = await svc.collection_listing_filter(vis, "cat-a")
    assert result.deny_all
    # Only the catalog-scope compile was issued; no col-1/col-2 compile
    assert perms.call_count == 1


@pytest.mark.asyncio
async def test_collection_per_collection_loop_respects_deny():
    """Catalog-scope allows; col-1 deny_all, col-2 allows → filter = {col-2}."""
    perms = _StubPermissions({
        ("cat-a", None): AccessFilter.allow_everything(),
        ("cat-a", "col-1"): AccessFilter.deny_everything(),
        ("cat-a", "col-2"): AccessFilter.allow_everything(),
    })
    stub = _StubCatalogs(
        catalog_ids=["cat-a"],
        collection_map={"cat-a": ["col-1", "col-2"]},
    )
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("user:alice")
        result = await svc.collection_listing_filter(vis, "cat-a")
    ids = extract_id_constraint(result)
    assert ids == frozenset({"col-2"})


@pytest.mark.asyncio
async def test_collection_no_allow_all_shortcut_even_with_catalog_allow_all():
    """Even when catalog-scope returns allow_all, per-collection loops must run.

    A DENY policy pinned to a single collection's paths does not match at
    catalog scope (no collection_id in the probe). Without the loop, that
    collection-pinned DENY would be invisible and under-denied.
    """
    perms = _StubPermissions({
        # Catalog scope says allow_all — shortcut is unsound for collections
        ("cat-a", None): AccessFilter.allow_everything(),
        ("cat-a", "col-secret"): AccessFilter.deny_everything(),
        ("cat-a", "col-public"): AccessFilter.allow_everything(),
    })
    stub = _StubCatalogs(
        catalog_ids=["cat-a"],
        collection_map={"cat-a": ["col-secret", "col-public"]},
    )
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("user:alice")
        result = await svc.collection_listing_filter(vis, "cat-a")
    ids = extract_id_constraint(result)
    # col-secret must be absent even though catalog scope was allow_all
    assert "col-secret" not in ids
    assert "col-public" in ids
    # Per-collection calls were issued (loop ran)
    assert perms.call_count >= 3  # 1 catalog-scope + 2 collection-scope


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_second_identical_call_uses_cache():
    """The second call with the same visibility snapshot does not re-invoke compile."""
    perms = _StubPermissions(
        {(None, None): AccessFilter.allow_everything()}
    )
    stub = _StubCatalogs(catalog_ids=[])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        vis = _vis("role:sysadmin")
        await svc.catalog_listing_filter(vis)
        count_after_first = perms.call_count
        await svc.catalog_listing_filter(vis)
    assert perms.call_count == count_after_first, (
        "Second call with same visibility must be served from cache"
    )


@pytest.mark.asyncio
async def test_different_principals_recompute():
    """Different principals tuple → cache miss → compile called again."""
    perms = _StubPermissions(
        {(None, None): AccessFilter.allow_everything()}
    )
    stub = _StubCatalogs(catalog_ids=[])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(perms)
        await svc.catalog_listing_filter(_vis("role:sysadmin"))
        count_after_first = perms.call_count
        await svc.catalog_listing_filter(_vis("user:alice"))
    assert perms.call_count > count_after_first, (
        "Different principals must trigger a cache miss"
    )


# ---------------------------------------------------------------------------
# Compile failure → deny_everything
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compile_raising_returns_deny_everything():
    """If compile_read_filter raises, the provider must fail closed."""

    class _BrokenPerms:
        async def compile_read_filter(self, *args, **kwargs):
            raise RuntimeError("DB offline")

    stub = _StubCatalogs(catalog_ids=["cat-a"])
    with _patch_catalogs(stub):
        svc = IamListingVisibility(_BrokenPerms())  # type: ignore[arg-type]
        vis = _vis("user:alice")
        result = await svc.catalog_listing_filter(vis)
    assert result.deny_all
