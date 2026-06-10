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

"""Unit tests for the pluggable catalog-picker providers (#1981, #1990).

Covers:
- CatalogListProvider structural matching, including the #1990 regression:
  a storage-layer object with ``list_catalogs(limit, offset, ...)`` and a
  ``priority`` attribute must NOT satisfy the provider protocol.
- Winner-takes-all resolution: get_protocols orders providers by priority
  (lower wins) regardless of registration order.
- DefaultCatalogListProvider: localized titles, id fallback, pagination stop.
- IamCatalogListProvider: sysadmin sees the full list, a principal gets the
  principal_id-filtered list (with TypeError fallback for older
  CatalogsProtocol implementations), and an anonymous caller gets nothing.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

from dynastore.extensions.iam.catalog_source import IamCatalogListProvider
from dynastore.extensions.web.catalog_source import DefaultCatalogListProvider
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.catalog_source import (
    CatalogListProvider,
    CatalogOption,
)
from dynastore.tools.discovery import (
    get_protocols,
    register_plugin,
    unregister_plugin,
)


@dataclass
class _CatalogRow:
    id: str
    title: Optional[Any] = None


class _StubCatalogs:
    """Stands in for the CatalogsProtocol storage implementation."""

    def __init__(
        self, rows: List[_CatalogRow], supports_principal_filter: bool = True
    ) -> None:
        self._rows = rows
        self._supports_principal_filter = supports_principal_filter
        self.calls: List[dict] = []

    async def list_catalogs(self, limit: int, offset: int, **kwargs: Any):
        if (
            "principal_id" in kwargs
            and not self._supports_principal_filter
        ):
            raise TypeError("unexpected keyword argument 'principal_id'")
        self.calls.append({"limit": limit, "offset": offset, **kwargs})
        rows = self._rows
        if kwargs.get("principal_id"):
            rows = [r for r in rows if r.id.startswith("granted")]
        return rows[offset : offset + limit]


def _request(roles: Any = None, principal: Any = None) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(principal_role=roles, principal=principal)
    )


# ---------------------------------------------------------------------------
# Protocol structural matching
# ---------------------------------------------------------------------------


def test_providers_satisfy_protocol() -> None:
    assert isinstance(DefaultCatalogListProvider(), CatalogListProvider)
    assert isinstance(IamCatalogListProvider(), CatalogListProvider)


def test_storage_impostor_does_not_match_protocol() -> None:
    """Regression for #1990: a CatalogsProtocol-shaped object (which has
    ``list_catalogs`` and ``priority``) must not satisfy the provider
    protocol, or /web/catalogs would call it with a Request as ``limit``."""

    class StorageImpostor:
        priority = 5

        async def list_catalogs(
            self, limit: int, offset: int, **kwargs: Any
        ): ...

    assert not isinstance(StorageImpostor(), CatalogListProvider)


# ---------------------------------------------------------------------------
# Winner-takes-all registry resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("iam_first", [True, False])
def test_iam_provider_wins_regardless_of_registration_order(
    iam_first: bool,
) -> None:
    iam = IamCatalogListProvider()
    default = DefaultCatalogListProvider()
    plugins = [iam, default] if iam_first else [default, iam]
    for p in plugins:
        register_plugin(p)
    try:
        providers = [
            p
            for p in get_protocols(CatalogListProvider)
            if p in (iam, default)
        ]
        assert providers == [iam, default]
    finally:
        for p in plugins:
            unregister_plugin(p)


# ---------------------------------------------------------------------------
# DefaultCatalogListProvider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_provider_localizes_and_falls_back_to_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs(
        [
            _CatalogRow("cat_a", {"en": "Catalog A", "fr": "Catalogue A"}),
            _CatalogRow("cat_b", None),
        ]
    )
    monkeypatch.setattr(
        "dynastore.extensions.web.catalog_source.get_protocol",
        lambda proto: stub,
    )
    options = await DefaultCatalogListProvider().list_catalog_options(
        _request(), language="fr"
    )
    assert options == [
        CatalogOption(id="cat_a", title="Catalogue A"),
        CatalogOption(id="cat_b", title="cat_b"),
    ]


@pytest.mark.asyncio
async def test_default_provider_stops_paging_on_short_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs([_CatalogRow(f"cat_{i}") for i in range(3)])
    monkeypatch.setattr(
        "dynastore.extensions.web.catalog_source.get_protocol",
        lambda proto: stub,
    )
    options = await DefaultCatalogListProvider().list_catalog_options(
        _request()
    )
    assert [o.id for o in options] == ["cat_0", "cat_1", "cat_2"]
    assert len(stub.calls) == 1  # short first page ends the loop


@pytest.mark.asyncio
async def test_default_provider_empty_without_catalogs_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dynastore.extensions.web.catalog_source.get_protocol",
        lambda proto: None,
    )
    assert (
        await DefaultCatalogListProvider().list_catalog_options(_request())
        == []
    )


# ---------------------------------------------------------------------------
# IamCatalogListProvider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iam_provider_sysadmin_gets_full_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs(
        [_CatalogRow("granted_1"), _CatalogRow("other_1")]
    )
    monkeypatch.setattr(
        "dynastore.extensions.iam.catalog_source.get_protocol",
        lambda proto: stub,
    )
    sysadmin = IamRolesConfig().sysadmin_role_name
    options = await IamCatalogListProvider().list_catalog_options(
        _request(roles=[sysadmin])
    )
    assert [o.id for o in options] == ["granted_1", "other_1"]
    assert "principal_id" not in stub.calls[0]


@pytest.mark.asyncio
async def test_iam_provider_filters_by_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs(
        [_CatalogRow("granted_1"), _CatalogRow("other_1")]
    )
    monkeypatch.setattr(
        "dynastore.extensions.iam.catalog_source.get_protocol",
        lambda proto: stub,
    )
    options = await IamCatalogListProvider().list_catalog_options(
        _request(principal=SimpleNamespace(id="user-1", subject_id=None))
    )
    assert [o.id for o in options] == ["granted_1"]
    assert stub.calls[0]["principal_id"] == "user-1"


@pytest.mark.asyncio
async def test_iam_provider_falls_back_when_filter_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs(
        [_CatalogRow("granted_1"), _CatalogRow("other_1")],
        supports_principal_filter=False,
    )
    monkeypatch.setattr(
        "dynastore.extensions.iam.catalog_source.get_protocol",
        lambda proto: stub,
    )
    options = await IamCatalogListProvider().list_catalog_options(
        _request(principal=SimpleNamespace(id="user-1", subject_id=None))
    )
    # Older CatalogsProtocol without principal_id support → unfiltered call.
    assert [o.id for o in options] == ["granted_1", "other_1"]
    assert "principal_id" not in stub.calls[0]


@pytest.mark.asyncio
async def test_iam_provider_denies_anonymous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubCatalogs([_CatalogRow("granted_1")])
    monkeypatch.setattr(
        "dynastore.extensions.iam.catalog_source.get_protocol",
        lambda proto: stub,
    )
    options = await IamCatalogListProvider().list_catalog_options(_request())
    assert options == []
    assert stub.calls == []  # never touches storage for anonymous callers
