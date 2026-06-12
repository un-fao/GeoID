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
- DefaultCatalogListProvider: localized titles, id fallback, pagination stop.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

from dynastore.extensions.web.catalog_source import DefaultCatalogListProvider
from dynastore.models.protocols.catalog_source import (
    CatalogListProvider,
    CatalogOption,
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
