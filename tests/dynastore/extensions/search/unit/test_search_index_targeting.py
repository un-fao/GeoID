"""Index-targeting contract for SearchService.search_items / _search_generic.

The items writer produces per-catalog ``{prefix}-items-{catalog_id}`` indexes
with public alias ``{prefix}-items-public`` (PR #82). The collection metadata
writer produces per-catalog ``{prefix}_collection_metadata_{catalog_id}``
indexes. The search service must query the indexes the writers actually
produce — querying singletons that no driver writes raises
``index_not_found_exception`` at runtime.
"""

import pytest

from dynastore.extensions.search.search_models import CatalogSearchBody, SearchBody
from dynastore.extensions.search.search_service import SearchService


def _service() -> SearchService:
    svc = SearchService.__new__(SearchService)
    svc._es = object()  # type: ignore[attr-defined]
    return svc


def _capturing_es():
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, **kwargs):
            captured["index"] = index
            captured["body"] = body
            captured["kwargs"] = kwargs
            return {"hits": {"hits": [], "total": {"value": 0}}}

    return _FakeES(), captured


@pytest.fixture(autouse=True)
def _patch_index_prefix(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.search.search_service._get_index_prefix",
        lambda: "test",
    )


async def test_search_items_scoped_targets_per_catalog_index(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_items(SearchBody(catalog_id="acme", limit=10))

    assert captured["index"] == "test-items-acme"


async def test_search_items_unscoped_targets_public_alias(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_items(SearchBody(catalog_id=None, limit=10))

    assert captured["index"] == "test-items-public"


async def test_search_items_uses_ignore_unavailable(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_items(SearchBody(catalog_id="missing-catalog", limit=10))

    assert captured["kwargs"].get("ignore_unavailable") is True


async def test_search_collections_scoped_targets_per_catalog_metadata_index(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_collections(CatalogSearchBody(catalog_id="acme", limit=10))

    assert captured["index"] == "test_collection_metadata_acme"


async def test_search_collections_unscoped_targets_metadata_wildcard(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_collections(CatalogSearchBody(catalog_id=None, limit=10))

    assert captured["index"] == "test_collection_metadata_*"


async def test_search_catalogs_unchanged(monkeypatch):
    svc = _service()
    fake, captured = _capturing_es()
    monkeypatch.setattr(svc, "_get_es", lambda: fake)

    await svc.search_catalogs(CatalogSearchBody(limit=10))

    assert captured["index"] == "test-catalogs"
