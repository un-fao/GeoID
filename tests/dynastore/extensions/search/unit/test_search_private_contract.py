"""Validation contract for `SearchService.search_by_geoid`.

The tenant-first contract requires:
  - `catalog_id` (selects the per-tenant index)
  - either `geoids` or the pair `(external_id, collection_id)`.
Bare `external_id` is rejected to prevent enumeration across collections.
"""

import pytest
from fastapi import HTTPException

from dynastore.extensions.search.search_service import SearchService


def _service() -> SearchService:
    """A SearchService stub that never reaches Elasticsearch."""
    svc = SearchService.__new__(SearchService)
    svc._es = object()  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_rejects_missing_catalog_id():
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_by_geoid(geoids=["g1"], catalog_id=None)
    assert exc.value.status_code == 400
    assert "catalog_id" in exc.value.detail


@pytest.mark.asyncio
async def test_rejects_external_id_without_collection():
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_by_geoid(catalog_id="cat", external_id="ext-1")
    assert exc.value.status_code == 400
    assert "collection_id" in exc.value.detail


@pytest.mark.asyncio
async def test_rejects_no_lookup_keys():
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        await svc.search_by_geoid(catalog_id="cat")
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_geoid_lookup_targets_tenant_index(monkeypatch):
    svc = _service()
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, ignore_unavailable):
            captured["index"] = index
            captured["body"] = body
            return {"hits": {"hits": []}}

    svc._es = _FakeES()  # type: ignore[attr-defined]
    monkeypatch.setattr(svc, "_get_es", lambda: svc._es)

    monkeypatch.setattr(
        "dynastore.extensions.search.search_service._get_index_prefix",
        lambda: "test",
    )

    await svc.search_by_geoid(geoids=["g1", "g2"], catalog_id="acme", limit=10)

    assert captured["index"] == "test-acme-private-items"
    assert captured["body"]["query"] == {"terms": {"geoid": ["g1", "g2"]}}


@pytest.mark.asyncio
async def test_external_id_pair_uses_bool_filter(monkeypatch):
    svc = _service()
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, ignore_unavailable):
            captured["index"] = index
            captured["body"] = body
            return {"hits": {"hits": []}}

    svc._es = _FakeES()  # type: ignore[attr-defined]
    monkeypatch.setattr(svc, "_get_es", lambda: svc._es)
    monkeypatch.setattr(
        "dynastore.extensions.search.search_service._get_index_prefix",
        lambda: "test",
    )

    await svc.search_by_geoid(
        catalog_id="acme", external_id="ext-42", collection_id="col-A",
    )

    assert captured["index"] == "test-acme-private-items"
    assert captured["body"]["query"]["bool"]["filter"] == [
        {"term": {"external_id": "ext-42"}},
        {"term": {"collection_id": "col-A"}},
    ]
