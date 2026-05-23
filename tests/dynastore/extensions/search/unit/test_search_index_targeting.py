"""End-to-end index targeting through SearchService — item-only (#819).

Pins the actual index argument the service passes to ``es.search`` for
each public method. Helper-level naming contracts live in
``tests/dynastore/modules/elasticsearch/test_index_naming.py``.
"""

import pytest

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import SearchService


def _service() -> SearchService:
    return SearchService.__new__(SearchService)


def _capturing_driver():
    """Return a (fake_driver, captured) pair.

    The fake mimics the surface ``SearchService`` reaches for through
    :meth:`SearchService._resolve_items_driver` — only ``es_client`` is
    consulted, so we wrap a no-result ES double behind that property.
    """
    captured: dict = {}

    class _FakeES:
        async def search(self, *, index, body, **kwargs):
            captured["index"] = index
            captured["body"] = body
            captured["kwargs"] = kwargs
            return {"hits": {"hits": [], "total": {"value": 0}}}

    class _FakeDriver:
        es_client = _FakeES()

    return _FakeDriver(), captured


@pytest.fixture(autouse=True)
def _patch_index_prefix(monkeypatch):
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "test",
    )


async def test_search_items_scoped_targets_per_catalog_index(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id="acme", limit=10))

    assert captured["index"] == "test-acme-items"


async def test_search_items_unscoped_targets_public_alias(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id=None, limit=10))

    assert captured["index"] == "test-items"


async def test_search_items_uses_ignore_unavailable(monkeypatch):
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id="missing-catalog", limit=10))

    assert captured["kwargs"].get("ignore_unavailable") is True


async def test_search_items_uses_allow_no_indices(monkeypatch):
    """``allow_no_indices=True`` is what protects a fresh-install ``/search``
    from 404'ing — the public items alias only materialises after the first
    catalog onboards (#803 remaining symptom).
    """
    svc = _service()
    driver, captured = _capturing_driver()
    monkeypatch.setattr(svc, "_resolve_items_driver", lambda: driver)

    await svc.search_items(SearchBody(catalog_id=None, limit=10))

    assert captured["kwargs"].get("allow_no_indices") is True


# ---------------------------------------------------------------------------
# Privacy: the unscoped public ``/search`` must never resolve a catalog's
# private items index, even when the catalog pins a private SEARCH driver and
# the request carries a ``catalog_id`` in the body. Only the catalog-scoped
# route family (``scoped=True``) may follow the private pin. Regression for the
# leak where ``POST /search {"catalog_id": <private cat>}`` returned the
# tenant-private items through the cross-tenant discovery surface.
# ---------------------------------------------------------------------------


def _service_with_private_pin(monkeypatch):
    """SearchService whose catalog 'acme'/'c' pins a private SEARCH driver
    resolving to ``test-acme-private-items``."""
    svc = _service()

    async def _pinned(catalog_id, collections, driver_hint):
        return "items_elasticsearch_private_driver"

    class _FakePrivateDriver:
        def _items_index_name(self, catalog_id: str) -> str:
            return f"test-{catalog_id}-private-items"

    monkeypatch.setattr(svc, "_resolve_items_search_driver_ref", _pinned)
    monkeypatch.setattr(
        svc, "_resolve_items_driver_by_ref", lambda ref: _FakePrivateDriver(),
    )
    return svc


async def test_scoped_search_follows_private_pin(monkeypatch):
    svc = _service_with_private_pin(monkeypatch)
    index = await svc._resolve_items_index("acme", ["c"], None, scoped=True)
    assert index == "test-acme-private-items"


async def test_unscoped_search_ignores_private_pin(monkeypatch):
    svc = _service_with_private_pin(monkeypatch)
    # Same catalog + collection, but reached through the unscoped public route:
    # the private pin MUST be ignored and the public per-catalog index used.
    index = await svc._resolve_items_index("acme", ["c"], None, scoped=False)
    assert index == "test-acme-items"
