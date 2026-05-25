"""Unit tests pinning the routing-aware OGC ``/items`` dispatch contract.

The OGC API - Features and OGC API - Records ``/items`` listing endpoints
resolve the items SEARCH driver via routing
(``router.get_items_search_driver`` — SEARCH→READ fallback, #989) and dispatch
the structural query to **that driver** via its streaming ``read_entities`` +
``count_entities`` contract — the same mechanism STAC ``/search`` uses, so they
are no longer hardcoded to one search backend.
``maybe_dispatch_items_to_search_driver`` is the shared helper both endpoints
call before falling through to the existing PostgreSQL ``stream_items`` path.

Decision matrix (mirrors ``stac/search.py::_maybe_dispatch_to_es_search``):

* a CQL2 / shorthand attribute filter is present → ``None`` (PG path);
* the resolved driver is a read-primary fallback (PostgreSQL,
  ``Capability.QUERY_FALLBACK_SOURCE``) → ``None`` (PG path);
* the resolved driver is not an ES items driver (no ``is_es_items_driver``
  marker) → ``None`` (PG path);
* otherwise stream via ``read_entities`` and return a ``QueryResponse`` whose
  ``items`` is the driver's async ``Feature`` iterator and whose
  ``total_count`` (numberMatched) comes from ``count_entities``.

Read-contract reconstruction (extras un-nesting, empty-geometry nulling,
internal-field stripping) is the driver's job inside ``read_entities`` and is
pinned at the driver layer (``test_elasticsearch_driver.py``), not here.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from dynastore.extensions.tools.query import (
    maybe_dispatch_items_to_search_driver,
)
from dynastore.models.ogc import Feature
from dynastore.models.protocols.access_filter import AccessFilter
from dynastore.models.protocols.storage_driver import Capability


@dataclass
class _Resolved:
    """Stand-in for ``ResolvedDriver`` — the dispatcher reads ``.driver``."""

    driver: Any


class _FakeEsItemsDriver:
    """An ES items driver: carries the ``is_es_items_driver`` marker and the
    streaming ``read_entities`` + ``count_entities`` contract.

    Stands in for either the public or the tenant-private ES driver — the
    dispatcher treats both identically because it dispatches through the
    streaming contract, not the concrete class. ``read_entities`` yields
    already-reconstructed read-contract ``Feature`` objects (the real driver
    does the un-projection internally).
    """

    is_es_items_driver = True
    capabilities = frozenset({Capability.READ})

    def __init__(self, features: List[Any], total: int):
        self._features = features
        self._total = total
        self.read_calls: list = []
        self.count_calls: list = []

    async def read_entities(
        self, catalog_id, collection_id, *,
        entity_ids=None, request=None, context=None,
        limit=100, offset=0, db_resource=None,
    ):
        self.read_calls.append(
            {"collection_id": collection_id, "request": request,
             "limit": limit, "offset": offset}
        )
        for f in self._features:
            yield f if isinstance(f, Feature) else Feature.model_validate(f)

    async def count_entities(
        self, catalog_id, collection_id, *, request=None, db_resource=None,
    ):
        self.count_calls.append({"collection_id": collection_id, "request": request})
        return self._total


class _FakePgFallbackDriver:
    """A read-primary driver with no dedicated search backend — the PG case.

    Advertises ``QUERY_FALLBACK_SOURCE`` so the dispatcher declines and lets
    the PostgreSQL ``stream_items`` path serve the listing.
    """

    capabilities = frozenset({Capability.READ, Capability.QUERY_FALLBACK_SOURCE})


def _feat(fid: str) -> Feature:
    return Feature.model_validate({
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    })


def _patch_resolver(monkeypatch, driver):
    """Patch ``get_items_search_driver`` to return ``driver``."""
    import dynastore.modules.storage.router as _router

    async def _fake_get_items_search_driver(cat, cid=None, **_kw):
        if driver is None:
            raise ValueError("no driver")
        return _Resolved(driver=driver)

    monkeypatch.setattr(
        _router, "get_items_search_driver", _fake_get_items_search_driver
    )


async def _dispatch(monkeypatch, driver, **overrides):
    _patch_resolver(monkeypatch, driver)
    kwargs: Dict[str, Any] = dict(
        catalog_id="cat-x",
        collection_id="col-a",
        bbox=None,
        intersects=None,
        datetime=None,
        ids=None,
        limit=10,
        offset=0,
        has_complex_filter=False,
    )
    kwargs.update(overrides)
    return await maybe_dispatch_items_to_search_driver(**kwargs)


@pytest.mark.asyncio
async def test_dispatch_streams_features_and_reports_total(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("item-1")], total=1)
    resp = await _dispatch(monkeypatch, drv)

    assert resp is not None
    assert resp.total_count == 1
    assert resp.catalog_id == "cat-x"
    assert resp.collection_id == "col-a"
    features = [f async for f in resp]
    assert len(features) == 1
    assert isinstance(features[0], Feature)
    assert features[0].id == "item-1"


@pytest.mark.asyncio
async def test_total_comes_from_count_entities_not_stream_length(monkeypatch):
    # numberMatched is the catalog-wide count, decoupled from the page streamed.
    drv = _FakeEsItemsDriver(features=[_feat("a")], total=137)
    resp = await _dispatch(monkeypatch, drv, limit=1)
    assert resp is not None
    assert resp.total_count == 137
    features = [f async for f in resp]
    assert len(features) == 1
    assert drv.count_calls, "count_entities must be called for numberMatched"


@pytest.mark.asyncio
async def test_structural_params_thread_into_query_request(monkeypatch):
    drv = _FakeEsItemsDriver(features=[], total=0)
    resp = await _dispatch(
        monkeypatch, drv,
        bbox=[1.0, 2.0, 3.0, 4.0],
        datetime="2024-01-01/2024-12-31",
        ids=["a", "b"],
        limit=25,
        offset=50,
    )
    # read_entities is a lazy async generator — iterate to run its body (as the
    # route handler does) so the recorded call is materialized.
    assert resp is not None
    _ = [f async for f in resp]
    assert drv.read_calls
    req = drv.read_calls[0]["request"]
    assert req.bbox == [1.0, 2.0, 3.0, 4.0]
    assert req.datetime == "2024-01-01/2024-12-31"
    assert req.item_ids == ["a", "b"]
    assert req.limit == 25
    assert req.offset == 50
    # Single-collection /items keeps the routed fast path: collections unset.
    assert req.collections is None
    assert drv.read_calls[0]["limit"] == 25
    assert drv.read_calls[0]["offset"] == 50
    # count_entities receives the same structural request.
    assert drv.count_calls[0]["request"].bbox == [1.0, 2.0, 3.0, 4.0]


@pytest.mark.asyncio
async def test_intersects_threads_into_query_request(monkeypatch):
    drv = _FakeEsItemsDriver(features=[], total=0)
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    resp = await _dispatch(monkeypatch, drv, intersects=geom)
    assert resp is not None
    _ = [f async for f in resp]
    assert drv.read_calls[0]["request"].intersects == geom


@pytest.mark.asyncio
async def test_dispatch_declines_for_pg_fallback_driver(monkeypatch):
    """A QUERY_FALLBACK_SOURCE (PG) driver → None so the PG path serves it."""
    resp = await _dispatch(monkeypatch, _FakePgFallbackDriver())
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_for_non_es_driver(monkeypatch):
    """A driver without the is_es_items_driver marker → None (PG path)."""

    class _NotEs:
        capabilities = frozenset({Capability.READ})

    resp = await _dispatch(monkeypatch, _NotEs())
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_when_complex_filter_present(monkeypatch):
    """CQL2 / shorthand attribute filters → None (PG path; no translator)."""
    drv = _FakeEsItemsDriver(features=[], total=0)
    resp = await _dispatch(monkeypatch, drv, has_complex_filter=True)
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_when_resolution_fails(monkeypatch):
    """No registered driver (resolver raises) → None (PG path)."""
    resp = await _dispatch(monkeypatch, None)
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_on_driver_error(monkeypatch):
    """A count_entities exception degrades to the PG path, never 500s."""

    class _Boom:
        is_es_items_driver = True
        capabilities = frozenset({Capability.READ})

        async def read_entities(self, *a, **k):
            if False:
                yield  # make it an async generator
            return

        async def count_entities(self, *a, **k):
            raise RuntimeError("ES down")

    resp = await _dispatch(monkeypatch, _Boom())
    assert resp is None


# ---------------------------------------------------------------------------
# Row-level ABAC: an access-aware (envelope) driver gets a compiled read scope
# threaded onto the dispatched QueryRequest, exactly as the STAC /search fast
# path and the search extension do (#1311 item 2). The driver fails closed
# without it, so a non-envelope driver — or an un-wired caller — never leaks.
# ---------------------------------------------------------------------------


class _FakeEnvelopeDriver(_FakeEsItemsDriver):
    """An ES items driver that opts into row-level access filtering."""

    applies_access_filter = True


class _FakePerms:
    """Records the compile_read_filter call and returns a canned filter."""

    def __init__(self, result: AccessFilter) -> None:
        self.result = result
        self.calls: list = []

    async def compile_read_filter(
        self, principals, catalog_id=None, collection_id=None, *, principal=None
    ):
        self.calls.append(
            {"principals": principals, "catalog_id": catalog_id,
             "collection_id": collection_id, "principal": principal}
        )
        return self.result


def _patch_perms(monkeypatch, perms) -> None:
    # compile_read_access_filter imports get_protocol at call time.
    import dynastore.tools.discovery as discovery

    monkeypatch.setattr(discovery, "get_protocol", lambda _proto: perms)


def _request(principal_id="user:alice", principal_role=("reader",)):
    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P", principal_id=principal_id,
            principal_role=list(principal_role),
        )
    )


@pytest.mark.asyncio
async def test_envelope_driver_threads_access_filter_at_collection_scope(monkeypatch):
    scope = AccessFilter.allow_everything()
    perms = _FakePerms(scope)
    _patch_perms(monkeypatch, perms)
    drv = _FakeEnvelopeDriver(features=[_feat("a")], total=1)

    resp = await _dispatch(monkeypatch, drv, request=_request())
    assert resp is not None
    _ = [f async for f in resp]

    # The compiled scope is set on the QueryRequest the driver receives.
    assert drv.read_calls[0]["request"].access_filter is scope
    assert drv.count_calls[0]["request"].access_filter is scope
    # /items is single-collection → compiled at collection scope (pins
    # collection-level grants), with the request-state principals threaded.
    assert perms.calls[0]["collection_id"] == "col-a"
    assert perms.calls[0]["catalog_id"] == "cat-x"
    assert perms.calls[0]["principals"] == ["user:alice", "reader"]
    assert perms.calls[0]["principal"] == "P"


@pytest.mark.asyncio
async def test_envelope_driver_without_request_does_not_compile(monkeypatch):
    # No request supplied → no compile (driver fails closed on its own).
    perms = _FakePerms(AccessFilter.allow_everything())
    _patch_perms(monkeypatch, perms)
    drv = _FakeEnvelopeDriver(features=[], total=0)

    resp = await _dispatch(monkeypatch, drv)  # request omitted
    assert resp is not None
    _ = [f async for f in resp]
    assert drv.read_calls[0]["request"].access_filter is None
    assert perms.calls == []


@pytest.mark.asyncio
async def test_non_envelope_driver_is_not_access_filtered(monkeypatch):
    # An ordinary ES driver (no applies_access_filter) is untouched even when a
    # request is supplied — behaviour-neutral for existing catalogs.
    perms = _FakePerms(AccessFilter.allow_everything())
    _patch_perms(monkeypatch, perms)
    drv = _FakeEsItemsDriver(features=[], total=0)

    resp = await _dispatch(monkeypatch, drv, request=_request())
    assert resp is not None
    _ = [f async for f in resp]
    assert drv.read_calls[0]["request"].access_filter is None
    assert perms.calls == []
