"""Unit tests for OGC API Features Phase-1 projection / CRS / cql2-json (#1385).

Covers the three sub-features added to ``GET /collections/{cid}/items``:

1. ``properties`` query parameter — comma-separated attribute names selecting
   which feature properties are echoed back. Validated against the collection's
   queryable field set; unknown names → 400. Empty value (`?properties=`) strips
   all attribute properties down to the OGC-mandatory ones (id/geometry/type/
   links). The validated list is also threaded onto ``QueryRequest.select`` so
   drivers that honour projection can narrow the read.
2. ``filter-crs`` query parameter — URI of the CRS the geometric literals in
   ``filter=`` are expressed in. Default = CRS84. Resolved via the existing
   ``_resolve_crs_srid`` helper, threaded into the CQL→SQL translator so the
   parsed Geometry literals carry the requested SRID.
3. ``filter-lang=cql2-json`` — currently rejected. Accept and route through the
   existing ``parse_cql2_json_filter`` path; ``cql2-text`` unchanged.

These are pure unit tests: ``OGCFeaturesService`` is instantiated bare and its
collaborators are monkeypatched. No DB, no FastAPI app.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pytest

import dynastore.extensions.tools.query as _query_mod
from dynastore.extensions.features.features_config import FeaturesPluginConfig
from dynastore.extensions.features.features_service import OGCFeaturesService
from dynastore.extensions.tools.formatters import OutputFormatEnum
from dynastore.models.ogc import Feature as _OGCFeature
from dynastore.models.query_builder import QueryResponse


# ---------------------------------------------------------------------------
# Fixtures / harness (mirrors test_features_items_search_dispatch.py)
# ---------------------------------------------------------------------------


def _make_request(
    path: str = "/features/catalogs/cat/collections/col/items",
    query_string: bytes = b"",
):
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "http",
        "server": ("test", 80),
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(b"host", b"test")],
        "root_path": "",
    }
    return Request(scope)


def _feature(fid: str, props: Optional[Dict[str, Any]] = None) -> _OGCFeature:
    return _OGCFeature(
        type="Feature",
        id=fid,
        geometry=None,
        properties=props if props is not None else {
            "title": fid,
            "rainfall_mm": 12.3,
            "country": "IT",
        },
    )


class _FakeCatalogs:
    def __init__(self, stream_features: List[_OGCFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.stream_called = False
        self.last_request = None

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

    async def stream_items(self, **kwargs):
        self.stream_called = True
        self.last_request = kwargs.get("request")

        async def _gen():
            for f in self._stream_features:
                yield f

        return QueryResponse(
            items=_gen(),
            total_count=self._total,
            catalog_id=kwargs["catalog_id"],
            collection_id=kwargs["collection_id"],
        )


def _wire(monkeypatch, svc, catalogs, valid_props=("title", "rainfall_mm", "country")):
    async def _get_catalogs():
        return catalogs

    async def _get_configs():
        class _Cfg:
            async def get_config(self, cls, catalog_id=None, ctx=None):
                return FeaturesPluginConfig()

        return _Cfg()

    async def _get_storage():
        return None

    async def _resolve_crs(conn, catalog_id, crs):
        if crs is None:
            return None
        upper = crs.upper()
        if "CRS84" in upper:
            return 4326
        if "3857" in crs:
            return 3857
        if "4326" in crs:
            return 4326
        return None

    async def _resolve_props(catalog_id, collection_id):
        return set(valid_props)

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    monkeypatch.setattr(svc, "_get_storage_service", _get_storage, raising=False)
    monkeypatch.setattr(svc, "_resolve_crs_srid", _resolve_crs, raising=False)
    monkeypatch.setattr(
        svc, "_resolve_property_names", _resolve_props, raising=False
    )

    # Default: search-driver dispatch declines → PG stream path used.
    async def _decline(**kwargs):
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _decline
    )


async def _read_body(resp) -> bytes:
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks)


def _call_get_items(svc, **overrides):
    """Invoke ``get_items`` with the full FastAPI keyword signature.

    Override only the fields the test cares about; everything else gets a
    sensible default that exercises the standard listing path.
    """
    kwargs = dict(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        bbox=None,
        datetime_param=None,
        filter=None,
        filter_lang="cql2-text",
        filter_crs=None,
        properties=None,
        crs=None,
        bbox_crs=None,
        sortby=None,
        f=OutputFormatEnum.GEOJSON,
    )
    kwargs.update(overrides)
    return svc.get_items(**kwargs)


# ---------------------------------------------------------------------------
# 1. ``properties`` query parameter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_properties_selects_subset(monkeypatch):
    """Only the requested property names survive in the response feature."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("f-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    resp = await _call_get_items(svc, properties="title,country")
    body = json.loads(await _read_body(resp))
    feat = body["features"][0]
    # Mandatory keys preserved.
    assert feat["id"] == "f-1"
    assert feat["type"] == "Feature"
    # Property set narrowed.
    assert set(feat["properties"].keys()) == {"title", "country"}


@pytest.mark.asyncio
async def test_properties_empty_strips_all(monkeypatch):
    """``?properties=`` returns OGC-mandatory fields with empty properties."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("f-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    resp = await _call_get_items(svc, properties="")
    body = json.loads(await _read_body(resp))
    feat = body["features"][0]
    assert feat["id"] == "f-1"
    # No attribute properties — only system-injected keys (if any) remain.
    # The OGC spec allows ``properties`` to be empty or omitted; we serialize
    # an empty dict so the field is present but empty.
    assert feat.get("properties") in (None, {}, {"properties": {}})


@pytest.mark.asyncio
async def test_properties_unknown_name_400(monkeypatch):
    """Unknown property names → HTTP 400 with the list of valid names."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        await _call_get_items(svc, properties="title,not_a_field")
    assert excinfo.value.status_code == 400
    msg = str(excinfo.value.detail)
    assert "not_a_field" in msg


@pytest.mark.asyncio
async def test_properties_threaded_into_query_request_select(monkeypatch):
    """Validated names are placed onto ``QueryRequest.select`` so PG/ES drivers
    that honour projection can narrow the read."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("f-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, properties="title,country")
    assert catalogs.last_request is not None
    selected = {sel.field for sel in catalogs.last_request.select}
    assert {"title", "country"}.issubset(selected)


# ---------------------------------------------------------------------------
# 2. ``filter-crs`` query parameter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_crs_threaded_into_request(monkeypatch):
    """``filter-crs`` resolves to an SRID and is threaded onto QueryRequest."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(
        svc,
        filter="title = 'x'",
        filter_crs="http://www.opengis.net/def/crs/EPSG/0/3857",
    )
    assert catalogs.last_request is not None
    # The translator carries the SRID for later use.
    assert getattr(catalogs.last_request, "filter_crs_srid", None) == 3857


@pytest.mark.asyncio
async def test_filter_crs_default_is_crs84(monkeypatch):
    """When ``filter-crs`` is absent, the SRID stays None (translator default
    = 4326 / CRS84)."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, filter="title = 'x'")
    assert catalogs.last_request is not None
    assert getattr(catalogs.last_request, "filter_crs_srid", None) is None


def test_cql_geometry_literal_srid_stamping_from_filter_crs():
    """``_stamp_geometry_srid`` injects the requested CRS member on every
    geometry literal in the parsed AST so pygeofilter's SQLAlchemy backend
    emits ``ST_GeomFromEWKT('SRID=<srid>;...')`` rather than defaulting to
    4326. This is the load-bearing wiring for the ``filter-crs`` query
    parameter — the full SQL compile path requires a geoalchemy2 column for
    ``geom``, which the unit-level test does not stand up."""
    from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
    from dynastore.modules.tools.cql import _stamp_geometry_srid

    ast = parse_cql2_text(
        "S_Intersects(geom, POLYGON((0 0, 0 1, 1 1, 1 0, 0 0)))"
    )
    # Before stamping: no ``crs`` member on the geometry literal.
    assert "crs" not in ast.rhs.geometry

    _stamp_geometry_srid(ast, 3857)
    crs_member = ast.rhs.geometry.get("crs")
    assert crs_member is not None
    assert crs_member["properties"]["name"].endswith("3857")

    # Idempotency: re-stamping with a different SRID does NOT clobber an
    # explicitly-set value — the user's input WKT/EWKT wins.
    _stamp_geometry_srid(ast, 4326)
    assert ast.rhs.geometry["crs"]["properties"]["name"].endswith("3857")


# ---------------------------------------------------------------------------
# 3. ``filter-lang=cql2-json``
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_lang_cql2_json_accepted(monkeypatch):
    """A JSON-encoded CQL2 filter with ``filter-lang=cql2-json`` is accepted
    and threaded into ``QueryRequest.cql_filter`` — the downstream parser
    decides cql2-text vs cql2-json from the carried language hint."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    cql_json = json.dumps({"op": "=", "args": [{"property": "title"}, "x"]})
    # No exception → accepted.
    await _call_get_items(
        svc, filter=cql_json, filter_lang="cql2-json",
    )
    assert catalogs.last_request is not None
    # We thread the raw JSON string through; the optimizer knows it is JSON
    # via ``filter_lang`` on the request.
    assert catalogs.last_request.cql_filter == cql_json
    assert catalogs.last_request.filter_lang == "cql2-json"


@pytest.mark.asyncio
async def test_filter_lang_invalid_400(monkeypatch):
    """An unsupported ``filter-lang`` still rejects with 400."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        await _call_get_items(svc, filter="x = 1", filter_lang="ecql")
    assert excinfo.value.status_code == 400


def test_parse_cql2_json_translator_runs():
    """The CQL2-JSON translator parses a simple equality and emits a WHERE
    clause referencing the mapped column."""
    from dynastore.modules.tools.cql import parse_cql2_json_filter
    from sqlalchemy import literal_column

    where, params = parse_cql2_json_filter(
        cql_json={"op": "=", "args": [{"property": "title"}, "x"]},
        field_mapping={"title": literal_column("h.title")},
    )
    assert "title" in where.lower()
    assert "x" in list(params.values())
