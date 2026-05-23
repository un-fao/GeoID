"""CQL2 ``filter`` support on the REST search path (SearchService).

``POST /search/catalogs/{cat}`` is served by ``SearchService.search_items``
(not the STAC ``/search`` dispatch). These pin that the CQL2 ``filter`` is
translated to an Elasticsearch clause through the shared ``es_common``
translator, that the private vs public field mapping is selected from the
resolved index, and that an un-honourable filter fails closed with 400
(this path queries Elasticsearch directly — there is no PostgreSQL fallback,
so silently dropping the filter would return the unfiltered set).
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from dynastore.extensions.search.search_models import SearchBody
from dynastore.extensions.search.search_service import SearchService
from dynastore.models.protocols.field_definition import FieldDefinition


def _patch_deps(monkeypatch, fields):
    """Stub config + queryables resolution so the translator runs offline."""

    class _FakeConfigs:
        async def get_config(self, _cls, *, catalog_id, collection_id, ctx):  # noqa: ARG002
            return object()  # opaque col_config sentinel

    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda _proto: _FakeConfigs()
    )

    class _FakeOptimizer:
        def __init__(self, _cfg, consumer=None):  # noqa: ARG002
            pass

        def get_all_queryable_fields(self):
            return fields

    monkeypatch.setattr(
        "dynastore.modules.catalog.query_optimizer.QueryOptimizer", _FakeOptimizer
    )


_FIELDS = {
    "CODE": FieldDefinition(name="CODE", data_type="string"),
    "area": FieldDefinition(name="area", data_type="double"),
}


def test_searchbody_accepts_filter_and_filter_lang_alias() -> None:
    body = SearchBody.model_validate(
        {
            "collections": ["adm2"],
            "filter": {"op": "=", "args": [{"property": "CODE"}, "ITA_01"]},
            "filter-lang": "cql2-json",
        }
    )
    assert body.filter == {"op": "=", "args": [{"property": "CODE"}, "ITA_01"]}
    assert body.filter_lang == "cql2-json"


async def test_no_filter_returns_none(monkeypatch) -> None:
    _patch_deps(monkeypatch, _FIELDS)
    svc = SearchService.__new__(SearchService)
    body = SearchBody(catalog_id="acme", collections=["adm2"])
    assert await svc._translate_cql_filter(body, "test-acme-private-items") is None


async def test_private_string_equality_targets_keyword(monkeypatch) -> None:
    _patch_deps(monkeypatch, _FIELDS)
    svc = SearchService.__new__(SearchService)
    body = SearchBody(
        catalog_id="acme",
        collections=["adm2"],
        filter={"op": "=", "args": [{"property": "CODE"}, "ITA_01"]},
    )
    clause = await svc._translate_cql_filter(body, "test-acme-private-items")
    # dynamic-mapped string under properties.* → exact match needs .keyword
    assert clause == {"term": {"properties.CODE.keyword": "ITA_01"}}


async def test_numeric_range_no_keyword(monkeypatch) -> None:
    _patch_deps(monkeypatch, _FIELDS)
    svc = SearchService.__new__(SearchService)
    body = SearchBody(
        catalog_id="acme",
        collections=["adm2"],
        filter={"op": ">", "args": [{"property": "area"}, 0]},
    )
    clause = await svc._translate_cql_filter(body, "test-acme-private-items")
    assert clause == {"range": {"properties.area": {"gt": 0}}}


async def test_envelope_external_id_is_root_keyword(monkeypatch) -> None:
    fields = {"external_id": FieldDefinition(name="external_id", data_type="string")}
    _patch_deps(monkeypatch, fields)
    svc = SearchService.__new__(SearchService)
    body = SearchBody(
        catalog_id="acme",
        collections=["adm2"],
        filter={"op": "=", "args": [{"property": "external_id"}, "ITA_02"]},
    )
    clause = await svc._translate_cql_filter(body, "test-acme-private-items")
    # external_id is an explicit root keyword — no properties.* / .keyword suffix
    assert clause == {"term": {"external_id": "ITA_02"}}


async def test_unknown_property_fails_closed_400(monkeypatch) -> None:
    _patch_deps(monkeypatch, _FIELDS)
    svc = SearchService.__new__(SearchService)
    body = SearchBody(
        catalog_id="acme",
        collections=["adm2"],
        filter={"op": "=", "args": [{"property": "does_not_exist"}, 1]},
    )
    with pytest.raises(HTTPException) as exc:
        await svc._translate_cql_filter(body, "test-acme-private-items")
    assert exc.value.status_code == 400


async def test_filter_requires_single_catalog_collection_scope(monkeypatch) -> None:
    _patch_deps(monkeypatch, _FIELDS)
    svc = SearchService.__new__(SearchService)
    # no catalog scope
    with pytest.raises(HTTPException) as exc:
        await svc._translate_cql_filter(
            SearchBody(collections=["adm2"], filter={"op": "=", "args": [{"property": "CODE"}, "x"]}),
            "test-items",
        )
    assert exc.value.status_code == 400
    # multi-collection scope
    with pytest.raises(HTTPException):
        await svc._translate_cql_filter(
            SearchBody(catalog_id="acme", collections=["a", "b"],
                       filter={"op": "=", "args": [{"property": "CODE"}, "x"]}),
            "test-items",
        )
