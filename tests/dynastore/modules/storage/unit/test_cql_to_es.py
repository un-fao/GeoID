"""CQL2 AST → Elasticsearch Query DSL translator (``es_common.cql_to_es``).

Each operator maps to its expected ES dict; field-mapping resolution covers
envelope vs ``properties`` vs ``extras`` and private-vs-public; an unknown
property raises; and a value carrying quotes/special chars stays a literal
``term`` value (injection-safe by construction).
"""
from __future__ import annotations

import pytest
from pygeofilter.parsers.cql2_json import parse as parse_cql2_json

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.drivers.es_common.cql_to_es import (
    UntranslatableFilterError,
    build_es_field_mapping,
    cql_ast_to_es_query,
    merge_es_filter,
)

# A flat mapping where every property resolves to ``properties.<name>`` and the
# geometry field to ``geometry`` (mirrors the private flat doc).
_MAP = {
    "cloud_cover": "properties.cloud_cover",
    "name": "properties.name",
    "count": "properties.count",
    "geometry": "geometry",
    "datetime": "properties.datetime",
    "external_id": "external_id",
}


def _es(cql_json):
    return cql_ast_to_es_query(parse_cql2_json(cql_json), _MAP)


# --------------------------------------------------------------------------- #
# Operator coverage
# --------------------------------------------------------------------------- #

def test_equal_to_term():
    assert _es({"op": "=", "args": [{"property": "cloud_cover"}, 10]}) == {
        "term": {"properties.cloud_cover": 10}
    }


def test_not_equal_to_must_not_term():
    assert _es({"op": "<>", "args": [{"property": "cloud_cover"}, 10]}) == {
        "bool": {"must_not": [{"term": {"properties.cloud_cover": 10}}]}
    }


@pytest.mark.parametrize(
    "op,es_op",
    [("<", "lt"), ("<=", "lte"), (">", "gt"), (">=", "gte")],
)
def test_comparison_to_range(op, es_op):
    assert _es({"op": op, "args": [{"property": "count"}, 5]}) == {
        "range": {"properties.count": {es_op: 5}}
    }


def test_and_to_bool_filter():
    node = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "cloud_cover"}, 10]},
            {"op": ">", "args": [{"property": "count"}, 5]},
        ],
    }
    assert _es(node) == {
        "bool": {
            "filter": [
                {"term": {"properties.cloud_cover": 10}},
                {"range": {"properties.count": {"gt": 5}}},
            ]
        }
    }


def test_or_to_bool_should():
    node = {
        "op": "or",
        "args": [
            {"op": "=", "args": [{"property": "cloud_cover"}, 10]},
            {"op": "=", "args": [{"property": "count"}, 5]},
        ],
    }
    assert _es(node) == {
        "bool": {
            "should": [
                {"term": {"properties.cloud_cover": 10}},
                {"term": {"properties.count": 5}},
            ],
            "minimum_should_match": 1,
        }
    }


def test_not_to_bool_must_not():
    node = {"op": "not", "args": [{"op": "=", "args": [{"property": "count"}, 5]}]}
    assert _es(node) == {
        "bool": {"must_not": [{"term": {"properties.count": 5}}]}
    }


def test_between_to_range():
    node = {"op": "between", "args": [{"property": "count"}, [1, 5]]}
    assert _es(node) == {
        "range": {"properties.count": {"gte": 1, "lte": 5}}
    }


def test_in_to_terms():
    node = {"op": "in", "args": [{"property": "count"}, [1, 2, 3]]}
    assert _es(node) == {"terms": {"properties.count": [1, 2, 3]}}


def test_like_to_wildcard():
    node = {"op": "like", "args": [{"property": "name"}, "%foo%"]}
    q = _es(node)
    assert q == {
        "wildcard": {
            "properties.name": {"value": "*foo*", "case_insensitive": False}
        }
    }


def test_isnull_to_must_not_exists():
    node = {"op": "isNull", "args": [{"property": "name"}]}
    assert _es(node) == {
        "bool": {"must_not": [{"exists": {"field": "properties.name"}}]}
    }


def test_spatial_intersects_to_geo_shape():
    node = {
        "op": "s_intersects",
        "args": [
            {"property": "geometry"},
            {"type": "Point", "coordinates": [1.0, 2.0]},
        ],
    }
    assert _es(node) == {
        "geo_shape": {
            "geometry": {
                "shape": {"type": "Point", "coordinates": [1.0, 2.0]},
                "relation": "intersects",
            }
        }
    }


def test_temporal_after_to_range():
    node = {
        "op": "t_after",
        "args": [
            {"property": "datetime"},
            {"timestamp": "2020-01-01T00:00:00Z"},
        ],
    }
    q = _es(node)
    assert "range" in q
    assert "properties.datetime" in q["range"]
    assert "gt" in q["range"]["properties.datetime"]


# --------------------------------------------------------------------------- #
# Field-mapping resolution
# --------------------------------------------------------------------------- #

def _fd(name, data_type="string", expose=True):
    return FieldDefinition(name=name, data_type=data_type, expose=expose)


def test_mapping_envelope_fields():
    fields = {
        "external_id": _fd("external_id"),
        "asset_id": _fd("asset_id"),
        "geoid": _fd("geoid"),
        "datetime": _fd("datetime", data_type="timestamp"),
        "the_geom": _fd("the_geom", data_type="geometry"),
    }
    m = build_es_field_mapping(fields, private=True)
    assert m["external_id"] == "external_id"
    assert m["asset_id"] == "asset_id"
    assert m["geoid"] == "geoid"
    assert m["datetime"] == "properties.datetime"
    # any geometry-typed field maps to the root geometry field
    assert m["the_geom"] == "geometry"


def test_mapping_private_flat_properties():
    fields = {"adm2_pcode": _fd("adm2_pcode"), "pop": _fd("pop", data_type="integer")}
    m = build_es_field_mapping(fields, private=True)
    assert m["adm2_pcode"] == "properties.adm2_pcode"
    assert m["pop"] == "properties.pop"


def test_mapping_public_known_vs_extras():
    fields = {
        "known": _fd("known", expose=True),
        "hidden": _fd("hidden", expose=False),
    }
    m = build_es_field_mapping(fields, private=False)
    # exposed → flat properties; not-exposed → extras bucket
    assert m["known"] == "properties.known"
    assert m["hidden"] == "properties.extras.hidden"


def test_mapping_public_geometry_and_envelope_unaffected_by_expose():
    fields = {
        "geom": _fd("geom", data_type="geometry", expose=False),
        "external_id": _fd("external_id", expose=False),
    }
    m = build_es_field_mapping(fields, private=False)
    assert m["geom"] == "geometry"
    assert m["external_id"] == "external_id"


# --------------------------------------------------------------------------- #
# Unknown property + injection safety
# --------------------------------------------------------------------------- #

def test_unknown_property_raises():
    node = {"op": "=", "args": [{"property": "does_not_exist"}, 1]}
    with pytest.raises(UntranslatableFilterError):
        cql_ast_to_es_query(parse_cql2_json(node), _MAP)


def test_unknown_property_is_value_error_subclass():
    # The dispatch catches ValueError to fall back to PG; ensure compatibility.
    assert issubclass(UntranslatableFilterError, ValueError)


def test_value_with_quotes_stays_literal_term():
    nasty = "o'brien\" OR 1=1 --"
    node = {"op": "=", "args": [{"property": "name"}, nasty]}
    q = _es(node)
    # The value is carried verbatim as a term value — no interpolation.
    assert q == {"term": {"properties.name": nasty}}
    assert q["term"]["properties.name"] == nasty


def test_in_values_with_special_chars_stay_literal():
    vals = ["a*b", "c?d", "{\"inject\": true}"]
    node = {"op": "in", "args": [{"property": "name"}, vals]}
    q = _es(node)
    assert q == {"terms": {"properties.name": vals}}


# --------------------------------------------------------------------------- #
# merge_es_filter
# --------------------------------------------------------------------------- #

def test_merge_into_match_all_collapses_to_clause():
    clause = {"term": {"properties.x": 1}}
    assert merge_es_filter({"match_all": {}}, clause) == clause
    assert merge_es_filter(None, clause) == clause
    assert merge_es_filter({}, clause) == clause


def test_merge_into_bool_appends_to_filter():
    base = {"bool": {"filter": [{"term": {"collection": "c"}}]}}
    clause = {"term": {"properties.x": 1}}
    out = merge_es_filter(base, clause)
    assert out == {
        "bool": {
            "filter": [
                {"term": {"collection": "c"}},
                {"term": {"properties.x": 1}},
            ]
        }
    }
    # input not mutated
    assert base == {"bool": {"filter": [{"term": {"collection": "c"}}]}}


def test_merge_wraps_leaf_base():
    base = {"term": {"collection": "c"}}
    clause = {"term": {"properties.x": 1}}
    assert merge_es_filter(base, clause) == {
        "bool": {
            "filter": [
                {"term": {"collection": "c"}},
                {"term": {"properties.x": 1}},
            ]
        }
    }
