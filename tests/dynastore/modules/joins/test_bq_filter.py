"""Smoke tests for the CQL2 → BigQuery WHERE-clause translator."""

import pytest

from dynastore.modules.joins.bq_filter import cql_to_bq_where


def test_basic_eq_and_gt_with_backtick_identifiers():
    sql = cql_to_bq_where(
        "geoid = 'abc' AND temprature > 30",
        field_mapping={"geoid": "geoid", "temprature": "temprature"},
    )
    assert "`geoid`" in sql
    assert "`temprature`" in sql
    assert "= 'abc'" in sql
    assert "> 30" in sql


def test_in_and_like():
    sql_in = cql_to_bq_where(
        "id IN ('a','b','c')", field_mapping={"id": "id"},
    )
    assert "`id`" in sql_in and "IN ('a', 'b', 'c')" in sql_in

    sql_like = cql_to_bq_where(
        "name LIKE 'TG%'", field_mapping={"name": "name"},
    )
    assert "`name`" in sql_like and "LIKE 'TG%'" in sql_like


def test_bbox_emits_bq_geography_polygon():
    sql = cql_to_bq_where(
        "S_INTERSECTS(geom, BBOX(0, 5, 2, 8))",
        field_mapping={"geom": "geom"},
    )
    assert "ST_Intersects" in sql
    assert "ST_GeogFromText" in sql
    # Polygon ring closes back to (0, 5)
    assert "POLYGON((0 5" in sql and "0 5))" in sql


def test_cql2_json_form():
    sql = cql_to_bq_where(
        '{"op":"=","args":[{"property":"geoid"},"abc"]}',
        cql_lang="cql2-json",
        field_mapping={"geoid": "geoid"},
    )
    assert "`geoid`" in sql and "'abc'" in sql


def test_unknown_property_raises_value_error():
    with pytest.raises(ValueError, match="unknown property"):
        cql_to_bq_where(
            "no_such_col = 1", field_mapping={"only_known": "only_known"},
        )


def test_invalid_cql_raises_value_error():
    with pytest.raises(ValueError, match="Invalid CQL2"):
        cql_to_bq_where("!!!", field_mapping={"x": "x"})
