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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""CQL bind-parameter names must be SQL-safe (#1141 / Refs #1043).

A queryable field can resolve to a quoted/dotted SQL expression — COLUMNAR
attribute columns are ``sc_attributes."col"`` (attributes.py:304) and JSONB keys
are ``attributes->>'col'``. ``literal_column(sql_expression)`` (the #1165 fix)
makes pygeofilter emit a real bound-parameter comparison, but SQLAlchemy derives
the bind-parameter *name* from the column text, so the name inherits the quote /
``->>`` characters (e.g. ``:sc_attributes_"adm2_pcode"_1``).

That name is not a valid SQLAlchemy ``text()`` placeholder: the ``:name`` scanner
stops at the first non-word character, so the parameter is truncated and binding
fails ("A value is required for bind parameter 'sc_attributes_'") when the WHERE
fragment is later executed via ``text(sql)`` (both the ``/items`` stream path and
the STAC ``POST /search`` path do this). The CQL boundary must therefore emit
placeholder names that ``text()`` can round-trip.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pygeofilter", reason="pygeofilter required for CQL tests")

from sqlalchemy import literal_column, text

from dynastore.modules.tools.cql import parse_cql_filter, parse_cql2_json_filter


def _assert_text_roundtrips(where: str, params: dict) -> None:
    """Every ``:name`` placeholder ``text()`` finds must have a value in params,
    and every param must be referenced — i.e. SQLAlchemy can bind the fragment."""
    detected = set(text(where)._bindparams.keys())
    assert detected == set(params.keys()), (
        f"text() placeholders {detected} != params {set(params.keys())} "
        f"for WHERE {where!r}"
    )


def test_columnar_quoted_expression_yields_bindable_param_name():
    mapping = {"adm2_pcode": literal_column('sc_attributes."adm2_pcode"')}
    where, params = parse_cql2_json_filter(
        {"op": "=", "args": [{"property": "adm2_pcode"}, "PK001"]},
        field_mapping=mapping,
    )
    assert where != "1=0"
    assert "PK001" in params.values()
    _assert_text_roundtrips(where, params)


def test_jsonb_accessor_expression_yields_bindable_param_name():
    mapping = {"adm2_pcode": literal_column("attributes->>'adm2_pcode'")}
    where, params = parse_cql_filter(
        "adm2_pcode='PK002'", field_mapping=mapping, parser_type="cql2"
    )
    assert where != "1=0"
    assert "PK002" in params.values()
    _assert_text_roundtrips(where, params)


def test_multi_clause_quoted_params_are_all_bindable():
    mapping = {
        "adm2_pcode": literal_column('sc_attributes."adm2_pcode"'),
        "adm1_pcode": literal_column('sc_attributes."adm1_pcode"'),
    }
    where, params = parse_cql_filter(
        "adm2_pcode='PK001' AND adm1_pcode='PK'",
        field_mapping=mapping,
        parser_type="cql2",
    )
    assert len(params) == 2
    assert set(params.values()) == {"PK001", "PK"}
    _assert_text_roundtrips(where, params)


def test_clean_identifier_param_names_are_unchanged():
    """A plain column already yields a safe name — sanitisation must be a no-op
    for the common case (no spurious churn for already-working filters)."""
    mapping = {"name": literal_column("name")}
    where, params = parse_cql_filter(
        "name='x'", field_mapping=mapping, parser_type="cql2"
    )
    _assert_text_roundtrips(where, params)
    assert list(params.values()) == ["x"]


def test_in_list_expansion_each_value_is_distinctly_bindable():
    """An IN-list on a quoted column expands to several params whose names differ
    only in a trailing index (``..._1`` vs ``..._10``). The whole-token rewrite
    must keep each one distinct and bindable — values must not collapse."""
    mapping = {"adm2_pcode": literal_column('sc_attributes."adm2_pcode"')}
    # 12 values so the expanded names reach two-digit suffixes (``_10``) that a
    # naive prefix replace of ``_1`` would corrupt.
    values = [f"PK{i:03d}" for i in range(12)]
    where, params = parse_cql_filter(
        "adm2_pcode IN (" + ", ".join(f"'{v}'" for v in values) + ")",
        field_mapping=mapping,
        parser_type="cql2",
    )
    _assert_text_roundtrips(where, params)
    # Every distinct value survives as its own bound parameter.
    assert sorted(params.values()) == sorted(values)
    assert len(params) == len(values)
