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

"""Unit tests for ``combine_cql_filters`` — the shared single-source-of-truth
helper that merges an explicit CQL2 ``filter`` with ``?{property}={value}``
shorthand into one CQL2-Text expression (#1141).

The same helper backs both the OGC API Features ``/items`` endpoint and the STAC
``/items`` listing, so both surfaces produce identical CQL and reuse the same
validated CQL parsing path downstream.
"""

from dynastore.extensions.tools.query import (
    OGC_RESERVED_QUERY_PARAMS,
    combine_cql_filters,
)


def test_shorthand_only_produces_unwrapped_equality():
    assert combine_cql_filters(None, {"adm2_pcode": "PK001"}) == "adm2_pcode = 'PK001'"


def test_explicit_filter_only_is_passed_through_unwrapped():
    assert combine_cql_filters("population > 1000", None) == "population > 1000"
    assert combine_cql_filters("population > 1000", {}) == "population > 1000"


def test_both_sides_are_parenthesised_and_anded():
    cql = combine_cql_filters("population > 1000", {"adm2_pcode": "PK001"})
    assert cql == "(population > 1000) AND (adm2_pcode = 'PK001')"


def test_multiple_shorthand_pairs_are_anded():
    cql = combine_cql_filters(None, {"adm2_pcode": "PK001", "status": "active"})
    assert cql == "adm2_pcode = 'PK001' AND status = 'active'"


def test_neither_side_returns_none():
    assert combine_cql_filters(None, None) is None
    assert combine_cql_filters(None, {}) is None
    assert combine_cql_filters("", {}) is None


def test_value_single_quote_is_escaped():
    cql = combine_cql_filters(None, {"name": "O'Brien"})
    assert cql == "name = 'O''Brien'"


def test_cache_buster_underscore_is_reserved():
    # Browsers/jQuery append ``?_=<ts>``; it must never be treated as an
    # attribute filter (which would 400 on an unknown property).
    assert "_" in OGC_RESERVED_QUERY_PARAMS
