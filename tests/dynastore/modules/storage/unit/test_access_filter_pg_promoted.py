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

"""Unit tests for access_filter_to_pg_clause with promoted-column support (F5).

Covers:
1. Promoted key emits ``"_attr_{key}" = ANY(:p)`` direct-column predicate.
2. Non-promoted key falls through to JSONB path predicate.
3. Mixed predicate set: promoted key → direct column, non-promoted → JSONB.
4. ``promoted=None`` (default) → all predicates fall through to JSONB (back-compat).
5. ``deny_all`` and ``allow_all`` semantics are unchanged.
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from dynastore.modules.storage.access_filter_pg import access_filter_to_pg_clause


# ---------------------------------------------------------------------------
# Minimal AccessFilter / AccessClause / FieldPredicate stubs
# ---------------------------------------------------------------------------

class _FieldPredicate:
    def __init__(self, field: str, values: Tuple[str, ...]):
        self.field = field
        self.values = values


class _AccessClause:
    def __init__(self, *predicates: _FieldPredicate):
        self.predicates = predicates


class _AccessFilter:
    def __init__(
        self,
        allow: Optional[List[_AccessClause]] = None,
        deny_all: bool = False,
        allow_all: bool = False,
    ):
        self.allow = allow or []
        self.deny_all = deny_all
        self.allow_all = allow_all


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_filter(field: str, values: Tuple[str, ...]) -> _AccessFilter:
    return _AccessFilter(allow=[_AccessClause(_FieldPredicate(field, values))])


# ---------------------------------------------------------------------------
# 1. Promoted key → direct-column predicate
# ---------------------------------------------------------------------------

def test_promoted_key_emits_direct_column_predicate():
    af = _make_filter("_attrs.dept", ("finance",))
    clause, params = access_filter_to_pg_clause(
        af, envelope_col="envelope", promoted=frozenset({"dept"})
    )
    assert clause is not None
    assert '"_attr_dept"' in clause, f"Expected direct column; got: {clause}"
    assert "ANY(" in clause
    # Must NOT contain JSONB arrow operator.
    assert "->'attrs'" not in clause and "->>" not in clause, (
        f"JSONB path leaked into promoted predicate: {clause}"
    )
    assert params, "Expected at least one bind param"


# ---------------------------------------------------------------------------
# 2. Non-promoted key → JSONB path predicate
# ---------------------------------------------------------------------------

def test_non_promoted_key_falls_through_to_jsonb():
    af = _make_filter("_attrs.dept", ("finance",))
    clause, params = access_filter_to_pg_clause(
        af, envelope_col="envelope", promoted=frozenset()  # empty promoted set
    )
    assert clause is not None
    assert "->>'dept'" in clause or "->'attrs'->>" in clause, (
        f"Expected JSONB path; got: {clause}"
    )
    assert '"_attr_dept"' not in clause


# ---------------------------------------------------------------------------
# 3. Mixed predicate set
# ---------------------------------------------------------------------------

def test_mixed_predicates_route_correctly():
    """dept promoted → direct column; sensitivity non-promoted → JSONB."""
    dept_pred = _FieldPredicate("_attrs.dept", ("finance",))
    sensitivity_pred = _FieldPredicate("_attrs.sensitivity", ("low",))
    af = _AccessFilter(allow=[_AccessClause(dept_pred, sensitivity_pred)])
    clause, params = access_filter_to_pg_clause(
        af, envelope_col="envelope", promoted=frozenset({"dept"})
    )
    assert clause is not None
    assert '"_attr_dept"' in clause, f"Promoted dept not found; clause={clause}"
    assert "sensitivity" in clause, f"sensitivity predicate missing; clause={clause}"
    # JSONB path present for sensitivity.
    assert "->>'sensitivity'" in clause or "->'attrs'->>'sensitivity'" in clause, (
        f"JSONB path for sensitivity missing; clause={clause}"
    )


# ---------------------------------------------------------------------------
# 4. promoted=None → back-compat (all JSONB)
# ---------------------------------------------------------------------------

def test_none_promoted_falls_back_to_jsonb():
    af = _make_filter("_attrs.dept", ("finance",))
    clause, params = access_filter_to_pg_clause(
        af, envelope_col="envelope", promoted=None
    )
    assert clause is not None
    # All through JSONB path.
    assert '"_attr_dept"' not in clause, (
        f"Direct column leaked with promoted=None; clause={clause}"
    )


# ---------------------------------------------------------------------------
# 5. deny_all / allow_all unchanged
# ---------------------------------------------------------------------------

def test_deny_all_returns_false_clause():
    af = _AccessFilter(deny_all=True)
    clause, params = access_filter_to_pg_clause(
        af, promoted=frozenset({"dept"})
    )
    assert clause == "FALSE"
    assert params == {}


def test_allow_all_no_clauses_returns_none():
    af = _AccessFilter(allow_all=True)
    clause, params = access_filter_to_pg_clause(
        af, promoted=frozenset({"dept"})
    )
    assert clause is None
    assert params == {}


# ---------------------------------------------------------------------------
# 6. Param name isolation — two clauses don't collide
# ---------------------------------------------------------------------------

def test_multiple_allow_clauses_have_unique_param_names():
    """Each allow clause gets its own distinct param name."""
    clause1 = _AccessClause(_FieldPredicate("_attrs.dept", ("finance",)))
    clause2 = _AccessClause(_FieldPredicate("_attrs.dept", ("ops",)))
    af = _AccessFilter(allow=[clause1, clause2])
    clause, params = access_filter_to_pg_clause(
        af, envelope_col="envelope", promoted=frozenset({"dept"})
    )
    assert clause is not None
    assert len(params) == 2, f"Expected 2 distinct params; got {params}"
    # Values are correctly separated.
    assert ("finance",) in (tuple(v) for v in params.values()) or \
           ["finance"] in params.values()
    assert ("ops",) in (tuple(v) for v in params.values()) or \
           ["ops"] in params.values()
