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

"""CQL2 AST → Elasticsearch Query DSL translator.

The shared translator that lets the STAC ``/search`` dispatch serve a CQL2
``filter`` from Elasticsearch instead of falling back to PostgreSQL. Used by
both ES items drivers (public + tenant-private) via :func:`merge_es_filter`.

Why a custom evaluator
======================

``pygeofilter`` ships an Elasticsearch backend
(``pygeofilter.backends.elasticsearch``) but importing it pulls in
``elasticsearch_dsl`` (not installed, and intentionally not a dependency of this
platform). Its operator coverage is the reference this module mirrors, but the
output here is **plain ES Query DSL dicts** (``term`` / ``terms`` / ``range`` /
``bool`` / ``exists`` / ``wildcard`` / ``geo_shape``) — exactly what
``Q(...).to_dict()`` would have produced — so no ``*_dsl`` package is required.

Injection safety
================

Every user value flows into the DSL as a ``term`` / ``range`` / ``terms`` value
(or a ``geo_shape`` geometry). There is no string interpolation into a query
string anywhere, so a property value carrying quotes or ES query-string
metacharacters is treated as a literal scalar — injection-safe by construction.

Field resolution
================

Property names are resolved through a ``field_mapping`` (built by
:func:`build_es_field_mapping` from the queryables SSOT). An unknown property —
one not present in the mapping — raises :class:`UntranslatableFilterError`; the
caller treats that (and any other translation failure) as "untranslatable" and
falls back to the PostgreSQL path, preserving today's behaviour.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Any, Dict, Optional

from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle


class UntranslatableFilterError(ValueError):
    """Raised when a CQL2 AST cannot be expressed as an ES query.

    Subclasses :class:`ValueError` so existing ``except ValueError`` callers
    (which already gate the PG fallback on unknown-property errors) keep
    working unchanged.
    """


# CQL2 ComparisonOp → ES ``range`` operator key.
_COMPARISON_OP_MAP: Dict[Any, str] = {
    ast.ComparisonOp.LT: "lt",
    ast.ComparisonOp.LE: "lte",
    ast.ComparisonOp.GT: "gt",
    ast.ComparisonOp.GE: "gte",
}


def _like_to_wildcard(
    value: str, wildcard: str, single_char: str, escape_char: str = "\\"
) -> str:
    """Adapt a CQL ``LIKE`` pattern to an ES ``wildcard`` pattern.

    Mirrors ``pygeofilter.backends.elasticsearch.util.like_to_wildcard`` (which
    cannot be imported without ``elasticsearch_dsl``): a non-escaped ``wildcard``
    char becomes ``*`` and a non-escaped ``single_char`` becomes ``?``.
    """
    x_wildcard = re.escape(wildcard)
    x_single_char = re.escape(single_char)

    if escape_char == "\\":
        x_escape_char = "\\\\\\\\"
    else:
        x_escape_char = re.escape(escape_char)

    if wildcard != "*":
        value = re.sub(f"(?<!{x_escape_char}){x_wildcard}", "*", value)
    if single_char != "?":
        value = re.sub(f"(?<!{x_escape_char}){x_single_char}", "?", value)
    return value


class _CqlToEsEvaluator(Evaluator):
    """Evaluates a pygeofilter AST into a plain ES Query DSL ``dict``.

    Operator coverage (mirrors the reference ES backend, plain-dict output):

    * logical: ``And`` / ``Or`` / ``Not``
    * equality: ``Equal`` (→ ``term``), ``NotEqual`` (→ ``bool.must_not[term]``)
    * comparison: ``LessThan`` / ``LessEqual`` / ``GreaterThan`` /
      ``GreaterEqual`` (→ ``range``)
    * ``Between`` (→ ``range`` gte/lte, negatable)
    * ``In`` (→ ``terms``, negatable)
    * ``Like`` (→ ``wildcard``, negatable)
    * ``IsNull`` (→ ``bool.must_not[exists]``; ``not_`` → ``exists``)
    * ``Exists`` (→ ``exists``, negatable)
    * spatial: ``GeometryIntersects`` / ``GeometryDisjoint`` /
      ``GeometryWithin`` / ``GeometryContains`` / ``BBox`` (→ ``geo_shape``)
    * temporal: ``TemporalPredicate`` subclasses (→ ``range`` / ``term``)
    * ``Attribute`` (resolved via ``field_mapping``), literals, geometry,
      envelope.
    """

    def __init__(self, field_mapping: Dict[str, str]):
        self.field_mapping = field_mapping

    # --- logical -----------------------------------------------------------

    @handle(ast.Not)
    def not_(self, _node, sub):
        return {"bool": {"must_not": [sub]}}

    @handle(ast.And)
    def and_(self, _node, lhs, rhs):
        return {"bool": {"filter": [lhs, rhs]}}

    @handle(ast.Or)
    def or_(self, _node, lhs, rhs):
        return {"bool": {"should": [lhs, rhs], "minimum_should_match": 1}}

    # --- comparison --------------------------------------------------------

    @handle(ast.Equal, ast.NotEqual)
    def equality(self, node, lhs, rhs):
        q: Dict[str, Any] = {"term": {lhs: rhs}}
        if node.op == ast.ComparisonOp.NE:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.LessThan, ast.LessEqual, ast.GreaterThan, ast.GreaterEqual)
    def comparison(self, node, lhs, rhs):
        return {"range": {lhs: {_COMPARISON_OP_MAP[node.op]: rhs}}}

    @handle(ast.Between)
    def between(self, node: ast.Between, lhs, low, high):
        q: Dict[str, Any] = {"range": {lhs: {"gte": low, "lte": high}}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.In)
    def in_(self, node, lhs, *options):
        q: Dict[str, Any] = {"terms": {lhs: list(options)}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.Like)
    def like(self, node: ast.Like, lhs):
        pattern = _like_to_wildcard(
            node.pattern, node.wildcard, node.singlechar, node.escapechar
        )
        expr: Dict[str, Any] = {"value": pattern, "case_insensitive": node.nocase}
        q: Dict[str, Any] = {"wildcard": {lhs: expr}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.IsNull)
    def null(self, node: ast.IsNull, lhs):
        exists = {"exists": {"field": lhs}}
        # IS NULL  → field does NOT exist; IS NOT NULL (not_) → field exists.
        if node.not_:
            return exists
        return {"bool": {"must_not": [exists]}}

    @handle(ast.Exists)
    def exists(self, node: ast.Exists, lhs):
        exists = {"exists": {"field": lhs}}
        if node.not_:
            return {"bool": {"must_not": [exists]}}
        return exists

    # --- temporal ----------------------------------------------------------

    @handle(ast.TemporalPredicate, subclasses=True)
    def temporal(self, node: ast.TemporalPredicate, lhs, rhs):
        op = node.op
        if isinstance(rhs, (_dt.date, _dt.datetime)):
            low = high = rhs
        else:
            try:
                low, high = rhs
            except (TypeError, ValueError) as exc:
                raise UntranslatableFilterError(
                    f"Unsupported temporal RHS for operator {op}: {rhs!r}"
                ) from exc

        query = "range"
        negate = False
        predicate: Dict[str, Any]
        if op == ast.TemporalComparisonOp.DISJOINT:
            negate = True
            predicate = {"gte": low, "lte": high}
        elif op == ast.TemporalComparisonOp.AFTER:
            predicate = {"gt": high}
        elif op == ast.TemporalComparisonOp.BEFORE:
            predicate = {"lt": low}
        elif op in (
            ast.TemporalComparisonOp.TOVERLAPS,
            ast.TemporalComparisonOp.OVERLAPPEDBY,
        ):
            predicate = {"gte": low, "lte": high}
        elif op == ast.TemporalComparisonOp.BEGINS:
            query = "term"
            predicate = {"value": low}
        elif op == ast.TemporalComparisonOp.BEGUNBY:
            query = "term"
            predicate = {"value": high}
        elif op == ast.TemporalComparisonOp.DURING:
            predicate = {"gt": low, "lt": high, "relation": "WITHIN"}
        elif op == ast.TemporalComparisonOp.TCONTAINS:
            predicate = {"gt": low, "lt": high, "relation": "CONTAINS"}
        else:
            raise UntranslatableFilterError(f"Unsupported temporal operator: {op}")

        q: Dict[str, Any] = {query: {lhs: predicate}}
        if negate:
            return {"bool": {"must_not": [q]}}
        return q

    # --- spatial -----------------------------------------------------------

    @handle(
        ast.GeometryIntersects,
        ast.GeometryDisjoint,
        ast.GeometryWithin,
        ast.GeometryContains,
    )
    def spatial_comparison(self, node, lhs: str, rhs):
        return {
            "geo_shape": {
                lhs: {"shape": rhs, "relation": node.op.value.lower()},
            }
        }

    @handle(ast.BBox)
    def bbox(self, node: ast.BBox, lhs):
        return {
            "geo_shape": {
                lhs: {
                    "shape": self.envelope(
                        values.Envelope(node.minx, node.maxx, node.miny, node.maxy)
                    ),
                    "relation": "intersects",
                }
            }
        }

    # --- terminals ---------------------------------------------------------

    @handle(ast.Attribute)
    def attribute(self, node: ast.Attribute):
        try:
            return self.field_mapping[node.name]
        except KeyError as exc:
            raise UntranslatableFilterError(
                f"Unknown queryable property '{node.name}' "
                f"(not in field mapping)"
            ) from exc

    @handle(*values.LITERALS)
    def literal(self, node):
        return node

    @handle(values.Geometry)
    def geometry(self, node: values.Geometry):
        return node.geometry

    @handle(values.Envelope)
    def envelope(self, node: values.Envelope):
        return {
            "type": "envelope",
            "coordinates": [
                [min(node.x1, node.x2), max(node.y1, node.y2)],
                [max(node.x1, node.x2), min(node.y1, node.y2)],
            ],
        }

    def adopt(self, node, *sub_args):
        # Any node without a registered handler is untranslatable → PG fallback.
        raise UntranslatableFilterError(
            f"Unsupported CQL2 node for ES translation: {type(node).__name__}"
        )


def cql_ast_to_es_query(node: Any, field_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Translate a pygeofilter CQL2 AST node to an ES Query DSL ``dict``.

    ``field_mapping`` maps each queryable property name to its ES field path
    (see :func:`build_es_field_mapping`). An unknown property — or any node the
    translator does not cover — raises :class:`UntranslatableFilterError`, which
    the caller treats as "untranslatable" and falls back to PostgreSQL.

    The returned dict is a single ES query clause (e.g. ``{"term": {...}}`` or a
    ``{"bool": {...}}`` composite) suitable for :func:`merge_es_filter`.
    """
    return _CqlToEsEvaluator(field_mapping).evaluate(node)


# Envelope (top-level doc) fields that are NOT nested under ``properties``.
# Both the public STAC item doc and the private tenant-feature doc keep these
# at the document root (see the public items mapping and
# ``elasticsearch_private/doc_builder.build_tenant_feature_doc``).
_ENVELOPE_FIELD_PATHS: Dict[str, str] = {
    "external_id": "external_id",
    "asset_id": "asset_id",
    "geoid": "geoid",
    "id": "id",
    "geometry": "geometry",
    "datetime": "properties.datetime",
}

# Queryable ``data_type`` values that mark a field as geometry — geometry maps
# to the top-level ``geometry`` field regardless of its declared name.
_GEOMETRY_DATA_TYPE_PREFIX = "geometry"

# Canonical string ``data_type`` values whose dynamically-mapped ES field is an
# analyzed ``text`` field with a ``.keyword`` sub-field (the Elasticsearch
# default for a string under a ``dynamic: true`` object — see the private
# tenant ``properties`` sub-tree and the public ``extras`` bucket). CQL2 exact
# matches (``=`` / ``<>`` / ``in`` / ``like``) must target ``.keyword`` or the
# ``term`` query misses the analyzed (lower-cased / tokenized) token. Numeric,
# temporal and boolean dynamic fields are queried directly. Envelope fields with
# an explicit ``keyword`` mapping (external_id, asset_id, …) are resolved before
# this and never need the suffix.
_KEYWORD_SUBFIELD_DATA_TYPES = frozenset({"string", "uuid"})
_KEYWORD_SUBFIELD = ".keyword"


def build_es_field_mapping(
    queryable_fields: Dict[str, "Any"],
    *,
    private: bool,
) -> Dict[str, str]:
    """Map each queryable field NAME to its Elasticsearch field path.

    ``queryable_fields`` is the queryables SSOT —
    ``QueryOptimizer.get_all_queryable_fields()`` — keyed by property name with
    :class:`~dynastore.models.protocols.field_definition.FieldDefinition` values.

    Resolution rules:

    * Envelope fields map to their document-root path:
      ``external_id`` / ``asset_id`` / ``geoid`` / ``id`` → same name at root,
      geometry → ``geometry``, ``datetime`` → ``properties.datetime``.
    * Any field whose ``data_type`` is a geometry type maps to ``geometry``.
    * The **private** flat tenant doc indexes every other attribute under
      ``properties.<name>`` (dynamic ``properties.*`` mapping).
    * The **public** doc keeps known queryables under ``properties.<name>`` and
      routes unknown/extension fields to the ``properties.extras.<name>``
      bucket the public projection writes them to.

    The live target is the private flat doc; the public mapping is pragmatic.
    """
    mapping: Dict[str, str] = {}
    for name, field_def in queryable_fields.items():
        data_type = str(getattr(field_def, "data_type", "") or "").lower()

        # Geometry fields always resolve to the root ``geometry`` field.
        if data_type.startswith(_GEOMETRY_DATA_TYPE_PREFIX):
            mapping[name] = "geometry"
            continue

        # Explicit envelope fields keep their root / well-known path.
        if name in _ENVELOPE_FIELD_PATHS:
            mapping[name] = _ENVELOPE_FIELD_PATHS[name]
            continue

        # Strings under a dynamic ``properties`` sub-tree need the ``.keyword``
        # sub-field for exact CQL matches; numeric / temporal / boolean don't.
        suffix = (
            _KEYWORD_SUBFIELD
            if data_type in _KEYWORD_SUBFIELD_DATA_TYPES
            else ""
        )

        if private:
            # Private tenant doc: every attribute lives under ``properties.*``.
            mapping[name] = f"properties.{name}{suffix}"
        else:
            # Public doc: known queryables are flat under ``properties``;
            # extension/unknown fields land in the ``extras`` bucket.
            if bool(getattr(field_def, "expose", True)):
                mapping[name] = f"properties.{name}{suffix}"
            else:
                mapping[name] = f"properties.extras.{name}{suffix}"

    return mapping


def merge_es_filter(
    base_query: Optional[Dict[str, Any]], extra_clause: Dict[str, Any]
) -> Dict[str, Any]:
    """AND ``extra_clause`` into ``base_query``'s ``bool.filter``.

    Used by both ES items drivers to fold a translated CQL2 clause into the
    structural query body built by ``build_items_query``.

    * ``base_query`` is the *inner* query (``{"bool": {...}}`` / ``{"match_all":
      {}}`` / a leaf clause), NOT the ``{"query": ...}`` envelope.
    * A ``match_all`` (or ``None``) base collapses to just the extra clause.
    * A ``bool`` base gains ``extra_clause`` in its ``filter`` list (created if
      absent), without mutating the input.
    * Any other leaf base is wrapped in a fresh ``bool.filter`` alongside the
      extra clause.
    """
    if base_query is None or base_query == {} or "match_all" in base_query:
        return extra_clause

    if "bool" in base_query:
        bool_body = dict(base_query["bool"])
        bool_body["filter"] = list(bool_body.get("filter", [])) + [extra_clause]
        return {"bool": bool_body}

    return {"bool": {"filter": [base_query, extra_clause]}}
