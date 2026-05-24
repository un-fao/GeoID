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
"""Neutral, backend-agnostic projection of a read-authorization decision.

This is the contract that lets document-level security (row-level ABAC) be
shared between the authorization engine and any storage driver **without
coupling them**. The authorization engine compiles an :class:`AccessFilter`
from its policy / role / condition graph for a given principal; a storage
driver translates that filter into its own native predicate language (an
Elasticsearch ``bool`` query, a SQL ``WHERE`` clause, a Parquet row-group
predicate, ...) at query time.

Why a neutral type:
    The driver must never import the IAM module, and the IAM module must never
    import a driver. Both depend only on this module (which lives in
    ``models/protocols`` and imports nothing from either side). The driver asks
    ``get_protocol(PermissionProtocol).compile_read_filter(...)`` and receives
    one of these; it never sees a ``Policy``, a ``Condition`` or any IAM
    internal.

Semantics (a faithful, *fail-closed* projection of ``evaluate_access``):
    A document ``d`` is visible to the principal iff::

        not deny_all
        and (allow_all or any(clause.matches(d) for clause in allow))
        and not any(clause.matches(d) for clause in deny)

    where an :class:`AccessClause` matches ``d`` when *every* one of its
    :class:`FieldPredicate` s matches. Read as: ALLOW policies contribute OR
    branches (``should``); DENY policies contribute negated branches
    (``must_not``) and therefore win over ALLOW, preserving the deny-precedence
    invariant of ``evaluate_access`` *structurally* rather than by re-ranking.

The cardinal safety rule (proved by the drift guard property test):
    The filter must be **equal-or-stricter** than ``evaluate_access``. A filter
    that admits a document the engine would deny is a security leak; a filter
    that hides a document the engine would allow is a safe annoyance
    (under-return — the document is still reachable by a direct GET that runs
    the full engine). Therefore anything the compiler cannot express as an
    index predicate is dropped *from the ALLOW side* (the grant simply does not
    contribute), and ``uncompilable`` is set so callers/telemetry know the
    filter is stricter than the engine. When nothing can be allowed, the
    compiler returns :meth:`AccessFilter.deny_everything`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence, Tuple

__all__ = [
    "FieldPredicate",
    "AccessClause",
    "AccessFilter",
]


@dataclass(frozen=True)
class FieldPredicate:
    """A single ``field IN values`` constraint over one envelope field.

    ``values`` is a set of acceptable values for ``field``. Matching is
    membership: a scalar document field matches when its value is in
    ``values``; an *array* document field (e.g. ``grant_subjects``) matches
    when it intersects ``values``. This maps directly onto an Elasticsearch
    ``terms`` query and onto a SQL ``field = ANY(values)`` / array-overlap.
    """

    field: str
    values: Tuple[str, ...]

    def matches(self, document: Mapping[str, Any]) -> bool:
        """Pure-Python evaluation — used by the drift guard property test.

        Mirrors the ``terms`` semantics a driver must implement: scalar field
        → membership; list/tuple/set field → non-empty intersection.
        """
        actual = document.get(self.field)
        allowed = set(self.values)
        if isinstance(actual, (list, tuple, set)):
            return any(v in allowed for v in actual)
        return actual in allowed


@dataclass(frozen=True)
class AccessClause:
    """A conjunction (AND) of :class:`FieldPredicate` s.

    A document satisfies the clause iff it satisfies *every* predicate. An
    empty clause matches every document (an unconditional grant within its
    resource scope).
    """

    predicates: Tuple[FieldPredicate, ...] = ()

    def matches(self, document: Mapping[str, Any]) -> bool:
        return all(p.matches(document) for p in self.predicates)


@dataclass(frozen=True)
class AccessFilter:
    """Compiled, backend-neutral read scope for one principal.

    Drivers translate this to native predicates. The pure-Python
    :meth:`admits` is the reference semantics every driver translation must
    match (the drift guard checks ``admits`` against ``evaluate_access``).
    """

    #: Nothing is visible — deny-by-default / suspended principal / no ALLOW
    #: could be compiled. A driver MUST return zero results (e.g. a
    #: ``match_none`` query), never fall back to an unfiltered scan.
    deny_all: bool = False

    #: No row-level restriction — an unconditional ALLOW (e.g. a platform
    #: super-admin). A driver applies no access clause.
    allow_all: bool = False

    #: OR of allow clauses. A document must match at least one (unless
    #: ``allow_all``). Empty + not ``allow_all`` ⟹ same as ``deny_all``.
    allow: Tuple[AccessClause, ...] = ()

    #: OR of deny clauses (``must_not``). A document matching any is excluded
    #: regardless of ``allow`` — this is how deny-precedence is preserved.
    deny: Tuple[AccessClause, ...] = ()

    #: True when at least one grant relevant to this read scope carried a
    #: condition the compiler could not express as an index predicate. Such a
    #: grant is omitted from ``allow`` (fail-closed under-return). Purely
    #: informational: lets callers log/alert that search may under-return.
    uncompilable: bool = False

    #: Free-form driver hints the generic fields cannot carry. Drivers that do
    #: not understand a key MUST ignore it (never fail-open on it).
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def deny_everything(cls, *, uncompilable: bool = False) -> "AccessFilter":
        """Fail-closed sentinel: the principal may see nothing via search."""
        return cls(deny_all=True, uncompilable=uncompilable)

    @classmethod
    def allow_everything(cls) -> "AccessFilter":
        """No row-level restriction (caller already passed coarse authz)."""
        return cls(allow_all=True)

    @classmethod
    def from_clauses(
        cls,
        allow: Sequence[AccessClause],
        deny: Sequence[AccessClause] = (),
        *,
        uncompilable: bool = False,
    ) -> "AccessFilter":
        """Build from allow/deny clauses, failing closed when allow is empty."""
        allow_t = tuple(allow)
        if not allow_t:
            return cls.deny_everything(uncompilable=uncompilable)
        return cls(allow=allow_t, deny=tuple(deny), uncompilable=uncompilable)

    @property
    def is_unconditional(self) -> bool:
        """True when no per-document clause needs to be applied at all."""
        return self.allow_all and not self.deny

    def admits(self, document: Mapping[str, Any]) -> bool:
        """Reference semantics — the contract every driver translation matches.

        This is intentionally pure and side-effect free so the drift guard can
        assert ``admits(doc) ⟹ evaluate_access(...) == ALLOW`` over generated
        policy/document combinations.
        """
        if self.deny_all:
            return False
        if any(clause.matches(document) for clause in self.deny):
            return False
        if self.allow_all:
            return True
        return any(clause.matches(document) for clause in self.allow)
