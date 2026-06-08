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

"""Unit tests for ``PolicyService.compile_read_filter`` (row-level ABAC).

``compile_read_filter`` projects a principal's read scope into a neutral
:class:`AccessFilter` that a storage driver translates to a native predicate
without importing IAM. The cardinal contract is that the filter is an
**equal-or-stricter** projection of ``evaluate_access``:

* ALLOW grants become OR clauses; DENY grants become negated clauses so
  deny-precedence is preserved structurally.
* An ALLOW whose condition cannot be expressed as an index predicate is
  dropped (fail-closed under-return) and ``uncompilable`` is set.
* A relevant DENY whose condition cannot be expressed forces a full
  ``deny_everything`` (over-denying is the safe direction).

The service is built with no DB / role-storage dependencies (mirrors
``test_evaluate_access_deny_precedence``): policies are fed via
``principal.custom_policies`` so the role-lookup branch is skipped, and
``catalog_id=None`` short-circuits ``_resolve_schema`` to the global ``iam``
schema without touching ``CatalogsProtocol``.
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from dynastore.models.auth import Condition, Policy, Principal
from dynastore.modules.iam.policies import PolicyService


def _service() -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    # ``_resolve_schema`` would otherwise reach for ``CatalogsProtocol`` to
    # translate a catalog_id into a physical schema. The schema only feeds the
    # role-storage branch, which is skipped here (policies arrive via
    # ``custom_policies``), so any constant value is correct for these tests.
    async def _fake_resolve_schema(catalog_id, conn=None):  # noqa: ANN001
        return "iam"

    svc._resolve_schema = _fake_resolve_schema  # type: ignore[method-assign]
    return svc


def _principal(policies: List[Policy], attributes: Optional[dict] = None) -> Principal:
    return Principal(custom_policies=policies, attributes=attributes or {})


async def _compile(
    svc: PolicyService,
    policies: List[Policy],
    *,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
    attributes: Optional[dict] = None,
):
    principal = _principal(policies, attributes=attributes)
    return await svc.compile_read_filter(
        principals=[],
        catalog_id=catalog_id,
        collection_id=collection_id,
        principal=principal,
    )


def _allow(pid: str, *, path: str = ".*", method: str = ".*", conditions=None) -> Policy:
    return Policy(
        id=pid,
        effect="ALLOW",
        actions=[method],
        resources=[path],
        conditions=conditions or [],
    )


def _deny(pid: str, *, path: str = ".*", method: str = ".*", conditions=None) -> Policy:
    return Policy(
        id=pid,
        effect="DENY",
        actions=[method],
        resources=[path],
        conditions=conditions or [],
    )


@pytest.mark.asyncio
async def test_no_matching_policies_denies_all() -> None:
    """No policies at all → deny-by-default."""
    f = await _compile(_service(), [], catalog_id="cat_a")
    assert f.deny_all is True
    assert f.admits({"catalog_id": "cat_a"}) is False


@pytest.mark.asyncio
async def test_policy_out_of_read_scope_denies_all() -> None:
    """An ALLOW whose resource cannot match the read scope is irrelevant →
    nothing is allowed → deny-by-default."""
    p = _allow("a1", path="/admin/.*", method="POST")
    f = await _compile(_service(), [p], catalog_id="cat_a")
    assert f.deny_all is True


@pytest.mark.asyncio
async def test_platform_super_admin_allows_all() -> None:
    """ALLOW ``.*`` / ``.*`` with no conditions and no catalog pin → no
    row-level restriction at all."""
    f = await _compile(_service(), [_allow("super")], catalog_id=None)
    assert f.allow_all is True
    assert f.is_unconditional is True
    assert f.admits({"anything": "goes"}) is True


@pytest.mark.asyncio
async def test_catalog_lookup_public_compiles_visibility_predicate() -> None:
    """A ``catalog_lookup_public_allowed`` condition compiles to
    ``visibility IN ("public",)`` plus the catalog scope pin."""
    p = _allow(
        "pub",
        path=r"/stac/catalogs/[^/]+/collections/[^/]+/items",
        method="GET",
        conditions=[Condition(type="catalog_lookup_public_allowed")],
    )
    f = await _compile(_service(), [p], catalog_id="cat_a")
    assert f.deny_all is False
    assert f.allow_all is False
    assert f.uncompilable is False
    # Exactly one allow clause carrying the visibility predicate.
    fields = {
        pred.field: pred.values
        for clause in f.allow
        for pred in clause.predicates
    }
    assert fields.get("visibility") == ("public",)
    assert fields.get("catalog_id") == ("cat_a",)
    # A public doc in the catalog is admitted; a private one is not.
    assert f.admits({"catalog_id": "cat_a", "visibility": "public"}) is True
    assert f.admits({"catalog_id": "cat_a", "visibility": "private"}) is False
    # A doc in another catalog is never admitted.
    assert f.admits({"catalog_id": "cat_b", "visibility": "public"}) is False


@pytest.mark.asyncio
async def test_compilable_deny_excludes_matching_doc() -> None:
    """A DENY with a compilable condition lands on the deny side and excludes
    a doc even when an ALLOW clause would include it — deny-precedence."""
    allow = _allow("a_all", path=".*", method="GET")
    deny = _deny(
        "d_pub",
        path=".*",
        method="GET",
        conditions=[Condition(type="catalog_lookup_public_allowed")],
    )
    f = await _compile(_service(), [allow, deny], catalog_id="cat_a")
    assert f.deny_all is False
    assert f.allow  # there is an allow clause
    assert f.deny  # and a deny clause
    # The bare ALLOW would admit any doc in cat_a; the DENY removes public ones.
    assert f.admits({"catalog_id": "cat_a", "visibility": "private"}) is True
    assert f.admits({"catalog_id": "cat_a", "visibility": "public"}) is False


@pytest.mark.asyncio
async def test_allow_with_uncompilable_condition_is_dropped() -> None:
    """An ALLOW gated by an uncompilable condition (rate_limit) contributes
    nothing and flips ``uncompilable``."""
    rate_limited = _allow(
        "a_rl",
        path=".*",
        method="GET",
        conditions=[
            Condition(type="rate_limit", config={"limit": 10, "window_seconds": 60})
        ],
    )
    f = await _compile(_service(), [rate_limited], catalog_id="cat_a")
    # The only ALLOW was dropped → nothing left to allow → deny-by-default,
    # but flagged uncompilable so callers know the filter is stricter.
    assert f.deny_all is True
    assert f.uncompilable is True


@pytest.mark.asyncio
async def test_uncompilable_allow_alongside_compilable_allow() -> None:
    """A dropped uncompilable ALLOW must not suppress a sibling compilable
    ALLOW — only the uncompilable flag is raised."""
    good = _allow(
        "a_pub",
        path=".*",
        method="GET",
        conditions=[Condition(type="catalog_lookup_public_allowed")],
    )
    bad = _allow(
        "a_rl",
        path=".*",
        method="GET",
        conditions=[
            Condition(type="rate_limit", config={"limit": 10, "window_seconds": 60})
        ],
    )
    f = await _compile(_service(), [good, bad], catalog_id="cat_a")
    assert f.deny_all is False
    assert f.uncompilable is True
    assert f.admits({"catalog_id": "cat_a", "visibility": "public"}) is True


@pytest.mark.asyncio
async def test_relevant_deny_with_uncompilable_condition_fails_closed() -> None:
    """A relevant DENY we cannot express must fully fail closed — dropping it
    would let an ALLOW leak documents the engine would deny."""
    allow = _allow("a_all", path=".*", method="GET")
    deny = _deny(
        "d_rl",
        path=".*",
        method="GET",
        conditions=[
            Condition(type="rate_limit", config={"limit": 1, "window_seconds": 60})
        ],
    )
    f = await _compile(_service(), [allow, deny], catalog_id="cat_a")
    assert f.deny_all is True
    assert f.uncompilable is True
    assert f.admits({"catalog_id": "cat_a", "visibility": "public"}) is False


@pytest.mark.asyncio
async def test_attribute_match_satisfied_gate_is_unconditional_in_scope() -> None:
    """A ``match`` on ``principal.attributes.<k>`` is a principal GATE, not a
    document predicate. When the principal satisfies it (its attribute equals
    the static ``value``), the ALLOW applies unconditionally within its
    resource scope and must NOT add a per-document ``owner`` predicate."""
    p = _allow(
        "a_owner",
        path=".*",
        method="GET",
        conditions=[
            Condition(
                type="match",
                config={
                    "attribute": "principal.attributes.owner",
                    "operator": "eq",
                    "value": "alice",
                },
            )
        ],
    )
    f = await _compile(
        _service(), [p], catalog_id="cat_a", attributes={"owner": "alice"}
    )
    assert f.deny_all is False
    assert f.uncompilable is False
    # No owner predicate: the gate was about the principal, not the document.
    owner_preds = {
        pred.field for clause in f.allow for pred in clause.predicates
    }
    assert "owner" not in owner_preds
    # Any document in the scoped catalog is admitted regardless of its owner.
    assert f.admits({"catalog_id": "cat_a", "owner": "bob"}) is True
    assert f.admits({"catalog_id": "cat_a", "owner": "alice"}) is True
    # Still pinned to the catalog scope.
    assert f.admits({"catalog_id": "cat_b", "owner": "alice"}) is False


@pytest.mark.asyncio
async def test_attribute_match_failed_gate_drops_the_grant() -> None:
    """When the principal does NOT satisfy the ``match`` gate, the grant is
    dropped exactly as the engine would deny it — deny-by-default, and NOT
    flagged uncompilable (the gate was evaluated, it simply does not hold).
    Folding the principal's own value into a document predicate here would be
    a leak (it would admit documents whose field equals the principal's value
    despite the failing gate)."""
    p = _allow(
        "a_owner",
        path=".*",
        method="GET",
        conditions=[
            Condition(
                type="match",
                config={
                    "attribute": "principal.attributes.owner",
                    "operator": "eq",
                    "value": "alice",
                },
            )
        ],
    )
    f = await _compile(
        _service(), [p], catalog_id="cat_a", attributes={"owner": "bob"}
    )
    assert f.deny_all is True
    assert f.uncompilable is False
    assert f.admits({"catalog_id": "cat_a", "owner": "bob"}) is False


@pytest.mark.asyncio
async def test_request_time_attribute_match_is_uncompilable() -> None:
    """A ``match`` against request state (query param) is not stable at
    compile time → uncompilable → the ALLOW is dropped."""
    p = _allow(
        "a_q",
        path=".*",
        method="GET",
        conditions=[
            Condition(
                type="match",
                config={"attribute": "query.tenant", "operator": "eq", "value": "x"},
            )
        ],
    )
    f = await _compile(_service(), [p], catalog_id="cat_a")
    assert f.deny_all is True
    assert f.uncompilable is True


@pytest.mark.asyncio
async def test_collection_scope_pin_is_added() -> None:
    """When a collection is named, the clause is pinned to it too."""
    p = _allow(
        "a_pub",
        path=r"/stac/catalogs/[^/]+/collections/[^/]+/items",
        method="GET",
        conditions=[Condition(type="catalog_lookup_public_allowed")],
    )
    f = await _compile(
        _service(), [p], catalog_id="cat_a", collection_id="col_x"
    )
    fields = {
        pred.field: pred.values
        for clause in f.allow
        for pred in clause.predicates
    }
    assert fields.get("collection_id") == ("col_x",)
    assert f.admits(
        {"catalog_id": "cat_a", "collection_id": "col_x", "visibility": "public"}
    ) is True
    assert f.admits(
        {"catalog_id": "cat_a", "collection_id": "col_y", "visibility": "public"}
    ) is False
