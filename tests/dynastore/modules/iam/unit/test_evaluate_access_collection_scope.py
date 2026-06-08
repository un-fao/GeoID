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

"""Pure-unit pins for collection-scoped grant enforcement in
``PolicyService.evaluate_access`` (un-fao/GeoID#1341).

A grant binds ``principal → role`` scoped to a specific collection
(``resource_kind='collection'``) or to the whole catalog
(``resource_kind=None``). Enforcement rules pinned here:

  * A collection-scoped ALLOW grants access on that collection.
  * A role scoped to collection A does NOT grant access on collection B.
  * A catalog-wide DENY overrides a collection ALLOW (deny-precedence).
  * A collection-scoped DENY overrides a catalog-wide ALLOW.
  * ``evaluate_access(principal_id=None)`` is unchanged vs. pre-#1341
    (no grant resolution attempted).

These run as pure unit tests: ``iam_storage`` (``resolve_effective_grants``
+ ``get_role``), ``get_policy`` and ``_resolve_schema`` are mocked, so no
DB / catalog protocol is needed. Mirrors the mocking style of
``test_evaluate_access_deny_precedence.py``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from dynastore.models.auth import Policy
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService


_SCHEMA = "s_test_catalog"
_CATALOG_ID = "test_catalog"
_COLL_A = "collA"
_COLL_B = "collB"


class _FakeIamStorage:
    """Minimal storage stub.

    ``grants`` is a list of rows mirroring ``resolve_effective_grants``
    output (``object_kind``/``object_ref``/``resource_kind``/
    ``resource_ref``/``effect``). ``roles`` maps role-name → ``Role``.
    ``resolve_effective_grants`` applies the resource-scope filter the
    real query applies: whole-catalog rows (``resource_kind is None``)
    plus rows matching the requested ``collection_id``.
    """

    def __init__(self, grants: List[Dict[str, Any]], roles: Dict[str, Role]):
        self._grants = grants
        self._roles = roles

    async def resolve_effective_grants(
        self,
        principal_id: Any,
        catalog_schema: Optional[str] = None,
        collection_id: Optional[str] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self._grants:
            rk = row.get("resource_kind")
            rr = row.get("resource_ref")
            if rk is None:
                out.append(row)
            elif collection_id is not None and rk == "collection" and rr == collection_id:
                out.append(row)
        return out

    async def get_role(
        self, role_id: str, schema: str = "iam", **_: Any
    ) -> Optional[Role]:
        return self._roles.get(role_id)


def _service(storage: _FakeIamStorage, policies: Dict[str, Policy]) -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = storage  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _fixed_schema(catalog_id, conn=None):  # noqa: ANN001
        return _SCHEMA if catalog_id else "iam"

    async def _get_policy(pid, catalog_id=None):  # noqa: ANN001
        return policies.get(pid)

    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    return svc


def _role(name: str, policy_ids: List[str]) -> Role:
    return Role(id=name, name=name, policies=policy_ids)


def _grant(
    role_name: str,
    *,
    effect: str = "allow",
    resource_kind: Optional[str] = None,
    resource_ref: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "object_kind": "role",
        "object_ref": role_name,
        "effect": effect,
        "resource_kind": resource_kind,
        "resource_ref": resource_ref,
    }


def _allow_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="ALLOW", actions=[".*"], resources=[".*"])


def _deny_policy(pid: str) -> Policy:
    return Policy(id=pid, effect="DENY", actions=[".*"], resources=[".*"])


async def _call(
    svc: PolicyService, *, collection_id: Optional[str], principal_id: Any
) -> tuple[bool, str]:
    return await svc.evaluate_access(
        principals=[],
        path=f"/catalogs/{_CATALOG_ID}/collections/{collection_id}/items",
        method="GET",
        catalog_id=_CATALOG_ID,
        principal_id=principal_id,
        collection_id=collection_id,
    )


@pytest.mark.asyncio
async def test_collection_scoped_allow_grants_access_on_that_collection() -> None:
    """An ALLOW role scoped to collA grants access when scope == collA."""
    storage = _FakeIamStorage(
        grants=[_grant("editor", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    allowed, reason = await _call(svc, collection_id=_COLL_A, principal_id=uuid4())
    assert allowed is True, reason
    assert "allow_pol" in reason


@pytest.mark.asyncio
async def test_collection_scoped_role_does_not_grant_other_collection() -> None:
    """A role scoped to collA must NOT grant on collB."""
    storage = _FakeIamStorage(
        grants=[_grant("editor", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    allowed, reason = await _call(svc, collection_id=_COLL_B, principal_id=uuid4())
    assert allowed is False, reason
    assert "Deny by Default" in reason


@pytest.mark.asyncio
async def test_catalog_wide_deny_overrides_collection_allow() -> None:
    """A whole-catalog DENY beats a collection-scoped ALLOW (deny-precedence)."""
    storage = _FakeIamStorage(
        grants=[
            _grant("editor", resource_kind="collection", resource_ref=_COLL_A),
            _grant("blocked", effect="allow", resource_kind=None, resource_ref=None),
        ],
        roles={
            "editor": _role("editor", ["allow_pol"]),
            "blocked": _role("blocked", ["deny_pol"]),
        },
    )
    svc = _service(
        storage,
        {"allow_pol": _allow_policy("allow_pol"), "deny_pol": _deny_policy("deny_pol")},
    )
    allowed, reason = await _call(svc, collection_id=_COLL_A, principal_id=uuid4())
    assert allowed is False, reason
    assert "deny_pol" in reason


@pytest.mark.asyncio
async def test_collection_scoped_deny_overrides_catalog_wide_allow() -> None:
    """A collection-scoped DENY beats a whole-catalog ALLOW."""
    storage = _FakeIamStorage(
        grants=[
            _grant("viewer", effect="allow", resource_kind=None, resource_ref=None),
            _grant(
                "blocked",
                effect="allow",
                resource_kind="collection",
                resource_ref=_COLL_A,
            ),
        ],
        roles={
            "viewer": _role("viewer", ["allow_pol"]),
            "blocked": _role("blocked", ["deny_pol"]),
        },
    )
    svc = _service(
        storage,
        {"allow_pol": _allow_policy("allow_pol"), "deny_pol": _deny_policy("deny_pol")},
    )
    allowed, reason = await _call(svc, collection_id=_COLL_A, principal_id=uuid4())
    assert allowed is False, reason
    assert "deny_pol" in reason


@pytest.mark.asyncio
async def test_collection_scoped_deny_not_applied_on_other_collection() -> None:
    """The collection-scoped DENY above only applies to its own collection;
    on collB the whole-catalog ALLOW stands."""
    storage = _FakeIamStorage(
        grants=[
            _grant("viewer", effect="allow", resource_kind=None, resource_ref=None),
            _grant(
                "blocked",
                effect="allow",
                resource_kind="collection",
                resource_ref=_COLL_A,
            ),
        ],
        roles={
            "viewer": _role("viewer", ["allow_pol"]),
            "blocked": _role("blocked", ["deny_pol"]),
        },
    )
    svc = _service(
        storage,
        {"allow_pol": _allow_policy("allow_pol"), "deny_pol": _deny_policy("deny_pol")},
    )
    allowed, reason = await _call(svc, collection_id=_COLL_B, principal_id=uuid4())
    assert allowed is True, reason
    assert "allow_pol" in reason


@pytest.mark.asyncio
async def test_direct_policy_grant_scoped_to_collection_grants_access() -> None:
    """A policy bound straight to the principal (object_kind='policy'),
    scoped to collA, grants access on collA and not on collB."""
    grants = [
        {
            "object_kind": "policy",
            "object_ref": "allow_pol",
            "effect": "allow",
            "resource_kind": "collection",
            "resource_ref": _COLL_A,
        }
    ]
    storage = _FakeIamStorage(grants=grants, roles={})
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})

    allowed_a, reason_a = await _call(svc, collection_id=_COLL_A, principal_id=uuid4())
    assert allowed_a is True, reason_a
    assert "allow_pol" in reason_a

    allowed_b, reason_b = await _call(svc, collection_id=_COLL_B, principal_id=uuid4())
    assert allowed_b is False, reason_b
    assert "Deny by Default" in reason_b


@pytest.mark.asyncio
async def test_principal_id_none_skips_grant_resolution() -> None:
    """With ``principal_id=None`` no grant resolution runs — the grant store
    is never consulted, behaviour is identical to pre-#1341. With no other
    matching policy this is deny-by-default."""

    consulted = {"called": False}

    class _TrackingStorage(_FakeIamStorage):
        async def resolve_effective_grants(self, *a: Any, **k: Any):  # noqa: ANN401
            consulted["called"] = True
            return await super().resolve_effective_grants(*a, **k)

    storage = _TrackingStorage(
        grants=[_grant("editor", resource_kind="collection", resource_ref=_COLL_A)],
        roles={"editor": _role("editor", ["allow_pol"])},
    )
    svc = _service(storage, {"allow_pol": _allow_policy("allow_pol")})
    allowed, reason = await _call(svc, collection_id=_COLL_A, principal_id=None)
    assert consulted["called"] is False, "grant store must not be consulted"
    assert allowed is False, reason
    assert "Deny by Default" in reason
