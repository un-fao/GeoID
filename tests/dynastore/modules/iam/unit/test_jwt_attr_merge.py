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

"""Unit tests for JWT-claim → Principal.attributes enrichment (F4 of #1443).

Covers:
- config absent / empty claim_map → no mutation
- claim present → attribute added
- DB attribute wins on collision
- invalid claim path (nested / array) → skipped, no crash
- missing claim key in token → skipped
- empty / None claim value → skipped
- service-account reserved keys not shadowed
- claim_map key validation at config load
- JWT-enriched attribute participates in compile_read_filter ABAC
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import pydantic

from dynastore.models.auth import Principal
from dynastore.modules.iam.iam_service import IamService
from dynastore.modules.iam.jwt_attr_config import JwtAttributeClaimsConfig, _resolve_claim_value


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(jwt_cfg: Optional[JwtAttributeClaimsConfig] = None) -> IamService:
    """Build a minimal IamService with only the JWT-merge path wired up."""
    svc = IamService.__new__(IamService)
    svc.storage = MagicMock()  # type: ignore[attr-defined]

    _cfg = jwt_cfg or JwtAttributeClaimsConfig()

    async def _get_jwt_attr_config() -> JwtAttributeClaimsConfig:
        return _cfg

    svc._get_jwt_attr_config = _get_jwt_attr_config  # type: ignore[method-assign]
    return svc


def _principal(**attrs: Any) -> Principal:
    return Principal(
        subject_id="user@example.com",
        provider="oidc",
        roles=["viewer"],
        attributes=dict(attrs),
    )


def _identity(raw_claims: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "sub": "user@example.com",
        "provider": "oidc",
        "raw_claims": raw_claims or {},
    }


# ---------------------------------------------------------------------------
# _resolve_claim_value unit tests
# ---------------------------------------------------------------------------

def test_resolve_bare_claim_name() -> None:
    assert _resolve_claim_value({"dept": "finance"}, "dept") == "finance"


def test_resolve_dollar_dot_prefix() -> None:
    assert _resolve_claim_value({"region": "eu"}, "$.region") == "eu"


def test_resolve_missing_claim_returns_none() -> None:
    assert _resolve_claim_value({}, "dept") is None


def test_resolve_none_claim_returns_none() -> None:
    assert _resolve_claim_value({"dept": None}, "dept") is None


def test_resolve_empty_string_returns_none() -> None:
    assert _resolve_claim_value({"dept": ""}, "dept") is None


def test_resolve_nested_path_rejected() -> None:
    # "a.b" looks nested after stripping the leading "$." prefix; must return None.
    assert _resolve_claim_value({"a": {"b": "v"}}, "a.b") is None


def test_resolve_array_selector_rejected() -> None:
    assert _resolve_claim_value({"a": ["v"]}, "a[0]") is None


def test_resolve_list_claim_returns_none() -> None:
    """List-typed claim (e.g. Keycloak one-element list) must return None, not
    the stringified repr '['finance']' which no predicate would ever match."""
    assert _resolve_claim_value({"dept": ["finance"]}, "dept") is None


def test_resolve_dict_claim_returns_none() -> None:
    """Dict-typed claim must return None rather than coercing to str."""
    assert _resolve_claim_value({"dept": {"name": "finance"}}, "dept") is None


def test_resolve_int_claim_coerced_to_str() -> None:
    """Scalar numeric claims are valid and coerced to string."""
    assert _resolve_claim_value({"level": 3}, "level") == "3"


def test_resolve_bool_claim_coerced_to_str() -> None:
    """Scalar bool claims are valid and coerced to string."""
    assert _resolve_claim_value({"verified": True}, "verified") == "True"


# ---------------------------------------------------------------------------
# _merge_jwt_attributes tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_config_no_mutation() -> None:
    """Empty claim_map (default config) leaves attributes untouched."""
    svc = _make_service(JwtAttributeClaimsConfig(claim_map={}))
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": "finance"}))
    assert p.attributes == {}


@pytest.mark.asyncio
async def test_claim_present_adds_attribute() -> None:
    """A mapped claim that is present in raw_claims is added to attributes."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": "finance"}))
    assert p.attributes["dept"] == "finance"


@pytest.mark.asyncio
async def test_db_attribute_wins_on_collision() -> None:
    """A key already present in principal.attributes is never overwritten."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"})
    svc = _make_service(cfg)
    p = _principal(dept="legal")  # DB-stored value
    await svc._merge_jwt_attributes(p, _identity({"dept": "finance"}))
    assert p.attributes["dept"] == "legal"


@pytest.mark.asyncio
async def test_invalid_claim_path_skipped_no_crash() -> None:
    """A nested path like 'a.b' is silently skipped (no AttributeError)."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "$.nested.path"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"nested": {"path": "v"}}))
    assert "dept" not in p.attributes


@pytest.mark.asyncio
async def test_missing_claim_key_skipped() -> None:
    """A claim_map entry whose key is absent in raw_claims adds nothing."""
    cfg = JwtAttributeClaimsConfig(claim_map={"region": "region"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": "finance"}))  # no "region"
    assert "region" not in p.attributes


@pytest.mark.asyncio
async def test_empty_claim_value_skipped() -> None:
    """Empty string claim value is treated as absent."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": ""}))
    assert "dept" not in p.attributes


@pytest.mark.asyncio
async def test_none_claim_value_skipped() -> None:
    """None claim value is treated as absent."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": None}))
    assert "dept" not in p.attributes


@pytest.mark.asyncio
async def test_service_account_reserved_keys_not_shadowed() -> None:
    """service_account / client_id / azp set by _auto_register_principal are DB attrs.

    If a JWT carries a claim named e.g. 'client_id', the DB value wins because
    _auto_register_principal already wrote that key into principals.attributes.
    """
    cfg = JwtAttributeClaimsConfig(claim_map={
        "service_account": "service_account",
        "client_id": "azp",
        "azp": "azp",
    })
    svc = _make_service(cfg)
    # Simulate the DB-stored attributes written by _auto_register_principal.
    p = _principal(service_account=True, client_id="my-client", azp="my-client")
    await svc._merge_jwt_attributes(p, _identity({
        "service_account": "injected",
        "azp": "evil-client",
    }))
    assert p.attributes["service_account"] is True
    assert p.attributes["client_id"] == "my-client"
    assert p.attributes["azp"] == "my-client"


@pytest.mark.asyncio
async def test_no_raw_claims_in_identity_is_safe() -> None:
    """Identity without raw_claims key is handled gracefully."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"})
    svc = _make_service(cfg)
    p = _principal()
    identity: Dict[str, Any] = {"sub": "u1", "provider": "oidc"}  # no raw_claims
    await svc._merge_jwt_attributes(p, identity)
    assert "dept" not in p.attributes


@pytest.mark.asyncio
async def test_multiple_claims_merged() -> None:
    """Multiple mapped claims all added when all absent from DB attributes."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept", "region": "region"})
    svc = _make_service(cfg)
    p = _principal()
    await svc._merge_jwt_attributes(p, _identity({"dept": "finance", "region": "eu"}))
    assert p.attributes["dept"] == "finance"
    assert p.attributes["region"] == "eu"


@pytest.mark.asyncio
async def test_issuer_not_in_allowlist_is_noop() -> None:
    """_merge_jwt_attributes is a no-op when the token's iss is not in
    issuer_allowlist — protects multi-provider deployments from cross-IdP
    attribute injection."""
    cfg = JwtAttributeClaimsConfig(
        claim_map={"dept": "dept"},
        issuer_allowlist=["https://trusted.example.com"],
    )
    svc = _make_service(cfg)
    p = _principal()
    identity = _identity({"dept": "finance", "iss": "https://untrusted.partner.com"})
    await svc._merge_jwt_attributes(p, identity)
    assert "dept" not in p.attributes


@pytest.mark.asyncio
async def test_issuer_in_allowlist_claims_are_merged() -> None:
    """Claims are merged when the token's iss is in the allowlist."""
    cfg = JwtAttributeClaimsConfig(
        claim_map={"dept": "dept"},
        issuer_allowlist=["https://trusted.example.com"],
    )
    svc = _make_service(cfg)
    p = _principal()
    identity = _identity({"dept": "finance", "iss": "https://trusted.example.com"})
    await svc._merge_jwt_attributes(p, identity)
    assert p.attributes["dept"] == "finance"


@pytest.mark.asyncio
async def test_none_issuer_allowlist_accepts_all_issuers() -> None:
    """None issuer_allowlist (default) accepts tokens from any issuer —
    backward-compatible single-IdP behaviour."""
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept"}, issuer_allowlist=None)
    svc = _make_service(cfg)
    p = _principal()
    identity = _identity({"dept": "finance", "iss": "https://any.issuer.io"})
    await svc._merge_jwt_attributes(p, identity)
    assert p.attributes["dept"] == "finance"


# ---------------------------------------------------------------------------
# JwtAttributeClaimsConfig validation tests
# ---------------------------------------------------------------------------

def test_valid_claim_map_keys_accepted() -> None:
    cfg = JwtAttributeClaimsConfig(claim_map={"dept": "dept", "my_region": "region"})
    assert "dept" in cfg.claim_map


def test_invalid_claim_map_key_rejected() -> None:
    """A key with a dot is rejected at config load time."""
    with pytest.raises(pydantic.ValidationError):
        JwtAttributeClaimsConfig(claim_map={"bad.key": "dept"})


def test_invalid_claim_map_key_with_space_rejected() -> None:
    with pytest.raises(pydantic.ValidationError):
        JwtAttributeClaimsConfig(claim_map={"bad key": "dept"})


def test_invalid_claim_map_key_starts_digit_rejected() -> None:
    with pytest.raises(pydantic.ValidationError):
        JwtAttributeClaimsConfig(claim_map={"1bad": "dept"})


# ---------------------------------------------------------------------------
# Integration: JWT-enriched attribute participates in compile_read_filter ABAC
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_jwt_enriched_attribute_participates_in_abac() -> None:
    """A principal whose 'dept' attribute comes from a JWT claim satisfies a
    grant with attribute_predicates=[{in, dept, [finance]}].

    This test drives compile_read_filter via a stubbed PolicyService and then
    checks that the resulting AccessFilter includes the expected FieldPredicate.
    The JWT merge itself is already tested above; here we verify end-to-end
    that a post-merge principal.attributes value surfaces correctly.
    """
    from dynastore.models.auth import Policy
    from dynastore.models.protocols.access_filter import AccessFilter
    from dynastore.modules.iam.policies import PolicyService

    # Build a minimal PolicyService stub (mirrors test_compile_read_filter_attribute_predicates.py).
    pid = uuid4()
    grant_rows = [
        {
            "id": str(pid),
            "object_kind": "role",
            "object_ref": "dept_reader",
            "attribute_predicates": [
                {"key": "dept", "op": "in", "values": ["finance", "global"]},
            ],
        }
    ]

    svc: Any = PolicyService.__new__(PolicyService)
    svc._state = None
    svc._engine = None
    svc.storage = None
    svc._role_config = None

    async def _fake_resolve_schema(catalog_id: Any, conn: Any = None) -> str:
        return "iam"

    svc._resolve_schema = _fake_resolve_schema

    class _StubIamStorage:
        async def resolve_effective_grants(
            self,
            principal_id: Any,
            catalog_schema: str = "iam",
            request_path: Optional[str] = None,
            collection_id: Optional[str] = None,
            conn: Any = None,
        ) -> List[Dict[str, Any]]:
            return grant_rows

    svc.iam_storage = _StubIamStorage()

    # Simulate post-JWT-merge principal: 'dept' attribute added from JWT claim.
    pol = Policy(id="allow_all", effect="ALLOW", actions=["GET"], resources=[".*"])
    principal = Principal(
        id=pid,
        custom_policies=[pol],
        attributes={"dept": "finance"},  # would have been set by _merge_jwt_attributes
    )

    af: AccessFilter = await svc.compile_read_filter(
        principals=[],
        catalog_id="cat1",
        collection_id="col1",
        principal=principal,
        principal_id=pid,
    )

    assert not af.deny_all
    attrs_clauses = [
        c for c in af.allow
        if any(fp.field == "_attrs.dept" for fp in c.predicates)
    ]
    assert len(attrs_clauses) >= 1
    dept_pred = next(
        fp for c in attrs_clauses for fp in c.predicates if fp.field == "_attrs.dept"
    )
    assert "finance" in dept_pred.values
