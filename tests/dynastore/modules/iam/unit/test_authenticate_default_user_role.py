"""Regression: every authenticated principal carries the baseline role.

PR #198 first fixed an asymmetry between the OIDC and HS256 token
verification branches in ``IamService.authenticate_and_get_role``: a JWT
with a missing or empty ``roles`` field left an authenticated principal
with zero effective roles, surfacing as ANONYMOUS-equivalent at the
policy layer and returning 403 from every ``/iam/me/*`` self-service
endpoint.

The rule has since broadened: the baseline ``default_user_role_name`` is
the public/self-info FLOOR — the role that ``self_service_authorization_api``
(``/iam/me``, ``/auth/userinfo``), ``auth_extension_public`` (``/auth/*``)
and the public-access policies bind to — so ``_normalize_authenticated_roles``
now appends it to a principal's declared realm roles, not only when the
declared list is empty. Without that, a role-carrying user (``admin`` /
``user`` / ``viewer``) dropped the baseline the instant it logged in and
was denied-by-default on its own profile endpoints, leaving the web UI
grayed/locked right after login.

These tests pin that contract so a future refactor of the role-resolution
path can't silently regress SelfServiceAPI access.
"""

from typing import Any, List

import pytest

from dynastore.models.protocols.authorization import IamRolesConfig

_DEFAULTS = IamRolesConfig()


class _FakeStorage:
    """Minimal IamStorage stub — get_role_hierarchy returns the input set."""

    async def get_role_hierarchy(self, roles: List[str], schema: str) -> List[str]:
        return list(roles)


class _FakeIdentityProvider:
    """Identity provider stub that always rejects (no OIDC validation)
    so the authenticate_and_get_role path falls through to HS256."""

    def get_provider_id(self) -> str:
        return "noop"

    async def validate_token(self, token: str) -> None:
        return None


def _build_iam_service_for_hs256(secret: str) -> Any:
    """Construct a minimal IamService instance wired just enough to exercise
    ``authenticate_and_get_role`` along the HS256 fallback path."""
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.authorization import IamRolesConfig

    svc = object.__new__(IamService)
    svc.storage = _FakeStorage()
    svc._identity_providers = [_FakeIdentityProvider()]
    svc._role_config = IamRolesConfig()

    async def _get_roles_config() -> IamRolesConfig:
        return svc._role_config

    svc._get_roles_config = _get_roles_config

    async def _get_jwt_secrets_for_verification() -> List[str]:
        return [secret]

    async def _resolve_schema(_catalog_id: Any) -> str:
        return "iam"

    def _extract_token_from_request(request: Any) -> str:
        return getattr(request, "token", "")

    def _get_identity_providers() -> List[Any]:
        return svc._identity_providers

    svc.get_jwt_secrets_for_verification = _get_jwt_secrets_for_verification
    svc._resolve_schema = _resolve_schema
    svc.extract_token_from_request = _extract_token_from_request
    svc.get_identity_providers = _get_identity_providers

    return svc


def _make_request(token: str) -> Any:
    class _Req:
        def __init__(self, t: str) -> None:
            self.token = t

            class _State:
                catalog_id = None

            self.state = _State()

    return _Req(token)


@pytest.mark.asyncio
async def test_hs256_jwt_with_empty_roles_defaults_to_user_role() -> None:
    """A JWT carrying ``"roles": []`` must surface as the configured
    default-user role, not ``[]`` (which would collapse to ANONYMOUS at
    policy time).

    Post role-trim, ``default_user_role_name`` collapses onto
    ``unauthenticated`` (same string as ``anonymous_role_name``). The
    distinction now lives at the *principal* level — an authenticated
    empty-roles JWT still surfaces a non-None Principal, while anonymous
    is principal=None.
    """
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-empty-roles-padded-to-32-chars-xx"
    payload = {
        "sub": "test-user-empty",
        "roles": [],
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None, "Principal should be resolved from valid HS256 token"
    assert _DEFAULTS.default_user_role_name in effective_roles, (
        f"Expected default-user role for empty-roles HS256 JWT, got {effective_roles!r}"
    )


@pytest.mark.asyncio
async def test_hs256_jwt_without_roles_field_defaults_to_user_role() -> None:
    """A JWT that omits the ``roles`` field entirely should also default
    to ``["user"]`` (matches the pre-existing payload.get default but
    pinned here so the .get('roles', ['user']) → ``or ["user"]`` shift
    can't silently lose the missing-key case)."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-no-roles-padded-to-32-chars-xxxxx"
    payload = {
        "sub": "test-user-noroles",
        # no "roles" key
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None
    assert _DEFAULTS.default_user_role_name in effective_roles, (
        f"Expected default-user role for roles-omitted HS256 JWT, got {effective_roles!r}"
    )


def test_normalize_authenticated_roles_helper_defaults() -> None:
    """Direct test of the extracted ``_normalize_authenticated_roles`` helper.

    The HS256 + OIDC paths now share this method — testing it directly
    pins the "default to USER" rule independently of the bigger
    authentication-flow integration tests above. Future refactors can
    grep for this helper instead of having to reverse-engineer the rule
    from two separate auth-flow branches.
    """
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols.authorization import IamRolesConfig

    # _normalize_authenticated_roles is an instance method — build a minimal stub.
    svc = object.__new__(IamService)
    svc._role_config = IamRolesConfig()
    norm = svc._normalize_authenticated_roles
    baseline = _DEFAULTS.default_user_role_name
    # None / empty list → just the baseline role
    assert norm(None) == [baseline]
    assert norm([]) == [baseline]
    # Single-string role → wrapped AND carries the baseline floor
    assert norm("editor") == ["editor", baseline]
    # Non-empty list → declared roles PLUS the baseline floor (order preserved)
    assert norm(["sysadmin", "admin"]) == ["sysadmin", "admin", baseline]
    assert norm(["editor"]) == ["editor", baseline]
    # Baseline already declared → not duplicated
    assert norm([baseline]) == [baseline]
    assert norm([baseline, "admin"]) == [baseline, "admin"]


@pytest.mark.asyncio
async def test_hs256_jwt_with_explicit_roles_preserves_them() -> None:
    """An HS256 JWT that DOES carry roles must keep them — and ALSO carry the
    baseline self-service role alongside them (the baseline is the public/
    self-info floor every authenticated principal holds, not a fallback that
    only fires when the declared list is empty)."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-explicit-roles-padded-to-32-chars"
    payload = {
        "sub": "test-user-explicit",
        "roles": ["sysadmin"],
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None
    assert "sysadmin" in effective_roles, (
        f"Expected sysadmin role to be preserved, got {effective_roles!r}"
    )


@pytest.mark.asyncio
async def test_role_carrying_principal_retains_self_service_baseline() -> None:
    """Regression: a role-carrying authenticated principal must STILL carry
    the baseline self-service role.

    A token mapping to a realm role (``admin``/``user``/``viewer``) used to
    resolve to that role ONLY — dropping the baseline ``default_user_role_name``
    the instant it authenticated. Since ``self_service_authorization_api``
    (``/iam/me``, ``/iam/me/*``, ``/auth/userinfo``) and ``auth_extension_public``
    (``/auth/*``) bind to that baseline, the user was then denied-by-default
    (403) on its OWN profile endpoints — the SPA's post-login profile fetch
    failed and left the page grayed/locked. The baseline must accompany the
    declared role so the self-info contract holds for every logged-in user."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    secret = "test-secret-role-carrying-padded-to-32-chars-x"
    payload = {
        "sub": "test-user-admin",
        "roles": ["admin"],
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "regression-test",
    }
    token = pyjwt.encode(payload, secret, algorithm="HS256")

    svc = _build_iam_service_for_hs256(secret)
    effective_roles, principal = await svc.authenticate_and_get_role(_make_request(token))

    assert principal is not None
    assert "admin" in effective_roles, f"declared role lost: {effective_roles!r}"
    assert _DEFAULTS.default_user_role_name in effective_roles, (
        "authenticated role-carrying principal dropped the self-service baseline "
        f"role — /iam/me + /auth/userinfo would 403: {effective_roles!r}"
    )
