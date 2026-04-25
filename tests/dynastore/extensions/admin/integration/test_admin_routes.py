"""
Integration tests for the Admin extension.

Covers read-only admin endpoints exercised by the sysadmin client:
- GET /admin/users              — list local users
- GET /admin/principals         — search principals
- GET /admin/roles              — list roles
- GET /admin/policies           — list policies

Write operations (create/update/delete) are tested for basic acceptance
(status 201/204) to exercise the route handlers without asserting
domain-specific invariants.
"""

import pytest
from httpx import AsyncClient


MARKER = pytest.mark.enable_extensions("admin", "features", "config", "iam")


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_users_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/users — returns 200 with a list."""
    r = await sysadmin_in_process_client.get("/admin/users")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


@MARKER
@pytest.mark.asyncio
async def test_list_users_pagination(sysadmin_in_process_client: AsyncClient):
    """GET /admin/users?limit=1 — pagination parameters accepted."""
    r = await sysadmin_in_process_client.get("/admin/users", params={"limit": 1})
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_get_unknown_user_404(sysadmin_in_process_client: AsyncClient):
    """GET /admin/users/{id} — nonexistent user returns 404.

    The route declares ``principal_id: UUID`` so a non-UUID path segment
    fails FastAPI path validation with 422 before reaching the handler.
    Use a well-formed UUID that won't exist in IAM to exercise the 404 path.
    """
    import uuid
    nonexistent = str(uuid.uuid4())
    r = await sysadmin_in_process_client.get(f"/admin/users/{nonexistent}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Principals
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_search_principals_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals — returns 200."""
    r = await sysadmin_in_process_client.get("/admin/principals")
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_search_principals_with_query(sysadmin_in_process_client: AsyncClient):
    """GET /admin/principals?q= — text search parameter accepted."""
    r = await sysadmin_in_process_client.get("/admin/principals", params={"q": "admin"})
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_roles_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/roles — returns a list of roles."""
    r = await sysadmin_in_process_client.get("/admin/roles")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


@MARKER
@pytest.mark.asyncio
async def test_create_and_delete_role(sysadmin_in_process_client: AsyncClient):
    """POST /admin/roles + DELETE /admin/roles/{name} — role lifecycle."""
    role_name = "test_role_coverage_fixture"

    # Create
    r = await sysadmin_in_process_client.post(
        "/admin/roles",
        json={
            "name": role_name,
            "description": "Temporary test role for coverage",
            "permissions": [],
        },
    )
    # Accept 201 or 409 (already exists from a prior run)
    assert r.status_code in (201, 409), f"Unexpected: {r.status_code} {r.text}"

    # Delete
    r = await sysadmin_in_process_client.delete(f"/admin/roles/{role_name}")
    assert r.status_code in (204, 404)


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_policies_returns_200(sysadmin_in_process_client: AsyncClient):
    """GET /admin/policies — returns a list."""
    r = await sysadmin_in_process_client.get("/admin/policies")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
