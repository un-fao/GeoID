"""
Integration tests for the Admin extension.

Covers read-only admin endpoints exercised by the sysadmin client:
- GET /admin/users              — list local users
- GET /admin/principals         — search principals
- GET /admin/roles              — list roles
- GET /admin/policies           — list policies
- GET /admin/catalogs/{id}/users           — list users assigned to a catalog
- POST   /admin/principals/{pid}/catalogs/{cid}/roles            — assign
- DELETE /admin/principals/{pid}/catalogs/{cid}/roles/{role}     — remove

Write operations (create/update/delete) are tested for basic acceptance
(status 201/204) to exercise the route handlers without asserting
domain-specific invariants.
"""

import uuid

import pytest
from httpx import AsyncClient

from .conftest import CreatedPrincipal


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


# ---------------------------------------------------------------------------
# Catalog users + catalog-scoped role assignment
#
# These pin down the API surface of the admin endpoints for catalog-role
# management:
#  - Unknown catalog ⇒ 404 (handler pre-checks via CatalogsProtocol).
#  - Unknown principal ⇒ 404.
#  - Round-trip: POST /catalogs/{cat}/roles ⇒ 204; GET /catalogs/{cat}/users
#    includes the principal; DELETE /catalogs/{cat}/roles/{role} ⇒ 204.
#
# These tests do NOT assert that the role is actually *scoped* to the
# catalog at the storage layer. ``postgres_iam_storage.{get_identity_roles,
# grant_roles, revoke_role}`` currently accept ``schema=`` but route to
# the global ``iam.principals.roles`` JSONB regardless. Resolving that —
# either by honoring ``schema=`` or by removing the parameter and dropping
# catalog-role scoping — is a separate architectural follow-up; these
# tests will gain a scoping assertion when it lands.
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_catalog_users_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """GET /admin/catalogs/{catalog_id}/users — 200 with a list."""
    catalog_id = setup_catalogs[0]
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{catalog_id}/users")
    assert r.status_code == 200, r.text
    assert isinstance(r.json(), list)


@MARKER
@pytest.mark.asyncio
async def test_list_catalog_users_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /admin/catalogs/{nonexistent}/users — 404."""
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{bogus}/users")
    assert r.status_code == 404


@MARKER
@pytest.mark.asyncio
async def test_assign_catalog_role_unknown_principal_404(
    sysadmin_in_process_client: AsyncClient, setup_catalogs
):
    """POST /admin/principals/{bogus_uuid}/catalogs/{cat}/roles — 404."""
    nonexistent = str(uuid.uuid4())
    r = await sysadmin_in_process_client.post(
        f"/admin/principals/{nonexistent}/catalogs/{setup_catalogs[0]}/roles",
        json={"role": "Editor"},
    )
    assert r.status_code == 404


@MARKER
@pytest.mark.asyncio
async def test_assign_catalog_role_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient, created_principal: CreatedPrincipal
):
    """POST /admin/principals/{pid}/catalogs/{bogus}/roles — 404.

    The handler pre-checks catalog existence via ``CatalogsProtocol``
    before touching IAM storage, so an unknown catalog short-circuits
    to 404 instead of silently writing to the global ``iam`` schema
    (the fallback ``IamService.resolve_schema`` would otherwise return).
    """
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.post(
        f"/admin/principals/{created_principal.principal_id}/catalogs/{bogus}/roles",
        json={"role": "Editor"},
    )
    assert r.status_code == 404, (
        f"Expected 404 for unknown catalog, got {r.status_code}: {r.text}"
    )


@MARKER
@pytest.mark.asyncio
async def test_remove_catalog_role_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient, created_principal: CreatedPrincipal
):
    """DELETE /admin/principals/{pid}/catalogs/{bogus}/roles/{role} — 404.

    Same pre-check as the POST counterpart; an unknown catalog must not
    silently revoke against the global ``iam`` schema.
    """
    bogus = f"nonexistent_{uuid.uuid4().hex[:8]}"
    r = await sysadmin_in_process_client.delete(
        f"/admin/principals/{created_principal.principal_id}"
        f"/catalogs/{bogus}/roles/Editor"
    )
    assert r.status_code == 404, (
        f"Expected 404 for unknown catalog, got {r.status_code}: {r.text}"
    )


@MARKER
@pytest.mark.asyncio
async def test_assign_and_remove_catalog_role_round_trip(
    sysadmin_in_process_client: AsyncClient,
    setup_catalogs,
    created_principal: CreatedPrincipal,
):
    """Round-trip: assign a catalog-scoped role, see the principal in
    the catalog-users listing, then remove the role."""
    catalog_id = setup_catalogs[0]

    # Assign
    r = await sysadmin_in_process_client.post(
        f"/admin/principals/{created_principal.principal_id}/catalogs/{catalog_id}/roles",
        json={"role": "Editor"},
    )
    assert r.status_code == 204, f"Assign failed: {r.status_code} {r.text}"

    # Listing should now include the principal
    r = await sysadmin_in_process_client.get(f"/admin/catalogs/{catalog_id}/users")
    assert r.status_code == 200
    user_ids = [u.get("id") for u in r.json()]
    assert created_principal.principal_id in user_ids, (
        f"Principal {created_principal.principal_id} not in catalog-users "
        f"after assign; got {user_ids}"
    )

    # Remove
    r = await sysadmin_in_process_client.delete(
        f"/admin/principals/{created_principal.principal_id}"
        f"/catalogs/{catalog_id}/roles/Editor"
    )
    assert r.status_code == 204, f"Remove failed: {r.status_code} {r.text}"
