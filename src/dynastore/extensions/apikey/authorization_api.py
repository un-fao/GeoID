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

"""
Simplified IAG REST API Endpoints (v2.1)

Email-based authorization management with self-service and admin endpoints.
"""

from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime

from dynastore.modules import get_protocol
from dynastore.models.protocols import ApiKeyProtocol, CatalogsProtocol
from dynastore.models.auth import Principal
from dynastore.extensions.tools.exception_handlers import http_errors


def _require_admin_role(request: Request) -> Principal:
    """Defense-in-depth: verify admin/sysadmin role even though middleware enforces policies."""
    principal: Principal | None = getattr(request.state, "principal", None)
    if not principal:
        raise HTTPException(status_code=401, detail="Authentication required.")
    admin_roles = {"sysadmin", "admin"}
    if not admin_roles.intersection(set(principal.roles or [])):
        raise HTTPException(status_code=403, detail="Admin role required.")
    return principal


router = APIRouter(
    prefix="/admin",
    tags=["Authorization Management"],
    dependencies=[Depends(_require_admin_role)],
)
me_router = APIRouter(prefix="/me", tags=["Self-Service Authorization"])

# --- DTOs ---


class RoleGrantRequest(BaseModel):
    """Request to grant roles to a user."""

    roles: List[str]
    granted_by: Optional[str] = None


class PolicyCreateRequest(BaseModel):
    """Request to create a custom policy."""

    policy_name: Optional[str] = None
    actions: List[str]
    resources: List[str] = ["*"]
    effect: str = "ALLOW"
    conditions: Optional[List[Dict[str, Any]]] = None


class EffectiveAuthorizationResponse(BaseModel):
    """Response showing effective authorization for a user in a catalog."""

    email: str
    provider: str
    subject_id: str
    global_roles: List[str]
    catalog_roles: List[str]
    effective_roles: List[str]
    is_active: bool
    valid_until: Optional[datetime] = None


class CatalogAccessResponse(BaseModel):
    """Response showing catalog access for a user."""

    catalog_id: str
    roles: List[str]


# --- Helper Functions ---


async def get_storage():
    """Get storage instance."""
    apikey_protocol = get_protocol(ApiKeyProtocol)
    if not apikey_protocol:
        raise HTTPException(
            status_code=500, detail="ApiKey protocol implementation not available"
        )
    return apikey_protocol.storage


async def get_catalogs_protocol() -> CatalogsProtocol:
    """Get catalog protocol implementation."""
    catalogs_protocol = get_protocol(CatalogsProtocol)
    if not catalogs_protocol:
        raise HTTPException(
            status_code=500, detail="Catalogs protocol implementation not available"
        )
    return catalogs_protocol


async def get_current_identity(request: Request) -> Dict[str, Any]:
    """Extract current user identity from request state."""
    if hasattr(request.state, "identity") and request.state.identity:
        return request.state.identity

    if hasattr(request.state, "principal") and request.state.principal:
        principal = request.state.principal
        return {
            "provider": principal.provider,
            "sub": principal.subject_id,
            "email": principal.display_name,
        }

    raise HTTPException(status_code=401, detail="Not authenticated")


async def resolve_catalog_schema(catalog_id: str) -> str:
    """Resolve catalog code to schema name."""
    catalog_module = await get_catalogs_protocol()
    try:
        schema = await catalog_module.resolve_physical_schema(
            catalog_id, db_resource=catalog_module.engine
        )
        if not schema:
            raise HTTPException(
                status_code=404,
                detail=f"Catalog not found or schema not resolved: {catalog_id}",
            )
        return schema
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail=f"Catalog not found: {catalog_id}")


# --- Self-Service Endpoints ---


@me_router.get("/available-roles")
async def get_available_roles(
    request: Request,
    catalog_id: Optional[str] = None,
):
    """List roles available in the system (self-service discovery)."""
    storage = await get_storage()
    await get_current_identity(request)  # Require authentication

    schema = "apikey"
    if catalog_id:
        schema = await resolve_catalog_schema(catalog_id)

    roles = await storage.list_roles(schema=schema)
    return [
        {
            "name": r.name,
            "description": r.description,
        }
        for r in roles
    ]


@me_router.get("/roles/global", response_model=List[str])
async def get_my_global_roles(request: Request):
    """Get my global roles."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")

    roles = await storage.get_identity_roles(provider, subject_id, schema="apikey")
    return roles


@me_router.get("/roles/catalogs/{catalog_id}", response_model=List[str])
async def get_my_catalog_roles(request: Request, catalog_id: str):
    """Get my roles in a specific catalog."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    catalog_schema = await resolve_catalog_schema(catalog_id)

    roles = await storage.get_identity_roles(
        provider, subject_id, schema=catalog_schema
    )
    return roles


@me_router.get("/catalogs", response_model=List[CatalogAccessResponse])
async def get_my_catalogs(request: Request):
    """Get all catalogs I have access to."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")

    # Get global roles
    global_roles = await storage.get_identity_roles(
        provider, subject_id, schema="apikey"
    )

    # Get catalog-specific access
    catalog_ids = await storage.get_catalogs_for_identity(provider, subject_id)

    result = []

    # Add global access if exists
    if global_roles:
        result.append(
            CatalogAccessResponse(
                catalog_id="*",  # Global
                roles=global_roles,
            )
        )

    # 1. Get all catalogs
    catalog_module = await get_catalogs_protocol()
    all_catalogs = await catalog_module.list_catalogs(db_resource=catalog_module.engine)

    for catalog in all_catalogs:
        # Get roles for this catalog
        try:
            # Re-fetch catalog_module if needed or use the existing one
            catalog_schema = await catalog_module.resolve_physical_schema(
                catalog.id, db_resource=catalog_module.engine
            )
            catalog_roles = await storage.get_identity_roles(
                provider, subject_id, schema=catalog_schema
            )
            if catalog_roles:
                result.append(
                    CatalogAccessResponse(catalog_id=catalog.id, roles=catalog_roles)
                )
        except Exception:
            continue

    return result


@me_router.get("/catalogs/{catalog_id}", response_model=EffectiveAuthorizationResponse)
async def get_my_effective_authorization(request: Request, catalog_id: str):
    """Get my effective authorization in a specific catalog."""
    storage = await get_storage()
    identity = await get_current_identity(request)
    provider = identity.get("provider")
    subject_id = identity.get("sub")
    email = identity.get("email", "unknown@example.com")

    # Get global and catalog roles
    catalog_schema = await resolve_catalog_schema(catalog_id)
    global_roles = await storage.get_identity_roles(
        provider, subject_id, schema="apikey"
    )
    catalog_roles = await storage.get_identity_roles(
        provider, subject_id, schema=catalog_schema
    )

    # Get authorization metadata (prefer catalog over global)
    auth = await storage.get_identity_authorization(
        provider, subject_id, schema=catalog_schema
    )
    if not auth:
        auth = await storage.get_identity_authorization(
            provider, subject_id, schema="apikey"
        )

    return EffectiveAuthorizationResponse(
        email=email,
        provider=provider,
        subject_id=subject_id,
        global_roles=global_roles,
        catalog_roles=catalog_roles,
        effective_roles=global_roles + catalog_roles,
        is_active=auth.get("is_active", True) if auth else True,
        valid_until=auth.get("valid_until") if auth else None,
    )


# --- Admin Endpoints ---


@router.post("/users/{email}/roles/global", status_code=201)
async def grant_global_roles(email: EmailStr, request: RoleGrantRequest):
    """Grant global roles to a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    await storage.grant_roles(
        provider=provider,
        subject_id=subject_id,
        roles=request.roles,
        schema="apikey",
        granted_by=request.granted_by,
    )

    return {"message": f"Granted global roles to {email}", "roles": request.roles}


@router.get("/users/{email}/roles/global", response_model=List[str])
async def get_user_global_roles(email: EmailStr):
    """Get global roles for a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    roles = await storage.get_identity_roles(provider, subject_id, schema="apikey")
    return roles


@router.delete("/users/{email}/roles/global/{role}", status_code=204)
async def revoke_global_role(email: EmailStr, role: str):
    """Revoke a global role from a user (admin only)."""
    storage = await get_storage()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    await storage.revoke_role(provider, subject_id, role, schema="apikey")
    return None


@router.post("/users/{email}/roles/catalogs/{catalog_id}", status_code=201)
async def grant_catalog_roles(
    email: EmailStr, catalog_id: str, request: RoleGrantRequest
):
    """Grant catalog-specific roles to a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    await storage.grant_roles(
        provider=provider,
        subject_id=subject_id,
        roles=request.roles,
        schema=catalog_schema,
        granted_by=request.granted_by,
    )

    return {
        "message": f"Granted catalog roles to {email}",
        "catalog": catalog_id,
        "roles": request.roles,
    }


@router.get("/users/{email}/roles/catalogs/{catalog_id}", response_model=List[str])
async def get_user_catalog_roles(email: EmailStr, catalog_id: str):
    """Get catalog-specific roles for a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    roles = await storage.get_identity_roles(
        provider, subject_id, schema=catalog_schema
    )
    return roles


@router.delete("/users/{email}/roles/catalogs/{catalog_id}/{role}", status_code=204)
async def revoke_catalog_role(email: EmailStr, catalog_id: str, role: str):
    """Revoke a catalog-specific role from a user (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    await storage.revoke_role(provider, subject_id, role, schema=catalog_schema)
    return None


@router.get("/users/{email}/catalogs", response_model=List[CatalogAccessResponse])
async def get_user_catalogs(email: EmailStr):
    """Get all catalogs a user has access to (admin only)."""
    storage = await get_storage()
    catalog_module = await get_catalogs_protocol()

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    result = []

    # Get global roles
    global_roles = await storage.get_identity_roles(
        provider, subject_id, schema="apikey"
    )
    if global_roles:
        result.append(CatalogAccessResponse(catalog_id="*", roles=global_roles))

    # 2. Get specific catalogs from storage discovery
    catalog_ids = await storage.get_catalogs_for_identity(provider, subject_id)
    
    for cat_id in catalog_ids:
        # Get roles for this catalog
        try:
            catalog_schema = await catalog_module.resolve_physical_schema(
                cat_id, db_resource=catalog_module.engine
            )
            catalog_roles = await storage.get_identity_roles(
                provider, subject_id, schema=catalog_schema
            )
            if catalog_roles:
                result.append(
                    CatalogAccessResponse(catalog_id=cat_id, roles=catalog_roles)
                )
        except Exception:
            continue

    return result


@router.get(
    "/users/{email}/catalogs/{catalog_id}",
    response_model=EffectiveAuthorizationResponse,
)
async def get_user_effective_authorization(email: EmailStr, catalog_id: str):
    """Get effective authorization for a user in a specific catalog (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    async with http_errors({ValueError: 404}):
        provider, subject_id = await storage.resolve_identity(email)

    # Get global and catalog roles
    global_roles = await storage.get_identity_roles(
        provider, subject_id, schema="apikey"
    )
    catalog_roles = await storage.get_identity_roles(
        provider, subject_id, schema=catalog_schema
    )

    # Get authorization metadata
    auth = await storage.get_identity_authorization(
        provider, subject_id, schema=catalog_schema
    )
    if not auth:
        auth = await storage.get_identity_authorization(
            provider, subject_id, schema="apikey"
        )

    return EffectiveAuthorizationResponse(
        email=email,
        provider=provider,
        subject_id=subject_id,
        global_roles=global_roles,
        catalog_roles=catalog_roles,
        effective_roles=global_roles + catalog_roles,
        is_active=auth.get("is_active", True) if auth else True,
        valid_until=auth.get("valid_until") if auth else None,
    )


@router.get("/catalogs/{catalog_id}/users", response_model=List[Dict[str, Any]])
async def get_catalog_users(catalog_id: str):
    """Get all users with access to a catalog (admin only)."""
    storage = await get_storage()
    catalog_schema = await resolve_catalog_schema(catalog_id)

    users = await storage.get_catalog_users(schema=catalog_schema)
    return users
