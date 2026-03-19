#    Copyright 2025 FAO
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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from dynastore.extensions import ExtensionProtocol
from dynastore.modules import get_protocol
from dynastore.models.protocols import ApiKeyProtocol, WebModuleProtocol
from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role, Principal
from dynastore.models.auth_models import ApiKeyCreate

from .models import (
    UserCreate, UserUpdate, UserResponse,
    RoleCreate, RoleUpdate, RoleResponse,
    PolicyCreate, PolicyUpdate, PolicyResponse,
    PrincipalResponse, AssignRoleRequest, CatalogRoleAssignment,
)
from .policies import register_admin_policies
from dynastore.extensions.web.decorators import expose_web_page

logger = logging.getLogger(__name__)

# Default role names that can only be modified/deleted by sysadmin
DEFAULT_ROLE_NAMES = frozenset({"sysadmin", "admin", "anonymous", "user"})


# --- Auth dependency ---

def _require_admin(request: Request):
    """Dependency: checks that the caller has sysadmin or admin role."""
    principal: Optional[Principal] = getattr(request.state, "principal", None)
    if not principal:
        raise HTTPException(status_code=401, detail="Authentication required.")
    admin_roles = {"sysadmin", "admin"}
    if not admin_roles.intersection(set(principal.roles or [])):
        raise HTTPException(status_code=403, detail="Admin role required.")
    return principal


def _get_apikey_manager():
    """Dependency: returns the ApiKeyService from the protocol registry."""
    mgr = get_protocol(ApiKeyProtocol)
    if not mgr or not hasattr(mgr, "_apikey_manager"):
        raise HTTPException(status_code=503, detail="Auth service not available.")
    return mgr._apikey_manager
class AdminService(ExtensionProtocol):
    priority: int = 200
    """Admin REST API — user, role, policy, and catalog assignment management."""

    router: APIRouter = APIRouter(tags=["Admin"], prefix="/admin")

    def configure_app(self, app: FastAPI):
        # Include migration admin sub-routers
        from .migration_routes import router as migration_router, schema_router, configs_router
        self.router.include_router(migration_router, prefix="")
        self.router.include_router(schema_router, prefix="")
        self.router.include_router(configs_router, prefix="")

        # Register the admin panel page in the web console sidebar
        web = get_protocol(WebModuleProtocol)
        if web:
            web.scan_and_register_providers(self)
            logger.info("AdminService: Web page registered via WebModuleProtocol.")
        else:
            logger.debug("WebModuleProtocol not available yet; admin page will be registered at Web lifespan scan.")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        register_admin_policies()
        logger.info("AdminService: Policies registered.")
        yield

    # -------------------------------------------------------------------------
    # User Management (/admin/users)
    # -------------------------------------------------------------------------

    @router.get("/users", summary="List local users")
    async def list_users(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        principals = await mgr.list_principals(limit=limit, offset=offset)
        # Filter to local (system) principals only
        local = [p for p in principals if p.provider in ("local", "system", None)]
        return [
            PrincipalResponse(
                id=str(p.id),
                provider=p.provider,
                subject_id=p.subject_id,
                display_name=p.display_name,
                roles=p.roles,
                is_active=p.is_active,
            )
            for p in local
        ]

    @router.post("/users", summary="Create a new local user", status_code=201)
    async def create_user(
        body: UserCreate,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        # Resolve the local identity provider
        providers = mgr.get_identity_providers()
        local_provider = next(
            (p for p in providers if getattr(p, "get_provider_id", lambda: None)() == "local"), None
        )

        # Create the user in the local users table first to obtain the authoritative UUID.
        # local_provider.create_user hashes the password internally.
        if local_provider and hasattr(local_provider, "create_user"):
            user_uuid = await local_provider.create_user(
                username=body.username,
                password=body.password,
                email=body.email,
            )
            subject_id = str(user_uuid)
        else:
            # Fallback: no local provider — use username as subject_id
            subject_id = body.username

        # Build the principal using the UUID as subject_id so that JWT sub claims
        # (which also encode the UUID) resolve correctly via get_effective_permissions.
        new_principal = Principal(
            provider="local",
            subject_id=subject_id,
            display_name=body.username,
            roles=body.roles or ["user"],
            is_active=True,
        )
        created = await mgr.create_principal(new_principal)

        return PrincipalResponse(
            id=str(created.id),
            provider=created.provider,
            subject_id=created.subject_id,
            display_name=created.display_name,
            roles=created.roles,
            is_active=created.is_active,
        )

    @router.get("/users/{principal_id}", summary="Get user details")
    async def get_user(
        principal_id: UUID,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="User not found.")
        return PrincipalResponse(
            id=str(p.id), provider=p.provider, subject_id=p.subject_id,
            display_name=p.display_name, roles=p.roles, is_active=p.is_active,
        )

    @router.put("/users/{principal_id}", summary="Update user")
    async def update_user(
        principal_id: UUID,
        body: UserUpdate,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="User not found.")
        if body.is_active is not None:
            p.is_active = body.is_active
        if body.roles is not None:
            p.roles = body.roles
        updated = await mgr.update_principal(p)
        return PrincipalResponse(
            id=str(updated.id), provider=updated.provider, subject_id=updated.subject_id,
            display_name=updated.display_name, roles=updated.roles, is_active=updated.is_active,
        )

    @router.delete("/users/{principal_id}", status_code=204, summary="Delete user")
    async def delete_user(
        principal_id: UUID,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        deleted = await mgr.delete_principal(principal_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="User not found.")

    # -------------------------------------------------------------------------
    # Principal Management (/admin/principals)
    # -------------------------------------------------------------------------

    @router.get("/principals", summary="Search principals (all providers)")
    async def search_principals(
        identifier: Optional[str] = Query(None),
        role: Optional[str] = Query(None),
        catalog_id: Optional[str] = Query(None),
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        results = await mgr.search_principals(
            identifier=identifier, role=role, limit=limit, offset=offset, catalog_id=catalog_id
        )
        return [
            PrincipalResponse(
                id=str(p.id), provider=p.provider, subject_id=p.subject_id,
                display_name=p.display_name, roles=p.roles, is_active=p.is_active,
            )
            for p in results
        ]

    @router.post("/principals/{principal_id}/roles", summary="Assign global role to principal", status_code=204)
    async def assign_global_role(
        principal_id: UUID,
        body: AssignRoleRequest,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        if body.role not in p.roles:
            p.roles = list(p.roles) + [body.role]
        await mgr.update_principal(p)

    @router.delete("/principals/{principal_id}/roles/{role_name}", status_code=204, summary="Remove global role")
    async def remove_global_role(
        principal_id: UUID,
        role_name: str,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        p.roles = [r for r in (p.roles or []) if r != role_name]
        await mgr.update_principal(p)

    @router.post("/principals/{principal_id}/catalogs/{catalog_id}/roles", status_code=204, summary="Assign catalog-scoped role")
    async def assign_catalog_role(
        principal_id: UUID,
        catalog_id: str,
        body: AssignRoleRequest,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        if not p.provider or not p.subject_id:
            raise HTTPException(status_code=400, detail="Principal has no identity provider link.")
        try:
            await mgr.storage.grant_roles(
                provider=p.provider,
                subject_id=p.subject_id,
                roles=[body.role],
                schema=await mgr.resolve_schema(catalog_id),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete("/principals/{principal_id}/catalogs/{catalog_id}/roles/{role_name}", status_code=204)
    async def remove_catalog_role(
        principal_id: UUID,
        catalog_id: str,
        role_name: str,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        p = await mgr.get_principal(principal_id)
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")
        if not p.provider or not p.subject_id:
            raise HTTPException(status_code=400, detail="Principal has no identity provider link.")
        try:
            await mgr.storage.revoke_roles(
                provider=p.provider,
                subject_id=p.subject_id,
                roles=[role_name],
                schema=await mgr.resolve_schema(catalog_id),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # -------------------------------------------------------------------------
    # Role Management (/admin/roles)
    # -------------------------------------------------------------------------

    @router.get("/roles", summary="List all roles")
    async def list_roles(
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        roles = await mgr.list_roles(catalog_id=catalog_id)
        return [
            RoleResponse(
                name=r.name,
                description=r.description,
                policies=r.policies or [],
                parent_roles=r.parent_roles or [],
            )
            for r in roles
        ]

    @router.post("/roles", summary="Create a new role", status_code=201)
    async def create_role(
        body: RoleCreate,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        role = Role(
            name=body.name,
            description=body.description,
            policies=body.policies,
            parent_roles=body.parent_roles,
        )
        created = await mgr.create_role(role, catalog_id=catalog_id)
        return RoleResponse(
            name=created.name, description=created.description,
            policies=created.policies or [], parent_roles=created.parent_roles or [],
        )

    @router.put("/roles/{role_name}", summary="Update a role")
    async def update_role(
        role_name: str,
        body: RoleUpdate,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        roles = await mgr.list_roles(catalog_id=catalog_id)
        existing = next((r for r in roles if r.name == role_name), None)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
        if role_name in DEFAULT_ROLE_NAMES and "sysadmin" not in (principal.roles or []):
            raise HTTPException(status_code=403, detail="Only sysadmin can modify default roles.")
        if body.description is not None:
            existing.description = body.description
        if body.policies is not None:
            existing.policies = body.policies
        if body.parent_roles is not None:
            existing.parent_roles = body.parent_roles
        updated = await mgr.update_role(existing, catalog_id=catalog_id)
        return RoleResponse(
            name=updated.name, description=updated.description,
            policies=updated.policies or [], parent_roles=updated.parent_roles or [],
        )

    @router.delete("/roles/{role_name}", status_code=204, summary="Delete a role")
    async def delete_role(
        role_name: str,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        roles = await mgr.list_roles(catalog_id=catalog_id)
        existing = next((r for r in roles if r.name == role_name), None)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Role '{role_name}' not found.")
        if role_name in DEFAULT_ROLE_NAMES and "sysadmin" not in (principal.roles or []):
            raise HTTPException(status_code=403, detail="Only sysadmin can delete default roles.")
        await mgr.delete_role(role_name, catalog_id=catalog_id)

    # -------------------------------------------------------------------------
    # Policy Management (/admin/policies)
    # -------------------------------------------------------------------------

    @router.get("/policies", summary="List all policies")
    async def list_policies(
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        policies = await pm.list_policies(catalog_id=catalog_id)
        return [
            PolicyResponse(
                id=p.id, description=p.description, actions=p.actions,
                resources=p.resources, effect=p.effect, partition_key=p.partition_key,
            )
            for p in policies
        ]

    @router.post("/policies", summary="Create a new policy", status_code=201)
    async def create_policy(
        body: PolicyCreate,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        policy = Policy(
            id=body.id,
            description=body.description,
            actions=body.actions,
            resources=body.resources,
            effect=body.effect,
        )
        try:
            created = await pm.create_policy(policy, catalog_id=catalog_id)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return PolicyResponse(
            id=created.id, description=created.description, actions=created.actions,
            resources=created.resources, effect=created.effect, partition_key=created.partition_key,
        )

    @router.put("/policies/{policy_id}", summary="Update a policy")
    async def update_policy(
        policy_id: str,
        body: PolicyUpdate,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        existing = await pm.get_policy(policy_id, catalog_id=catalog_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found.")
        if body.description is not None:
            existing.description = body.description
        if body.actions is not None:
            existing.actions = body.actions
        if body.resources is not None:
            existing.resources = body.resources
        if body.effect is not None:
            existing.effect = body.effect
        updated = await pm.update_policy(existing, catalog_id=catalog_id)
        return PolicyResponse(
            id=updated.id, description=updated.description, actions=updated.actions,
            resources=updated.resources, effect=updated.effect, partition_key=updated.partition_key,
        )

    @router.delete("/policies/{policy_id}", status_code=204, summary="Delete a policy")
    async def delete_policy(
        policy_id: str,
        catalog_id: Optional[str] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        deleted = await pm.delete_policy(policy_id, catalog_id=catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found.")

    # -------------------------------------------------------------------------
    # System Defaults (/admin/reset-defaults)
    # -------------------------------------------------------------------------

    @router.post("/reset-defaults", summary="Reset default policies and roles")
    async def reset_defaults(
        catalog_id: Optional[str] = Query(None, description="Catalog ID for tenant-scoped reset, or None for global"),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        if "sysadmin" not in (principal.roles or []):
            raise HTTPException(status_code=403, detail="Only sysadmin can reset defaults.")
        pm = mgr.get_policy_service()
        if not pm:
            raise HTTPException(status_code=503, detail="Policy manager not available.")
        await pm.provision_default_policies(catalog_id=catalog_id, force=True)
        return {"message": "Default policies and roles have been reset.", "catalog_id": catalog_id or "global"}

    # -------------------------------------------------------------------------
    # API Key Management (/admin/apikeys)
    # -------------------------------------------------------------------------

    @router.get("/apikeys", summary="List all API Keys")
    async def list_apikeys(
        principal_id: Optional[UUID] = Query(None),
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        keys = await mgr.list_api_keys(principal_id=principal_id)
        return [
            {
                "key_hash": k.key_hash,
                "key_prefix": k.key_prefix,
                "principal_id": k.principal_id,
                "name": k.name,
                "is_active": k.is_active,
                "expires_at": k.expires_at,
                "max_usage": k.max_usage
            }
            for k in keys
        ]

    @router.post("/apikeys", summary="Create an API Key", status_code=201)
    async def create_apikey(
        body: ApiKeyCreate,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        if not body.principal_id and not body.principal_identifier:
            raise HTTPException(status_code=400, detail="Principal ID or identifier is required.")
        
        # Verify principal exists
        p = None
        if body.principal_id:
            p = await mgr.get_principal(body.principal_id)
        elif body.principal_identifier:
             p = await mgr.get_principal(identifier=body.principal_identifier)
             
        if not p:
            raise HTTPException(status_code=404, detail="Principal not found.")

        # Ensure we bind by ID explicitly
        body.principal_id = p.id
        
        try:
            apikey_str, apikey_obj = await mgr.create_api_key(body)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
            
        return {
            "api_key": apikey_str,
            "key_hash": apikey_obj.key_hash,
            "key_prefix": apikey_obj.key_prefix,
            "principal_id": apikey_obj.principal_id,
            "name": apikey_obj.name,
        }

    @router.delete("/apikeys/{key_hash}", status_code=204, summary="Delete an API Key")
    async def delete_apikey(
        key_hash: str,
        principal: Principal = Depends(_require_admin),
        mgr=Depends(_get_apikey_manager),
    ):
        deleted = await mgr.delete_api_key(key_hash)
        if not deleted:
            raise HTTPException(status_code=404, detail="API Key not found.")

    # -------------------------------------------------------------------------
    # Migrations Dashboard (/web/pages/migrations_panel)
    # -------------------------------------------------------------------------

    @expose_web_page(
        page_id="migrations_panel",
        title="Database Migrations",
        icon="fa-database",
        description="Manage structural database migrations, view history, and monitor schema health.",
        required_roles=["sysadmin", "admin"],
        section="admin",
        priority=10,
    )
    def provide_migrations_panel(self, request: Request):
        import os
        html_path = os.path.join(os.path.dirname(__file__), "static", "migrations_panel.html")
        with open(html_path, "r") as f:
            return HTMLResponse(f.read())

