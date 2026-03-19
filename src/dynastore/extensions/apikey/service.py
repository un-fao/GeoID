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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/extensions/apikey/service.py

import logging
import uuid
import re
import secrets
from types import SimpleNamespace
from pydantic import BaseModel, Field, model_validator
from contextlib import asynccontextmanager
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Request,
    FastAPI,
    Security,
    Query,
)
from fastapi.openapi.utils import get_openapi
import hashlib
from typing import List, Optional, Any, Dict, Literal
from datetime import datetime
from uuid import UUID
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials

import os
from fastapi.responses import HTMLResponse
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_web_page
from dynastore.models.protocols import ApiKeyProtocol, WebModuleProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.tools import normalize_db_url
from dynastore.modules.db_config.query_executor import DbResource

from dynastore.modules.apikey.models import (
    Principal,
    Role,
    Policy,
    ApiKey,
    ApiKeyCreate,
    ApiKeyPolicy,
    ApiKeyValidationRequest,
    TokenExchangeRequest,
    TokenResponse,
    RefreshToken,
    ApiKeyStatus,
    ApiKeyStatusFilter,
    Condition,
)
from dynastore.modules.stats.storage import (
    get_access_logs,
    get_stats_summary,
    AccessLogPage,
    StatsSummary,
)
from dynastore.models.protocols import ApiKeyProtocol
from dynastore.modules import get_protocol
from dynastore.modules.apikey.policies import PolicyService
from dynastore.modules.apikey.exceptions import (
    ConflictingResourceError,
    PrincipalNotFoundError,
)
from dynastore.extensions.apikey.middleware import ApiKeyMiddleware
logger = logging.getLogger(__name__)


def register_apikey_service_policies():
    """Register policies for authorization API endpoints via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.tools.discovery import get_protocol as _get_protocol

    pm = _get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; apikey service policies not registered.")
        return

    pm.register_policy(
        Policy(
            id="admin_authorization_api",
            description="Allows admin users to manage user roles and permissions",
            actions=["GET", "POST", "PUT", "DELETE", "PATCH"],
            resources=["/admin/users/.*", "/admin/roles/.*", "/admin/policies/.*"],
            effect="ALLOW",
            partition_key="global",
        )
    )

    pm.register_policy(
        Policy(
            id="self_service_authorization_api",
            description="Allows authenticated users to view their own roles and catalog access",
            actions=["GET"],
            resources=["/me/roles/.*", "/me/catalogs.*"],
            effect="ALLOW",
            partition_key="global",
        )
    )

    pm.register_role(Role(name="admin", policies=["admin_authorization_api"]))
    pm.register_role(Role(name="sysadmin", policies=["admin_authorization_api"]))
    pm.register_role(Role(name="user", policies=["self_service_authorization_api"]))

    logger.debug("ApiKey service policies registered via PermissionProtocol.")


# Security Schemes
http_bearer = HTTPBearer(auto_error=False, scheme_name="HTTPBearer")

# --- DTOs (Data Transfer Objects) ---


class TokenCreateRequest(BaseModel):
    """Input model for token generation. Hides api_key string."""

    ttl_seconds: int = Field(default=3600, ge=60, le=2592000)  # Max 30 days
    scoped_policy: Optional[ApiKeyPolicy] = None


class LoginRequest(BaseModel):
    """Standardized login request."""

    api_key: str
    ttl_seconds: int = Field(default=3600, ge=60, le=2592000)


class TokenRefreshRequest(BaseModel):
    """Standardized refresh request."""

    refresh_token: str
    ttl_seconds: int = Field(default=3600, ge=60, le=2592000)


class PolicyCreateRequest(BaseModel):
    """
    Input model for Policy Creation.
    """

    id: str = Field(..., description="Unique slug for the policy.")
    description: Optional[str] = Field(
        None, description="Optional description of the policy."
    )

    actions: List[str] = Field(
        ..., description="List of allowed actions, e.g., ['READ', 'LIST', 'STAC:GET']"
    )
    resources: List[str] = Field(
        default=["*"],
        description="Regex list for resource targeting, e.g., ['catalogs/A/collections/*']",
    )
    effect: Literal["ALLOW", "DENY"] = "ALLOW"

    conditions: Optional[List[Condition]] = None
    partition_key: str = "global"


class PolicyUpdateRequest(BaseModel):
    """Input model for Policy Update."""

    description: Optional[str] = None
    actions: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    effect: Optional[Literal["ALLOW", "DENY"]] = None
    conditions: Optional[List[Condition]] = None


class PrincipalCreateRequest(BaseModel):
    """Input model for Principal Creation."""

    provider: str = Field(
        "local", description="Identity provider (e.g. 'local', 'keycloak')."
    )
    subject_id: str = Field(..., description="Unique subject ID from the provider.")
    roles: List[str] = Field(
        default_factory=list, description="Roles assigned to this principal."
    )
    attributes: Dict[str, Any] = Field(default_factory=dict)
    policy: Optional[ApiKeyPolicy] = None


class PrincipalUpdateRequest(BaseModel):
    """Input model for Principal Update."""

    roles: Optional[List[str]] = None
    attributes: Optional[Dict[str, Any]] = None
    policy: Optional[ApiKeyPolicy] = None


# --- Robust Security Dependencies ---


async def require_admin_privileges(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
):
    """
    Guards Administrative Endpoints (The /apikey/admin/* scope).
    Allows access if:
    1. Valid Bearer Token belongs to SYSADMIN or ADMIN role.
    2. OR, Valid Bearer Token is the System Admin Key.
    """
    # The middleware has already validated the token and set the principal/role.
    if getattr(request.state, "policy_allowed", False):
        return  # Access Granted via Policy Engine decision

    principal_role = getattr(request.state, "principal_role", None)

    # Check for list of roles if generalized
    roles = (
        getattr(request.state, "principal", SimpleNamespace(roles=[])).roles
        if hasattr(request.state, "principal") and request.state.principal
        else []
    )
    if not roles and principal_role:
        roles = (
            [principal_role]
            if isinstance(principal_role, str)
            else [principal_role.value]
        )

    if any(r in ["sysadmin", "admin"] for r in roles):
        return  # Access Granted as Admin User

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Administrative privileges required (Admin Role).",
    )


async def require_sysadmin_privileges(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
):
    """
    Guards System Bootstrap Endpoints (The /apikey/sysadmin/* scope).
    Strictly requires a Principal explicitly marked as SYSADMIN or the System Key.
    """
    # Sysadmin Role Check (from Middleware)
    principal_role = getattr(request.state, "principal_role", None)

    roles = (
        getattr(request.state, "principal", SimpleNamespace(roles=[])).roles
        if hasattr(request.state, "principal") and request.state.principal
        else []
    )
    if not roles and principal_role:
        roles = (
            [principal_role]
            if isinstance(principal_role, str)
            else [principal_role.value]
        )

    if "sysadmin" in roles:
        return

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="System Administrator privileges required.",
    )

from dynastore.extensions.registry import get_extension_instance

async def get_apikey_principal(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Principal:
    """Standard dependency for user-facing endpoints."""
    # 1. Check if Middleware already authenticated a User Principal
    if hasattr(request.state, "principal") and request.state.principal:
        return request.state.principal

    # 2. Check if Middleware authenticated a System Admin
    principal_role = getattr(request.state, "principal_role", None)
    if principal_role == "sysadmin":
        return Principal(
            id=UUID("00000000-0000-0000-0000-000000000000"),
            display_name="sysadmin",
            subject_id="sysadmin",
            attributes={
                "role": "sysadmin",
                "description": "System Administrator (Synthetic)",
            },
        )

    # Fallback
    key = bearer.credentials if bearer else None
    if not key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing credentials."
        )

    ext = get_extension_instance("apikey")
    if not ext or not hasattr(ext, "apikey_manager"):
        raise RuntimeError("ApiKeyExtension or its manager not initialized.")

    principal, _, _ = await ext.apikey_manager.authenticate_apikey(key)
    if not principal:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials."
        )
    return principal


async def get_apikey_principal_optional(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Optional[Principal]:
    """
    Standard dependency for optional authentication.
    Returns Principal if authenticated, None otherwise (instead of 401).
    """
    # 1. Check if Middleware already authenticated a User Principal
    if hasattr(request.state, "principal") and request.state.principal:
        return request.state.principal

    # 2. Check if Middleware authenticated a System Admin
    principal_role = getattr(request.state, "principal_role", None)
    if principal_role == "sysadmin":
        return Principal(
            id=UUID("00000000-0000-0000-0000-000000000000"),
            display_name="sysadmin",
            subject_id="sysadmin",
            attributes={
                "role": "sysadmin",
                "description": "System Administrator (Synthetic)",
            },
        )

    # Fallback (Manual Check)
    key = bearer.credentials if bearer else None
    if key:
        try:
            # We need the extension instance here. This is slightly tricky for functions outside of classes.
            # But the middleware usually handles this. If needed, we can get it from app.state.
            from dynastore.extensions.registry import get_extension_instance
            ext = get_extension_instance("apikey")
            if ext and hasattr(ext, "apikey_manager"):
                principal, _, _ = await ext.apikey_manager.authenticate_apikey(key)
                return principal
        except Exception:
            pass

    return None


# --- Helper: Privilege Escalation Check ---


def ensure_sysadmin_if_targeting_admin(request: Request, target_role: str):
    """
    Business Logic Rule: Only a SYSADMIN can manage (create/delete) an ADMIN or SYSADMIN.
    Admins can only manage regular users.
    """
    if target_role in ["sysadmin", "admin"]:
        # Check current caller
        caller_role = getattr(request.state, "principal_role", [])
        if isinstance(caller_role, str):
            caller_role = [caller_role]

        if "sysadmin" in caller_role:
            return  # Allowed

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges: Only System Administrators can manage Admin accounts.",
        )
class ApiKeyExtension(ExtensionProtocol):
    priority: int = 100
    # Base router for high-level categorization
    router: APIRouter = APIRouter(
        prefix="/apikey", tags=["Identity & Access Governance"]
    )

    # Standardized Auth Endpoints (OIDC/OAuth2 compatible)
    auth_router: APIRouter = APIRouter(prefix="/auth", tags=["Authentication"])

    # Governance Endpoints (Principals, Roles, Policies)
    gov_router: APIRouter = APIRouter(prefix="/governance", tags=["IAM Governance"])

    # Credential Endpoints (API Keys, Machines)
    cred_router: APIRouter = APIRouter(prefix="/credentials", tags=["Credentials"])

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._apikey_manager: Optional[ApiKeyProtocol] = None
        self._policy_service: Optional[PolicyService] = None
        self._engine: Optional[DbResource] = None

        self._register_routes()

        # Include divided routers into the main router
        self.router.include_router(self.auth_router)
        self.router.include_router(self.gov_router)
        self.router.include_router(self.cred_router)

        # Include new simplified IAG routers
        from dynastore.extensions.apikey.authorization_api import (
            router as admin_router,
            me_router,
        )
        self.router.include_router(admin_router)
        self.router.include_router(me_router)

    def _register_routes(self):
        # Public / Auth
        self.router.add_api_route(
            "/token", self.create_token_for_client, methods=["POST"], response_model=TokenResponse
        )
        self.auth_router.add_api_route(
            "/login", self.login, methods=["POST"], response_model=TokenResponse
        )
        self.auth_router.add_api_route(
            "/token/refresh", self.refresh_token, methods=["POST"], response_model=TokenResponse
        )
        self.auth_router.add_api_route(
            "/jwks.json", self.get_jwks, methods=["GET"]
        )
        self.router.add_api_route(
            "/usage", self.get_my_usage_and_quotas, methods=["GET"], response_model=Dict[str, Any]
        )

        # Governance
        self.gov_router.add_api_route(
            "/policies", self.create_access_policy, methods=["POST"], response_model=Policy, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/policies/{policy_id}", self.update_access_policy, methods=["PUT"], response_model=Policy, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/policies", self.search_access_policies, methods=["GET"], response_model=List[Policy], dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/policies/{policy_id}", self.delete_access_policy, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/roles", self.list_roles, methods=["GET"], response_model=List[Role], dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/roles", self.create_role, methods=["POST"], response_model=Role, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/roles/{name}", self.update_role, methods=["PUT"], response_model=Role, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/roles/{name}", self.delete_role, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/hierarchies", self.add_role_hierarchy, methods=["POST"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/hierarchies", self.remove_role_hierarchy, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/hierarchies/{role_name}", self.get_role_hierarchy, methods=["GET"], response_model=List[str], dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/principals", self.create_principal, methods=["POST"], response_model=Principal, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/principals/{principal_id}", self.update_principal, methods=["PUT"], response_model=Principal, dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/principals", self.search_principals, methods=["GET"], response_model=List[Principal], dependencies=[Depends(require_admin_privileges)]
        )
        self.gov_router.add_api_route(
            "/principals/{principal_id}", self.delete_principal, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )

        # Credentials
        self.cred_router.add_api_route(
            "/validate", self.validate_key, methods=["POST"], response_model=ApiKeyStatus, dependencies=[Depends(require_admin_privileges)]
        )
        self.cred_router.add_api_route(
            "/keys", self.create_key, methods=["POST"], dependencies=[Depends(require_admin_privileges)]
        )
        self.cred_router.add_api_route(
            "/keys", self.search_keys, methods=["GET"], response_model=List[ApiKey], dependencies=[Depends(require_admin_privileges)]
        )
        self.cred_router.add_api_route(
            "/keys/{key_hash}/invalidate", self.invalidate_key, methods=["POST"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )
        self.cred_router.add_api_route(
            "/keys/{key_hash}", self.delete_key, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin_privileges)]
        )

        # Stats
        self.cred_router.add_api_route(
            "/stats/summary", self.get_system_stats_summary, methods=["GET"], response_model=StatsSummary, dependencies=[Depends(require_admin_privileges)]
        )
        self.cred_router.add_api_route(
            "/stats/logs", self.get_system_access_logs, methods=["GET"], response_model=AccessLogPage, dependencies=[Depends(require_admin_privileges)]
        )

        # Sysadmin
        self.cred_router.add_api_route(
            "/sysadmin/keys/{key_hash}/regenerate", self.regenerate_key, methods=["PUT"], dependencies=[Depends(require_sysadmin_privileges)]
        )
        self.cred_router.add_api_route(
            "/sysadmin/properties/system_admin_key", self.get_system_admin_key_property, methods=["GET"], response_model=dict, dependencies=[Depends(require_sysadmin_privileges)]
        )
        self.cred_router.add_api_route(
            "/sysadmin/properties/system_admin_key", self.regenerate_system_admin_key_property, methods=["PUT"], response_model=dict, dependencies=[Depends(require_sysadmin_privileges)]
        )

    def configure_app(self, app: FastAPI):
        app.add_middleware(ApiKeyMiddleware)

        def custom_openapi():
            if app.openapi_schema:
                return app.openapi_schema

            openapi_schema = get_openapi(
                title=app.title,
                version=app.version,
                description=app.description,
                routes=app.routes,
                servers=app.servers,
            )

            if "components" not in openapi_schema:
                openapi_schema["components"] = {}
            if "securitySchemes" not in openapi_schema["components"]:
                openapi_schema["components"]["securitySchemes"] = {}

            # Define Schemes
            openapi_schema["components"]["securitySchemes"]["HTTPBearer"] = {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
                "description": "Admin/User Token or API Key",
            }

            app.openapi_schema = openapi_schema
            return app.openapi_schema

        app.openapi = custom_openapi

        # Register admin panel web page
        web = get_protocol(WebModuleProtocol)
        if web:
            web.scan_and_register_providers(self)

        # Sub-routers are mounted to self.router in __init__
        # The central extension loader will handle app.include_router(self.router)

    @expose_web_page(
        page_id="admin",
        title="Admin",
        icon="fa-shield-halved",
        description="Administration and platform management.",
        required_roles=["sysadmin", "admin"],
        priority=10,
    )
    async def provide_admin_hub(self, request: Request):
        """Admin landing hub — lists accessible admin sub-pages as cards."""
        return HTMLResponse(content="""
<div class="space-y-6">
  <div>
    <h2 class="text-2xl font-bold text-white mb-1">Administration</h2>
    <p class="text-slate-400 text-sm">Manage the platform resources available to you.</p>
  </div>
  <div id="admin-hub-cards" class="grid md:grid-cols-2 gap-5">
    <div class="text-slate-600 text-sm py-4"><i class="fa-solid fa-spinner fa-spin mr-2"></i>Loading...</div>
  </div>
</div>
<script>
(function() {
  const iconColors = {
    'fa-users-gear': 'text-blue-400 bg-blue-500/10 border-blue-500/20',
    'fa-sliders':    'text-purple-400 bg-purple-500/10 border-purple-500/20',
  };
  const grid = document.getElementById('admin-hub-cards');
  if (!grid) return;

  const tkey = (typeof TOKEN_KEY !== 'undefined') ? TOKEN_KEY : 'ds_token';
  const token = (typeof authToken !== 'undefined' && authToken) || localStorage.getItem(tkey) || sessionStorage.getItem(tkey);
  const headers = token ? { 'Authorization': 'Bearer ' + token } : {};

  fetch('/web/config/pages', { headers })
    .then(r => r.json())
    .then(pages => {
      const subPages = pages.filter(p => p.section === 'admin');
      grid.innerHTML = '';
      if (!subPages.length) {
        grid.innerHTML = '<p class="text-slate-500 text-sm">No admin tools available.</p>';
        return;
      }
      subPages.forEach(p => {
        const colorClass = iconColors[p.icon] || 'text-slate-400 bg-slate-500/10 border-slate-500/20';
        const btn = document.createElement('button');
        btn.onclick = () => switchTab(p.id);
        btn.className = 'glass-panel text-left p-6 rounded-2xl border border-white/5 hover:border-white/10 transition-all group';
        btn.innerHTML = `
          <div class="w-12 h-12 rounded-xl ${colorClass} border flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
            <i class="fa-solid ${p.icon} text-xl"></i>
          </div>
          <h3 class="font-semibold text-white mb-1">${p.title}</h3>
          <p class="text-slate-400 text-sm">${p.description || ''}</p>`;
        grid.appendChild(btn);
      });
    })
    .catch(() => { grid.innerHTML = '<p class="text-red-400 text-sm">Failed to load admin tools.</p>'; });
})();
</script>
""")

    @expose_web_page(
        page_id="admin_panel",
        title="Admin Panel",
        icon="fa-users-gear",
        description="Manage users, roles, policies and catalog permissions.",
        required_roles=["sysadmin", "admin"],
        section="admin",
        priority=20,
    )
    async def provide_admin_panel(self, request: Request):
        file_path = os.path.join(os.path.dirname(__file__), "..", "admin", "static", "admin_panel.html")
        with open(os.path.normpath(file_path), "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("ApiKeyExtension: Initializing lifecycle.")
        
        # Initialize instance attributes
        self._apikey_manager = await self._get_apikey_manager()
        self._policy_service = self._apikey_manager.get_policy_service()
        
        from dynastore.models.protocols import DatabaseProtocol
        db_protocol = get_protocol(DatabaseProtocol)
        if not db_protocol:
            raise RuntimeError("DatabaseProtocol implementation not found.")
        self._engine = db_protocol.engine

        # Seed default policies (idempotent)
        try:
            # Seed both global (apikey) and system (catalog) schemas
            await self._policy_service.provision_default_policies(catalog_id=None)
            await self._policy_service.provision_default_policies(catalog_id="_system_")
        except Exception as e:
            logger.error(f"Failed to seed default policies: {e}")

        # Register apikey service policies via protocol
        register_apikey_service_policies()

        yield

    async def _get_apikey_manager(self) -> ApiKeyProtocol:
        if self._apikey_manager is None:
            svc = get_protocol(ApiKeyProtocol)
            if not svc:
                raise RuntimeError("ApiKey protocol implementation not found")
            self._apikey_manager = svc
        return self._apikey_manager

    @property
    def apikey_manager(self) -> ApiKeyProtocol:
        if self._apikey_manager is None:
             # This might happen if accessed before lifespan, try to resolve
             import asyncio
             try:
                 # We can't use await in property, but we can return the protocol if available
                 return get_protocol(ApiKeyProtocol)
             except:
                 pass
        return self._apikey_manager

    @property
    def policy_service(self) -> PolicyService:
        return self._policy_service

    # ==========================================
    # 1. PUBLIC / SELF-SERVICE (Any Auth User)
    # ==========================================

    async def create_token_for_client(
        self,
        token_req: TokenCreateRequest,
        request: Request,
        _: Principal = Depends(get_apikey_principal),
    ):
        """Exchange the current API Key for a short-lived Bearer Token."""
        api_key_hash = getattr(request.state, "api_key_hash", None)
        catalog_id = getattr(request.state, "catalog_id", None)
        if not api_key_hash:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Authentication via API Key required.",
            )

        try:
            return await self.apikey_manager.exchange_token(
                api_key_hash=api_key_hash,
                ttl_seconds=token_req.ttl_seconds,
                scoped_policy=token_req.scoped_policy,
                catalog_id=catalog_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    # --- New IAG Standardized Auth Endpoints ---

    async def login(self, request: Request, login_req: LoginRequest):
        """Standardized login endpoint using API Key."""
        catalog_id = getattr(request.state, "catalog_id", None)
        key_hash = hashlib.sha256(login_req.api_key.encode()).hexdigest()

        try:
            return await self.apikey_manager.exchange_token(
                api_key_hash=key_hash,
                ttl_seconds=login_req.ttl_seconds,
                scoped_policy=None,
                catalog_id=catalog_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    async def refresh_token(self, request: Request, refresh_req: TokenRefreshRequest):
        """Standardized refresh token endpoint."""
        catalog_id = getattr(request.state, "catalog_id", None)
        try:
            return await self.apikey_manager.refresh_token_exchange(
                refresh_token=refresh_req.refresh_token,
                ttl_seconds=refresh_req.ttl_seconds,
                catalog_id=catalog_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    async def get_jwks(self):
        """Public endpoint for JWKS discovery."""
        return await self.apikey_manager.get_jwks()

    async def get_my_usage_and_quotas(
        self,
        request: Request, principal: Principal = Depends(get_apikey_principal)
    ):
        """Check quotas and rate limits for the current user."""
        api_key_hash = getattr(request.state, "api_key_hash", None)
        catalog_id = getattr(request.state, "catalog_id", None)

        if not api_key_hash:
            # Try to extract 'kid' from JWT if present
            auth = request.headers.get("Authorization")
            if auth and auth.lower().startswith("bearer "):
                import jwt

                try:
                    token = auth.split(" ")[1]
                    payload = jwt.decode(token, options={"verify_signature": False})
                    api_key_hash = payload.get("kid")
                except:
                    pass

        return await self.apikey_manager.get_usage_status(
            principal, api_key_hash, catalog_id=catalog_id
        )

    # ==========================================
    # 2. ADMIN OPERATIONS (/apikey/admin)
    # Accessible by: Sysadmin, Admin
    # ==========================================

    async def validate_key(self, request: Request, validation_req: ApiKeyValidationRequest):
        """Checks validity of an API key string."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.apikey_manager.validate_key(validation_req, catalog_id=catalog_id)

    # --- Policies ---

    async def create_access_policy(self, request: Request, policy_req: PolicyCreateRequest):
        """Creates a global access policy."""
        catalog_id = getattr(request.state, "catalog_id", None)

        policy_model = Policy(
            id=policy_req.id,
            description=policy_req.description,
            effect=policy_req.effect,
            actions=policy_req.actions,
            resources=policy_req.resources,
            conditions=policy_req.conditions or [],
            partition_key=policy_req.partition_key,
        )
        try:
            return await self.policy_service.create_policy(
                policy_model, catalog_id=catalog_id
            )
        except Exception as e:
            logger.error(f"Policy creation error: {e}")
            if "already exists" in str(e).lower():
                raise HTTPException(
                    status_code=409, detail=f"Policy already exists: {str(e)}"
                )
            raise HTTPException(
                status_code=500, detail=f"Unable to create policy: {str(e)}"
            )

    async def update_access_policy(
        self,
        request: Request, policy_id: str, policy_req: PolicyUpdateRequest
    ):
        """Updates an existing access policy."""
        catalog_id = getattr(request.state, "catalog_id", None)

        # 1. Fetch existing
        existing = await self.policy_manager.get_policy(policy_id, catalog_id=catalog_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Policy not found.")

        # 2. Update fields
        update_data = policy_req.model_dump(exclude_unset=True)
        updated_model = existing.model_copy(update=update_data)

        try:
            result = await self.policy_service.update_policy(
                updated_model, catalog_id=catalog_id
            )
            if not result:
                raise HTTPException(
                    status_code=404, detail="Policy not found during update."
                )
            return result
        except Exception as e:
            logger.error(f"Policy update error: {e}")
            raise HTTPException(status_code=500, detail="Unable to update policy.")

    async def search_access_policies(
        self,
        request: Request,
        resource: Optional[str] = Query(
            None, description="Filter by resource regex pattern"
        ),
        action: Optional[str] = Query(None, description="Filter by action pattern"),
        limit: int = 100,
        offset: int = 0,
    ):
        """Search and list policies."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.policy_service.search_policies(
            resource_pattern=resource,
            action_pattern=action,
            limit=limit,
            offset=offset,
            catalog_id=catalog_id,
        )

    async def delete_access_policy(self, request: Request, policy_id: UUID):
        catalog_id = getattr(request.state, "catalog_id", None)
        deleted = await self.policy_service.delete_policy(policy_id, catalog_id=catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Policy not found")

    # --- Roles & Hierarchies ---

    async def list_roles(self, request: Request):
        """Lists all dynamic roles."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.apikey_manager.list_roles(catalog_id=catalog_id)

    async def create_role(self, role_req: Role, request: Request):
        """Creates a new dynamic role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.apikey_manager.create_role(role_req, catalog_id=catalog_id)

    async def update_role(self, name: str, role_req: Role, request: Request):
        """Updates an existing role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        role_req.name = name  # Ensure name matches path
        return await self.apikey_manager.update_role(role_req, catalog_id=catalog_id)

    async def delete_role(self, name: str, request: Request, cascade: bool = False):
        """Deletes a role with optional cascading removal from principals."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.apikey_manager.delete_role(name, cascade=cascade, catalog_id=catalog_id)

    # --- Hierarchy ---

    async def add_role_hierarchy(self, parent: str, child: str, request: Request):
        """Links two roles in a parent-child inheritance relationship."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.apikey_manager.add_role_hierarchy(parent, child, catalog_id=catalog_id)

    async def remove_role_hierarchy(self, parent: str, child: str, request: Request):
        """Removes a parent-child inheritance relationship."""
        catalog_id = getattr(request.state, "catalog_id", None)
        await self.apikey_manager.remove_role_hierarchy(
            parent, child, catalog_id=catalog_id
        )

    async def get_role_hierarchy(self, role_name: str, request: Request):
        """Gets all effective roles (descendants) for a given role."""
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.apikey_manager.get_role_hierarchy(
            role_name, catalog_id=catalog_id
        )

    # --- Principals ---

    async def create_principal(self, principal_req: PrincipalCreateRequest, request: Request):
        """
        Creates a new Principal.
        SECURITY: Admin cannot create another Admin/Sysadmin.
        """
        catalog_id = getattr(request.state, "catalog_id", None)

        # Check roles for privilege escalation
        for role in principal_req.roles:
            ensure_sysadmin_if_targeting_admin(request, role)

        principal_model = Principal(
            provider=principal_req.provider,
            subject_id=principal_req.subject_id,
            display_name=principal_req.subject_id,
            roles=principal_req.roles,
            attributes=principal_req.attributes,
            custom_policies=principal_req.policy.statements
            if principal_req.policy
            else [],
        )
        try:
            return await self.apikey_manager.create_principal(
                principal_model, catalog_id=catalog_id
            )
        except Exception as e:
            logger.error(f"Principal creation error: {e}")
            if "duplicate key" in str(e).lower():
                raise HTTPException(status_code=409, detail="Principal already exists.")
            raise HTTPException(
                status_code=500, detail=f"Internal server error: {str(e)}"
            )

    async def update_principal(
        self,
        request: Request, principal_id: str, principal_req: PrincipalUpdateRequest
    ):
        """Updates an existing Principal."""
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.apikey_manager._resolve_schema(catalog_id)

        # 1. Fetch existing
        existing = await self.apikey_manager.storage.get_principal(
            principal_id, schema=schema
        )
        if not existing:
            raise HTTPException(status_code=404, detail="Principal not found.")

        # 2. Security Check (Admin cannot elevate/manage Admin/Sysadmin)
        for role in existing.roles:
            ensure_sysadmin_if_targeting_admin(request, role)

        # 3. Update fields
        update_data = principal_req.model_dump(exclude_unset=True)

        # If new roles are provided, check they are safe
        if "roles" in update_data:
            for role in update_data["roles"]:
                ensure_sysadmin_if_targeting_admin(request, role)

        updated_model = existing.model_copy(update=update_data)

        try:
            result = await self.apikey_manager.update_principal(
                updated_model, catalog_id=catalog_id
            )
            if not result:
                raise HTTPException(
                    status_code=404, detail="Principal not found during update."
                )
            return result
        except Exception as e:
            logger.error(f"Principal update error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")

    async def search_principals(
        self, # Added self
        request: Request,
        identifier: Optional[str] = Query(
            None, description="Filter by identifier (partial match)"
        ),
        role: Optional[str] = Query(
            None, description="Filter by metadata 'role' field"
        ),
        limit: int = 100,
        offset: int = 0,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        return await self.apikey_manager.search_principals(
            identifier=identifier,
            role=role,
            limit=limit,
            offset=offset,
            catalog_id=catalog_id,
        )

    async def delete_principal(self, principal_id: UUID, request: Request): # Added self
        """
        Deletes a Principal.
        SECURITY: Admin cannot delete another Admin/Sysadmin.
        """
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.apikey_manager._resolve_schema(catalog_id)
        principal = await self.apikey_manager.storage.get_principal(
            principal_id, schema=schema
        )
        if principal:
            target_role = principal.attributes.get("role")
            if target_role:
                ensure_sysadmin_if_targeting_admin(request, target_role)

        deleted = await self.apikey_manager.delete_principal( # Changed _apikey_manager to self.apikey_manager
            principal_id, catalog_id=catalog_id
        )
        if not deleted:
            raise HTTPException(status_code=404, detail="Principal not found")

    # --- Keys ---

    async def create_key(self, key_req: ApiKeyCreate, request: Request): # Added self
        """
        Issues a new API Key.
        SECURITY: Admin cannot issue keys for another Admin/Sysadmin.
        """
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.apikey_manager._resolve_schema(catalog_id)

        # Resolve Target
        target_principal = None
        if key_req.principal_id:
            logger.info(
                f"Resolving principal by ID: {key_req.principal_id} in schema: {schema}"
            )
            target_principal = await self.apikey_manager.storage.get_principal( # Changed _apikey_manager to self.apikey_manager
                key_req.principal_id, schema=schema
            )
        elif key_req.provider and key_req.subject_id:
            logger.info(
                f"Resolving principal by identity: {key_req.provider}:{key_req.subject_id} in schema: {schema}"
            )
            target_principal = await self.apikey_manager.storage.get_principal_by_identity( # Changed _apikey_manager to self.apikey_manager
                provider=key_req.provider, subject_id=key_req.subject_id, schema=schema
            )
        elif key_req.principal_identifier:
            logger.info(
                f"Resolving principal by identifier: {key_req.principal_identifier} in schema: {schema}"
            )
            target_principal = (
                await self.apikey_manager.storage.get_principal_by_identifier( # Changed _apikey_manager to self.apikey_manager
                    key_req.principal_identifier, schema=schema
                )
            )

        if not target_principal:
            identifier_msg = (
                f"ID: {key_req.principal_id}"
                if key_req.principal_id
                else f"Identity: {key_req.provider}:{key_req.subject_id}"
                if (key_req.provider and key_req.subject_id)
                else f"Identifier: {key_req.principal_identifier}"
            )
            logger.warning(f"Principal NOT found. {identifier_msg}, Schema: {schema}")
            raise HTTPException(
                status_code=404,
                detail=f"Principal not found. {identifier_msg}, Schema: {schema}",
            )

        # Check Privilege
        target_role = target_principal.attributes.get("role")
        if target_role:
            ensure_sysadmin_if_targeting_admin(request, target_role)

        try:
            api_key, raw_key = await self.apikey_manager.create_key( # Changed _apikey_manager to self.apikey_manager
                key_req, catalog_id=catalog_id
            )
            return {"api_key": api_key, "raw_key": raw_key}
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    async def search_keys(
        self, # Added self
        principal_identifier: Optional[str] = None,
        status: ApiKeyStatusFilter = ApiKeyStatusFilter.ALL,
        limit: int = 100,
        offset: int = 0,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        try:
            return await self.apikey_manager.search_keys( # Changed _apikey_manager to self.apikey_manager
                principal_identifier=principal_identifier,
                status_filter=status,
                limit=limit,
                offset=offset,
                catalog_id=catalog_id,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def invalidate_key(self, key_hash: str, request: Request): # Added self
        """
        Invalidates a key (Soft Delete).
        SECURITY: Admin cannot invalidate an Admin/Sysadmin key.
        """
        key_metadata = await self.apikey_manager.storage.get_key_metadata(key_hash)
        if not key_metadata:
            raise HTTPException(status_code=404, detail="Key not found")

        owner = await self.apikey_manager.storage.get_principal(key_metadata.principal_id)
        if owner and owner.metadata.get("role"):
            ensure_sysadmin_if_targeting_admin(request, owner.metadata.get("role"))

        success = await self.apikey_manager.invalidate_key(key_hash, catalog_id=catalog_id)
        if not success:
            raise HTTPException(status_code=404, detail="Key not found")

    async def delete_key(self, key_hash: str, request: Request): # Added self
        """
        Permanently deletes a key.
        SECURITY: Admin cannot delete an Admin/Sysadmin key.
        """
        key_metadata = await self.apikey_manager.storage.get_key_metadata(key_hash)
        if not key_metadata:
            raise HTTPException(status_code=404, detail="Key not found")

        owner = await self.apikey_manager.storage.get_principal(key_metadata.principal_id)
        if owner and owner.metadata.get("role"):
            ensure_sysadmin_if_targeting_admin(request, owner.metadata.get("role"))

        deleted = await self.apikey_manager.delete_api_key(key_hash, catalog_id=catalog_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Key not found")

    # --- Stats ---

    async def get_system_stats_summary(
        self, # Added self
        request: Request,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.apikey_manager._resolve_schema(catalog_id)
        return await get_stats_summary(
            self._engine, # Changed _engine to self._engine
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
            schema=schema,
        )

    async def get_system_access_logs(
        self, # Added self
        request: Request,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        cursor: Optional[str] = None,
        page_size: int = 100,
    ):
        catalog_id = getattr(request.state, "catalog_id", None)
        schema = await self.apikey_manager._resolve_schema(catalog_id)
        return await get_access_logs(
            self._engine, # Changed _engine to self._engine
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
            cursor=cursor,
            page_size=page_size,
            schema=schema,
        )

    # ==========================================
    # 3. SYSADMIN OPERATIONS (/apikey/sysadmin)
    # Accessible by: Sysadmin Only
    # ==========================================

    async def regenerate_key(self, key_hash: str):
        """Emergency key rotation (Bypass standard checks)."""
        try:
            new_key_obj, raw_key = await self.apikey_manager.storage.regenerate_api_key(
                key_hash, conn=self._engine
            )
            if not new_key_obj:
                raise HTTPException(status_code=404, detail="Key not found")
            return {"api_key": new_key_obj, "raw_key": raw_key}
        except Exception as e:
            logger.error(f"Error regenerating key: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_system_admin_key_property(self):
        key_value = await self.apikey_manager.get_system_admin_key()
        if not key_value:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="System admin key property not found.",
            )
        return {"value": key_value}

    async def regenerate_system_admin_key_property(self):
        new_key = await self.apikey_manager.regenerate_system_admin_key()
        return {"value": new_key}
