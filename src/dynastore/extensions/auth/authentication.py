import os
import hashlib
import secrets
from typing import List, Any, Dict, Optional
from fastapi import APIRouter, FastAPI, Request, Depends, HTTPException, Form, Header
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from pydantic import BaseModel

from dynastore.extensions import (
    ExtensionProtocol,
)
from dynastore.extensions.web import expose_static
from dynastore.extensions.tools.url import build_sibling_redirect
from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    CatalogsProtocol,
    ConfigsProtocol,
    PropertiesProtocol,
)
from contextlib import asynccontextmanager
from starlette.middleware.sessions import SessionMiddleware
import logging

logger = logging.getLogger(__name__)

_SYSADMIN_PROP_KEY = "auth_sysadmin_password_enc"


class _LoginRateLimiter:
    """In-memory per-IP/username failed login tracker with lockout."""

    def __init__(self, max_attempts: int = 5, lockout_seconds: int = 300):
        self.max_attempts = max_attempts
        self.lockout_seconds = lockout_seconds
        self._attempts: dict = {}  # key -> (count, first_attempt_time)

    def _key(self, ip: str, username: str) -> str:
        return f"{ip}:{username}"

    def is_locked(self, ip: str, username: str) -> bool:
        import time as _time
        key = self._key(ip, username)
        entry = self._attempts.get(key)
        if not entry:
            return False
        count, first_ts = entry
        if _time.monotonic() - first_ts > self.lockout_seconds:
            del self._attempts[key]
            return False
        return count >= self.max_attempts

    def record_failure(self, ip: str, username: str) -> None:
        import time as _time
        key = self._key(ip, username)
        entry = self._attempts.get(key)
        now = _time.monotonic()
        if entry:
            count, first_ts = entry
            if now - first_ts > self.lockout_seconds:
                self._attempts[key] = (1, now)
            else:
                self._attempts[key] = (count + 1, first_ts)
        else:
            self._attempts[key] = (1, now)

    def record_success(self, ip: str, username: str) -> None:
        self._attempts.pop(self._key(ip, username), None)


_login_limiter = _LoginRateLimiter()


def _derive_fernet_key(jwt_secret: str) -> bytes:
    """Derive a 256-bit Fernet-compatible key from the JWT secret via SHA-256."""
    import base64
    raw = hashlib.sha256(jwt_secret.encode()).digest()  # 32 bytes
    return base64.urlsafe_b64encode(raw)


# OAuth2 configuration
# JWT_SECRET will be loaded from shared properties (managed by sysadmin)
KEYCLOAK_ISSUER_URL = os.getenv("KEYCLOAK_ISSUER_URL")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID")


class Authentication(ExtensionProtocol):
    priority: int = 110  # Must run after ApiKeyModule (100) which owns users schema init
    """
    Authentication Extension - OAuth2 Identity Validation

    Responsibilities:
    - Validate OAuth2 tokens (JWT)
    - Provide OAuth2 endpoints (/authorize, /token, /userinfo, /refresh)
    - Support multiple identity providers (LocalDB, Keycloak)
    - Return validated Identity objects

    Does NOT handle:
    - Principal management (that's in apikey module)
    - Permission checking (that's authorization, not authentication)
    """

    router: APIRouter = APIRouter(tags=["Authentication"], prefix="/auth")

    # Identifiers for protocols are used late in lifespan

    # Identity Providers (Keycloak only — local OAuth removed in v1.0)
    keycloak_identity_provider = None

    @expose_static("auth")
    def _provide_auth_static(self) -> List[str]:
        """Expose auth static files."""
        static_dir = os.path.join(os.path.dirname(__file__), "static", "auth")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def configure_app(self, app: FastAPI):
        """Early configuration - add session middleware."""
        session_secret = os.environ.get("SESSION_SECRET_KEY")
        if not session_secret:
            logger.warning("SESSION_SECRET_KEY not set. Using temporary key.")
            session_secret = secrets.token_hex(32)

        app.add_middleware(SessionMiddleware, secret_key=session_secret)

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app
        # Setup routes will be called after lifespan initialization

    def _setup_routes(self):
        """Setup routes with closures to access instance state."""

        @self.router.get("/authorize")
        async def authorize(
            response_type: str,
            client_id: str,
            redirect_uri: str,
            state: str,
            scope: str = "openid email profile",
            provider: Optional[str] = None,
        ):
            """
            OAuth2 Authorization Endpoint

            Selection Logic:
            1. If provider='keycloak' and it is configured, use it.
            2. Otherwise, default to 'local' provider.
            """
            # Keycloak is the only IdP in v1.0
            if self.keycloak_identity_provider:
                auth_url = await self.keycloak_identity_provider.get_authorization_url(
                    redirect_uri=redirect_uri, state=state, scope=scope
                )
                return RedirectResponse(url=auth_url)

            raise HTTPException(
                501, "No identity provider configured. Set KEYCLOAK_ISSUER_URL."
            )

        @self.router.get("/userinfo")
        @self.router.get("/me")
        async def userinfo(authorization: str = Header(None)):
            """
            OAuth2 UserInfo Endpoint.
            Returns user profile from Keycloak token.
            """
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = authorization[7:]

            if self.keycloak_identity_provider:
                try:
                    user_info = await self.keycloak_identity_provider.get_user_info(token)
                    return user_info
                except Exception as e:
                    logger.debug(f"Keycloak userinfo failed: {e}")

            raise HTTPException(401, "Invalid access token")

        @self.router.get("/logout")
        async def logout(request: Request, redirect_uri: Optional[str] = None):
            """Logout endpoint - clears session."""
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/login"
            elif redirect_uri.startswith("/"):
                # If absolute path on same domain, prepend root_path
                redirect_uri = f"{root_path}{redirect_uri}"

            request.session.clear()
            return RedirectResponse(url=redirect_uri)

        @self.router.get("/debug")
        async def debug_auth(request: Request, authorization: str = Header(None)):
            """Debug endpoint to inspect authentication state. Requires a valid Bearer token."""
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = authorization[7:]
            user_info = None

            if self.keycloak_identity_provider:
                try:
                    user_info = await self.keycloak_identity_provider.get_user_info(token)
                except Exception:
                    pass

            if not user_info:
                raise HTTPException(403, "Valid authenticated token required")

            return {
                "identity_providers": {
                    "keycloak": "enabled"
                    if self.keycloak_identity_provider
                    else "disabled",
                },
            }

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Initialize identity providers during lifespan."""
        logger.info("Authentication Extension: Initializing identity providers")
        self.app_state = app

        from dynastore.modules.apikey.identity_providers import KeycloakIdentityProvider
        from dynastore.modules.apikey.postgres_apikey_storage import PostgresApiKeyStorage

        storage = PostgresApiKeyStorage(app_state=app)

        # --- Auto-provision sysadmin principal (principals-only, no local users) ---
        try:
            from dynastore.modules.apikey.models import Principal as _P
            from uuid import uuid5, NAMESPACE_DNS
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.models.protocols import DatabaseProtocol

            _sa_id = uuid5(NAMESPACE_DNS, "sysadmin.dynastore.local")
            db = get_protocol(DatabaseProtocol)
            if db and db.engine:
                async with managed_transaction(db.engine) as conn:
                    existing = await storage.get_principal(_sa_id, schema="apikey", conn=conn)
                    if not existing:
                        await storage.create_principal(
                            _P(
                                id=_sa_id,
                                identifier="sysadmin",
                                display_name="System Administrator",
                                roles=["sysadmin"],
                                is_active=True,
                            ),
                            schema="apikey",
                            conn=conn,
                        )
                        logger.info(f"Provisioned sysadmin principal (id={_sa_id})")
                    else:
                        logger.info(f"sysadmin principal already exists (id={_sa_id})")
        except Exception as _e:
            logger.error(f"Failed to provision sysadmin: {_e}", exc_info=True)

        # Keycloak provider — primary IdP for v1.0
        if KEYCLOAK_ISSUER_URL and KEYCLOAK_CLIENT_ID:
            self.keycloak_identity_provider = KeycloakIdentityProvider(
                issuer_url=KEYCLOAK_ISSUER_URL, client_id=KEYCLOAK_CLIENT_ID
            )
            logger.info(
                f"✓ Keycloak identity provider initialized: {KEYCLOAK_ISSUER_URL}"
            )
        else:
            logger.info("⊘ Keycloak not configured (optional)")

        # Setup routes after providers are initialized
        self._setup_routes()
        logger.info("✓ Authentication routes configured")

        # Register authentication policies via PermissionProtocol
        try:
            from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role

            pm = get_protocol(PermissionProtocol)
            if pm:
                auth_policy = Policy(
                    id="auth_extension_public",
                    description="Public access to authentication endpoints.",
                    actions=["GET", "POST"],
                    resources=[r"/auth/.*", r"/web/auth/.*"],
                    effect="ALLOW",
                )
                pm.register_policy(auth_policy)
                pm.register_role(Role(name="anonymous", policies=["auth_extension_public"]))
                logger.info("Authentication policies registered via PermissionProtocol.")
            else:
                logger.warning("PermissionProtocol not available; auth policies not registered.")
        except Exception as e:
            logger.error(f"Failed to register authentication policies: {e}")

        try:
            yield
        finally:
            pass
