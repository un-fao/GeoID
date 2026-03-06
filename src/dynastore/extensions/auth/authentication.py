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

# OAuth2 configuration
# JWT_SECRET will be loaded from shared properties (managed by sysadmin)
KEYCLOAK_ISSUER_URL = os.getenv("KEYCLOAK_ISSUER_URL")
KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID")


class Authentication(ExtensionProtocol):
    priority: int = 100
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

    # Identity Providers
    local_identity_provider = None
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
            # Explicit request for Keycloak
            if provider == "keycloak" and self.keycloak_identity_provider:
                auth_url = await self.keycloak_identity_provider.get_authorization_url(
                    redirect_uri=redirect_uri, state=state, scope=scope
                )
                return RedirectResponse(url=auth_url)

            # Default: LocalDB
            login_url = await self.local_identity_provider.get_authorization_url(
                redirect_uri=redirect_uri, state=state, scope=scope
            )
            return RedirectResponse(url=login_url)

        @self.router.get("/login", include_in_schema=False)
        async def login_page(
            request: Request,
            redirect_uri: Optional[str] = None,
            state: str = "init",
            scope: str = "openid email profile",
        ):
            """Serve login HTML page for on-premise authentication."""
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/debug"
            login_file = os.path.join(
                os.path.dirname(__file__), "static", "auth", "login.html"
            )
            if os.path.exists(login_file):
                return FileResponse(login_file)
            else:
                return HTMLResponse(
                    content="<h1>Login page not found</h1>", status_code=404
                )

        @self.router.post("/login", include_in_schema=False)
        async def login_submit(
            request: Request,
            username: str = Form(...),
            password: str = Form(...),
            redirect_uri: Optional[str] = Form(None),
            state: str = Form("init"),
            scope: str = Form("openid email profile"),
        ):
            """Process login form and generate authorization code."""
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/debug"
            # Authenticate user
            user = await self.local_identity_provider.authenticate_user(
                username, password
            )

            if not user:
                # Return login page with error
                html_error = f"""
                <script>
                    alert('Invalid credentials');
                    window.history.back();
                </script>
                """
                return HTMLResponse(content=html_error, status_code=401)

            # Generate authorization code
            code = await self.local_identity_provider.create_authorization_code(
                user_id=user["id"], redirect_uri=redirect_uri, scope=scope
            )

            # Redirect back to client with code
            if "?" in redirect_uri:
                return RedirectResponse(url=f"{redirect_uri}&code={code}&state={state}")
            return RedirectResponse(url=f"{redirect_uri}?code={code}&state={state}")

        @self.router.get("/register", include_in_schema=False)
        async def register_page(
            request: Request, redirect_uri: Optional[str] = None, state: str = "init"
        ):
            """Serve registration HTML page."""
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/debug"
            register_file = os.path.join(
                os.path.dirname(__file__), "static", "auth", "register.html"
            )
            if os.path.exists(register_file):
                return FileResponse(register_file)
            else:
                return HTMLResponse(
                    content="<h1>Register page not found</h1>", status_code=404
                )

        @self.router.post("/register", include_in_schema=False)
        async def register_submit(
            request: Request,
            username: str = Form(...),
            email: str = Form(...),
            password: str = Form(...),
            confirm_password: str = Form(...),
            redirect_uri: Optional[str] = Form(None),
            state: str = Form("init"),
        ):
            root_path = request.scope.get("root_path", "").rstrip("/")
            if not redirect_uri:
                redirect_uri = f"{root_path}/auth/debug"
            if password != confirm_password:
                return HTMLResponse(
                    "<script>alert('Passwords do not match'); window.history.back();</script>",
                    status_code=400,
                )

            try:
                # Create user in LocalDB
                # Note: This requires the storage backend to support user creation which LocalDBIdentitySPI should facilitate
                # Accessing the underlying storage directly

                # Check if user exists
                existing = await self.local_identity_provider.storage.get_local_user_by_username(
                    username
                )
                if existing:
                    return HTMLResponse(
                        "<script>alert('Username already taken'); window.history.back();</script>",
                        status_code=400,
                    )

                # Create user
                await self.local_identity_provider.create_user(
                    username=username, password=password, email=email
                )

                # Auto login
                return await self.login_submit(
                    username=username,
                    password=password,
                    redirect_uri=redirect_uri,
                    state=state,
                    scope="openid email profile",
                )

            except Exception as e:
                logger.error(f"Registration failed: {e}")
                return HTMLResponse(
                    f"<script>alert('Registration failed: {str(e)}'); window.history.back();</script>",
                    status_code=500,
                )

        @self.router.post("/token")
        async def token(
            grant_type: str = Form(...),
            code: Optional[str] = Form(None),
            refresh_token: Optional[str] = Form(None),
            redirect_uri: Optional[str] = Form(None),
            client_id: Optional[str] = Form(None),
        ):
            """
            OAuth2 Token Endpoint

            Exchanges authorization code or refresh token for access token.
            """
            if grant_type == "authorization_code":
                if not code or not redirect_uri:
                    raise HTTPException(400, "Missing code or redirect_uri")

                # Exchange code for tokens
                token_response = (
                    await self.local_identity_provider.exchange_code_for_token(
                        code=code, redirect_uri=redirect_uri
                    )
                )
                return token_response

            elif grant_type == "refresh_token":
                if not refresh_token:
                    raise HTTPException(400, "Missing refresh_token")

                # Refresh access token
                token_response = (
                    await self.local_identity_provider.refresh_access_token(
                        refresh_token=refresh_token
                    )
                )
                return token_response

            else:
                raise HTTPException(400, f"Unsupported grant_type: {grant_type}")

        @self.router.get("/userinfo")
        async def userinfo(authorization: str = Header(None)):
            """
            OAuth2 UserInfo Endpoint

            Returns user profile for a valid access token.
            """
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = authorization[7:]  # Remove "Bearer " prefix

            # Try LocalDB first
            try:
                user_info = await self.local_identity_provider.get_user_info(token)
                return user_info
            except:
                pass

            # Try Keycloak
            if self.keycloak_identity_provider:
                try:
                    user_info = await self.keycloak_identity_provider.get_user_info(
                        token
                    )
                    return user_info
                except:
                    pass

            raise HTTPException(401, "Invalid access token")

        @self.router.post("/refresh")
        async def refresh(refresh_token: str = Form(...)):
            """Refresh an access token using a refresh token."""
            try:
                token_response = (
                    await self.local_identity_provider.refresh_access_token(
                        refresh_token
                    )
                )
                return token_response
            except Exception as e:
                raise HTTPException(401, f"Token refresh failed: {str(e)}")

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
        async def debug_auth(request: Request):
            """Debug endpoint to inspect authentication state."""
            return {
                "identity_providers": {
                    "local": "enabled" if self.local_identity_provider else "disabled",
                    "keycloak": "enabled"
                    if self.keycloak_identity_provider
                    else "disabled",
                },
                "session": dict(request.session) if hasattr(request, "session") else {},
                "headers": {
                    "authorization": request.headers.get("Authorization", "MISSING")[
                        :50
                    ]
                    + "..."
                    if request.headers.get("Authorization")
                    else "MISSING"
                },
            }

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Initialize identity providers during lifespan."""
        logger.info("Authentication Extension: Initializing identity providers")
        self.app_state = app

        # Initialize identity providers
        from dynastore.modules.apikey.identity_providers import (
            LocalDBIdentityProvider,
            KeycloakIdentityProvider,
        )
        from dynastore.modules.apikey.postgres_apikey_storage import (
            PostgresApiKeyStorage,
        )

        # Get storage instance
        storage = PostgresApiKeyStorage(app_state=app)

        # Load or generate JWT secret from shared properties
        jwt_secret = None
        try:
            props = get_protocol(PropertiesProtocol)
            if props:
                jwt_secret = await props.get_property("jwt_secret")
                if not jwt_secret:
                    # Generate new secret and store it
                    jwt_secret = secrets.token_urlsafe(32)
                    await props.set_property(
                        "jwt_secret", jwt_secret, owner_code="system"
                    )
                    logger.info(
                        "✓ Generated new JWT secret and stored via PropertiesProtocol"
                    )
                else:
                    logger.info("✓ Loaded JWT secret via PropertiesProtocol")
            else:
                logger.warning(
                    "PropertiesProtocol not available for JWT secret loading."
                )
        except Exception as e:
            logger.warning(f"Could not load JWT secret: {e}")

        # Fallback: generate temporary secret
        if not jwt_secret:
            jwt_secret = secrets.token_urlsafe(32)
            logger.warning("⚠ Using temporary JWT secret (no database available)")

        # LocalDB provider (on-premise)
        self.local_identity_provider = LocalDBIdentityProvider(
            storage=storage, jwt_secret=jwt_secret
        )
        logger.info("✓ LocalDB identity provider initialized")

        # Keycloak provider (cloud) - optional
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

        # Register and provision policies
        try:
            from dynastore.modules.apikey.models import Policy, Role
            from dynastore.modules.apikey.policies import register_policy, register_role
            from dynastore.modules.apikey.module import ApiKeyModule

            # Define policy for public auth access
            auth_policy = Policy(
                id="auth_extension_public",
                description="Public access to authentication endpoints.",
                actions=["GET", "POST"],
                resources=[r"/auth/.*", r"/web/auth/.*"],
                effect="ALLOW",
            )
            register_policy(auth_policy)

            # Add to anonymous role
            register_role(Role(name="anonymous", policies=["auth_extension_public"]))

            # Trigger provisioning
            from dynastore.models.protocols import ApiKeyProtocol

            apikey_protocol = get_protocol(ApiKeyProtocol)
            if apikey_protocol:
                await apikey_protocol.get_policy_manager().provision_default_policies()
                logger.info("✓ Authentication policies registered and provisioned")
        except Exception as e:
            logger.error(f"Failed to provision authentication policies: {e}")

        try:
            yield
        finally:
            pass
