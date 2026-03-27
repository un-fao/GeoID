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

            client_ip = request.client.host if request.client else "unknown"

            # Rate limit check
            if _login_limiter.is_locked(client_ip, username):
                raise HTTPException(
                    status_code=429,
                    detail="Too many failed login attempts. Please try again later.",
                )

            # Authenticate user
            user = await self.local_identity_provider.authenticate_user(
                username, password
            )

            if not user:
                _login_limiter.record_failure(client_ip, username)
                login_url = build_sibling_redirect("login", {
                    "error": "Invalid credentials",
                    "redirect_uri": redirect_uri,
                    "state": state,
                })
                return RedirectResponse(url=login_url, status_code=303)

            _login_limiter.record_success(client_ip, username)

            # Validate redirect_uri: block dangerous schemes before issuing a code.
            # Relative paths (no scheme, no netloc) are allowed and normalized with root_path.
            from urllib.parse import urlparse as _urlparse
            _parsed_redirect = _urlparse(redirect_uri)
            if _parsed_redirect.scheme:
                _allowed_schemes = {"http", "https"}
                _allowed_schemes_env = os.environ.get("ALLOWED_REDIRECT_SCHEMES", "")
                if _allowed_schemes_env:
                    _allowed_schemes = {s.strip().lower() for s in _allowed_schemes_env.split(",")}
                if _parsed_redirect.scheme.lower() not in _allowed_schemes:
                    raise HTTPException(
                        status_code=400,
                        detail=f"redirect_uri scheme '{_parsed_redirect.scheme}' is not permitted.",
                    )
            else:
                # Relative path — prepend root_path so the redirect lands on the correct prefix
                redirect_uri = f"{root_path}{redirect_uri}"

            # Generate authorization code
            code = await self.local_identity_provider.create_authorization_code(
                user_id=user["id"], redirect_uri=redirect_uri, scope=scope
            )

            # Redirect back to client with code
            if "?" in redirect_uri:
                return RedirectResponse(url=f"{redirect_uri}&code={code}&state={state}", status_code=303)
            return RedirectResponse(url=f"{redirect_uri}?code={code}&state={state}", status_code=303)


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
                url = build_sibling_redirect("register", {
                    "error": "Passwords do not match",
                    "redirect_uri": redirect_uri,
                    "state": state,
                })
                return RedirectResponse(url=url, status_code=303)

            try:
                # Check if user exists
                existing = await self.local_identity_provider.storage.get_local_user_by_username(
                    username
                )
                if existing:
                    url = build_sibling_redirect("register", {
                        "error": "Username already taken",
                        "redirect_uri": redirect_uri,
                        "state": state,
                    })
                    return RedirectResponse(url=url, status_code=303)

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
                logger.error(f"Registration failed: {e}", exc_info=True)
                url = build_sibling_redirect("register", {
                    "error": "Registration failed",
                    "redirect_uri": redirect_uri,
                    "state": state,
                })
                return RedirectResponse(url=url, status_code=303)

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
        @self.router.get("/me")
        async def userinfo(authorization: str = Header(None)):
            """
            OAuth2 UserInfo / Current User Profile Endpoint
            Returns user profile for a valid access token.
            """
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = authorization[7:]  # Remove "Bearer " prefix

            # Try LocalDB first
            try:
                user_info = await self.local_identity_provider.get_user_info(token)
                return user_info
            except Exception as e:
                logger.debug(f"Local userinfo failed: {e}")
                pass


            # Try Keycloak
            if self.keycloak_identity_provider:
                try:
                    user_info = await self.keycloak_identity_provider.get_user_info(
                        token
                    )
                    return user_info
                except Exception as e:
                    logger.debug(f"Keycloak userinfo failed: {e}")

            raise HTTPException(401, "Invalid access token")

        @self.router.put("/password")
        async def update_password(
            request: Request,
            current_password: str = Form(...),
            new_password: str = Form(...),
            authorization: str = Header(None)
        ):
            """Update the current user's password."""
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(401, "Missing or invalid Authorization header")

            token = authorization[7:]

            # Try LocalDB first
            user_info = None
            try:
                user_info = await self.local_identity_provider.get_user_info(token)
            except Exception as e:
                logger.debug(f"Local userinfo failed: {e}")

            if not user_info:
                raise HTTPException(401, "Invalid access token or not a local user")

            username = user_info.get("username")
            if not username:
                raise HTTPException(400, "Username not found in token")

            # Authenticate with current password to verify
            auth_result = await self.local_identity_provider.authenticate_user(username, current_password)
            if not auth_result:
                raise HTTPException(401, "Incorrect current password")

            # Hash the new password and update
            from argon2 import PasswordHasher
            ph = PasswordHasher()
            pw_hash = ph.hash(new_password)
            
            # Use the local provider's set_password method if available
            if hasattr(self.local_identity_provider, "set_password"):
                await self.local_identity_provider.set_password(username, pw_hash)
            else:
                # Fallback to direct DB update if set_password isn't implemented
                user_id = auth_result.get("id")
                await self.local_identity_provider.storage.update_local_user(
                    user_id, password_hash=pw_hash, schema="users"
                )

            return {"message": "Password updated successfully"}

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
                logger.warning(f"Token refresh failed: {e}")
                raise HTTPException(401, "Token refresh failed")

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
            try:
                user_info = await self.local_identity_provider.get_user_info(token)
            except Exception:
                pass

            if not user_info:
                raise HTTPException(403, "Valid authenticated token required")

            return {
                "identity_providers": {
                    "local": "enabled" if self.local_identity_provider else "disabled",
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
                jwt_secret = await props.get_property("apikey_jwt_secret")
                if not jwt_secret:
                    # Generate new secret and store it
                    jwt_secret = os.environ.get("JWT_SECRET", secrets.token_urlsafe(32))
                    await props.set_property(
                        "apikey_jwt_secret", jwt_secret, owner_code="system"
                    )
                    logger.info(
                        "✓ Generated new JWT secret (apikey_jwt_secret) and stored via PropertiesProtocol"
                    )
                else:
                    logger.info("✓ Loaded JWT secret via PropertiesProtocol (apikey_jwt_secret)")
            else:
                logger.warning(
                    "PropertiesProtocol not available for JWT secret loading."
                )
        except Exception as e:
            logger.warning(f"Could not load JWT secret: {e}")

        # Fallback: generate temporary secret
        if not jwt_secret:
            jwt_secret = os.environ.get("JWT_SECRET", secrets.token_urlsafe(32))
            logger.warning("⚠ Using temporary JWT secret (no database available)")

        # LocalDB provider (on-premise)
        self.local_identity_provider = LocalDBIdentityProvider(
            storage=storage, jwt_secret=jwt_secret
        )
        logger.info("✓ LocalDB identity provider initialized")

        # --- Auto-provision sysadmin user (on-premise) ---
        # Password resolution order:
        #   1. PropertiesProtocol: key=auth_sysadmin_password_enc (Fernet-encrypted,
        #      key derived from the JWT secret via SHA-256). Update this row to change
        #      the sysadmin password — it will be applied on next restart.
        #   2. SYSADMIN_PASSWORD environment variable (plain-text, not persisted).
        #   3. Generated random (24-char URL-safe), encrypted and stored in properties.
        # The plaintext password is only used transiently to derive the argon2 hash
        # stored in users.users; it is never written to disk or logs.
        try:
            from cryptography.fernet import Fernet as _Fernet
            from argon2 import PasswordHasher as _PasswordHasher
            from argon2.exceptions import VerifyMismatchError as _VerifyMismatchError

            _sa_user = "sysadmin"
            _sa_password: Optional[str] = None
            _password_from_props = False

            # 1. Try PropertiesProtocol (encrypted)
            _props = get_protocol(PropertiesProtocol)
            if _props:
                try:
                    _enc_val = await _props.get_property(_SYSADMIN_PROP_KEY)
                    if _enc_val:
                        _fk = _Fernet(_derive_fernet_key(jwt_secret))
                        _sa_password = _fk.decrypt(_enc_val.encode()).decode()
                        _password_from_props = True
                        logger.info("✓ Loaded sysadmin password from properties (encrypted)")
                except Exception as _pe:
                    logger.warning(f"Could not decrypt sysadmin password from properties: {_pe}")

            # 2. Env var fallback
            if not _sa_password:
                _sa_password = os.environ.get("SYSADMIN_PASSWORD")
                if _sa_password:
                    logger.info("✓ Loaded sysadmin password from SYSADMIN_PASSWORD env var")

            # 3. Default well-known password — change via admin panel after first login
            if not _sa_password:
                _sa_password = "sysadmin"
                logger.warning(
                    "SYSADMIN_PASSWORD not set and no stored password found. "
                    "Using default sysadmin password 'sysadmin' — CHANGE THIS IMMEDIATELY "
                    "via the admin panel or update the properties table key "
                    f"'{_SYSADMIN_PROP_KEY}' and restart."
                )

            # Persist encrypted password to properties if not already there
            if _props and not _password_from_props:
                try:
                    _fk = _Fernet(_derive_fernet_key(jwt_secret))
                    _enc_password = _fk.encrypt(_sa_password.encode()).decode()
                    await _props.set_property(
                        _SYSADMIN_PROP_KEY, _enc_password, owner_code="system"
                    )
                    logger.info("✓ Persisted encrypted sysadmin password to properties table")
                except Exception as _se:
                    logger.warning(f"Could not persist encrypted sysadmin password: {_se}")

            existing = await self.local_identity_provider.storage.get_local_user_by_username(
                _sa_user, schema="users"
            )

            if not existing:
                # Create the identity — argon2 hashing is done inside create_user
                _sa_id = await self.local_identity_provider.create_user(
                    username=_sa_user, password=_sa_password, email=f"{_sa_user}@localhost"
                )
                logger.info(f"✓ Provisioned sysadmin user (id={_sa_id}) in users.users (argon2)")
                try:
                    from dynastore.modules.apikey.models import Principal as _P
                    await storage.create_principal(
                        _P(id=_sa_id, identifier=_sa_user, display_name="System Administrator",
                           roles=["sysadmin"], is_active=True),
                        schema="apikey",
                    )
                    await storage.create_identity_link(
                        principal_id=_sa_id, provider="local", subject_id=str(_sa_id),
                        email=f"{_sa_user}@localhost", schema="apikey",
                    )
                    await storage.grant_roles(
                        provider="local", subject_id=str(_sa_id),
                        roles=["sysadmin"], schema="apikey",
                    )
                    logger.info("✓ sysadmin provisioned with sysadmin role in identity_roles")
                except Exception as _le:
                    logger.warning(f"Could not link sysadmin principal (may exist): {_le}")
            else:
                # User exists: verify current hash against the resolved password
                # (from properties, env var, or default) and update if it no longer matches.
                _sa_id = existing.get("id")
                logger.info(f"✓ sysadmin already exists (id={_sa_id})")
                try:
                    _ph = _PasswordHasher()
                    _ph.verify(existing.get("password_hash", ""), _sa_password)
                    logger.debug("sysadmin password unchanged — no update needed")
                except _VerifyMismatchError:
                    _new_hash = _PasswordHasher().hash(_sa_password)
                    await storage.update_local_user(
                        _sa_id, password_hash=_new_hash, schema="users"
                    )
                    _source = "properties" if _password_from_props else "env var / default"
                    logger.info(f"✓ sysadmin password updated from {_source} (argon2)")
                except Exception as _ve:
                    logger.warning(f"Could not verify/update sysadmin password: {_ve}")
        except Exception as _e:
            logger.error(f"Failed to provision sysadmin: {_e}", exc_info=True)

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
