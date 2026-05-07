from fastapi import Depends, HTTPException, Header, status, Request
from typing import Optional

from dynastore.modules import get_protocol
from dynastore.tools.discovery import get_protocols
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.modules.iam.interfaces import IdentityProviderProtocol
from dynastore.models.auth import Principal


async def get_authenticator() -> AuthenticatorProtocol:
    protocol = get_protocol(AuthenticatorProtocol)
    if not protocol:
        raise HTTPException(status_code=500, detail="Authenticator implementation not available")
    return protocol


async def get_current_active_user(
    request: Request,
    authorization: Optional[str] = Header(None),
    manager: AuthenticatorProtocol = Depends(get_authenticator),
) -> Principal:
    """
    Validates the bearer token and returns the current active principal.

    Extracts the token from the ``Authorization: Bearer <token>`` header
    directly. The previous OAuth2PasswordBearer-based extraction advertised
    a Resource-Owner-Password-Credentials flow that the IdP (Keycloak
    ``geoid-fe`` client) does not enable, so we read the header explicitly.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = authorization[7:]

    # We use the manager's central authentication method which handles
    # Identity Provider tokens (JWTs via Keycloak).

    # We might need the catalog_id from the path to verify scoped permissions
    catalog_id = request.path_params.get("catalog_id")

    # Try V2 Identity (JWT via identity providers)
    for provider in get_protocols(IdentityProviderProtocol):
        try:
            identity = await provider.validate_token(token)
            if identity:
                principal = await manager.authenticate_and_get_principal(
                    identity=identity,
                    target_schema=catalog_id or "_system_"
                )
                if principal:
                    if not principal.is_active:
                        raise HTTPException(status_code=400, detail="Inactive user")
                    return principal
        except HTTPException:
            raise
        except Exception:
            pass  # Try next provider

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
