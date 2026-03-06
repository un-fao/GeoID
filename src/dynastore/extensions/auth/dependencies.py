from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from typing import Optional, List

from dynastore.modules import get_protocol
from dynastore.models.protocols import ApiKeyProtocol
from dynastore.models.auth import Principal

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_apikey_protocol() -> ApiKeyProtocol:
    protocol = get_protocol(ApiKeyProtocol)
    if not protocol:
        raise HTTPException(status_code=500, detail="ApiKey protocol implementation not available")
    return protocol

async def get_current_active_user(
    request: Request,
    token: str = Depends(oauth2_scheme),
    manager: ApiKeyProtocol = Depends(get_apikey_protocol)
) -> Principal:
    """
    Validates the token and returns the current active principal (user).
    """
    # We use the manager's central authentication method which handles both 
    # API Keys and Identity Provider tokens (JWTs)
    
    # We might need the catalog_id from the path to verify scoped permissions
    # For now, we'll try to get it from request state or path params if available, 
    # but the authenticate_apikey method usually takes it optionally.
    catalog_id = request.path_params.get("catalog_id")
    
    # authenticate_and_get_role extracts token from request, but here we have the token dependency.
    # We can use authenticate_apikey (which handles keys) or authenticate_and_get_principal (for IdPs).
    # Since we want to support both, we should replicate the logic from ApiKeyManager.authenticate_and_get_role
    # but without re-extracting the token.
    
    # Actually, let's use a helper in manager if possible, or manual logic here.
    
    # 1. Try V2 Identity (JWT)
    for provider_id, provider in manager._identity_providers.items():
        try:
            identity = await provider.validate_token(token)
            if identity:
                principal = await manager.authenticate_and_get_principal(
                    identity=identity,
                    target_schema=catalog_id or "_system_"
                )
                if principal:
                    return principal
        except Exception:
            pass # Try next provider
            
    # 2. Try API Key
    principal, _, reason = await manager.authenticate_apikey(token, catalog_id=catalog_id)
    
    if not principal:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    if not principal.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
        
    return principal
