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

import logging
from typing import Optional
from uuid import UUID
from fastapi import Request, Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from dynastore.tools.discovery import get_protocol
from dynastore.models.auth import Principal, AuthenticationProtocol

logger = logging.getLogger(__name__)

# Security Schemes
http_bearer = HTTPBearer(auto_error=False, scheme_name="HTTPBearer")

async def get_principal(
    request: Request,
    bearer: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> Optional[Principal]:
    """
    Generic dependency to retrieve the current resolved Principal.
    
    It attempts to find the Principal in the following order:
    1. Request state (if Middleware already resolved it).
    2. Lazy-loaded authentication via ApiKeyModule (if available and enabled).
    
    If authentication fails or is not configured, it returns None (Anonymous).
    """

    # 1. Check if Middleware already authenticated a User Principal
    if hasattr(request.state, "principal") and request.state.principal:
        return request.state.principal

    # 2. Check if Middleware authenticated a System Admin
    principal_role = getattr(request.state, "principal_role", None)
    if principal_role == "sysadmin":
        return Principal(
            id=UUID('00000000-0000-0000-0000-000000000000'), 
            identifier="sysadmin", 
            metadata={"role": "sysadmin", "description": "System Administrator (Synthetic)"}
        )

    # 3. Fallback: Try to use any module implementing AuthenticationProtocol
    # This decouples the security tool from specific implementations (like ApiKeyModule).
    try:
        provider = get_protocol(AuthenticationProtocol)
        if not provider:
            return None
            
        key = bearer.credentials if bearer else None
        if key:
            return await provider.resolve_principal(key)

    except Exception as e:
        logger.debug(f"Failed to authenticate via protocol provider: {e}")
        pass
            
    return None
