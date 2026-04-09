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

"""Keycloak Identity Provider - Validates JWT tokens from external IdP"""

import logging
import httpx
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import jwt
from jwt.exceptions import PyJWTError as JWTError, ExpiredSignatureError, InvalidTokenError as JWTClaimsError

from ..interfaces import IdentityProviderProtocol

logger = logging.getLogger(__name__)


class KeycloakIdentityProvider(IdentityProviderProtocol):
    """
    Validates JWT tokens from Keycloak using OIDC/JWKS.
    Supports token validation, JWKS caching, and user info retrieval.
    """
    
    def __init__(self, issuer_url: str, client_id: str, audience: Optional[str] = None):
        """
        Initialize Keycloak identity provider.
        
        Args:
            issuer_url: Keycloak issuer URL (e.g., https://keycloak.example.com/realms/myrealm)
            client_id: OAuth2 client ID
            audience: Expected audience claim (defaults to client_id)
        """
        self.issuer_url = issuer_url.rstrip("/")
        self.client_id = client_id
        self.audience = audience or client_id
        self._jwks_cache: Optional[Dict[str, Any]] = None
        self._jwks_cache_time: Optional[datetime] = None
        self._jwks_cache_ttl = 3600  # 1 hour
        
    def get_provider_id(self) -> str:
        """Returns the unique identifier for this provider."""
        return "keycloak"
    
    async def _get_jwks(self) -> Dict[str, Any]:
        """
        Fetches and caches JWKS from Keycloak.
        
        Returns:
            JWKS dictionary
        """
        now = datetime.now(timezone.utc)
        
        # Check cache
        if self._jwks_cache and self._jwks_cache_time:
            age = (now - self._jwks_cache_time).total_seconds()
            if age < self._jwks_cache_ttl:
                return self._jwks_cache
        
        # Fetch from Keycloak
        try:
            jwks_url = f"{self.issuer_url}/protocol/openid-connect/certs"
            async with httpx.AsyncClient() as client:
                response = await client.get(jwks_url, timeout=10.0)
                response.raise_for_status()
                jwks = response.json()
                
            self._jwks_cache = jwks
            self._jwks_cache_time = now
            logger.info(f"Refreshed JWKS cache from {jwks_url}")
            return jwks
            
        except Exception as e:
            logger.error(f"Failed to fetch JWKS from {jwks_url}: {e}")
            # Return stale cache if available
            if self._jwks_cache:
                logger.warning("Using stale JWKS cache")
                return self._jwks_cache
            raise
    
    async def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Validates a JWT token from Keycloak.
        
        Args:
            token: JWT access token
            
        Returns:
            Identity dict with provider, sub, email, name, or None if invalid
        """
        if not token:
            return None
        
        try:
            # Get JWKS for signature validation
            jwks = await self._get_jwks()
            
            # Decode and validate JWT
            # Note: PyJWT RS256 requires PyJWKSet or manual key resolution
            # For simplicity, we use PyJWKClient internally or just unverified claims if signature bypass is intended
            # But here we want validation.
            from jwt import PyJWKSet
            jwk_set = PyJWKSet.from_dict(jwks)
            
            claims = jwt.decode(
                token,
                key=jwk_set,
                algorithms=["RS256"],
                audience=self.audience,
                issuer=self.issuer_url,
                options={
                    "verify_signature": True,
                    "verify_aud": True,
                    "verify_iss": True,
                    "verify_exp": True,
                }
            )
            
            # Detect service account (client_credentials grant):
            # Keycloak sets client_id claim but typically no email for service accounts
            azp = claims.get("azp")
            client_id_claim = claims.get("client_id")
            is_service_account = client_id_claim is not None and not claims.get("email")

            # Extract identity information
            identity = {
                "provider": "keycloak:service_account" if is_service_account else "keycloak",
                "sub": claims.get("sub"),
                "email": claims.get("email"),
                "name": claims.get("name") or claims.get("preferred_username") or client_id_claim,
                "email_verified": claims.get("email_verified", False),
                "groups": claims.get("groups", []),
                "realm_roles": claims.get("realm_access", {}).get("roles", []),
                "client_roles": claims.get("resource_access", {}).get(self.client_id, {}).get("roles", []),
                "azp": azp,
                "client_id": client_id_claim,
                "is_service_account": is_service_account,
                "raw_claims": claims,
            }

            logger.debug(
                f"Validated Keycloak token for {'service account' if is_service_account else 'user'}: "
                f"{identity.get('email') or client_id_claim}"
            )
            return identity
            
        except ExpiredSignatureError:
            logger.debug("JWT token has expired")
            return None
        except JWTClaimsError as e:
            logger.warning(f"JWT claims validation failed: {e}")
            return None
        except JWTError as e:
            logger.warning(f"JWT validation failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error validating Keycloak token: {e}")
            return None
    
    async def get_authorization_url(self, redirect_uri: str, state: str, scope: str = "openid email profile") -> str:
        """
        Generates Keycloak authorization URL.
        
        Args:
            redirect_uri: Callback URL after authentication
            state: CSRF protection state parameter
            scope: OAuth2 scopes
            
        Returns:
            Authorization URL
        """
        from urllib.parse import urlencode
        
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": scope
        }
        
        auth_url = f"{self.issuer_url}/protocol/openid-connect/auth"
        return f"{auth_url}?{urlencode(params)}"
    
    async def exchange_code_for_token(self, code: str, redirect_uri: str, client_secret: Optional[str] = None) -> Dict[str, Any]:
        """
        Exchanges authorization code for access token.
        
        Args:
            code: Authorization code
            redirect_uri: Callback URL (must match authorization request)
            client_secret: OAuth2 client secret (if confidential client)
            
        Returns:
            Token response with access_token, refresh_token, expires_in
        """
        token_url = f"{self.issuer_url}/protocol/openid-connect/token"
        
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self.client_id,
        }
        
        if client_secret:
            data["client_secret"] = client_secret
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, timeout=10.0)
                response.raise_for_status()
                return response.json()
                
        except Exception as e:
            logger.error(f"Failed to exchange code for token: {e}")
            raise
    
    async def get_user_info(self, access_token: str) -> Dict[str, Any]:
        """
        Retrieves user information from Keycloak userinfo endpoint.
        
        Args:
            access_token: Valid access token
            
        Returns:
            User profile information
        """
        userinfo_url = f"{self.issuer_url}/protocol/openid-connect/userinfo"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    userinfo_url,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=10.0
                )
                response.raise_for_status()
                return response.json()
                
        except Exception as e:
            logger.error(f"Failed to get user info: {e}")
            raise
    
    async def refresh_token(self, refresh_token: str, client_secret: Optional[str] = None) -> Dict[str, Any]:
        """
        Refreshes an access token using a refresh token.
        
        Args:
            refresh_token: Valid refresh token
            client_secret: OAuth2 client secret (if confidential client)
            
        Returns:
            New token response
        """
        token_url = f"{self.issuer_url}/protocol/openid-connect/token"
        
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
        }
        
        if client_secret:
            data["client_secret"] = client_secret
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, timeout=10.0)
                response.raise_for_status()
                return response.json()
                
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            raise
