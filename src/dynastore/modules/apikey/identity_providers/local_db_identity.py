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

"""Local Database Identity Provider - On-premise username/password authentication"""

import logging
import secrets
import hashlib
import bcrypt
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
import jwt
from jwt.exceptions import PyJWTError as JWTError

from ..interfaces import IdentityProviderProtocol

logger = logging.getLogger(__name__)


class LocalDBIdentityProvider(IdentityProviderProtocol):
    """
    Implements OAuth2 Authorization Code Flow for on-premise users.
    Stores credentials in users schema tables.
    """
    
    def __init__(self, storage, jwt_secret: Optional[str] = None, jwt_algorithm: str = "HS256"):
        """
        Initialize LocalDB identity provider.
        
        Args:
            storage: PostgresApiKeyStorage instance for database access
            jwt_secret: Secret key for JWT signing (generated if not provided)
            jwt_algorithm: JWT signing algorithm
        """
        self.storage = storage
        self.jwt_secret = jwt_secret or secrets.token_urlsafe(32)
        self.jwt_algorithm = jwt_algorithm
        self.access_token_ttl = 3600  # 1 hour
        self.refresh_token_ttl = 604800  # 7 days
        self.auth_code_ttl = 300  # 5 minutes
        
    def get_provider_id(self) -> str:
        """Returns the unique identifier for this provider."""
        return "local"
    
    async def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Validates a JWT access token.
        
        Args:
            token: JWT access token
            
        Returns:
            Identity dict with provider, sub, email, or None if invalid
        """
        if not token:
            return None
        
        try:
            # Decode and validate JWT
            logger.debug(f"LocalDBIdentityProvider.validate_token: decoding with algorithm {self.jwt_algorithm}, secret prefix: {self.jwt_secret[:8]}")
            claims = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm],
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                }
            )
            
            logger.debug(f"claims: {claims}")
            
            # Verify token type
            if claims.get("token_type") != "access":
                logger.debug(f"Token is not an access token: {claims.get('token_type')}")
                return None
            
            # Extract identity
            identity = {
                "provider": claims.get("prv", "local"),
                "sub": claims.get("sub"),
                "email": claims.get("email"),
                "username": claims.get("username"),
                "name": claims.get("name"),
            }
            
            return identity
            
        except JWTError as e:
            logger.debug(f"LocalDBIdentityProvider validation failed (JWTError): {e}")
            return None
        except Exception as e:
            logger.error(f"LocalDBIdentityProvider unexpected error validating token: {e}", exc_info=True)
            return None
    
    async def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticates a user with username and password.
        
        Args:
            username: Username
            password: Plain text password
            
        Returns:
            User dict if valid, None otherwise
        """
        try:
            # Fetch user from database
            user = await self.storage.get_local_user_by_username(username, schema="users")  # users schema for identities
            
            if not user:
                logger.debug(f"User not found: {username}")
                return None
            
            if not user.get("is_active"):
                logger.debug(f"User account disabled: {username}")
                return None
            
            # Verify password
            password_hash = user.get("password_hash")
            if not password_hash:
                logger.error(f"User {username} has no password hash")
                return None
            
            if not bcrypt.checkpw(password.encode(), password_hash.encode()):
                logger.debug(f"Invalid password for user: {username}")
                return None
            
            return user
            
        except Exception as e:
            logger.error(f"Error authenticating user: {e}")
            return None
    
    async def create_authorization_code(self, user_id: UUID, redirect_uri: str, scope: Optional[str] = None) -> str:
        """
        Creates an authorization code for OAuth2 flow.
        
        Args:
            user_id: User UUID
            redirect_uri: Redirect URI for callback
            scope: Requested scopes
            
        Returns:
            Authorization code
        """
        code = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self.auth_code_ttl)
        
        await self.storage.create_oauth_code(
            code=code,
            user_id=user_id,
            redirect_uri=redirect_uri,
            scope=scope,
            expires_at=expires_at,
            schema="users"
        )
        
        return code
    
    async def get_authorization_url(self, redirect_uri: str, state: str, scope: str = "openid email profile") -> str:
        """
        Generates authorization URL (points to login page).
        
        Args:
            redirect_uri: Callback URL after authentication
            state: CSRF protection state parameter
            scope: OAuth2 scopes
            
        Returns:
            Authorization URL
        """
        from urllib.parse import urlencode
        
        params = {
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": scope
        }
        
        # This points to our own login page, relative to the authorize endpoint
        return f"login?{urlencode(params)}"
    
    async def exchange_code_for_token(self, code: str, redirect_uri: str) -> Dict[str, Any]:
        """
        Exchanges authorization code for access and refresh tokens.
        
        Args:
            code: Authorization code
            redirect_uri: Redirect URI (must match authorization request)
            
        Returns:
            Token response with access_token, refresh_token, expires_in
        """
        try:
            # Fetch and validate authorization code
            auth_code = await self.storage.get_oauth_code(code, schema="users")
            
            if not auth_code:
                raise ValueError("Invalid authorization code")
            
            if auth_code.get("redirect_uri") != redirect_uri:
                raise ValueError("Redirect URI mismatch")
            
            if auth_code.get("expires_at") < datetime.now(timezone.utc):
                raise ValueError("Authorization code expired")
            
            # Delete code (one-time use)
            await self.storage.delete_oauth_code(code, schema="users")
            
            # Fetch user
            user_id = auth_code.get("user_id")
            user = await self.storage.get_local_user_by_id(user_id, schema="users")
            
            if not user:
                raise ValueError("User not found")
            
            # Generate tokens
            access_token = self._generate_access_token(user)
            refresh_token = self._generate_refresh_token(user)
            
            # Store refresh token
            await self.storage.create_oauth_token(
                user_id=user_id,
                token_hash=hashlib.sha256(refresh_token.encode()).hexdigest(),
                token_type="refresh",
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=self.refresh_token_ttl),
                schema="users"
            )
            
            return {
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": self.access_token_ttl,
                "refresh_token": refresh_token,
                "scope": auth_code.get("scope", "openid email profile")
            }
            
        except Exception as e:
            logger.error(f"Failed to exchange code for token: {e}")
            raise
    
    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refreshes an access token using a refresh token.
        
        Args:
            refresh_token: Valid refresh token
            
        Returns:
            New token response
        """
        try:
            token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()
            
            # Fetch and validate refresh token
            token_record = await self.storage.get_oauth_token_by_hash(token_hash, schema="users")
            
            if not token_record:
                raise ValueError("Invalid refresh token")
            
            if token_record.get("token_type") != "refresh":
                raise ValueError("Token is not a refresh token")
            
            if token_record.get("expires_at") < datetime.now(timezone.utc):
                raise ValueError("Refresh token expired")
            
            # Fetch user
            user_id = token_record.get("user_id")
            user = await self.storage.get_local_user_by_id(user_id, schema="users")
            
            if not user:
                raise ValueError("User not found")
            
            # Generate new access token
            access_token = self._generate_access_token(user)
            
            return {
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": self.access_token_ttl,
                "refresh_token": refresh_token  # Same refresh token
            }
            
        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise
    
    async def get_user_info(self, access_token: str) -> Dict[str, Any]:
        """
        Retrieves user information from access token.
        
        Args:
            access_token: Valid access token
            
        Returns:
            User profile information
        """
        identity = await self.validate_token(access_token)
        
        if not identity:
            raise ValueError("Invalid access token")
        
        # Fetch full user details
        user = await self.storage.get_local_user_by_id(UUID(identity["sub"]), schema="users")
        
        if not user:
            raise ValueError("User not found")
        
        return {
            "sub": str(user.get("id")),
            "email": user.get("email"),
            "username": user.get("username"),
            "email_verified": True,  # Local users are pre-verified
            "name": user.get("email"),  # Use email as display name
        }
    
    def _generate_access_token(self, user: Dict[str, Any]) -> str:
        """Generates a JWT access token for a user."""
        now = datetime.now(timezone.utc)
        claims = {
            "sub": str(user.get("id")),
            "email": user.get("email"),
            "username": user.get("username"),
            "name": user.get("email"),
            "iat": now,
            "exp": now + timedelta(seconds=self.access_token_ttl),
            "token_type": "access",
            "iss": "dynastore-local"
        }
        
        return jwt.encode(claims, self.jwt_secret, algorithm=self.jwt_algorithm)
    
    def _generate_refresh_token(self, user: Dict[str, Any]) -> str:
        """Generates a JWT refresh token for a user."""
        now = datetime.now(timezone.utc)
        claims = {
            "sub": str(user.get("id")),
            "iat": now,
            "exp": now + timedelta(seconds=self.refresh_token_ttl),
            "token_type": "refresh",
            "iss": "dynastore-local"
        }
        
        return jwt.encode(claims, self.jwt_secret, algorithm=self.jwt_algorithm)
    
    async def create_user(self, username: str, password: str, email: str) -> UUID:
        """
        Creates a new local user.
        
        Args:
            username: Username
            password: Plain text password
            email: Email address
            
        Returns:
            User UUID
        """
        # Hash password
        password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        
        # Create user
        user_id = uuid4()
        await self.storage.create_local_user(
            user_id=user_id,
            username=username,
            password_hash=password_hash,
            email=email,
            schema="users"
        )
        
        return user_id
