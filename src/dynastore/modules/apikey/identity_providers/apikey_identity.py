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

"""API Key Identity Provider - Wraps existing API key validation logic"""

import hashlib
import logging
from typing import Optional, Dict, Any

from ..interfaces import IdentityProviderProtocol

logger = logging.getLogger(__name__)


class ApiKeyIdentityProvider(IdentityProviderProtocol):
    """
    Validates API keys and maps them to identity objects.
    This wraps the existing API key validation logic.
    """

    def __init__(self, apikey_manager=None):
        """
        Initialize with ApiKeyService instance.

        Args:
            apikey_manager: ApiKeyService instance for key validation
        """
        self.manager = apikey_manager

    def get_provider_id(self) -> str:
        """Returns the unique identifier for this provider."""
        return "apikey"

    async def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Validates an API key and returns identity information.

        Args:
            token: API key string (format: sk_xxxxx)

        Returns:
            Identity dict with provider, sub (principal_id), email, or None if invalid
        """
        if not self.manager:
            logger.warning("ApiKeyIdentitySPI: No ApiKeyService configured")
            return None

        if not token or not token.startswith("sk_"):
            return None

        try:
            # Hash the key for lookup
            key_hash = hashlib.sha256(token.encode()).hexdigest()

            # Validate via ApiKeyService (checks expiration, status, etc.)
            principal, metadata, error = await self.manager.authenticate_apikey(token)

            if error or not principal:
                logger.debug(f"API key validation failed: {error}")
                return None

            # Map to identity format
            identity = {
                "provider": "apikey",
                "sub": str(principal.id),
                "email": getattr(principal, "email", None) or principal.display_name,
                "principal_id": str(principal.id),
                "key_hash": key_hash,
                "metadata": {
                    "key_name": metadata.name if metadata else None,
                    "key_prefix": metadata.key_prefix if metadata else None,
                },
            }

            return identity

        except Exception as e:
            logger.error(f"Error validating API key: {e}")
            return None

    # API keys don't support OAuth2 flows
    async def get_authorization_url(self, redirect_uri: str, state: str) -> str:
        raise NotImplementedError("API keys do not support OAuth2 authorization flow")

    async def exchange_code_for_token(
        self, code: str, redirect_uri: str
    ) -> Dict[str, Any]:
        raise NotImplementedError("API keys do not support OAuth2 code exchange")

    async def get_user_info(self, access_token: str) -> Dict[str, Any]:
        raise NotImplementedError("API keys do not support user info endpoint")
