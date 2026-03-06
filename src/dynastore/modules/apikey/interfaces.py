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
#    Company: FAO, Viale delle Terme d' Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/modules/apikey/interfaces.py

from typing import Optional, List, Dict, Any, Protocol, runtime_checkable
from dynastore.models.auth import Principal
from dynastore.models.auth_models import IdentityLink

@runtime_checkable
class IdentityProviderProtocol(Protocol):
    """
    Protocol for Authentication Providers.
    Supports both stateless validation (JWT) and stateful flows (OAuth2 Code Grant).
    """

    def get_provider_id(self) -> str:
        """Returns the unique identifier for this provider (e.g., 'keycloak-prod', 'local-db')."""
        ...

    # --- Token Validation (Resource Server Role) ---

    async def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Validates a raw token (Access Token).
        Returns a standardized Identity Dictionary:
        {
            "sub": "immutable_id",
            "email": "user@example.com",
            "roles": ["global_role_1"], # Optional claims
            "provider": "google"
        }
        Returns None if invalid.
        """
        ...

    # --- Login Flow Support (Auth Server Role - Optional) ---
    # These methods are used when DynaStore needs to act as the Auth Broker
    # or when implementing the "Pseudo-Login" flow on-premise.

    async def get_authorization_url(self, redirect_uri: str, state: str) -> str:
        """Returns the URL to redirect the user's browser to for login."""
        ...

    async def exchange_code_for_token(self, code: str, redirect_uri: str) -> Dict[str, Any]:
        """Exchanges an authorization code for an Access Token / ID Token."""
        ...

    async def get_user_info(self, access_token: str) -> Dict[str, Any]:
        """
        Fetches user profile from the UserInfo endpoint using the Access Token.
        Crucial for the 'Pseudo-Login' flow.
        """
        ...


@runtime_checkable
class AuthorizationStorageProtocol(Protocol):
    """
    Interface for persistence of IAG entities (Principals, Roles).
    Must be schema-aware for Multi-Tenancy.
    """

    async def get_principal_by_identity(self, provider: str, subject_id: str, schema: str) -> Optional[Principal]:
        """
        Resolves an external Identity (e.g., 'bob@fao.org') to an internal Principal 
        within a specific tenant schema (e.g., 'catalog_a').
        """
        ...

    async def create_principal_link(self, principal: Principal, identity: Dict[str, Any], schema: str) -> Principal:
        """
        Registers a new Principal in the schema and links it to the Identity.
        Used for 'Auto-Registration' or 'Invite' flows.
        """
        ...

    async def get_effective_roles(self, principal_id: str, schema: str) -> List[str]:
        """
        Returns the flattened list of Role IDs for a principal, 
        resolving inheritance (e.g., Admin -> Editor -> Viewer).
        """
        ...

@runtime_checkable
class PolicyValidatorProtocol(Protocol):
    """
    Interface for the Authorization Decision Point.
    """
    
    def evaluate(self, principal: Principal, action: str, resource: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """
        Evaluates permissions based on Policies linked to the Principal's Roles.
        """
        ...