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

from typing import Protocol, Optional, Any, Dict, List, runtime_checkable
from uuid import UUID

@runtime_checkable
class ApiKeyProtocol(Protocol):
    """Protocol for API Key and Identity management."""
    storage: Any

    async def get_jwt_secret(self) -> str:
        """Retrieves or generates the active JWT secret."""
        ...

    async def get_jwks(self) -> Dict[str, Any]:
        """Returns the JWKS for token verification."""
        ...

    async def get_effective_permissions(self, identity: Dict[str, Any], catalog_id: str, conn: Optional[Any] = None) -> Any:
        """Resolves permissions for an identity in a catalog."""
        ...

    async def authenticate_and_get_principal(self, identity: Dict[str, Any], target_schema: str, auto_register: bool = False) -> Any:
        """Authenticates an identity and returns a principal with permissions."""
        ...

    async def authenticate_apikey(self, key: str, catalog_id: Optional[str] = None, origin: Optional[str] = None) -> Any:
        """Authenticates an API key."""
        ...

    async def authenticate_and_get_role(self, request: Any) -> Any:
        """Authenticated a request and returns effective role and principal."""
        ...

    async def exchange_token(self, api_key_hash: str, ttl_seconds: int, scoped_policy: Optional[Any] = None, catalog_id: Optional[str] = None) -> Any:
        """Exchanges an API key hash for a JWT."""
        ...

    async def refresh_token_exchange(self, refresh_token: str, ttl_seconds: int, catalog_id: Optional[str] = None) -> Any:
        """Refreshes a JWT using a refresh token."""
        ...

    async def validate_key(self, validation_req: Any, catalog_id: Optional[str] = None) -> Any:
        """Validates an API key definition."""
        ...

    async def list_roles(self, catalog_id: Optional[str] = None) -> List[Any]:
        """Lists roles in a catalog context."""
        ...

    async def create_role(self, role: Any, catalog_id: Optional[str] = None) -> Any:
        """Creates a new role."""
        ...

    async def update_role(self, role: Any, catalog_id: Optional[str] = None) -> Any:
        """Updates an existing role."""
        ...

    async def delete_role(self, name: str, cascade: bool = False, catalog_id: Optional[str] = None) -> None:
        """Deletes a role."""
        ...

    async def add_role_hierarchy(self, parent_name: str, child_name: str, catalog_id: Optional[str] = None) -> None:
        """Adds a role hierarchy link."""
        ...

    async def remove_role_hierarchy(self, parent_name: str, child_name: str, catalog_id: Optional[str] = None) -> None:
        """Removes a role hierarchy link."""
        ...

    async def get_role_hierarchy(self, role_name: str, catalog_id: Optional[str] = None) -> List[str]:
        """Gets the effective hierarchy for a role."""
        ...

    async def create_principal(self, principal: Any, catalog_id: Optional[str] = None) -> Any:
        """Creates a new principal."""
        ...

    async def update_principal(self, principal: Any, catalog_id: Optional[str] = None) -> Any:
        """Updates an existing principal."""
        ...

    async def get_principal(self, principal_id: UUID, catalog_id: Optional[str] = None) -> Any:
        """Retrieves a principal by ID."""
        ...

    async def search_principals(self, identifier: Optional[str] = None, role: Optional[str] = None, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None) -> List[Any]:
        """Searches for principals."""
        ...

    async def delete_principal(self, principal_id: UUID, catalog_id: Optional[str] = None) -> bool:
        """Deletes a principal."""
        ...

    async def create_key(self, key_req: Any, catalog_id: Optional[str] = None) -> Any:
        """Creates a new API key."""
        ...

    async def search_keys(self, principal_identifier: Optional[str] = None, status_filter: Any = None, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None) -> List[Any]:
        """Searches for API keys."""
        ...

    async def invalidate_key(self, key_hash: str, catalog_id: Optional[str] = None) -> bool:
        """Invalidates an API key."""
        ...

    async def delete_api_key(self, key_hash: str, catalog_id: Optional[str] = None) -> bool:
        """Deletes an API key permanently."""
        ...

    async def get_system_admin_key(self) -> str:
        """Retrieves the system admin key (bootstrap)."""
        ...

    async def get_usage_status(self, principal: Any, api_key_hash: Optional[str] = None, catalog_id: Optional[str] = None) -> Dict[str, Any]:
        """Returns usage and quota information."""
        ...

    def extract_token_from_request(self, request: Any) -> Optional[str]:
        """Extracts Bearer/ApiKey token from HTTP request."""
        ...

    def get_policy_manager(self) -> Any:
        """Returns the policy manager for checking permissions."""
        ...
