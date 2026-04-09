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
from .policies import PermissionProtocol


from datetime import datetime


@runtime_checkable
class IamProtocol(Protocol):
    """Protocol for Identity and Access Management."""

    storage: Any

    async def resolve_schema(
        self, catalog_id: Optional[str] = None, conn: Optional[Any] = None
    ) -> str:
        """Resolves the database schema for a catalog."""
        ...

    async def get_jwt_secret(self) -> str:
        """Retrieves or generates the active JWT secret."""
        ...

    async def get_jwks(self) -> Dict[str, Any]:
        """Returns the JWKS for token verification."""
        ...

    async def get_effective_permissions(
        self, identity: Dict[str, Any], catalog_id: str, conn: Optional[Any] = None
    ) -> Any:
        """Resolves permissions for an identity in a catalog."""
        ...

    async def authenticate_and_get_principal(
        self, identity: Dict[str, Any], target_schema: str, auto_register: bool = False
    ) -> Any:
        """Authenticates an identity and returns a principal with permissions."""
        ...

    async def authenticate_and_get_role(self, request: Any) -> Any:
        """Authenticated a request and returns effective role and principal."""
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

    async def delete_role(
        self, name: str, cascade: bool = False, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes a role."""
        ...

    async def add_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> None:
        """Adds a role hierarchy link."""
        ...

    async def remove_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Removes a role hierarchy link."""
        ...

    async def get_role_hierarchy(
        self, role_name: str, catalog_id: Optional[str] = None
    ) -> List[str]:
        """Gets the effective hierarchy for a role."""
        ...

    async def create_principal(
        self, principal: Any, catalog_id: Optional[str] = None
    ) -> Any:
        """Creates a new principal."""
        ...

    async def update_principal(
        self, principal: Any, catalog_id: Optional[str] = None
    ) -> Any:
        """Updates an existing principal."""
        ...

    async def get_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> Any:
        """Retrieves a principal by ID."""
        ...

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Any]:
        """Searches for principals."""
        ...

    async def delete_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes a principal."""
        ...

    def extract_token_from_request(self, request: Any) -> Optional[str]:
        """Extracts Bearer token from HTTP request."""
        ...

    def get_policy_service(self) -> PermissionProtocol:
        """Returns the policy service for checking permissions."""
        ...
