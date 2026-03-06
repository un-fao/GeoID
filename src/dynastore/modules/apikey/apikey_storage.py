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

import abc
from datetime import datetime
from typing import List, Any, Optional
from uuid import UUID
from .models import Principal, ApiKey, ApiKeyCreate, ApiKeyStatusFilter, Role, RefreshToken

# --- Storage Implementation ---

class AbstractApiKeyStorage(abc.ABC):
    """
    Abstract interface for API Key storage.
    
    The 'conn' parameter is typed as Optional[Any] to allow for:
    1. SQL implementations receiving a DbResource/Connection for transactions.
    2. NoSQL/Memory implementations ignoring it or receiving a different client type.
    3. Middleware usage where the storage manages its own connection context internally.
    """

    # No initialize() or shutdown() - use lifespan() if needed.
    
    @abc.abstractmethod
    async def create_principal(self, principal: Principal, conn: Optional[Any] = None, schema: str = "apikey") -> Principal: ...
    
    @abc.abstractmethod
    async def get_principal(self, principal_id: UUID, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Principal]: ...
    
    @abc.abstractmethod
    async def update_principal(self, principal: Principal, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Principal]: ...

    @abc.abstractmethod
    async def get_principal_by_identifier(self, identifier: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Principal]: ...
    
    @abc.abstractmethod
    async def delete_principal(self, principal_id: UUID, conn: Optional[Any] = None, schema: str = "apikey") -> bool: ...
    
    @abc.abstractmethod
    async def create_api_key(self, key_data: ApiKeyCreate, conn: Optional[Any] = None, schema: str = "apikey") -> tuple[ApiKey, str]: ...
    
    @abc.abstractmethod
    async def delete_api_key(self, key_hash: str, conn: Optional[Any] = None, schema: str = "apikey") -> bool: ...

    @abc.abstractmethod
    async def get_key_metadata(self, key_hash: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[ApiKey]: ...
    
    @abc.abstractmethod
    async def list_keys_for_principal(self, principal_id: UUID, conn: Optional[Any] = None, schema: str = "apikey") -> List[ApiKey]: ...
    
    @abc.abstractmethod
    async def get_principal_id_by_identifier(self, identifier: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[UUID]: ...

    @abc.abstractmethod
    async def list_principals(self, offset: int, limit: int, conn: Optional[Any] = None, schema: str = "apikey") -> List[Principal]: ...

    @abc.abstractmethod
    async def increment_usage(self, key_hash: str, period_start: datetime, amount: int = 1, last_access: Optional[datetime] = None, conn: Optional[Any] = None, schema: str = "apikey"): ...

    @abc.abstractmethod
    async def get_usage(self, key_hash: str, period_start: datetime, conn: Optional[Any] = None, schema: str = "apikey") -> int: ...

    @abc.abstractmethod
    async def regenerate_api_key(self, key_hash: str, conn: Optional[Any] = None, schema: str = "apikey") -> tuple[Optional[ApiKey], Optional[str]]: ...

    # --- Search & Management Extensions ---

    @abc.abstractmethod
    async def search_principals(self, identifier: Optional[str] = None, role: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "apikey") -> List[Principal]:
        """Search principals by identifier pattern or role."""
        ...

    @abc.abstractmethod
    async def search_keys(self, principal_id: Optional[UUID] = None, status_filter: ApiKeyStatusFilter = ApiKeyStatusFilter.ALL, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "apikey") -> List[ApiKey]:
        """Search API keys by owner ID or status."""
        ...

    @abc.abstractmethod
    async def invalidate_api_key(self, key_hash: str, conn: Optional[Any] = None, schema: str = "apikey") -> bool:
        """Sets is_active=False without deleting the record (Revocation)."""
        ...

    # --- Role & Hierarchy Management ---

    @abc.abstractmethod
    async def create_role(self, role: Role, conn: Optional[Any] = None, schema: str = "apikey") -> Role: ...

    @abc.abstractmethod
    async def get_role(self, name: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Role]: ...

    @abc.abstractmethod
    async def list_roles(self, conn: Optional[Any] = None, schema: str = "apikey") -> List[Role]: ...

    @abc.abstractmethod
    async def update_role(self, role: Role, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Role]: ...

    @abc.abstractmethod
    async def delete_role(self, name: str, cascade: bool = False, conn: Optional[Any] = None, schema: str = "apikey") -> bool: ...

    @abc.abstractmethod
    async def add_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "apikey"): ...

    @abc.abstractmethod
    async def get_role_hierarchy(self, role_names: List[str], conn: Optional[Any] = None, schema: str = "apikey") -> List[str]: ...

    @abc.abstractmethod
    async def remove_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "apikey") -> bool: ...

    # --- Refresh Token Management ---

    @abc.abstractmethod
    async def create_refresh_token(self, token: RefreshToken, conn: Optional[Any] = None, schema: str = "apikey") -> RefreshToken: ...

    @abc.abstractmethod
    async def get_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[RefreshToken]: ...

    @abc.abstractmethod
    async def invalidate_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "apikey") -> bool: ...

    @abc.abstractmethod
    async def run_maintenance(self, conn: Optional[Any] = None, schema: str = "apikey") -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        ...

    @abc.abstractmethod
    async def get_catalogs_for_identity(self, provider: str, subject_id: str) -> List[str]:
        """Get list of catalog IDs where an identity has roles."""
        ...

    @abc.abstractmethod
    async def get_principal_by_identity(
        self, provider: str, subject_id: str, schema: str, conn: Optional[Any] = None
    ) -> Optional[Principal]:
        """Resolves an external identity to a principal."""
        ...

    @abc.abstractmethod
    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: str = "apikey",
    ) -> bool:
        """Links a principal to an external identity."""
        ...