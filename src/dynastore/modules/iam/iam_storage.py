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
from .models import Principal, Role, RefreshToken

# --- Storage Implementation ---

class AbstractIamStorage(abc.ABC):
    """
    Abstract interface for IAM storage.
    
    The 'conn' parameter is typed as Optional[Any] to allow for:
    1. SQL implementations receiving a DbResource/Connection for transactions.
    2. NoSQL/Memory implementations ignoring it or receiving a different client type.
    3. Middleware usage where the storage manages its own connection context internally.
    """

    # No initialize() or shutdown() - use lifespan() if needed.
    
    @abc.abstractmethod
    async def create_principal(self, principal: Principal, conn: Optional[Any] = None, schema: str = "iam") -> Principal: ...
    
    @abc.abstractmethod
    async def get_principal(self, principal_id: UUID, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Principal]: ...
    
    @abc.abstractmethod
    async def update_principal(self, principal: Principal, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Principal]: ...

    @abc.abstractmethod
    async def get_principal_by_identifier(self, identifier: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Principal]: ...
    
    @abc.abstractmethod
    async def delete_principal(self, principal_id: UUID, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def get_principal_id_by_identifier(self, identifier: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[UUID]: ...

    @abc.abstractmethod
    async def list_principals(self, offset: int, limit: int, conn: Optional[Any] = None, schema: str = "iam") -> List[Principal]: ...

    # --- Search & Management Extensions ---

    @abc.abstractmethod
    async def search_principals(self, identifier: Optional[str] = None, role: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "iam") -> List[Principal]:
        """Search principals by identifier pattern or role."""
        ...

    # --- Role & Hierarchy Management ---

    @abc.abstractmethod
    async def create_role(self, role: Role, conn: Optional[Any] = None, schema: str = "iam") -> Role: ...

    @abc.abstractmethod
    async def get_role(self, name: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Role]: ...

    @abc.abstractmethod
    async def list_roles(self, conn: Optional[Any] = None, schema: str = "iam") -> List[Role]: ...

    @abc.abstractmethod
    async def update_role(self, role: Role, conn: Optional[Any] = None, schema: str = "iam") -> Optional[Role]: ...

    @abc.abstractmethod
    async def delete_role(self, name: str, cascade: bool = False, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def add_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "iam"): ...

    @abc.abstractmethod
    async def get_role_hierarchy(self, role_names: List[str], conn: Optional[Any] = None, schema: str = "iam") -> List[str]: ...

    @abc.abstractmethod
    async def remove_role_hierarchy(self, parent_role: str, child_role: str, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    # --- Refresh Token Management ---

    @abc.abstractmethod
    async def create_refresh_token(self, token: RefreshToken, conn: Optional[Any] = None, schema: str = "iam") -> RefreshToken: ...

    @abc.abstractmethod
    async def get_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "iam") -> Optional[RefreshToken]: ...

    @abc.abstractmethod
    async def invalidate_refresh_token(self, token_id: str, conn: Optional[Any] = None, schema: str = "iam") -> bool: ...

    @abc.abstractmethod
    async def run_maintenance(self, conn: Optional[Any] = None, schema: str = "iam") -> dict:
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
        schema: str = "iam",
    ) -> bool:
        """Links a principal to an external identity."""
        ...