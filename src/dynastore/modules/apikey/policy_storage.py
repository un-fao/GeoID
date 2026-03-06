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

# File: dynastore/modules/apikey/policy_storage.py

import abc
from typing import Optional, List, Any
from .models import Policy

class AbstractPolicyStorage(abc.ABC):
    """
    Abstract interface for Access Policy storage.
    Optimized for high-scale retrieval using partitioning keys.
    """

    # No initialize() or shutdown() - use lifespan() if needed.
    
    @abc.abstractmethod
    async def create_policy(self, policy: Policy, conn: Optional[Any] = None, schema: str = "apikey") -> Policy:
        """
        Persist a new access policy.
        """
        ...

    @abc.abstractmethod
    async def get_policy(self, policy_id: str, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Policy]:
        """
        Retrieve a single policy by ID.
        """
        ...

    @abc.abstractmethod
    async def update_policy(self, policy: Policy, conn: Optional[Any] = None, schema: str = "apikey") -> Optional[Policy]:
        """
        Update an existing policy. Returns the updated policy or None if not found.
        """
        ...

    @abc.abstractmethod
    async def delete_policy(self, policy_id: str, conn: Optional[Any] = None, schema: str = "apikey") -> bool:
        """
        Delete a policy by ID. Returns True if deleted, False if not found.
        """
        ...

    @abc.abstractmethod
    async def list_policies(self, partition_key: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "apikey") -> List[Policy]:
        """
        List policies, optionally filtering by the partition key.
        """
        ...

    @abc.abstractmethod
    async def search_policies(self, resource_pattern: Optional[str] = None, action_pattern: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[Any] = None, schema: str = "apikey") -> List[Policy]:
        """
        Search policies by partial patterns.
        """
        ...

    @abc.abstractmethod
    async def ensure_policy_partition(self, conn: Any, partition_key: str, schema: str = "apikey"):
        """Ensures a partition exists for the given key."""
        ...