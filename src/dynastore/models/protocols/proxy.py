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

from typing import Protocol, Optional, Any, runtime_checkable
from datetime import datetime

@runtime_checkable
class ProxyProtocol(Protocol):
    """Protocol for short URL proxying and analytics."""

    async def create_short_url(self, engine: Any, catalog_id: str, long_url: str, custom_key: Optional[str] = None, collection_id: Optional[str] = None, comment: Optional[str] = None) -> str:
        """Public API function to create a short URL."""
        ...

    async def get_long_url(self, engine: Any, catalog_id: str, short_key: str) -> Optional[str]:
        """Public API function to retrieve a long URL."""
        ...

    async def log_redirect(self, engine: Any, catalog_id: str, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime) -> None:
        """Public API function to log a redirect event."""
        ...
