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

from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class MovingFeaturesProtocol(Protocol):
    """Protocol for the OGC API - Moving Features service."""

    async def list_moving_features(
        self, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0
    ) -> List[Any]:
        """Lists moving features for a collection."""
        ...

    async def get_moving_feature(
        self, catalog_id: str, collection_id: str, mf_id: Any
    ) -> Optional[Any]:
        """Retrieves a single moving feature."""
        ...

    async def list_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: Any,
        dt_start: Optional[Any] = None,
        dt_end: Optional[Any] = None,
    ) -> List[Any]:
        """Lists temporal geometry sequences for a moving feature."""
        ...
