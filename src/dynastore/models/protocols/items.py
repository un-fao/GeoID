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

"""
Item-related protocol definitions.
"""

from typing import Protocol, Optional, Any, List, Dict, Union, runtime_checkable, TYPE_CHECKING, AsyncIterator

from dynastore.models.query_builder import QueryRequest
from dynastore.models.protocols.configs import ConfigsProtocol


@runtime_checkable
class ItemsProtocol(Protocol):
    """
    Protocol for all feature/item operations within a collection.
    
    Exposes primarily logical methods to avoid exposing internal 
    physical storage details to the public API.
    """

    async def upsert(
        self, 
        catalog_id: str, 
        collection_id: str, 
        items: Union[Dict[str, Any], Any], 
        db_resource: Optional[Any] = None,
        processing_context: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Create or update items (single or bulk).
        
        Args:
            catalog_id: Catalog identifier
            collection_id: Collection identifier
            items: Feature, FeatureCollection, STACItem, or raw dict/list
            db_resource: Optional database resource
            
        Returns:
            Created/Updated item(s) as dict(s) or models
        """
        ...

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        geoid: Any,
        db_resource: Optional[Any] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves a specific version of an item by its internal geoid.
        """
        ...

    async def get_item_by_external_id(
        self,
        catalog_id: str,
        collection_id: str,
        ext_id: str,
        db_resource: Optional[Any] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves the active (latest, non-deleted) version of an item 
        by its external ID.
        
        This is a logical replacement for get_active_row_by_external_id.
        """
        ...

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[Any] = None
    ) -> int:
        """
        Soft-deletes all versions of an item by its external ID.
        """
        ...

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        db_resource: Optional[Any] = None
    ) -> int:
        """
        Deletes a specific language translation for an item.
        """
        ...

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Search and retrieve items using optimized query generation.
        """
        ...

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream search results using an async iterator.
        """
        ...

    @property
    def count_items_by_asset_id_query(self) -> Any:
        """
        Query builder for counting items by asset ID.
        Used by OGC extensions for metadata generation.
        """
        ...
