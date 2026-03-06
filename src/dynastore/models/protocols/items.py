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

from typing import (
    Protocol,
    Optional,
    Any,
    List,
    Dict,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
    AsyncIterator,
    Tuple,
)

from dynastore.models.query_builder import QueryRequest, QueryResponse
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.ogc import Feature, FeatureCollection


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
        items: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        db_resource: Optional[Any] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Feature, List[Feature]]:
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
        db_resource: Optional[Any] = None,
        lang: str = "en",
        context: Optional[Any] = None,
    ) -> Optional[Feature]:
        """
        Retrieves a specific version of an item by its internal geoid.
        """
        ...

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource: Optional[Any] = None,
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
        db_resource: Optional[Any] = None,
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
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """
        Search and retrieve items using optimized query generation.
        """
        ...

    async def get_features(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[Any] = None,
        item_ids: Optional[List[str]] = None,
        asset_id: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        geometry: Optional[Any] = None,
        where: Optional[str] = None,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0,
        sort_by: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> List[Feature]:
        """
        Retrieves a list of items using unified logic.
        """
        ...

    async def get_features_query(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        params: Dict[str, Any],
        param_suffix: str = "",
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Exposes the underlying query generation for features (used by Tiles/MVT).
        Returns raw SQL string and bind params.
        """
        ...

    async def get_features_count(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[Any] = None,
        item_ids: Optional[List[str]] = None,
        asset_id: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        geometry: Optional[Any] = None,
        where: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> int:
        """
        Retrieves total count of items matching criteria.
        """
        ...

    async def stream_features(
        self,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Optional[Any] = None,
        item_ids: Optional[List[str]] = None,
        asset_id: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        geometry: Optional[Any] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        sort_by: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> QueryResponse:
        """
        Streams features using unified logic.
        Returns a context object containing the stream and metadata.
        """
        ...

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        db_resource: Optional[Any] = None,
    ) -> QueryResponse:
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

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns a dictionary of field definitions for the collection.
        Aggregates fields from the physical table introspection and configured sidecars.
        
        Returns:
            Dict[str, FieldDefinition]
        """
        ...

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Optional[Any] = None,
    ) -> Feature:
        """
        Transforms a database row into a GeoJSON Feature object.
        
        Delegates to sidecars in configuration order to populate the feature.
        Each sidecar contributes its portion (geometry, attributes, STAC metadata, etc.)
        based on its configuration and specific behavior.
        
        Args:
            row: The database row as a dictionary (from SQLAlchemy or dict)
            col_config: Collection configuration containing sidecar definitions
            include_internal: If True, internal metadata columns are preserved in properties
            
        Returns:
            A GeoJSON Feature object with contributions from all sidecars
        """
        ...

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns the composed JSON Schema for the collection's Feature output.
        Aggregated from all active sidecars via QueryOptimizer.
        """
        ...

