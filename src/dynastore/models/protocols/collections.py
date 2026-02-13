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
Collection-related protocol definitions.
"""

from typing import Protocol, Optional, Any, List, Dict, Set, Union, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.models.shared_models import Collection, CollectionUpdate
    from dynastore.modules.catalog.catalog_config import CollectionPluginConfig


@runtime_checkable
class CollectionsProtocol(Protocol):
    """
    Protocol for collection management operations.
    """

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        db_resource: Optional[Any] = None
    ) -> Optional["Collection"]:
        """
        Retrieves a collection by ID.
        """
        ...

    async def create_collection(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], "Collection"],
        lang: str = "en",
        db_resource: Optional[Any] = None,
        **kwargs
    ) -> "Collection":
        """
        Creates a new collection.
        """
        ...

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Union[Dict[str, Any], "CollectionUpdate"],
        lang: str = "en",
        db_resource: Optional[Any] = None
    ) -> Optional["Collection"]:
        """
        Updates an existing collection.
        """
        ...

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        db_resource: Optional[Any] = None
    ) -> bool:
        """
        Deletes a collection.
        """
        ...

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        db_resource: Optional[Any] = None
    ) -> bool:
        """
        Deletes a specific language translation for a collection.
        """
        ...

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        db_resource: Optional[Any] = None
    ) -> List[Any]:
        """
        Lists all collections in a catalog.
        """
        ...

    # === Collection Localized Helpers ===

    async def create_collection_localized(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], "Collection"],
        lang: str,
        db_resource: Optional[Any] = None
    ) -> "Collection":
        """
        Creates a collection with single-language content.
        """
        ...

    async def create_collection_multilanguage(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], "Collection"],
        db_resource: Optional[Any] = None
    ) -> "Collection":
        """
        Creates a collection with multilanguage content.
        """
        ...

    async def get_collection_localized(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        db_resource: Optional[Any] = None
    ) -> "Collection":
        """
        Retrieves a collection with content resolved to a specific language.
        """
        ...

    async def get_collection_multilanguage(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None
    ) -> "Collection":
        """
        Retrieves a collection with all available language translations.
        """
        ...

    # === Metadata and Physical Resolution ===

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None
    ) -> Optional[str]:
        """
        Resolves the physical table name for a collection.
        """
        ...

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[Any] = None
    ) -> None:
        """
        Sets the physical table name for a collection.
        """
        ...

    async def get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None
    ) -> "CollectionPluginConfig":
        """Retrieves the configuration for a collection."""
        ...

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None
    ) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        ...
