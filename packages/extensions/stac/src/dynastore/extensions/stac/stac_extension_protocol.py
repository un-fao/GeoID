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

"""
STAC Extension Protocol - Defines interface for STAC-aware extensions.

Extensions implementing this protocol can contribute platform-managed assets
and extensions to STAC Items and Collections.
"""

from typing import Protocol, List, Set, runtime_checkable, Literal, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pystac


@runtime_checkable
class StacExtensionProtocol(Protocol):
    """
    Protocol for extensions that contribute STAC assets and metadata.
    
    Platform-managed assets are dynamically generated based on available
    services (Features, Maps, Tiles, etc.). External/user-provided assets
    are stored in the stac_items_sidecar.
    """
    
    def can_provide_assets(
        self,
        target_type: Literal["Item", "Collection", "Catalog"],
        context: "StacExtensionContext"
    ) -> bool:
        """
        Check if this extension can provide assets for the given context.
        
        Args:
            target_type: Type of STAC object
            context: Context with catalog, collection, and request info
            
        Returns:
            True if this extension can contribute assets
        """
        ...
    
    async def add_assets_to_item(
        self,
        item: "pystac.Item",
        context: "StacExtensionContext"
    ) -> None:
        """
        Add platform-managed assets to a STAC Item.
        
        Args:
            item: STAC Item to enhance
            context: Context with catalog, collection, and request info
        """
        ...
    
    async def add_assets_to_collection(
        self,
        collection: "pystac.Collection",
        context: "StacExtensionContext"
    ) -> None:
        """
        Add platform-managed assets to a STAC Collection.
        
        Args:
            collection: STAC Collection to enhance
            context: Context with catalog and request info
        """
        ...
    
    def get_stac_extensions(self) -> List[str]:
        """
        Return STAC extension URIs this provider implements.
        
        These are platform-managed extensions that will be added to
        the stac_extensions array.
        
        Returns:
            List of extension URIs (e.g., ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"])
        """
        ...
    
    def get_managed_asset_keys(self) -> Set[str]:
        """
        Return asset keys managed by this provider.
        
        Used during pruning to identify which assets are platform-generated
        and should not be stored in the sidecar.
        
        Returns:
            Set of asset keys (e.g., {"geojson", "thumbnail"})
        """
        ...


class StacExtensionContext:
    """
    Context passed to STAC extension providers.
    
    Contains all information needed to generate assets and metadata.
    """
    
    def __init__(
        self,
        base_url: str,
        catalog_id: str,
        collection_id: str,
        item_id: Optional[str] = None,
        geoid: Optional[str] = None,
        lang: str = "en",
        **kwargs
    ):
        self.base_url = base_url
        self.catalog_id = catalog_id
        self.collection_id = collection_id
        self.item_id = item_id
        self.geoid = geoid
        self.lang = lang
        self.extra = kwargs
