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
Asset management protocol definitions.
"""

from typing import Protocol, Optional, Any, List, Dict, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.catalog.asset_manager import Asset, AssetUploadDefinition, AssetUpdate, AssetFilter


@runtime_checkable
class AssetsProtocol(Protocol):
    """
    Protocol for asset management operations, enabling decoupled access to asset
    CRUD operations without direct dependency on AssetManager.
    
    This protocol is used by extensions and services to interact with assets
    in a loosely-coupled manner, supporting the protocol-based discovery pattern.
    """
    
    async def get_asset(
        self,
        asset_id: str,
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> "Asset":
        """
        Retrieves an asset by ID.
        
        Args:
            asset_id: The asset ID to retrieve
            catalog_id: The catalog ID containing the asset
            collection_id: Optional collection ID for scoping
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
            Asset model instance
            
        Raises:
            ValueError: If asset not found
        """
        ...
    
    async def list_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[Any] = None
    ) -> List["Asset"]:
        """
        Lists assets in a catalog/collection.
        
        Args:
            catalog_id: The catalog ID to list assets from
            collection_id: Optional collection ID for scoping
            limit: Maximum number of assets to return
            offset: Offset for pagination
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
            List of Asset model instances
        """
        ...
    
    async def create_asset(
        self,
        asset: "AssetUploadDefinition",
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> "Asset":
        """
        Creates a new asset.
        
        Args:
            asset: Asset definition to create
            catalog_id: The catalog ID to create the asset in
            collection_id: Optional collection ID for scoping
            db_resource: Optional database resource for transaction-aware operations
            
        Returns:
            Created Asset model instance
        """
        ...
    
    async def update_asset(
        self,
        asset_id: str,
        update: "AssetUpdate",
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> "Asset":
        """
        Updates an existing asset.
        
        Args:
            asset_id: The asset ID to update
            update: Asset update data
            catalog_id: The catalog ID containing the asset
            collection_id: Optional collection ID for scoping
            db_resource: Optional database resource for transaction-aware operations
            
        Returns:
            Updated Asset model instance
        """
        ...
    
    async def delete_asset(
        self,
        asset_id: str,
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> None:
        """
        Deletes an asset.
        
        Args:
            asset_id: The asset ID to delete
            catalog_id: The catalog ID containing the asset
            collection_id: Optional collection ID for scoping
            db_resource: Optional database resource for transaction-aware operations
        """
        ...

    async def delete_assets(
        self,
        catalog_id: str,
        asset_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        hard: bool = False,
        propagate: bool = False,
        db_resource: Optional[Any] = None
    ) -> int:
        """
        Deletes assets matching criteria. Supports soft and hard deletes.
        
        Args:
            catalog_id: The catalog ID
            asset_id: Optional asset ID to delete specific asset
            collection_id: Optional collection ID to delete assets in collection
            hard: If True, performs physical delete. If False, soft delete.
            propagate: If True, propagates delete to storage/external systems.
            db_resource: Optional database resource.
            
        Returns:
            Number of assets deleted.
        """
        ...
    
    async def search_assets(
        self,
        catalog_id: str,
        filters: List["AssetFilter"],
        limit: int = 10,
        offset: int = 0,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> List["Asset"]:
        """
        Searches for assets using filters.
        
        Args:
            catalog_id: The catalog ID to search in
            filters: List of filters to apply
            limit: Maximum number of assets to return
            offset: Offset for pagination
            collection_id: Optional collection ID for scoping
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
        """
        ...

    async def ensure_asset_cleanup_trigger(
        self,
        schema: str,
        table: str,
        db_resource: Optional[Any] = None
    ) -> None:
        """
        Ensures that the asset cleanup trigger is present on the specified table.
        """
        ...
