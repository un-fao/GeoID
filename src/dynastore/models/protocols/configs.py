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
Configuration management protocol definitions.
"""

from typing import Protocol, Optional, Any, List, Dict, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.db_config.platform_config_manager import PluginConfig


@runtime_checkable
class ConfigsProtocol(Protocol):
    """
    Protocol for configuration management operations, enabling decoupled access
    to hierarchical configuration (platform/catalog/collection levels).
    
    This protocol is used by extensions and services to retrieve and manage
    configurations in a loosely-coupled manner, supporting the protocol-based
    discovery pattern.
    """
    
    async def get_config(
        self,
        plugin_id: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> "PluginConfig":
        """
        Retrieves configuration with 4-tier waterfall:
        1. Collection (if provided)
        2. Catalog (if provided)
        3. Platform (global)
        4. Code-level Defaults
        
        Args:
            plugin_id: The plugin/extension ID
            catalog_id: Optional catalog ID for catalog-level config
            collection_id: Optional collection ID for collection-level config
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
            PluginConfig instance
        """
        ...
    
    async def set_config(
        self,
        plugin_id: str,
        config: "PluginConfig",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        db_resource: Optional[Any] = None
    ) -> None:
        """
        Sets configuration at the appropriate level based on provided parameters:
        - If collection_id is provided: sets at collection level
        - If only catalog_id is provided: sets at catalog level
        - If neither is provided: sets at platform level
        
        Args:
            plugin_id: The plugin/extension ID
            config: Configuration to set
            catalog_id: Optional catalog ID for catalog-level config
            collection_id: Optional collection ID for collection-level config
            check_immutability: Whether to enforce immutability checks
            db_resource: Optional database resource for transaction-aware operations
        """
        ...
    
    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Lists all configurations at the specified level with pagination:
        - If collection_id is provided: lists collection-level configs
        - If only catalog_id is provided: lists catalog-level configs
        - If neither is provided: lists platform-level configs
        
        Args:
            catalog_id: Optional catalog ID
            collection_id: Optional collection ID
            limit: Pagination limit
            offset: Pagination offset
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
            Dictionary containing 'total' and 'results' (list of configuration dictionaries)
        """
        ...
    
    async def delete_config(
        self,
        plugin_id: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None
    ) -> None:
        """
        Deletes configuration at the specified level:
        - If collection_id is provided: deletes collection-level config
        - If only catalog_id is provided: deletes catalog-level config
        - If neither is provided: deletes platform-level config (acts as reset to defaults)
        
        Note: Deleting a platform config effectively resets to code-level defaults.
        
        Args:
            plugin_id: The plugin/extension ID
            catalog_id: Optional catalog ID
            collection_id: Optional collection ID
            db_resource: Optional database resource for transaction-aware operations
        """
        ...

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Searches for configurations across the hierarchy.
        
        Args:
            query: Optional search query (matches against plugin_id)
            catalog_id: Optional catalog ID filter
            collection_id: Optional collection ID filter
            limit: Pagination limit
            offset: Pagination offset
            db_resource: Optional database resource for transaction-aware queries
            
        Returns:
            Dictionary containing 'total' and 'results'
        """
        ...
