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
Configuration management protocol definitions (class-as-identity only).
"""

from typing import Protocol, Optional, Any, Dict, Type, TypeVar, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    from dynastore.modules.db_config.platform_config_service import PluginConfig
    from dynastore.models.driver_context import DriverContext

_T_Config = TypeVar("_T_Config", bound="PluginConfig")


@runtime_checkable
class ConfigsProtocol(Protocol):
    """
    Protocol for configuration management operations (class-as-identity only).

    ``get_config(SomeConfigClass, ...)`` — every PluginConfig subclass is its
    own identity.  Return type narrows to that class.  No string-form legacy
    id is accepted.
    """

    async def get_config(
        self,
        config_cls: Type[_T_Config],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> _T_Config:
        """
        Retrieves configuration with 4-tier waterfall:
        1. Collection (if provided)
        2. Catalog (if provided)
        3. Platform (global)
        4. Code-level Defaults
        """
        ...

    async def set_config(
        self,
        config_cls: Type["PluginConfig"],
        config: "PluginConfig",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Sets configuration at the appropriate level based on provided parameters:
        - If collection_id is provided: sets at collection level
        - If only catalog_id is provided: sets at catalog level
        - If neither is provided: sets at platform level
        """
        ...

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Lists all configurations at the specified level with pagination.
        Returns ``{"total": int, "results": list[{"class_key": str, "config": dict}]}``.
        """
        ...

    async def list_catalog_configs(
        self,
        catalog_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Returns all configurations for a catalog as a dict keyed by class_key.
        """
        ...

    async def delete_config(
        self,
        config_cls: Type["PluginConfig"],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """
        Deletes configuration at the specified level:
        - If collection_id is provided: deletes collection-level config
        - If only catalog_id is provided: deletes catalog-level config
        - If neither is provided: deletes platform-level config (acts as reset to defaults)
        """
        ...

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        """
        Searches for configurations across the hierarchy.
        ``query`` is matched against class_key (ILIKE).
        """
        ...
