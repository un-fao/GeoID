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

import logging
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, status, Request, Query
from fastapi.responses import JSONResponse, Response

from dynastore.extensions import dynastore_extension
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.extensions.tools.exception_handlers import handle_exception
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules import get_protocol
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_manager import ConfigRegistry, enforce_config_immutability
from dynastore.modules.db_config.exceptions import ImmutableConfigError, PluginNotRegisteredError, ConfigValidationError, is_conflict_error

logger = logging.getLogger(__name__)

@dynastore_extension
class ConfigsService(ExtensionProtocol):
    """
    Unified Configuration Extension.
    Manages the configuration endpoints for all modules/extensions across hierarchies.
    """
    router: APIRouter = APIRouter(prefix="/configs", tags=["Configurations"])

    def __init__(self, app):
        self.app = app

    # --- Discovery Endpoint ---

    @router.get("/plugins", summary="List all registered configuration plugins", description="""
Lists all registered configuration `plugin_id`s. This is the primary way to discover what can be configured.
\n\n
Use the `with_schema=true` query parameter to include the full JSON schema for each plugin's configuration model, along with its description. This is useful for building dynamic UIs or for documentation.
    """)
    async def list_registered_plugins(with_schema: bool = Query(False, description="If true, includes the JSON schema for each configuration model.")):
        """
        Lists all registered configuration plugin IDs.
        This endpoint provides a discoverable list of all possible `plugin_id`
        values that can be used in other configuration endpoints.
        """
        plugins = ConfigRegistry._registry
        if with_schema:
            return JSONResponse(content={
                plugin_id: {
                    "description": model.__doc__ or "No description provided.",
                    "schema": model.model_json_schema()
                }
                for plugin_id, model in plugins.items()
            })
        return list(plugins.keys())



    # --- Configuration Listing Endpoints ---

    @router.get("/", summary="List all platform-level configurations", description="""
Retrieves a paginated list of all configurations that have been *explicitly set* at the global **Platform** level. This does not show inherited code defaults.
    """)
    async def list_platform_configs(
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set at the global platform level.
        """
        config_manager = get_protocol(ConfigsProtocol)
        return await config_manager.list_configs(limit=limit, offset=offset)

    @router.get("/catalogs/{catalog_id}/configs", summary="List all catalog-level configurations", description="""
Retrieves a paginated list of all configurations that have been *explicitly set* for a specific catalog. This does not show inherited platform defaults.
    """)
    async def list_catalog_configs(
        catalog_id: str,
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set for a specific catalog.
        """
        config_manager = get_protocol(ConfigsProtocol)
        return await config_manager.list_configs(catalog_id=catalog_id, limit=limit, offset=offset)

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/configs", summary="List all collection-level configurations", description="""
Retrieves a paginated list of all configurations that have been *explicitly set* for a specific collection. This does not show inherited catalog or platform defaults.
    """)
    async def list_collection_configs(
        catalog_id: str,
        collection_id: str,
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set for a specific collection.
        """
        config_manager = get_protocol(ConfigsProtocol)
        return await config_manager.list_configs(catalog_id=catalog_id, collection_id=collection_id, limit=limit, offset=offset)


    # --- Specific Plugin Configuration (GET/PUT) ---

    # Collection Level

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}", summary="Get effective configuration for a collection", description="""
Retrieves the effective (fully resolved) configuration for a specific plugin on a collection.
\n\n
The configuration is resolved by checking for overrides at each level in the following order of precedence:
1. **Collection Level**
2. **Catalog Level**
3. **Platform Level**
4. **Code Default** (from the Pydantic model)
    """)
    async def get_collection_config(catalog_id: str, collection_id: str, plugin_id: str):
        """
        Retrieves the effective configuration for a specific plugin (extension) on a collection.
        Resolves hierarchy: Collection > Catalog > Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(status_code=404, detail=f"Configuration plugin '{plugin_id}' is not registered.")

            config_manager = get_protocol(ConfigsProtocol).get_config_manager()
            # This call uses the JIT logic: it returns the resolved config (default if nothing else exists)
            config = await config_manager.get_config(plugin_id, catalog_id, collection_id)
            return config
        except Exception as e:
            logger.error(f"Error fetching collection config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @router.put("/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}", summary="Set or update a collection-level configuration", description="""
Sets or updates the configuration for a plugin specifically at the **Collection level**.
\n\n
This operation will fail with a `409 Conflict` error if it attempts to modify a field that is marked as `Immutable` in the configuration model and an existing configuration is already present.
    """)
    async def update_collection_config(catalog_id: str, collection_id: str, plugin_id: str, body: Dict[str, Any]):
        """
        Overrides the configuration for a specific plugin at the Collection level.
        This writes to the 'collection_configs' table.
        """
        try:
            config_manager: ConfigsProtocol = get_protocol(ConfigsProtocol)
            validated_config = await config_manager.set_config(plugin_id, body, catalog_id, collection_id)
            return validated_config
        except Exception as e:
            raise handle_exception(e, resource_name="Collection Config", resource_id=f"{catalog_id}:{collection_id}:{plugin_id}", operation="Collection configuration update")

    @router.delete("/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}", summary="Delete a collection-level configuration", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_collection_config(catalog_id: str, collection_id: str, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Collection level.
        The effective configuration will revert to Catalog, Platform, or Code defaults.
        """
        config_manager = get_protocol(ConfigsProtocol)
        await config_manager.delete_config(plugin_id, catalog_id=catalog_id, collection_id=collection_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Catalog Level

    @router.get("/catalogs/{catalog_id}/plugins/{plugin_id}", summary="Get effective configuration for a catalog", description="""
Retrieves the effective (resolved) configuration for a plugin on a specific catalog.
\n\n
The configuration is resolved by checking for overrides at each level in the following order of precedence:
1. **Catalog Level**
2. **Platform Level**
3. **Code Default** (from the Pydantic model)
    """)
    async def get_catalog_config(catalog_id: str, plugin_id: str):
        """
        Retrieves the configuration for a plugin at the Catalog level.
        Resolves hierarchy: Catalog > Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(status_code=404, detail=f"Configuration plugin '{plugin_id}' is not registered.")

            config_manager: ConfigsProtocol = get_protocol(ConfigsProtocol)
            # Passing None for collection_id triggers resolution up to Catalog level
            config = await config_manager.get_config(plugin_id, catalog_id, collection_id=None)
            return config
        except Exception as e:
            logger.error(f"Error fetching catalog config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @router.put("/catalogs/{catalog_id}/plugins/{plugin_id}", summary="Set or update a catalog-level configuration", description="""
Sets or updates the configuration for a plugin specifically at the **Catalog level**.
\n\n
This operation will fail with a `409 Conflict` error if it attempts to modify a field that is marked as `Immutable` in the configuration model and an existing configuration is already present.
    """)
    async def update_catalog_config(catalog_id: str, plugin_id: str, body: Dict[str, Any]):
        """
        Overrides the configuration for a plugin at the Catalog level.
        """
        try:
            config_manager: ConfigsProtocol = get_protocol(ConfigsProtocol)
            validated_config = await config_manager.set_config(plugin_id, body, catalog_id, collection_id=None)
            return validated_config
        except Exception as e:
            raise handle_exception(e, resource_name="Catalog Config", resource_id=f"{catalog_id}:{plugin_id}", operation="Catalog configuration update")

    @router.delete("/catalogs/{catalog_id}/plugins/{plugin_id}", summary="Delete a catalog-level configuration", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_catalog_config(catalog_id: str, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Catalog level.
        The effective configuration will revert to Platform or Code defaults.
        """
        config_manager = get_protocol(ConfigsProtocol)
        await config_manager.delete_config(plugin_id, catalog_id=catalog_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Platform Level

    @router.get("/plugins/{plugin_id}", summary="Get effective platform-level configuration", description="""
Retrieves the global platform default configuration for a plugin.
\n\n
The configuration is resolved by checking for overrides at each level in the following order of precedence:
1. **Platform Level**
2. **Code Default** (from the Pydantic model)
    """)
    async def get_platform_config(plugin_id: str):
        """
        Retrieves the global platform default configuration for a plugin.
        Resolves hierarchy: Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(status_code=404, detail=f"Configuration plugin '{plugin_id}' is not registered.")

            config_manager: ConfigsProtocol = get_protocol(ConfigsProtocol)
            config = await config_manager.get_config(plugin_id, catalog_id=None, collection_id=None)
            return config
        except Exception as e:
            logger.error(f"Error fetching platform config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @router.put("/plugins/{plugin_id}", summary="Set or update the global platform configuration", description="""
Sets or updates the global platform default configuration for a plugin.
\n\n
This operation will fail with a `409 Conflict` error if it attempts to modify a field that is marked as `Immutable` in the configuration model and an existing configuration is already present.
    """)
    async def update_platform_config(plugin_id: str, body: Dict[str, Any]):
        """
        Sets the global platform default configuration for a plugin.
        """
        model_class = ConfigRegistry.get_model(plugin_id)
        if not model_class:
            raise HTTPException(status_code=404, detail=f"Configuration plugin '{plugin_id}' is not registered.")

        try:
            config_manager: ConfigsProtocol = get_protocol(ConfigsProtocol)
            validated_config = await config_manager.set_config(plugin_id, body, catalog_id=None, collection_id=None)
            return validated_config
        except Exception as e:
            raise handle_exception(e, resource_name="Platform Config", resource_id=plugin_id, operation="Platform configuration update")

    @router.delete("/plugins/{plugin_id}", summary="Delete a global platform configuration", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_platform_config(plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Platform level.
        The effective configuration will revert to the Code defaults.
        """
        config_manager = get_protocol(ConfigsProtocol)
        await config_manager.delete_config(plugin_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Search ---

    @router.get("/catalogs/{catalog_id}/search", summary="Search configurations for a catalog", description="""
Searches across all configurations (catalog and collection level) for a specific catalog.
\n\n
Optional `q` parameter matches against `plugin_id`.
    """)
    async def search_catalog_configs(
        catalog_id: str,
        q: Optional[str] = Query(None, alias="q", description="Filter by plugin name (LIKE search)"),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ):
        """Searches configurations for a catalog."""
        config_manager = get_protocol(ConfigsProtocol)
        return await config_manager.search(query=q, catalog_id=catalog_id, limit=limit, offset=offset)

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/search", summary="Search configurations for a collection", description="""
Searches all configurations at the collection level for a specific collection.
\n\n
Optional `q` parameter matches against `plugin_id`.
    """)
    async def search_collection_configs(
        catalog_id: str,
        collection_id: str,
        q: Optional[str] = Query(None, alias="q", description="Filter by plugin name (LIKE search)"),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ):
        """Searches configurations for a collection."""
        config_manager = get_protocol(ConfigsProtocol)
        return await config_manager.search(query=q, catalog_id=catalog_id, collection_id=collection_id, limit=limit, offset=offset)