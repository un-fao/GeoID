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

from fastapi import APIRouter, HTTPException, status, Request, Query, FastAPI
from fastapi.responses import JSONResponse, Response

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.extensions.tools.exception_handlers import handle_exception
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules import get_protocol
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.db_config.platform_config_manager import (
    ConfigRegistry,
    enforce_config_immutability,
)
from dynastore.modules.db_config.exceptions import (
    ImmutableConfigError,
    PluginNotRegisteredError,
    ConfigValidationError,
    is_conflict_error,
)

logger = logging.getLogger(__name__)



class ConfigsService(ExtensionProtocol):
    priority: int = 100
    """
    Unified Configuration Extension.
    Manages the configuration endpoints for all modules/extensions across hierarchies.
    """

    def __init__(self, app: FastAPI):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/configs", tags=["Configurations"])
        self._setup_routes()

    def _setup_routes(self):
        self.router.add_api_route(
            "/plugins",
            self.list_registered_plugins,
            methods=["GET"],
            summary="List all registered configuration plugins",
        )
        self.router.add_api_route(
            "/",
            self.list_platform_configs,
            methods=["GET"],
            summary="List all platform-level configurations",
        )
        self.router.add_api_route(
            "/{plugin_id}",
            self.get_platform_config,
            methods=["GET"],
            summary="Get platform-level configuration",
        )
        self.router.add_api_route(
            "/{plugin_id}",
            self.update_platform_config,
            methods=["PUT"],
            summary="Set platform-level configuration",
        )
        self.router.add_api_route(
            "/{plugin_id}",
            self.delete_platform_config,
            methods=["DELETE"],
            summary="Delete platform-level configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/configs",
            self.list_catalog_configs,
            methods=["GET"],
            summary="List all catalog-level configurations",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/configs",
            self.list_collection_configs,
            methods=["GET"],
            summary="List all collection-level configurations",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.get_collection_config,
            methods=["GET"],
            summary="Get effective configuration for a collection",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.update_collection_config,
            methods=["PUT"],
            summary="Set or update a collection-level configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.delete_collection_config,
            methods=["DELETE"],
            summary="Delete a collection-level configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.get_catalog_config,
            methods=["GET"],
            summary="Get effective configuration for a catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.update_catalog_config,
            methods=["PUT"],
            summary="Set or update a catalog-level configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.delete_catalog_config,
            methods=["DELETE"],
            summary="Delete a catalog-level configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        # Search routes
        self.router.add_api_route(
            "/catalogs/{catalog_id}/search",
            self.search_catalog_configs,
            methods=["GET"],
            summary="Search configurations for a catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/search",
            self.search_collection_configs,
            methods=["GET"],
            summary="Search configurations for a collection",
        )

    @property
    def configs(self) -> ConfigsProtocol:
        return self.get_protocol(ConfigsProtocol)


    # --- Discovery Endpoint ---


    async def list_registered_plugins(
        self,
        with_schema: bool = Query(
            False,
            description="If true, includes the JSON schema for each configuration model.",
        ),
    ):
        """
        Lists all registered configuration plugin IDs.
        This endpoint provides a discoverable list of all possible `plugin_id`
        values that can be used in other configuration endpoints.
        """
        plugins = ConfigRegistry._registry
        if with_schema:
            return JSONResponse(
                content={
                    plugin_id: {
                        "description": model.__doc__ or "No description provided.",
                        "schema": model.model_json_schema(),
                    }
                    for plugin_id, model in plugins.items()
                }
            )
        return list(plugins.keys())

    # --- Configuration Listing Endpoints ---


    async def list_platform_configs(
        self, limit: int = Query(10, ge=1, le=1000), offset: int = Query(0, ge=0)
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set at the global platform level.
        """
        return await self.configs.list_configs(limit=limit, offset=offset)


    async def list_catalog_configs(
        self,
        catalog_id: str,
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set for a specific catalog.
        """
        return await self.configs.list_configs(
            catalog_id=catalog_id, limit=limit, offset=offset
        )


    async def list_collection_configs(
        self,
        catalog_id: str,
        collection_id: str,
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> Dict[str, Any]:
        """
        Retrieves paginated configurations explicitly set for a specific collection.
        """
        return await self.configs.list_configs(
            catalog_id=catalog_id,
            collection_id=collection_id,
            limit=limit,
            offset=offset,
        )

    # --- Specific Plugin Configuration (GET/PUT) ---

    # Collection Level


    async def get_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ):
        """
        Retrieves the effective configuration for a specific plugin (extension) on a collection.
        Resolves hierarchy: Collection > Catalog > Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            # This call uses the JIT logic: it returns the resolved config (default if nothing else exists)
            config = await self.configs.get_config(
                plugin_id, catalog_id, collection_id
            )
            return config
        except Exception as e:
            logger.error(f"Error fetching collection config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


    async def update_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str, body: Dict[str, Any]
    ):
        """
        Overrides the configuration for a specific plugin at the Collection level.
        This writes to the 'collection_configs' table.
        """
        try:
            # Validate input against registered schema
            config_model = ConfigRegistry.validate_config(plugin_id, body)

            validated_config = await self.configs.set_config(
                plugin_id, config_model, catalog_id, collection_id
            )
            return validated_config

        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Collection Config",
                resource_id=f"{catalog_id}:{collection_id}:{plugin_id}",
                operation="Collection configuration update",
            )


    async def delete_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ):
        """
        Deletes the configuration override for a plugin at the Collection level.
        The effective configuration will revert to Catalog, Platform, or Code defaults.
        """
        await self.configs.delete_config(
            plugin_id, catalog_id=catalog_id, collection_id=collection_id
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Catalog Level


    async def get_catalog_config(self, catalog_id: str, plugin_id: str):
        """
        Retrieves the configuration for a plugin at the Catalog level.
        Resolves hierarchy: Catalog > Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            # Passing None for collection_id triggers resolution up to Catalog level
            config = await self.configs.get_config(
                plugin_id, catalog_id, collection_id=None
            )
            return config
        except Exception as e:
            logger.error(f"Error fetching catalog config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


    async def update_catalog_config(
        self, catalog_id: str, plugin_id: str, body: Dict[str, Any]
    ):
        """
        Overrides the configuration for a plugin at the Catalog level.
        """
        try:
            # Validate input against registered schema
            config_model = ConfigRegistry.validate_config(plugin_id, body)

            validated_config = await self.configs.set_config(
                plugin_id, config_model, catalog_id, collection_id=None
            )
            return validated_config
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Catalog Config",
                resource_id=f"{catalog_id}:{plugin_id}",
                operation="Catalog configuration update",
            )


    async def delete_catalog_config(self, catalog_id: str, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Catalog level.
        The effective configuration will revert to Platform or Code defaults.
        """
        await self.configs.delete_config(plugin_id, catalog_id=catalog_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Platform Level


    async def get_platform_config(self, plugin_id: str):
        """
        Retrieves the global platform default configuration for a plugin.
        Resolves hierarchy: Platform > Default.
        """
        try:
            if not ConfigRegistry.get_model(plugin_id):
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            config = await self.configs.get_config(
                plugin_id, catalog_id=None, collection_id=None
            )
            return config
        except Exception as e:
            logger.error(f"Error fetching platform config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


    async def update_platform_config(self, plugin_id: str, body: Dict[str, Any]):
        """
        Sets the global platform default configuration for a plugin.
        """
        model_class = ConfigRegistry.get_model(plugin_id)
        if not model_class:
            raise HTTPException(
                status_code=404,
                detail=f"Configuration plugin '{plugin_id}' is not registered.",
            )

        try:
            # Validate input against registered schema
            config_model = ConfigRegistry.validate_config(plugin_id, body)

            validated_config = await self.configs.set_config(
                plugin_id, config_model, catalog_id=None, collection_id=None
            )
            return validated_config
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Platform Config",
                resource_id=plugin_id,
                operation="Platform configuration update",
            )


    async def delete_platform_config(self, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Platform level.
        The effective configuration will revert to the Code defaults.
        """
        await self.configs.delete_config(plugin_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Search ---


    async def search_catalog_configs(
        self,
        catalog_id: str,
        q: Optional[str] = Query(
            None, alias="q", description="Filter by plugin name (LIKE search)"
        ),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """Searches configurations for a catalog."""
        return await self.configs.search(
            query=q, catalog_id=catalog_id, limit=limit, offset=offset
        )


    async def search_collection_configs(
        self,
        catalog_id: str,
        collection_id: str,
        q: Optional[str] = Query(
            None, alias="q", description="Filter by plugin name (LIKE search)"
        ),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """Searches configurations for a collection."""
        return await self.configs.search(
            query=q,
            catalog_id=catalog_id,
            collection_id=collection_id,
            limit=limit,
            offset=offset,
        )
