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
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Path, Query, status, Request, FastAPI
from fastapi.responses import Response, HTMLResponse
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.extensions.tools.exception_handlers import handle_exception
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules import get_protocol
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.models.protocols import WebModuleProtocol, ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    ConfigRegistry,
    enforce_config_immutability,
)
from dynastore.modules.db_config.exceptions import (
    ImmutableConfigError,
    PluginNotRegisteredError,
    ConfigValidationError,
    is_conflict_error,
)

from .dto import (
    BulkApplyResponse,
    BulkApplyResultEntry,
    ConfigEntry,
    ConfigListResponse,
    PluginSchemaInfo,
    QuickStartConfigSet,
    WellKnownPlugin,
    get_plugin_examples,
    PLUGIN_EXAMPLES,
)
from .policies import register_configs_policies

logger = logging.getLogger(__name__)

# Ensure core plugins are registered by importing their config modules if available
try:
    import dynastore.modules.catalog.catalog_config
    import dynastore.modules.tiles.tiles_config
    import dynastore.modules.stac.stac_config
    import dynastore.modules.tasks.tasks_config
    import dynastore.modules.gcp.gcp_config
    import dynastore.extensions.wfs.wfs_config
    import dynastore.extensions.features.features_config
except ImportError as e:
    logger.debug(f"ConfigsService: Some core configs not available for pre-registration: {e}")
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

    def configure_app(self, app: FastAPI):
        """Register the Configuration Editor as a web page via WebModuleProtocol."""
        web = get_protocol(WebModuleProtocol)
        if web:
            web.scan_and_register_providers(self)
            logger.info("ConfigsService: Web page registered via WebModuleProtocol.")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        register_configs_policies()
        logger.info("ConfigsService: Policies registered.")
        yield

    @expose_web_page(
        page_id="configs_editor",
        title="Configurations",
        icon="fa-sliders",
        description="Dynamic platform configuration management.",
        required_roles=["sysadmin"],
        section="admin",
        priority=30,
    )
    async def provide_configs_editor(self, request: Request):
        """Serve the configurations editor HTML page."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        html_path = os.path.join(static_dir, "configurations.html")
        if not os.path.exists(html_path):
             raise HTTPException(status_code=404, detail="Configuration editor template not found.")
        with open(html_path, "r") as f:
            return HTMLResponse(f.read())

    def _setup_routes(self):
        self.router.add_api_route(
            "/schemas",
            self.get_config_schemas,
            methods=["GET"],
            summary="Get JSON schemas for all registered configurations",
        )
        self.router.add_api_route(
            "/plugins",
            self.list_registered_plugins,
            methods=["GET"],
            summary="List all registered configuration plugins",
        )
        self.router.add_api_route(
            "/storage/drivers",
            self.list_storage_drivers,
            methods=["GET"],
            summary="List all registered storage drivers with capabilities and hints",
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
            "/catalogs/{catalog_id}/configs/{plugin_id}",
            self.get_catalog_config,
            methods=["GET"],
            summary="Get effective configuration for a catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/configs/{plugin_id}",
            self.update_catalog_config,
            methods=["PUT"],
            summary="Set or update a catalog-level configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/configs/{plugin_id}",
            self.delete_catalog_config,
            methods=["DELETE"],
            summary="Delete a catalog-level configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/configs",
            self.list_collection_configs,
            methods=["GET"],
            summary="List all collection-level configurations",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/configs/{plugin_id}",
            self.get_collection_config,
            methods=["GET"],
            summary="Get effective configuration for a collection",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/configs/{plugin_id}",
            self.update_collection_config,
            methods=["PUT"],
            summary="Set or update a collection-level configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/configs/{plugin_id}",
            self.delete_collection_config,
            methods=["DELETE"],
            summary="Delete a collection-level configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        # Examples & Quick-start
        self.router.add_api_route(
            "/examples",
            self.list_all_examples,
            methods=["GET"],
            summary="Get configuration examples for all known plugins",
            tags=["Configurations", "Examples"],
        )
        self.router.add_api_route(
            "/examples/{plugin_id:path}",
            self.get_plugin_examples,
            methods=["GET"],
            summary="Get configuration examples for a specific plugin",
            tags=["Configurations", "Examples"],
        )
        # Bulk-apply
        self.router.add_api_route(
            "/catalogs/{catalog_id}/bulk",
            self.bulk_apply_catalog_configs,
            methods=["PUT"],
            summary="Apply multiple configurations to a catalog in one call",
            tags=["Configurations", "Bulk"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/bulk",
            self.bulk_apply_collection_configs,
            methods=["PUT"],
            summary="Apply multiple configurations to a collection in one call",
            tags=["Configurations", "Bulk"],
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


    async def get_config_schemas(self) -> Dict[str, Any]:
        """Retrieves JSON schemas for all registered configuration models."""
        schemas: Dict[str, Any] = {}
        for plugin_id, config_class in ConfigRegistry._registry.items():
            schemas[plugin_id] = config_class.model_json_schema()
        return schemas

    # --- Discovery Endpoint ---

    async def list_registered_plugins(
        self,
        with_schema: bool = Query(
            False,
            description="If true, includes the JSON schema and description for each plugin.",
        ),
    ) -> Any:
        """Lists all registered configuration plugin IDs.

        This endpoint provides a discoverable list of all possible ``plugin_id``
        values that can be used in other configuration endpoints.

        Use ``?with_schema=true`` to also retrieve each plugin's JSON Schema
        and docstring, which is useful for building dynamic UIs.
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

    async def list_storage_drivers(self) -> Any:
        """List all registered storage drivers grouped by driver_type.

        Returns a dict keyed by ``driver_type`` (e.g. ``"driver:records:postgresql"``).
        Use the ``driver_id`` values in ``collection:drivers`` or ``assets:drivers``
        routing config.
        """
        from collections import defaultdict

        from dynastore.extensions.configs.dto import DriverInfo, DriverListResponse
        from dynastore.models.protocols.asset_driver import AssetDriverProtocol
        from dynastore.models.protocols.metadata_driver import CollectionMetadataDriverProtocol
        from dynastore.models.protocols.storage_driver import CollectionStorageDriverProtocol
        from dynastore.modules.storage.routing_config import derive_supported_operations
        from dynastore.tools.discovery import get_protocols

        grouped: dict = defaultdict(list)

        def _driver_description(driver) -> dict:
            desc = getattr(driver, "description", None)
            if desc is None:
                return {}
            if isinstance(desc, dict):
                return desc
            # LocalizedText or similar
            if hasattr(desc, "model_dump"):
                return {k: v for k, v in desc.model_dump().items() if v}
            return {}

        for driver in get_protocols(CollectionStorageDriverProtocol):
            caps = sorted(getattr(driver, "capabilities", frozenset()))
            driver_type = type(driver).__name__
            plugin_id = getattr(driver, "_plugin_id", f"driver:records:{driver.driver_id}")
            driver_config_caps = []
            try:
                dcfg = ConfigRegistry.create_default(plugin_id)
                driver_config_caps = sorted(getattr(dcfg, "capabilities", frozenset()))
            except Exception:
                pass
            grouped[driver_type].append(DriverInfo(
                driver_id=driver.driver_id,
                driver_type=driver_type,
                domain="collections",
                description=_driver_description(driver),
                capabilities=caps,
                driver_capabilities=driver_config_caps,
                supported_operations=sorted(derive_supported_operations(
                    getattr(driver, "capabilities", frozenset())
                )),
                supported_hints=sorted(getattr(driver, "supported_hints", frozenset())),
                preferred_for=sorted(getattr(driver, "preferred_for", frozenset())),
            ))

        for driver in get_protocols(AssetDriverProtocol):
            caps = sorted(getattr(driver, "capabilities", frozenset()))
            driver_type = type(driver).__name__
            plugin_id = getattr(driver, "_plugin_id", f"driver:asset:{driver.driver_id}")
            driver_config_caps = []
            try:
                dcfg = ConfigRegistry.create_default(plugin_id)
                driver_config_caps = sorted(getattr(dcfg, "capabilities", frozenset()))
            except Exception:
                pass
            grouped[driver_type].append(DriverInfo(
                driver_id=driver.driver_id,
                driver_type=driver_type,
                domain="assets",
                description=_driver_description(driver),
                capabilities=caps,
                driver_capabilities=driver_config_caps,
                supported_operations=sorted(derive_supported_operations(
                    getattr(driver, "capabilities", frozenset())
                )),
                supported_hints=sorted(getattr(driver, "supported_hints", frozenset())),
                preferred_for=sorted(getattr(driver, "preferred_for", frozenset())),
            ))

        for driver in get_protocols(CollectionMetadataDriverProtocol):
            driver_type = type(driver).__name__
            plugin_id = getattr(driver, "_plugin_id", "")
            grouped[driver_type].append(DriverInfo(
                driver_id=driver.driver_id,
                driver_type=driver_type,
                domain="collection_metadata",
                description=_driver_description(driver),
                capabilities=sorted(getattr(driver, "capabilities", frozenset())),
                driver_capabilities=[],
                supported_operations=[],
                supported_hints=sorted(getattr(driver, "supported_hints", frozenset())),
                preferred_for=sorted(getattr(driver, "preferred_for", frozenset())),
            ))

        return DriverListResponse(drivers=dict(grouped))

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

    # --- Examples & Quick-start ---

    async def list_all_examples(self) -> Dict[str, List[Dict[str, Any]]]:
        """Returns configuration examples for every well-known plugin.

        Use these payloads as a starting point when setting up a new catalog
        or collection.  Each example includes a ``summary`` and ``description``
        explaining the use-case, plus the ready-to-POST ``value`` body.
        """
        return PLUGIN_EXAMPLES

    async def get_plugin_examples(
        self,
        plugin_id: str = Path(
            ...,
            description="Plugin identifier (e.g. ``driver:records:postgresql``, ``collection:drivers``, ``stac``).",
            examples=["driver:records:postgresql", "collection:drivers", "stac"],
        ),
    ) -> List[Dict[str, Any]]:
        """Returns configuration examples for a specific plugin.

        The returned list contains one or more example payloads that can be
        sent directly to ``PUT /configs/{plugin_id}`` (or the catalog/collection variant).
        """
        examples = get_plugin_examples(plugin_id)
        if examples is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"No examples available for plugin '{plugin_id}'. "
                    f"Known plugins with examples: {sorted(PLUGIN_EXAMPLES.keys())}"
                ),
            )
        return examples

    # --- Bulk Apply ---

    async def bulk_apply_catalog_configs(
        self,
        catalog_id: str,
        body: QuickStartConfigSet = Body(
            ...,
            openapi_examples={
                "postgresql_defaults": {
                    "summary": "PostgreSQL quick-start (all defaults)",
                    "description": (
                        "Applies routing, driver, STAC, tiles, features, and task configs "
                        "in a single call.  All values use PostgreSQL defaults."
                    ),
                    "value": {
                        "configs": {
                            "collection:drivers": {
                                "enabled": True,
                                "operations": {
                                    "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                    "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                },
                            },
                            "assets:drivers": {
                                "enabled": True,
                                "operations": {
                                    "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                    "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                },
                            },
                            "stac": {
                                "enabled": True,
                                "enabled_extensions": [],
                                "asset_tracking": {"enabled": True, "access_mode": "DIRECT"},
                            },
                            "tiles": {"enabled": True, "min_zoom": 0, "max_zoom": 12},
                            "features": {"enabled": True},
                            "tasks": {"enabled": True, "queue_poll_interval": 30.0},
                        }
                    },
                },
            },
        ),
    ) -> BulkApplyResponse:
        """Apply multiple plugin configurations to a catalog in one call.

        Iterates over every entry in ``configs`` and applies each via
        ``PUT /configs/catalogs/{catalog_id}/configs/{plugin_id}``.
        Failures on individual plugins do not abort the entire operation;
        check the ``results`` array for per-plugin status.
        """
        return await self._bulk_apply(body, catalog_id=catalog_id, collection_id=None)

    async def bulk_apply_collection_configs(
        self,
        catalog_id: str,
        collection_id: str,
        body: QuickStartConfigSet = Body(
            ...,
            openapi_examples={
                "postgresql_vector_collection": {
                    "summary": "Vector collection with PG defaults",
                    "description": (
                        "Configures a vector collection with default geometries + attributes "
                        "sidecars, no partitioning.  Includes routing and STAC."
                    ),
                    "value": {
                        "configs": {
                            "driver:records:postgresql": {
                                "enabled": True,
                                "collection_type": "VECTOR",
                                "sidecars": [
                                    {
                                        "sidecar_type": "geometries",
                                        "enabled": True,
                                        "target_srid": 4326,
                                        "target_dimension": "force_2d",
                                        "geom_column": "geom",
                                        "bbox_column": "bbox_geom",
                                        "invalid_geom_policy": "attempt_fix",
                                        "srid_mismatch_policy": "transform",
                                    },
                                    {
                                        "sidecar_type": "attributes",
                                        "enabled": True,
                                        "storage_mode": "automatic",
                                        "enable_external_id": True,
                                        "enable_asset_id": True,
                                    },
                                ],
                                "partitioning": {"enabled": False, "partition_keys": []},
                            },
                            "collection:drivers": {
                                "enabled": True,
                                "operations": {
                                    "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                    "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                },
                            },
                            "stac": {
                                "enabled": True,
                                "enabled_extensions": [],
                                "asset_tracking": {"enabled": True, "access_mode": "DIRECT"},
                            },
                        }
                    },
                },
            },
        ),
    ) -> BulkApplyResponse:
        """Apply multiple plugin configurations to a collection in one call.

        Same semantics as the catalog-level bulk endpoint, but writes to
        the collection-level config tier.
        """
        return await self._bulk_apply(body, catalog_id=catalog_id, collection_id=collection_id)

    async def _bulk_apply(
        self,
        payload: QuickStartConfigSet,
        catalog_id: Optional[str],
        collection_id: Optional[str],
    ) -> BulkApplyResponse:
        """Internal helper for bulk-apply at any hierarchy level."""
        results: List[BulkApplyResultEntry] = []
        applied = 0
        failed = 0

        for plugin_id, config_data in payload.configs.items():
            try:
                config_model = ConfigRegistry.validate_config(plugin_id, config_data)
                await self.configs.set_config(
                    plugin_id, config_model, catalog_id, collection_id
                )
                results.append(BulkApplyResultEntry(plugin_id=plugin_id, status="ok"))
                applied += 1
            except Exception as exc:
                logger.error("Bulk apply failed for %s: %s", plugin_id, exc, exc_info=True)
                results.append(
                    BulkApplyResultEntry(
                        plugin_id=plugin_id, status="error", detail=str(exc)
                    )
                )
                failed += 1

        return BulkApplyResponse(applied=applied, failed=failed, results=results)

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
