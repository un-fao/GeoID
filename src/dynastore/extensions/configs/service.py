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
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.modules.db_config.platform_config_service import (
    enforce_config_immutability,
    require_config_class,
    resolve_config_class,
    list_registered_configs,
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
    get_plugin_examples,
    PLUGIN_EXAMPLES,
)
from .config_api_service import ConfigApiService
from .policies import register_configs_policies

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

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def configure_app(self, app: FastAPI):
        """Web pages are discovered by WebModule via the WebPageContributor
        capability protocol (see get_web_pages above)."""
        return None

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
        required_roles=[DefaultRole.SYSADMIN.value],
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
            summary="List all registered config schemas grouped by scope and protocol",
        )
        self.router.add_api_route(
            "/schemas/{class_key}",
            self.get_config_schema,
            methods=["GET"],
            summary="Get JSON Schema + description for a specific config class",
        )
        self.router.add_api_route(
            "/graph",
            self.get_config_graph,
            methods=["GET"],
            summary="Config dependency graph — nodes = config classes, edges = apply-handler cross-references",
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
            summary="List all registered storage drivers grouped by Protocol qualname",
        )
        self.router.add_api_route(
            "/",
            self.list_platform_configs,
            methods=["GET"],
            summary="List all platform-level configurations",
        )
        # ---- Config API — composed views at all scopes (must be before /{plugin_id}) ----
        self.router.add_api_route(
            "/config",
            self.get_platform_config_composed,
            methods=["GET"],
            summary="Platform config — all effective platform configs composed",
            tags=["Config API"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/config",
            self.get_catalog_config_composed,
            methods=["GET"],
            summary="Catalog config — all effective catalog configs composed",
            tags=["Config API"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/config",
            self.get_collection_config_composed,
            methods=["GET"],
            summary="Collection config — all effective collection configs composed",
            tags=["Config API"],
        )
        # Examples & Quick-start routes — MUST be registered BEFORE the generic
        # ``/{plugin_id}`` catch-all, otherwise FastAPI routes ``GET /configs/examples``
        # through the catch-all with plugin_id="examples" and returns
        # "plugin 'examples' is not registered".
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
            "/catalogs/{catalog_id}/collections/{collection_id}/configs/{plugin_id}/effective",
            self.get_effective_collection_config,
            methods=["GET"],
            summary="Waterfall-resolved config with per-field source annotation",
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
        return self.get_protocol(ConfigsProtocol)  # type: ignore[return-value]

    @property
    def _config_api(self) -> ConfigApiService:
        from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol

        return ConfigApiService(
            config_service=self.configs,
            catalogs_service=self.get_protocol(CatalogsProtocol),
            assets_service=self.get_protocol(AssetsProtocol),
        )


    async def get_config_schemas(self) -> Dict[str, Any]:
        """List all registered config schemas grouped by scope and protocol (M9).

        Response shape::

            {
                class_key: {
                    "json_schema": {...},
                    "description": "...",
                    "scope": "platform_waterfall | collection_intrinsic | deployment_env",
                }
            }

        ``scope`` is read from ``ConfigScopeMixin.config_scope`` when present,
        or defaults to ``"platform_waterfall"``.
        """
        from dynastore.modules.storage.schema_types import ConfigScopeMixin

        schemas: Dict[str, Any] = {}
        for class_key, config_class in list_registered_configs().items():
            scope = getattr(config_class, "config_scope", "platform_waterfall")
            schemas[class_key] = {
                "json_schema": config_class.model_json_schema(),
                "description": (config_class.__doc__ or "").strip().split("\n")[0],
                "scope": scope,
            }
        return schemas

    async def get_config_schema(self, class_key: str) -> Dict[str, Any]:
        """Return full JSON Schema, description, and example for a single config class (M9).

        Response shape::

            {
                "class_key": "CollectionRoutingConfig",
                "json_schema": {...},
                "description": "...",
                "scope": "platform_waterfall",
            }
        """
        from dynastore.modules.storage.schema_types import ConfigScopeMixin

        config_class = resolve_config_class(class_key)
        if config_class is None:
            raise HTTPException(status_code=404, detail=f"Config class '{class_key}' not registered.")

        scope = getattr(config_class, "config_scope", "platform_waterfall")
        return {
            "class_key": class_key,
            "json_schema": config_class.model_json_schema(),
            "description": (config_class.__doc__ or "").strip(),
            "scope": scope,
        }

    async def get_config_graph(self) -> Dict[str, Any]:
        """Return a simple config dependency graph (M9).

        Each node is a registered config class; edges represent ``register_apply_handler``
        cross-references (i.e., handler A reads config B to validate against).

        Response shape::

            {
                "nodes": ["CollectionRoutingConfig", "CollectionSchema", ...],
                "edges": [
                    {"from": "CollectionWritePolicy", "to": "CollectionSchema",
                     "label": "cross-validates external_id_field against fields"},
                    ...
                ]
            }
        """
        nodes = list(list_registered_configs().keys())

        # Known cross-validator edges (hard-coded from apply-handler docs)
        edges = [
            {
                "from": "CollectionWritePolicy",
                "to": "CollectionSchema",
                "label": "cross-validates external_id_field against schema.fields",
            },
            {
                "from": "CollectionSchema",
                "to": "CollectionRoutingConfig",
                "label": "validates required/unique constraints against primary write driver capabilities",
            },
        ]

        return {"nodes": nodes, "edges": edges}

    async def get_effective_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ) -> Dict[str, Any]:
        """Waterfall-resolved config with per-field source annotation (M9).

        Loads the config at every scope tier and annotates each field with the
        tier that last overrode it:

        ``"default"`` — code-level class default
        ``"platform"`` — explicitly set at platform level
        ``"catalog"``  — overridden at catalog level
        ``"collection"`` — overridden at collection level

        Response shape::

            {
                "class_key": "CollectionRoutingConfig",
                "resolved": {
                    "on_conflict": {
                        "value": "REFUSE_RETURN",
                        "source": "catalog",
                        "overrides": ["platform: null", "default: null"]
                    }
                }
            }
        """
        cls = resolve_config_class(plugin_id)
        if cls is None:
            raise HTTPException(
                status_code=404,
                detail=f"Config class '{plugin_id}' not registered.",
            )

        configs = self.configs

        default_config = cls()
        platform_config = await configs.get_config(cls)
        catalog_config = await configs.get_config(cls, catalog_id=catalog_id)
        effective_config = await configs.get_config(
            cls, catalog_id=catalog_id, collection_id=collection_id
        )

        default_data = default_config.model_dump()
        platform_data = platform_config.model_dump()
        catalog_data = catalog_config.model_dump()
        effective_data = effective_config.model_dump()

        annotated: Dict[str, Any] = {}
        for field_name, value in effective_data.items():
            catalog_val = catalog_data.get(field_name)
            platform_val = platform_data.get(field_name)
            default_val = default_data.get(field_name)

            if value != catalog_val:
                source = "collection"
            elif catalog_val != platform_val:
                source = "catalog"
            elif platform_val != default_val:
                source = "platform"
            else:
                source = "default"

            overrides: list = []
            if source == "collection" and catalog_val != default_val:
                overrides.append(f"catalog: {catalog_val!r}")
            if source in ("collection", "catalog") and platform_val != default_val:
                overrides.append(f"platform: {platform_val!r}")
            if source != "default":
                overrides.append(f"default: {default_val!r}")

            entry: Dict[str, Any] = {"value": value, "source": source}
            if overrides:
                entry["overrides"] = overrides
            annotated[field_name] = entry

        return {"class_key": plugin_id, "resolved": annotated}

    # =========================================================================
    # Config API — Composed views at Platform / Catalog / Collection
    # =========================================================================

    async def get_platform_config_composed(
        self,
        request: Request,
        depth: int = Query(0, ge=0, le=3, description="Child levels to expand (0 = configs only)."),
        catalogs_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at platform scope."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_platform_config(
            base_url=base_url,
            depth=depth,
            catalogs_page=catalogs_page,
            page_size=page_size,
            resolved=resolved,
        )
        return JSONResponse(content=response.model_dump())

    async def get_catalog_config_composed(
        self,
        catalog_id: str,
        request: Request,
        depth: int = Query(0, ge=0, le=3),
        collections_page: int = Query(1, ge=1),
        assets_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at this catalog scope."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_catalog_config(
            base_url=base_url,
            catalog_id=catalog_id,
            depth=depth,
            collections_page=collections_page,
            assets_page=assets_page,
            page_size=page_size,
            resolved=resolved,
        )
        return JSONResponse(content=response.model_dump())

    async def get_collection_config_composed(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        depth: int = Query(0, ge=0, le=3),
        assets_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at this collection scope."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_collection_config(
            base_url=base_url,
            catalog_id=catalog_id,
            collection_id=collection_id,
            depth=depth,
            assets_page=assets_page,
            page_size=page_size,
            resolved=resolved,
        )
        return JSONResponse(content=response.model_dump())

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
        plugins = list_registered_configs()
        if with_schema:
            return JSONResponse(
                content={
                    class_key: {
                        "description": model.__doc__ or "No description provided.",
                        "schema": model.model_json_schema(),
                    }
                    for class_key, model in plugins.items()
                }
            )
        return list(plugins.keys())

    async def list_storage_drivers(self) -> Any:
        """List all registered drivers grouped by the Protocol they implement (M9 §7).

        Response shape: ``{ProtocolQualname: {class_name: DriverInfo}}``.
        Keys are the ``__qualname__`` of ``CollectionItemsStore``, ``AssetStore``,
        and ``CollectionMetadataStore``.  Use the inner class names as driver_id
        values in the routing config's operations list.
        """
        from dynastore.extensions.configs.dto import DriverInfo, DriverListResponse
        from dynastore.models.protocols.asset_driver import AssetStore
        from dynastore.models.protocols.metadata_driver import CollectionMetadataStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.modules.storage.routing_config import derive_supported_operations
        from dynastore.tools.discovery import get_all_protocols

        def _driver_description(driver) -> dict:
            desc = getattr(driver, "description", None)
            if desc is None:
                return {}
            if isinstance(desc, dict):
                return desc
            if hasattr(desc, "model_dump"):
                return {k: v for k, v in desc.model_dump().items() if v}
            return {}

        def _config_caps(class_key: str) -> list:
            try:
                dcls = resolve_config_class(class_key)
                if dcls is not None:
                    return sorted(getattr(dcls(), "capabilities", frozenset()))
            except Exception:
                pass
            return []

        def _routable_info(driver, config_class_key: str, available: bool) -> DriverInfo:
            caps = driver.capabilities
            return DriverInfo(
                description=_driver_description(driver),
                capabilities=sorted(caps),
                driver_capabilities=_config_caps(config_class_key),
                supported_operations=sorted(derive_supported_operations(caps)),
                supported_hints=sorted(driver.supported_hints),
                preferred_for=sorted(driver.preferred_for),
                available=available,
            )

        def _metadata_info(driver, available: bool) -> DriverInfo:
            return DriverInfo(
                description=_driver_description(driver),
                capabilities=sorted(driver.capabilities),
                available=available,
            )

        # Group by Protocol qualname (M9 — §7 protocol-based driver grouping)
        drivers: Dict[str, Dict[str, DriverInfo]] = {
            CollectionItemsStore.__qualname__: {
                type(d).__name__: _routable_info(d, f"{type(d).__name__}Config", d.is_available())
                for d in get_all_protocols(CollectionItemsStore)
            },
            AssetStore.__qualname__: {
                type(d).__name__: _routable_info(d, f"{type(d).__name__}Config", d.is_available())
                for d in get_all_protocols(AssetStore)
            },
            CollectionMetadataStore.__qualname__: {
                type(d).__name__: _metadata_info(d, await d.is_available())
                for d in get_all_protocols(CollectionMetadataStore)
            },
        }

        return DriverListResponse(drivers=drivers)

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
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            config = await self.configs.get_config(cls, catalog_id, collection_id)
            return config
        except HTTPException:
            raise
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
            cls = require_config_class(plugin_id)
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id, collection_id
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
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(
            cls, catalog_id=catalog_id, collection_id=collection_id
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Catalog Level


    async def get_catalog_config(self, catalog_id: str, plugin_id: str):
        """
        Retrieves the configuration for a plugin at the Catalog level.
        Resolves hierarchy: Catalog > Platform > Default.
        """
        try:
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            config = await self.configs.get_config(
                cls, catalog_id, collection_id=None
            )
            return config
        except HTTPException:
            raise
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
            cls = require_config_class(plugin_id)
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id, collection_id=None
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
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(cls, catalog_id=catalog_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Platform Level


    async def get_platform_config(self, plugin_id: str):
        """
        Retrieves the global platform default configuration for a plugin.
        Resolves hierarchy: Platform > Default.
        """
        try:
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Configuration plugin '{plugin_id}' is not registered.",
                )

            config = await self.configs.get_config(
                cls, catalog_id=None, collection_id=None
            )
            return config
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching platform config: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


    async def update_platform_config(self, plugin_id: str, body: Dict[str, Any]):
        """
        Sets the global platform default configuration for a plugin.
        """
        cls = resolve_config_class(plugin_id)
        if cls is None:
            raise HTTPException(
                status_code=404,
                detail=f"Configuration plugin '{plugin_id}' is not registered.",
            )

        try:
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id=None, collection_id=None
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
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(cls)
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
            description="Plugin identifier (e.g. ``CollectionPostgresqlDriverConfig``, ``CollectionRoutingConfig``, ``stac``).",
            examples=["CollectionPostgresqlDriverConfig", "CollectionRoutingConfig", "stac"],
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
                            "CollectionRoutingConfig": {
                                "enabled": True,
                                "operations": {
                                    "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                    "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
                                },
                            },
                            "AssetRoutingConfig": {
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
                            "CollectionPostgresqlDriverConfig": {
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
                            "CollectionRoutingConfig": {
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
                cls = require_config_class(plugin_id)
                config_model = cls.model_validate(config_data)
                await self.configs.set_config(
                    cls, config_model, catalog_id, collection_id
                )
                results.append(BulkApplyResultEntry(plugin_id=plugin_id, status="ok"))  # type: ignore[call-arg]
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
