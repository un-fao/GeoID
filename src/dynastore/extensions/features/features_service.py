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

# dynastore/extensions/features/features_service.py

from typing import Optional, List, Dict, Any, Tuple, Union, cast

import logging
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status, FastAPI
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.exception_handlers import handle_exception
from contextlib import asynccontextmanager
import asyncio
from dynastore.models.protocols import CatalogsProtocol, StorageProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.features.features_config import FeaturesPluginConfig, FEATURES_PLUGIN_CONFIG_ID
from dynastore.extensions.features import ogc_generator, ogc_models
from .policies import register_features_policies

from datetime import datetime, timezone, UTC
from dynastore.models.shared_models import (
    Link, FunctionDescription, FunctionsResponse, ItemDataForDB, Catalog, Collection
)
from dynastore.extensions.tools.url import get_root_url, get_url
from dynastore.extensions.tools.language_utils import get_language
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from dynastore.extensions import dynastore_extension
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.tools import managed_transaction
from dynastore.extensions.tools.conformance import register_conformance_uris, Conformance
import re
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig, COLLECTION_PLUGIN_CONFIG_ID
from . import features_db
from dynastore.extensions.tools.formatters import OutputFormatEnum, format_response

logger = logging.getLogger(__name__)

# Optional dependency on the CRS module
crs_module_available = False
try:
    from dynastore.modules.crs import crs_module
    crs_module_available = True
except ImportError:
    logger.warning("CRS module not available. Custom CRS resolution will be disabled in features extension.")

# Define the conformance classes this specific extension provides.
OGC_API_FEATURES_URIS = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/features-filter",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/queryables",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/sort",
    "http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete",
]

# A static list of supported CQL2 functions. In a future implementation, this could be
# made dynamic based on the actual capabilities of the query backend.
SUPPORTED_CQL_FUNCTIONS = [
    FunctionDescription(name="S_Intersects", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Equals", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Disjoint", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Touches", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Within", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Overlaps", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Crosses", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
    FunctionDescription(name="S_Contains", returns=["boolean"], arguments=[{"type": ["geometry", "geometry"]}]),
]


def _generate_pagination_links(request: Request, offset: int, limit: int, count: int) -> List[Link]:
    """Generates 'self', 'next', and 'prev' links for paginated responses."""
    links = [Link(href=str(request.url), rel="self", type="application/geo+json", title="This document")]

    if (offset + limit) < count:
        next_url = request.url.replace_query_params(offset=offset + limit)
        links.append(Link(href=str(next_url), rel="next", type="application/geo+json", title="Next page"))
    
    if offset > 0:
        prev_offset = max(0, offset - limit)
        prev_url = request.url.replace_query_params(offset=prev_offset)
        links.append(Link(href=str(prev_url), rel="prev", type="application/geo+json", title="Previous page"))
    
    return links


async def _resolve_crs_uri_to_srid(crs_uri: str, conn: AsyncConnection, catalog_id: str) -> Optional[int]:
    """Resolves a CRS URI to a PostGIS SRID."""
    if not crs_uri: return None
    if 'CRS84' in crs_uri.upper(): return 4326

    match = re.search(r'[/|:](\d+)$', crs_uri)
    if match: return int(match.group(1))

    if crs_module_available:
        crs_def = await crs_module.get_crs_by_uri(conn, catalog_id, crs_uri)
        if crs_def and hasattr(crs_def, 'srid'): return crs_def.srid
    
    raise HTTPException(status_code=400, detail=f"Unsupported or unknown CRS URI: {crs_uri}")

@dynastore_extension
class OGCFeaturesService(ExtensionProtocol):
    router:APIRouter = APIRouter(prefix="/features", tags=["OGC API - Features"])
    
    def configure_app(self, app: FastAPI):
        """Early configuration for the Features extension."""
        # Register OGC API - Features specific policies
        register_features_policies()
        logger.info("OGCFeaturesService: Policies registered.")

        # Explicit mounting for robustness (e.g. if centralized mounting is skipped in tests)
        app.include_router(self.router)
        logger.info("OGCFeaturesService: Router mounted.")

    def __init__(self):
        """Initializes the service and registers its routes."""
        register_conformance_uris(OGC_API_FEATURES_URIS)
        self._register_routes()

    def _register_routes(self):
        """Registers all OGC API Features routes."""
        self.router.add_api_route("/", self.get_landing_page, methods=["GET"], response_model=ogc_models.LandingPage)
        self.router.add_api_route("/conformance", self.get_conformance, methods=["GET"], response_model=ogc_models.Conformance)
        self.router.add_api_route("/functions", self.get_supported_functions, methods=["GET"], response_model=FunctionsResponse)
        
        # --- Catalog Endpoints ---
        self.router.add_api_route("/catalogs", self.list_catalogs, methods=["GET"], response_model=ogc_models.Catalogs)
        self.router.add_api_route("/catalogs", self.create_catalog, methods=["POST"], response_model=Catalog, status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}", self.get_catalog, methods=["GET"], response_model=Catalog)
        self.router.add_api_route("/catalogs/{catalog_id}", self.update_catalog, methods=["PUT"], response_model=Catalog)
        self.router.add_api_route("/catalogs/{catalog_id}", self.delete_catalog, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT)
        
        # --- Collection Endpoints ---
        self.router.add_api_route("/catalogs/{catalog_id}/collections", self.list_collections_in_catalog, methods=["GET"], response_model=ogc_models.Collections)
        self.router.add_api_route("/catalogs/{catalog_id}/collections", self.create_collection, methods=["POST"], response_model=ogc_models.OGCCollection, status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.get_collection, methods=["GET"], response_model=ogc_models.OGCCollection)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.update_collection, methods=["PUT"], response_model=ogc_models.OGCCollection)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.delete_collection, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/queryables", self.get_queryables, methods=["GET"], response_model=ogc_models.Queryables)
        
        # --- Item Endpoints ---
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items", self.get_items, methods=["GET"], response_model=ogc_models.FeatureCollection)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items", self.add_item, methods=["POST"], response_model=Union[ogc_models.Feature, ogc_models.BulkCreationResponse], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.get_item, methods=["GET"], response_model=ogc_models.Feature)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.replace_item, methods=["PUT"], response_model=ogc_models.Feature)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.delete_item, methods=["DELETE"], status_code=status.HTTP_204_NO_CONTENT)

    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """
        Registers OGC API Features conformance classes at application startup.
        """
        register_conformance_uris(OGC_API_FEATURES_URIS)
        register_features_policies()
        logger.info("OGCFeaturesService: Successfully registered conformance classes and policies.")
        yield

    async def _get_catalogs_service(self) -> CatalogsProtocol:
        """Helper to get the catalogs service protocol."""
        svc = get_protocol(CatalogsProtocol)
        if not svc:
            raise HTTPException(status_code=500, detail="Catalogs service not available.")
        return svc

    async def _get_storage_service(self) -> Optional[StorageProtocol]:
        """Helper to get the storage service protocol."""
        return get_protocol(StorageProtocol)

    async def _get_configs_service(self) -> ConfigsProtocol:
        """Helper to get the configs service protocol."""
        svc = get_protocol(ConfigsProtocol)
        if not svc:
            raise HTTPException(status_code=500, detail="Configs service not available.")
        return svc

    async def get_landing_page(self, request: Request, language: str = Depends(get_language)):
        landing_page = ogc_generator.create_landing_page(request, language=language)
        return JSONResponse(content=landing_page.model_dump())

    async def get_conformance(self, request: Request):
        """Returns the list of conformance classes (Part 1)."""
        return ogc_models.Conformance(conformsTo=OGC_API_FEATURES_URIS)

    # --- Catalog Endpoints ---
    async def list_catalogs(self, request: Request,
                          limit: int = Query(10, ge=1), offset: int = Query(0, ge=0), language: str = Depends(get_language)):
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(limit=limit, offset=offset, lang=language)
        self_url = get_url(request)
        self_link = Link(href=self_url, rel="self", type="application/json")
        
        # Convert returned models to CatalogDefinition models and add links
        result_catalogs = []
        for catalog in catalogs:
            catalog_dict, _ = catalog.localize(language)
            # Add links to each catalog
            catalog_dict['links'] = [
                Link(href=f"{self_url}/{catalog.id}", rel="self", type="application/json").model_dump(),
                Link(href=f"{self_url}/{catalog.id}/collections", rel="items", type="application/json").model_dump()
            ]
            result_catalogs.append(ogc_models.CatalogDefinition(**catalog_dict))
            
        return ogc_models.Catalogs(catalogs=result_catalogs, links=[self_link])

    async def create_catalog(
        self,
        definition: ogc_models.CatalogDefinition, 
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language)
    ):
        """Creates a new catalog, its data schema, and required table partitions."""
        try:
            catalogs_svc = await self._get_catalogs_service()
            # Use id as code in the internal representation
            # Pass the definition fields individually as required by CatalogsProtocol
            created_catalog_model = await catalogs_svc.create_catalog(
                catalog_data={
                    "id": definition.id,
                    "title": definition.title,
                    "description": definition.description,
                    "keywords": definition.keywords,
                    "license": definition.license,
                    "extra_metadata": definition.extra_metadata
                },
                lang="*",
                db_resource=conn
            )
            return JSONResponse(content=created_catalog_model.model_dump(by_alias=True, exclude_none=True), status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise handle_exception(e, resource_name="Catalog", resource_id=definition.id, operation="OGC Features Catalog creation")

    async def get_catalog(self, catalog_id: str, request: Request, language: str = Depends(get_language)):
        catalogs_svc = await self._get_catalogs_service()
        catalog = await catalogs_svc.get_catalog(catalog_id, lang=language)
        if not catalog: raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        
        catalog_dict, languages = catalog.localize(language)
        self_url = get_url(request)
        catalog_dict['links'] = [ 
            Link(href=self_url, rel="self", type="application/json").model_dump(),
            Link(href=f"{get_root_url(request)}/features/catalogs", rel="parent", type="application/json").model_dump(),
            Link(href=f"{self_url}/collections", rel="items", type="application/json").model_dump()
        ]
        return JSONResponse(content=catalog_dict)
    
    async def update_catalog(
        self,
        catalog_id: str, 
        definition: ogc_models.CatalogDefinition, 
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language)
    ):
        """Updates an existing catalog."""
        catalogs_svc = await self._get_catalogs_service()
        # Use id as code in the internal representation
        catalog_dict = definition.model_dump(exclude_unset=True)
        # Pass raw dict, manager handles validation
        catalog = await catalogs_svc.update_catalog(catalog_id, catalog_dict, lang=language, db_resource=conn)
        if not catalog:
            raise HTTPException(status_code=404, detail="Catalog not found")
        return JSONResponse(content=catalog.model_dump(by_alias=True, exclude_none=True))

    async def delete_catalog(self, catalog_id: str, force: bool = Query(False), conn: AsyncConnection = Depends(get_async_connection)):
        catalogs_svc = await self._get_catalogs_service()
        # Catalog Protocol supports force deletion of catalogs
        if not await catalogs_svc.delete_catalog(catalog_id, force=force, db_resource=conn):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Collection Endpoints ---
    async def list_collections_in_catalog(self, catalog_id: str, request: Request,
                                        limit: int = Query(10, ge=1), offset: int = Query(0, ge=0), language: str = Depends(get_language)):
        catalogs_svc = await self._get_catalogs_service()
        collections = await catalogs_svc.list_collections(catalog_id, lang=language, limit=limit, offset=offset)
        # Convert returned models to OGCCollection models and add links
        ogc_collections = [ogc_models.OGCCollection(**c.localize(language)[0]) for c in collections]

        self_url = get_url(request)
        self_link = Link(href=self_url, rel="self", type="application/json")
        parent_link = Link(href=f"{get_root_url(request)}/features/catalogs/{catalog_id}", rel="parent", type="application/json")

        for collection in ogc_collections:
            collection.links.append(Link(href=f"{self_url}/{collection.id}", rel="self", type="application/json"))
            collection.links.append(Link(href=f"{self_url}/{collection.id}/items", rel="items", type="application/json"))

        # Part 2: Add global CRS list to Collections response
        supported_crs = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]

        return ogc_models.Collections(collections=ogc_collections, links=[self_link, parent_link], crs=supported_crs)

    async def get_supported_functions(self, request: Request):
        """Returns a list of supported filter functions (Part 3)."""
        # The list of functions is defined at the module level.
        # In a future implementation, this could be made dynamic based on backend capabilities.
        return FunctionsResponse(functions=SUPPORTED_CQL_FUNCTIONS)

    async def get_queryables(self, catalog_id: str, collection_id: str, request: Request, conn: AsyncConnection = Depends(get_async_connection), language: str = Depends(get_language)):
        """Returns the filterable properties for a collection as a JSON Schema (Part 3).""" # type: ignore
        catalogs_svc = await self._get_catalogs_service()
        # Resolve physical mapping
        phys_schema = await catalogs_svc.resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await catalogs_svc.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
        
        if not phys_schema or not phys_table:
            return await ogc_generator.create_queryables_response(request, catalog_id, collection_id, [])

        table_exists = await shared_queries.table_exists_query.execute(conn, schema=phys_schema, table=phys_table)
        if not table_exists:
            # If no table, there are no queryable properties yet, but we can return a base schema.
            columns = []
        else:
            columns_result = await shared_queries.get_table_column_names_query.execute(conn, schema=phys_schema, table=phys_table)
            columns = [col[0] for col in columns_result]

        return await ogc_generator.create_queryables_response(request, catalog_id, collection_id, columns, language=language)

    async def create_collection(
        self, 
        catalog_id: str, 
        collection_def: ogc_models.CollectionDefinition, 
        language: str = Depends(get_language),
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        """Creates a new collection in a catalog."""
        try:
            catalogs_svc = await self._get_catalogs_service()
            # Use id as code in the internal representation
            collection_dict = collection_def.model_dump(exclude_unset=True)
            
            # Pass raw dict, manager handles localization
            # Pass conn as db_resource for transactional integrity
            collection = await catalogs_svc.create_collection(catalog_id, collection_dict, lang="*", db_resource=conn)
            return JSONResponse(content=collection.model_dump(by_alias=True, exclude_none=True), status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise handle_exception(e, resource_name="Collection", resource_id=f"{catalog_id}:{collection_def.id}", operation="OGC Features Collection creation")

    async def get_collection(self, catalog_id: str, collection_id: str, request: Request, language: str = Depends(get_language)):
        catalogs_svc = await self._get_catalogs_service()
        collection = await catalogs_svc.get_collection(catalog_id, collection_id, lang=language)
        if not collection: raise HTTPException(status_code=404, detail="Collection not found")
        
        # We need to construct the OGC response wrapper
        collection_dict, languages = collection.localize(language)
        ogc_collection = ogc_models.OGCCollection(**collection_dict)
        
        self_url = get_url(request)
        ogc_collection.links = [
            Link(href=self_url, rel="self", type="application/json"),
            Link(href=f"{get_root_url(request)}/features/catalogs/{catalog_id}/collections", rel="parent", type="application/json"),
            Link(href=f"{self_url}/items", rel="items", type="application/geo+json", title="Items in this collection"),
            Link(href=f"{self_url}/queryables", rel="queryables", type="application/schema+json", title="Queryable properties"),
        ]
        return JSONResponse(content=ogc_collection, status_code=status.HTTP_200_OK)

    async def update_collection(
        self,
        catalog_id: str, 
        collection_id: str, 
        collection_def: ogc_models.CollectionDefinition,
        language: str = Depends(get_language)
    ):
        """Updates an existing collection's metadata."""
        catalogs_svc = await self._get_catalogs_service()
        updated_collection = await catalogs_svc.update_collection(
            catalog_id=catalog_id, collection_id=collection_id, updates=collection_def.model_dump(exclude_unset=True), lang="*"
        )
        if not updated_collection:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Collection '{catalog_id}:{collection_id}' not found.")
        collection, _ = updated_collection.localize(language)
        return JSONResponse(content=collection, status_code=status.HTTP_200_OK)

    async def delete_collection(self, catalog_id: str, collection_id: str, force: bool = Query(False), conn: AsyncConnection = Depends(get_async_connection)):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_collection(catalog_id, collection_id, force, db_resource=conn):
            raise HTTPException(status_code=404, detail="Collection not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Item Endpoints ---
    async def get_items(
        self,
        request: Request,
        catalog_id: str, 
        collection_id: str, 
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(10, ge=1, le=1000, description="The maximum number of features to return."), 
        offset: int = Query(0, ge=0, description="The offset of the first feature to return."),
        bbox: Optional[str] = Query(None, description="Bounding box filter. Comma-separated: minx,miny,maxx,maxy"),
        datetime_param: Optional[str] = Query(None, alias="datetime", description="Temporal filter. A single datetime or a '/' separated interval."),
        filter: Optional[str] = Query(None, description="A CQL2-TEXT filter expression."),
        filter_lang: str = Query("cql2-text", description="Language of the filter expression. Only 'cql2-text' is supported."),
        crs: Optional[str] = Query(None, description="CRS URI for output geometry."),
        bbox_crs: Optional[str] = Query(None, description="CRS URI for the bbox parameter."),
        sortby: Optional[str] = Query(None, description="Sort order for features. Comma-separated list of properties. Use '-' for descending order (e.g., '-propertyA,+propertyB')."),
        f: OutputFormatEnum = Query(OutputFormatEnum.GEOJSON, alias="f", description="The output format for the features.")
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        storage_svc = await self._get_storage_service()

        collection_metadata = await catalogs_svc.get_collection(catalog_id, collection_id, lang="en") # Default language for internal check
        if not collection_metadata:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found or logically deleted.")

        # --- Caching Support ---
        
        configs_svc = await self._get_configs_service()
        storage_svc = await self._get_storage_service()
        
        plugin_config: FeaturesPluginConfig = await configs_svc.get_config(FEATURES_PLUGIN_CONFIG_ID, catalog_id=catalog_id, db_resource=conn)
        
        cache_key = None
        if plugin_config.cache_on_demand and storage_svc:
            import hashlib
            # Generate a stable cache key based on sorted parameters
            params = dict(request.query_params)
            cache_params = {k: v for k, v in params.items() if k not in ('_', 'access_token', 'token')}
            param_str = "&".join(f"{k}={v}" for k, v in sorted(cache_params.items()))
            cache_key_hash = hashlib.md5(param_str.encode()).hexdigest()
            
            bucket_name = await storage_svc.get_bucket_name_for_catalog(catalog_id)
            if bucket_name:
                cache_key = f"gs://{bucket_name}/features_cache/{cache_key_hash}"
                
                if await storage_svc.file_exists(cache_key):
                    logger.info(f"Features Cache HIT: {cache_key}")
                    import tempfile
                    with tempfile.NamedTemporaryFile() as tmp:
                        await storage_svc.download_file(cache_key, tmp.name)
                        tmp.seek(0)
                        cached_data = tmp.read()
                        
                    return Response(content=cached_data, media_type="application/geo+json")

        try:
            # Resolve physical mapping
            phys_schema = await catalogs_svc.resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                raise HTTPException(status_code=404, detail="Catalog schema not found.")
                
            phys_table = await catalogs_svc.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            
            if not phys_table:
                 return ogc_models.FeatureCollection(
                    links=[Link(href=str(request.url), rel="self", type="application/geo+json")],
                    features=[]
                )

            # Check table existence
            table_exists = await shared_queries.table_exists_query.execute(conn, schema=phys_schema, table=phys_table)
            if not table_exists:
                 return ogc_models.FeatureCollection(
                    links=[Link(href=str(request.url), rel="self", type="application/geo+json")],
                    features=[]
                )
            
            # --- Argument Parsing & SRID Resolution ---
            
            # Check if crs_module is available
            crs_module_available = False
            try:
                import app.api.features.crs as crs_module
                crs_module_available = True
            except ImportError:
                logger.warning("CRS module not available. CRS resolution will be limited.")

            async def _resolve_crs(uri_str: str) -> Optional[int]:
                if not uri_str: return None
                if 'CRS84' in uri_str.upper(): return 4326
                match = re.search(r'[/|:](\d+)$', uri_str)
                if match: return int(match.group(1))
                if crs_module_available:
                    crs_def = await crs_module.get_crs_by_uri(conn, catalog_id, uri_str)
                    if crs_def and hasattr(crs_def, 'srid'): return crs_def.srid
                return None

            target_crs_srid = await _resolve_crs(crs)
            bbox_crs_srid = await _resolve_crs(bbox_crs)

            parsed_bbox = None
            if bbox:
                try:
                    parsed_bbox = tuple(map(float, bbox.split(',')))
                    if len(parsed_bbox) != 4: raise ValueError("BBOX must have 4 coordinates")
                except (ValueError, TypeError):
                    raise HTTPException(status_code=400, detail="Invalid 'bbox' format. Must be four comma-separated numbers.")

            # --- Feature Query ---
            count, rows = await features_db.get_items_filtered(
                conn=conn, 
                schema=phys_schema, 
                table=phys_table,
                catalog_id=catalog_id,
                collection_id=collection_id,
                limit=limit,
                offset=offset,
                bbox=parsed_bbox, 
                datetime_str=datetime_param, 
                cql_filter=filter,
                bbox_crs_srid=bbox_crs_srid, 
                target_crs_srid=target_crs_srid,
                sortby=sortby
            )
            
            # --- Conversion to OGC Features ---
            root_url = get_root_url(request)
            features = [ogc_generator._db_row_to_ogc_feature(row, catalog_id, collection_id, root_url) for row in rows]
            
            # --- Response Construction ---
            if f == OutputFormatEnum.GEOJSON:
                base_url = str(request.url).split('?')[0]
                query_params = dict(request.query_params)
                
                links = [
                    Link(href=str(request.url), rel="self", type="application/geo+json"),
                    Link(href=f"{base_url}?f=html", rel="alternate", type="text/html", title="This document as HTML"),
                ]
                
                if offset > 0:
                    prev_params = query_params.copy()
                    prev_params['offset'] = str(max(0, offset - limit))
                    links.append(Link(href=f"{base_url}?{'&'.join([f'{k}={v}' for k,v in prev_params.items()])}", rel="prev", type="application/geo+json"))
                    
                if (offset + limit) < count:
                    next_params = query_params.copy()
                    next_params['offset'] = str(offset + limit)
                    links.append(Link(href=f"{base_url}?{'&'.join([f'{k}={v}' for k,v in next_params.items()])}", rel="next", type="application/geo+json"))

                response_model = ogc_models.FeatureCollection(
                    links=links,
                    features=features,
                    numberMatched=count,
                    numberReturned=len(features),
                    timeStamp=datetime.now(timezone.utc).isoformat()
                )
                
                # Serialize for response and cache
                response = JSONResponse(content=response_model.model_dump(by_alias=True, exclude_none=True))
                
                # Background save to cache if enabled
                if cache_key and storage_svc and plugin_config.cache_on_demand:
                     # Render JSON body
                    body = response.body
                    asyncio.create_task(storage_svc.upload_file_content(cache_key, body.decode('utf-8'), content_type="application/geo+json"))

                return response
            else:
                # Use the centralized formatter for other formats
                return format_response(
                    request=request,
                    features=features,
                    output_format=f,
                    collection_id=collection_id,
                    target_srid=target_crs_srid or 4326 # Default to WGS84 if no CRS is specified
                )
        except Exception as e:
            raise handle_exception(e, resource_name="Features", resource_id=f"{catalog_id}:{collection_id}", operation="get items")

    async def get_item(self, catalog_id: str, collection_id: str, item_id: str, request: Request, conn: AsyncConnection = Depends(get_async_connection)):
        catalogs_svc = await self._get_catalogs_service()
        row = await catalogs_svc.get_item_by_external_id(
            catalog_id, 
            collection_id, 
            ext_id=item_id,
            db_resource=conn
        )

        if not row: raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found.")
        root_url = get_root_url(request)
        return ogc_generator._db_row_to_ogc_feature(row, catalog_id, collection_id, root_url)

    async def add_item(self, catalog_id: str, collection_id: str, payload: Union[ogc_models.FeatureDefinition, ogc_models.FeatureCollectionDefinition], request: Request, response: Response, conn: AsyncConnection = Depends(get_async_connection)):
        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        # 1. Get CatalogPluginConfig to correctly process the incoming feature payload.
        layer_config = await catalogs_svc.get_collection_config(catalog_id, collection_id, db_resource=conn)
        if not layer_config:
            raise HTTPException(status_code=404, detail=f"Collection '{catalog_id}:{collection_id}' not found or has no configuration.")
        root_url = get_root_url(request)

        catalogs_svc = await self._get_catalogs_service()
        
        # 2. Upsert (Discrimination happens inside ItemService or via payload type)
        # Note: valid payload types here are FeatureDefinition or FeatureCollectionDefinition
        
        created = await catalogs_svc.upsert(catalog_id, collection_id, items=payload, db_resource=conn)
        
        created_rows = created if isinstance(created, list) else [created]
        
        if not created_rows:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create items.")
        
        # 4. Return appropriate response
        if isinstance(payload, ogc_models.FeatureDefinition) or getattr(payload, "type", None) == "Feature":
            # Single Feature response
            new_row = created_rows[0]
            location_url = f"{root_url}/features/catalogs/{catalog_id}/collections/{collection_id}/items/{new_row['external_id']}"
            return JSONResponse(
                content=ogc_generator._db_row_to_ogc_feature(new_row, catalog_id, collection_id, root_url),
                status_code=status.HTTP_201_CREATED,
                headers={"Location": location_url}
            )
        else:
            # Bulk response
            created_ids = [str(row['id']) for row in created_rows]
            return JSONResponse(content=ogc_models.BulkCreationResponse(ids=created_ids).model_dump(), status_code=status.HTTP_201_CREATED)

    async def replace_item(self, catalog_id: str, collection_id: str, item_id: str, feature_def: ogc_models.FeatureDefinition, request: Request, conn: AsyncConnection = Depends(get_async_connection)):
        if item_id != feature_def.id:
            raise HTTPException(status_code=400, detail=f"Item ID mismatch: path '{item_id}' vs payload '{feature_def.id}'.")

        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        # 1. Get CatalogPluginConfig to correctly process the incoming feature payload.
        layer_config = await catalogs_svc.get_collection_config(catalog_id, collection_id)
        record_for_db = ogc_generator._process_feature_for_db(feature_def, layer_config)

        # 2. Delegate update, versioning, and extent recalculation to the catalog module.
        updated_row = await catalogs_svc.update_item(catalog_id, collection_id, item_data=ItemDataForDB(**record_for_db), db_resource=conn)
        if not updated_row:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update item.")

        # 3. Format the response.
        root_url = get_root_url(request)
        return ogc_generator._db_row_to_ogc_feature(updated_row, catalog_id, collection_id, root_url)

    async def delete_item(self, catalog_id: str, collection_id: str, item_id: str, engine = Depends(get_async_engine)):
        catalogs_svc = await self._get_catalogs_service()
        async with managed_transaction(engine) as conn:
            # Resolve physical mapping
            phys_schema = await catalogs_svc.resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await catalogs_svc.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            
            if not phys_schema or not phys_table:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Collection '{collection_id}' not found.")

            rows_affected = await catalogs_svc.soft_delete_item(
                phys_schema, 
                phys_table, 
                ext_id=item_id,
                db_resource=conn
            )
            if rows_affected > 0:
                await catalogs_svc.recalculate_and_update_extents(catalog_id, collection_id, db_resource=conn)
            if rows_affected == 0:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item '{item_id}' not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)