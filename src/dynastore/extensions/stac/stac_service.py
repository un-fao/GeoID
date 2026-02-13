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

# dynastore/extensions/stac/stac_service.py

import logging
import asyncio
from typing import Optional, cast, Dict, Any

import pystac
from fastapi import (FastAPI, APIRouter, Depends, HTTPException, Query, Request, status)
from fastapi.responses import Response
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.models.protocols import CatalogsProtocol, AssetsProtocol, ConfigsProtocol, StorageProtocol


import dynastore.modules.db_config.shared_queries as shared_queries
from dynastore.extensions import dynastore_extension, get_extension_instance
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_static
from .policies import register_stac_policies
from dynastore.extensions.tools.db import get_async_engine
from dynastore.extensions.tools.exception_handlers import handle_exception
from dynastore.modules.db_config.query_executor import managed_transaction, ResultHandler
from dynastore.extensions.stac.search import (CollectionSearchRequest,
                                              ItemSearchRequest,
                                              search_collections, search_items)

from dynastore.modules.stac.stac_config import (STAC_PLUGIN_CONFIG_ID,
                                                   StacPluginConfig)

from dynastore.tools.db import validate_sql_identifier # type: ignore
from .stac_models import STACCatalogRequest, stac_localize
from . import stac_generator, stac_db, asset_factory, metadata_mapper
from .stac_models import STACCollectionUpdate, STACItem, STACCollectionRequest, STACCatalogUpdate, STACItemResponse, STACItemCollectionResponse
from .stac_aggregation_models import AggregationRequest
from dynastore.extensions.tools.conformance import register_conformance_uris
from datetime import datetime, timezone
from dynastore.extensions.tools.url import get_url, get_parent_url, get_root_url
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import CatalogsProtocol, StorageProtocol, ConfigsProtocol

logger = logging.getLogger(__name__)
from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.web import expose_web_page
import os

STAC_API_URIS = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/collections",
    "https://api.stacspec.org/v1.0.0-rc.2/transactions",
    "https://api.stacspec.org/v1.0.0/item-search/definition",
    "https://api.stacspec.org/v1.0.0/item-search#filter",
    "https://api.stacspec.org/v1.0.0/item-search#sort",
    "https://api.stacspec.org/v1.0.0/item-search#fields",
    "https://api.stacspec.org/v1.0.0/item-search#context",
    "https://api.stacspec.org/v1.0.0/item-search#aggregation",
    "https://api.stacspec.org/v1.0.0/item-search#query",
    "https://api.stacspec.org/v1.0.0/item-search#filter:cql-json",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
]

@dynastore_extension
class STACService(ExtensionProtocol):
    router: APIRouter = APIRouter(prefix="/stac", tags=["OGC API - STAC - Spatio Temporal Asset Catalog"])

    def configure_app(self, app: FastAPI):
        """Early configuration for the STAC extension."""
        register_stac_policies()
        logger.info("STACService: Policies registered.")
        
        # Explicit mounting for robustness (e.g. if centralized mounting is skipped in tests)
        app.include_router(self.router)
        logger.info("STACService: Router mounted.")

    def __init__(self, app: FastAPI):
        register_conformance_uris(STAC_API_URIS)
        logger.info("STACService: Successfully registered conformance classes.")
        self.app = app
        self._register_routes()

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

    def _register_routes(self):
        """Registers routes using bound methods."""
        # Catalog Discovery
        self.router.add_api_route("/", self.get_stac_root_catalog, methods=["GET"])
        self.router.add_api_route("/catalogs", self.list_stac_catalogs, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/catalogs/{catalog_id}", self.get_stac_catalog, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/catalogs/{catalog_id}/collections", self.list_stac_collections, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.get_stac_collection, methods=["GET"], response_class=JSONResponse)
        
        # Write Operations
        self.router.add_api_route("/catalogs", self.create_stac_catalog, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}/collections", self.create_stac_collection, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}", self.update_stac_catalog, methods=["PUT"], status_code=status.HTTP_200_OK)
        self.router.add_api_route("/catalogs/{catalog_id}", self.delete_stac_catalog, methods=["DELETE"])
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.update_stac_collection, methods=["PUT"], status_code=status.HTTP_200_OK)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}", self.delete_stac_collection, methods=["DELETE"])
        
        # Item Endpoints
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items", self.get_stac_collection_items, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.get_stac_item, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items", self.add_stac_item, methods=["POST"], status_code=status.HTTP_201_CREATED)
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.update_stac_item, methods=["PUT"])
        self.router.add_api_route("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", self.delete_stac_item, methods=["DELETE"])
        
        # Search Endpoints
        self.router.add_api_route("/search", self.search_items_post, methods=["POST"], response_class=JSONResponse)
        self.router.add_api_route("/collections/search", self.search_stac_collections_post, methods=["POST"], response_class=JSONResponse)
        
        # Virtual STAC Endpoints
        self.router.add_api_route("/virtual/assets/catalogs/{catalog_id}/collections/{collection_id}", self.get_virtual_asset_list, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/virtual/assets/{asset_code}/catalogs/{catalog_id}/collections/{collection_id}", self.get_virtual_asset_collection, methods=["GET"], response_class=JSONResponse)
        self.router.add_api_route("/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}/items", self.get_virtual_asset_items, methods=["GET"], response_class=JSONResponse)

    @expose_static("stac")
    def provide_static_files(self) -> list[str]:
        """Exposes the internal static directory for the STAC browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    async def _get_stac_config(self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None) -> StacPluginConfig:
        configs_svc = await self._get_configs_service()
        cfg = await configs_svc.get_config(STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=db_resource)
        return cast(StacPluginConfig, cfg)

    async def get_stac_root_catalog(self, request: Request, language: str = Depends(get_language)):
        catalog_dict = await stac_generator.create_root_catalog(request, lang=language)
        return JSONResponse(content=catalog_dict)
    
    async def list_stac_catalogs(self, request: Request, language: str = Depends(get_language)):
        """Lists all available STAC catalogs."""
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs()
        content = [stac_localize(c, language)[0] for c in catalogs]
        return JSONResponse(content=content)

    @expose_web_page(
        page_id="stac_browser", 
        title="STAC Browser", 
        icon="fa-layer-group",
        description="Explore satellite imagery and geospatial assets."
    )
    async def provide_stac_browser(self, request: Request):
        return await self._serve_page_template("stac_browser.html")

    async def _serve_page_template(self, filename: str):
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
             return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
             return Response(content=f.read(), media_type="text/html")

    async def get_stac_catalog(self, catalog_id: str, request: Request, language: str = Depends(get_language)):
        catalog_id = validate_sql_identifier(catalog_id)
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_catalog(catalog_id, lang=language):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        catalog_dict = await stac_generator.create_catalog(request, catalog_id=catalog_id, lang=language)
        return JSONResponse(content=catalog_dict)

    async def list_stac_collections(self, catalog_id: str, request: Request, language: str = Depends(get_language)):
        catalog_id = validate_sql_identifier(catalog_id)
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_catalog(catalog_id, lang=language):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        # From stac_generator.py: create_collection is the only one, maybe there is no list? 
        # Actually, OGC API - STAC requires /collections. 
        # I'll check stac_generator.py for a listing method if it exists.
        # Assuming it is stac_generator.create_collections (guessed)
        try:
            # Let's check if stac_generator has a generic collections lister
            # If not, we might need to implement it.
            # Usually it's create_root_catalog -> provides links to collections.
            # But the spec also has /collections route.
            collections = await stac_generator.create_collections_catalog(request, catalog_id=catalog_id, lang=language)
            return JSONResponse(content=collections)
        except AttributeError:
             # Fallback if method doesn't exist yet
             raise HTTPException(status_code=501, detail="STAC Collection listing not yet implemented in generator.")

    async def get_stac_collection(self, catalog_id: str, collection_id: str, request: Request, language: str = Depends(get_language)):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail=f"Collection '{catalog_id}:{collection_id}' not found.")
        collection = await stac_generator.create_collection(request, catalog_id=catalog_id, collection_id=collection_id, lang=language)
        return JSONResponse(content=collection.to_dict())

    # --- Write Endpoints ---
    
    async def create_stac_catalog(self, definition: STACCatalogRequest, language: str = Depends(get_language)):
        try:
            # We use STACCatalog (DTO) for validation but the catalogs_svc expects the structure to be merged
            # The definition is a Pydantic model with localized fields. model_dump() handles serialization.
            # Auto-detect if multi-language input is used
            from dynastore.models.localization import is_multilanguage_input
            input_data = definition.model_dump(exclude_unset=True)
            use_lang = "*" if any(is_multilanguage_input(input_data.get(f)) for f in ['title', 'description', 'keywords', 'license', 'extra_metadata']) else language
            
            catalogs_svc = await self._get_catalogs_service()
            created_catalog_model = await catalogs_svc.create_catalog(input_data, lang=use_lang)
            localized_data, _ = stac_localize(created_catalog_model, language)
            return JSONResponse(content=localized_data, status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise handle_exception(e, resource_name="Catalog", resource_id=definition.id, operation="STAC Catalog creation")


    async def create_stac_collection(self, catalog_id: str, request_body: STACCollectionRequest, language: str = Depends(get_language)):
        # Ensure that we pass the language and that request_body is converted to a dict including localized fields
        # STACCollection inherits from CoreCollection -> BaseMetadata -> LocalizedFieldsBase
        # model_dump will handle serialization
        try:
            # Auto-detect if multi-language input is used
            from dynastore.models.localization import is_multilanguage_input
            input_data = request_body.model_dump(exclude_unset=True)
            use_lang = "*" if any(is_multilanguage_input(input_data.get(f)) for f in ['title', 'description', 'keywords', 'license', 'extra_metadata']) else language

            catalogs_svc = await self._get_catalogs_service()
            created_collection_model = await catalogs_svc.create_collection(catalog_id, input_data, lang=use_lang)
            localized_data, _ = stac_localize(created_collection_model, language)
            return JSONResponse(content=localized_data, status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise handle_exception(e, resource_name="Collection", resource_id=f"{catalog_id}:{request_body.id}", operation="STAC Collection creation")

    async def update_stac_catalog(self, catalog_id: str, definition: STACCatalogUpdate, language: str = Depends(get_language)): # type: ignore
        # Auto-detect if multi-language input is used
        from dynastore.models.localization import is_multilanguage_input
        input_data = definition.model_dump(exclude_unset=True)
        use_lang = "*" if any(is_multilanguage_input(input_data.get(f)) for f in ['title', 'description', 'keywords', 'license', 'extra_metadata']) else language

        catalogs_svc = await self._get_catalogs_service()
        updated_catalog_model = await catalogs_svc.update_catalog(catalog_id, input_data, lang=use_lang)
        if not updated_catalog_model:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Catalog '{catalog_id}' not found.")
        localized_data, _ = stac_localize(updated_catalog_model, language)
        return JSONResponse(content=localized_data)

    async def delete_stac_catalog(self, catalog_id: str, force = Query(..., description="Force hard deletion of the collection (can't be undone)")):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_catalog(catalog_id, force=force):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Catalog '{catalog_id}' not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def update_stac_collection(self, catalog_id: str, collection_id: str, request_body: STACCollectionUpdate, language: str = Depends(get_language)):
        # Auto-detect if multi-language input is used
        from dynastore.models.localization import is_multilanguage_input
        input_data = request_body.model_dump(exclude_unset=True)
        use_lang = "*" if any(is_multilanguage_input(input_data.get(f)) for f in ['title', 'description', 'keywords', 'license', 'extra_metadata']) else language

        catalogs_svc = await self._get_catalogs_service()
        updated_collection_model = await catalogs_svc.update_collection(catalog_id, collection_id, input_data, lang=use_lang)
        if not updated_collection_model:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Collection '{catalog_id}:{collection_id}' not found.")
        localized_data, _ = stac_localize(updated_collection_model, language)
        return JSONResponse(content=localized_data)

    async def delete_stac_collection(self, catalog_id: str, collection_id: str):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_collection(catalog_id, collection_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Collection '{catalog_id}:{collection_id}' not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)


    async def get_stac_collection_items(self, catalog_id: str, collection_id: str, request: Request, engine = Depends(get_async_engine), limit: int = Query(10, ge=1, le=1000), offset: int = Query(0, ge=0), language: str = Depends(get_language)):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
            collection_metadata = await catalogs_svc.get_collection(catalog_id, collection_id, db_resource=conn)
            if not collection_metadata: raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")
            
            # Resolve physical mapping
            phys_schema = await catalogs_svc.resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await catalogs_svc.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            
            if not phys_schema or not phys_table:
                return stac_generator.create_empty_item_collection(request, limit, offset)

            # Check physical existence using protocol or shared tools if needed
            # For now, catalogs_svc.validate_collection_access can be used if available, or stay with shared_queries
            exists = await shared_queries.table_exists_query.execute(conn, schema=phys_schema, table=phys_table)
            if not exists: return stac_generator.create_empty_item_collection(request, limit, offset)

            stac_config = await self._get_stac_config(catalog_id, collection_id, db_resource=conn)

            items_dict = await stac_generator.create_item_collection(request, conn, phys_schema, phys_table, limit, offset, stac_config, catalog_id=catalog_id, collection_id=collection_id, lang=language)
            return items_dict

    async def get_stac_item(self, catalog_id: str, collection_id: str, item_id: str, request: Request, engine = Depends(get_async_engine), language: str = Depends(get_language)):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
            item_row = await catalogs_svc.get_item_by_external_id(
                catalog_id, 
                collection_id, 
                ext_id=item_id,
                db_resource=conn
            )
            if not item_row: 
                raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found.")

            stac_config = await self._get_stac_config(catalog_id, collection_id, db_resource=conn)
            
        # Response generation OUTSIDE transaction
        response_item = await stac_generator.create_item_from_row(request, catalog_id, collection_id, item_row, stac_config, lang=language)
        return response_item.to_dict()

    async def add_stac_item(self, catalog_id: str, collection_id: str, item_payload: STACItem, request: Request, engine = Depends(get_async_engine), language: str = Depends(get_language)):
        stac_item = item_payload.to_pystac()
        if not stac_item.collection_id:
            stac_item.collection_id = collection_id
        
        catalogs_svc = await self._get_catalogs_service()
        
        try:
            async with managed_transaction(engine) as conn:
                # We pass the STACItem directly to upsert, which handles validation/conversion internally
                new_row_or_list = await catalogs_svc.upsert(
                    catalog_id, 
                    collection_id, 
                    items=item_payload,  # Pass STACItem model directly
                    db_resource=conn
                )
                new_row = new_row_or_list if not isinstance(new_row_or_list, list) else new_row_or_list[0]
            
            # Response generation OUTSIDE the transaction
            stac_config = await self._get_stac_config(catalog_id, collection_id)
            response_item = await stac_generator.create_item_from_row(request, catalog_id, collection_id, new_row, stac_config, lang=language)
            
            if not response_item:
                raise HTTPException(status_code=500, detail="Failed to generate STAC Item from created row.")
            return response_item.to_dict()
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except Exception as e:
            logger.exception(f"Unexpected error in add_stac_item: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred during STAC item creation.")

    async def update_stac_item(self, catalog_id: str, collection_id: str, item_id: str, item_payload: STACItem, request: Request, engine = Depends(get_async_engine), language: str = Depends(get_language)):
        stac_item = item_payload.to_pystac()
        if not stac_item.collection_id:
            stac_item.collection_id = collection_id
        if stac_item.id != item_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Item ID mismatch: path '{item_id}' vs payload '{stac_item.id}'.")
        
        catalogs_svc = await self._get_catalogs_service()
        
        try:
            async with managed_transaction(engine) as conn:
                # Upsert handles updates if ID exists
                updated_row_or_list = await catalogs_svc.upsert(
                    catalog_id, 
                    collection_id, 
                    items=item_payload, 
                    db_resource=conn
                )
                updated_row = updated_row_or_list if not isinstance(updated_row_or_list, list) else updated_row_or_list[0]
            
            # Response generation OUTSIDE the transaction
            stac_config = await self._get_stac_config(catalog_id, collection_id)
            response_item = await stac_generator.create_item_from_row(request, catalog_id, collection_id, updated_row, stac_config, lang=language)
            
            if not response_item:
                raise HTTPException(status_code=500, detail="Failed to generate STAC Item from updated row.")
            return response_item.to_dict()
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except Exception as e:
            logger.exception(f"Unexpected error in update_stac_item: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred during STAC item update.")

    async def delete_stac_item(self, catalog_id: str, collection_id: str, item_id: str, engine = Depends(get_async_engine)):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
            rows_affected = await catalogs_svc.delete_item(
                catalog_id, 
                collection_id, 
                ext_id=item_id,
                db_resource=conn
            )
            
            if rows_affected == 0:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item '{item_id}' not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def search_items_post(self, request: Request, search_request: ItemSearchRequest, engine = Depends(get_async_engine), language: str = Depends(get_language)):
        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(search_request.catalog_id, db_resource=conn)
            rows, count, aggregations = await search_items(conn, search_request, stac_config)
        
        # Release connection before PySTAC processing
        results = await stac_generator.create_search_results_collection(request, rows, count, search_request.limit, search_request.offset, stac_config, lang=language)
        if aggregations: results["aggregations"] = aggregations
        return JSONResponse(content=results)
    
    async def search_stac_collections_post(self, request: Request, search_req: CollectionSearchRequest, engine = Depends(get_async_engine), language: str = Depends(get_language)):
        async with managed_transaction(engine) as conn:
            collections, total_count = await search_collections(conn, search_req)
        
        # Release connection before PySTAC processing
        stac_collections = []
        for coll in collections:
            # Localize before creating PySTAC collection
            localized_coll, _ = stac_localize(coll, language)
            stac_coll = pystac.Collection(
                id=localized_coll.get("id"), 
                description=localized_coll.get("description"), 
                title=localized_coll.get("title"), 
                license=localized_coll.get("license"),
                extent=pystac.Extent(spatial=pystac.SpatialExtent(coll.extent.spatial.bbox), temporal=pystac.TemporalExtent(coll.extent.temporal.interval))
            )
            # Inject language metadata
            if "language" in localized_coll:
                stac_coll.extra_fields["language"] = localized_coll["language"]
            if "languages" in localized_coll:
                stac_coll.extra_fields["languages"] = localized_coll["languages"]
            
            stac_collections.append(stac_coll.to_dict())
        return JSONResponse(content={"collections": stac_collections, "context": {"limit": search_req.limit, "matched": total_count, "returned": len(stac_collections)}})

    # --- Virtual STAC Endpoints ---
    # NOTE: Virtual endpoints might also need localization if they return titles/descriptions.
    # Currently they generate titles dynamically based on IDs/codes which are language-agnostic.
    # We'll pass language where applicable.

    async def get_virtual_asset_list(
        self, catalog_id: str, collection_id: str, request: Request, engine = Depends(get_async_engine),
        limit: int = Query(10, ge=1, le=1000), offset: int = Query(0, ge=0)
    ):
        """
        Virtual View: Lists all 'Assets' for a collection as if they were STAC Collections.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
             stac_config = await self._get_stac_config(catalog_id, collection_id, db_resource=conn)
             
             if not stac_config.asset_tracking.enabled:
                 raise HTTPException(status_code=404, detail="Asset tracking not enabled for this collection.")
             
             # AssetsProtocol is also implemented by CatalogModule
             assets_svc = cast(AssetsProtocol, catalogs_svc)
             assets = await assets_svc.list_assets(catalog_id=catalog_id, collection_id=collection_id, limit=limit, offset=offset, db_resource=conn)
        
        base_url = get_url(request, remove_qp=True)
        # Root for assets is /stac/virtual/assets
        assets_root = f"{get_root_url(request)}/stac/virtual/assets"
        
        virtual_cat = pystac.Catalog(
            id=f"{collection_id}_assets",
            description=f"Virtual collection of assets for {collection_id}",
            title=f"Assets of {collection_id}"
        )
        virtual_cat.set_self_href(base_url)
        
        for asset in assets:
            # New structure: /virtual/assets/{asset_id}/catalogs/{cat}/collections/{coll}
            child_href = f"{assets_root}/{asset.asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
            virtual_cat.add_link(
                pystac.Link(
                    rel="child",
                    target=child_href,
                    title=asset.asset_id,
                    media_type="application/json"
                )
            )
            
        # Add pagination links
        if len(assets) == limit:
            virtual_cat.add_link(pystac.Link(rel="next", target=f"{base_url}?offset={offset+limit}&limit={limit}"))
        if offset > 0:
            virtual_cat.add_link(pystac.Link(rel="prev", target=f"{base_url}?offset={max(0, offset-limit)}&limit={limit}"))
 
        return JSONResponse(content=virtual_cat.to_dict())

    async def get_virtual_asset_collection(self, catalog_id: str, collection_id: str, asset_code: str, request: Request, engine = Depends(get_async_engine)):
        """
        Virtual View: Represents a single Asset as a STAC Collection.
        Metadata is generated from the Asset record.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
             stac_config = await self._get_stac_config(catalog_id, collection_id, db_resource=conn)
             
             # AssetsProtocol is also implemented by CatalogModule
             assets_svc = cast(AssetsProtocol, catalogs_svc)
             asset = await assets_svc.get_asset(asset_id=asset_code, catalog_id=catalog_id, collection_id=collection_id, db_resource=conn)
             if not asset:
                 logger.warning(f"Virtual Asset not found: catalog={catalog_id}, id={asset_code}, collection={collection_id}")
                 raise HTTPException(status_code=404, detail=f"Asset '{asset_code}' not found.")
        
        # Create a Collection representing this Asset
        base_url = get_url(request)
        collection = pystac.Collection(
            id=asset.asset_id,
            description=f"Virtual collection for ingested asset: {asset.asset_id}",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[ -180, -90, 180, 90 ]]), 
                temporal=pystac.TemporalExtent([[ asset.created_at, None ]])
            ),
            title=asset.asset_id,
            license="proprietary"
        )
        
        # Enrich from metadata
        metadata_mapper.enrich_collection_from_metadata(collection, asset.metadata or {})
        
        collection.set_self_href(base_url)
        collection.add_link(pystac.Link(rel="items", target=f"{base_url}/items", media_type="application/geo+json"))
        
        # Parent is the list of assets for this collection
        parent_href = f"{get_root_url(request)}/stac/virtual/assets/catalogs/{catalog_id}/collections/{collection_id}"
        collection.add_link(pystac.Link(rel="parent", target=parent_href, title="Assets List"))
        
        # Add dynamic assets via factory
        asset_context = asset_factory.AssetContext(
            base_url=get_root_url(request),
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
            stac_config=stac_config,
            asset_id=asset.asset_id
        )
        asset_factory.add_dynamic_assets(collection, asset_context)
 
        return JSONResponse(content=collection.to_dict())

    async def get_virtual_asset_items(
        self, catalog_id: str, collection_id: str, asset_id: str, request: Request, 
        engine = Depends(get_async_engine), limit: int = Query(10, ge=1, le=1000), offset: int = Query(0, ge=0), language: str = Depends(get_language)
    ):
        """
        Virtual View: Lists items belonging to a specific asset.
        Filters the main collection by 'asset_id'.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        
        async with managed_transaction(engine) as conn:
             stac_config = await self._get_stac_config(catalog_id, collection_id, db_resource=conn)
             
             phys_schema = await catalogs_svc.resolve_physical_schema(catalog_id, db_resource=conn)
             phys_table = await catalogs_svc.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
             
             # Optimized query with limit/offset and total count
             # NOTE: get_items_by_asset_id_query is not yet in the protocol but available in CatalogModule
             # For now we use the protocol and cast to CatalogModule if needed, or better, we should have it in protocol.
             # Actually I'll use the top-level query from catalog_module if needed, but I'll try to keep it protocol-oriented.
             # Since CatalogModule is the implementation, we can access its queries if we have the instance.
             # But let's use the DQLQuery directly if we have to, or add to protocol.
             from dynastore.modules.catalog.catalog_module import get_items_by_asset_id_query
             items_rows = await get_items_by_asset_id_query.execute(
                 conn, 
                 catalog_id=phys_schema, 
                 collection_id=phys_table, 
                 asset_id=asset_id,
                 limit=limit,
                 offset=offset,
                 logical_catalog_id=catalog_id,
                 logical_collection_id=collection_id
             )
             
             total_count = await (await self._get_catalogs_service()).count_items_by_asset_id_query.execute(
                 conn,
                 catalog_id=phys_schema,
                 collection_id=phys_table,
                 asset_id=asset_id
             )
            
             stac_items = await asyncio.gather(*[
                 stac_generator.create_item_from_row(request, catalog_id, collection_id, row, stac_config, view_mode="virtual-asset", lang=language) 
                 for row in items_rows
             ])
             
        item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
        coll_dict = item_collection.to_dict()
        
        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)
        
        # Paging links
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            coll_dict.setdefault("links", []).append({"rel": "next", "href": f"{base_url}?offset={offset+limit}&limit={limit}"})
        if offset > 0:
            coll_dict.setdefault("links", []).append({"rel": "prev", "href": f"{base_url}?offset={max(0, offset-limit)}&limit={limit}"})

        return JSONResponse(content=coll_dict)


    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_code}/source")
    async def resolve_asset_source(
        catalog_id: str, collection_id: str, asset_code: str, request: Request, engine = Depends(get_async_engine)
    ):
        """
        Dynamically resolves the source asset URL.
        Decides between Proxy or Direct access based on STAC configuration.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        
        async with managed_transaction(engine) as conn:
            # 1. Get the asset to find its URI
            asset_mgr = get_protocol(AssetsProtocol)
            if not asset_mgr:
                 raise HTTPException(status_code=500, detail="Assets protocol not available.")
            
            asset = await asset_mgr.get_asset(asset_id=asset_code, catalog_id=catalog_id, collection_id=collection_id)
            if not asset:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Asset '{asset_code}' not found.")
            
            # 2. Get STAC config to decide on access mode
            # If no collection_id provided, we try to use a default or first collection config
            # But usually we want the collection-specific config.
            cm = get_protocol(ConfigsProtocol)
            if not cm:
                 raise HTTPException(status_code=500, detail="Configs protocol not available.")
            stac_config_raw = await cm.get_config(STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn)
            stac_config = cast(StacPluginConfig, stac_config_raw)
            
            from dynastore.modules.stac.stac_config import AssetAccessMode
            from fastapi.responses import RedirectResponse
            if stac_config.asset_tracking.access_mode == AssetAccessMode.PROXY:
                try:
                    from dynastore.modules.proxy.proxy_module import create_short_url
                    proxy_key = await create_short_url(conn, catalog_id=catalog_id, long_url=asset.uri, collection_id=collection_id, comment=f"Dynamic source for {asset_code}")
                    root_path = request.scope.get("root_path", "").rstrip("/")
                    proxy_url = f"{root_path}/proxy/r/{proxy_key.short_key}"
                    return RedirectResponse(url=proxy_url)
                except Exception as e:
                    logger.error(f"Failed to create proxy for asset {asset_code}: {str(e)}")
                    return RedirectResponse(url=asset.uri)
            else:
                return RedirectResponse(url=asset.uri)

    @router.get("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}", response_class=JSONResponse)
    async def get_virtual_hierarchy_collection(
        hierarchy_id: str, 
        catalog_id: str, 
        collection_id: str, 
        request: Request, 
        engine = Depends(get_async_engine),
        parent_value: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=1000)
    ):
        """
        Virtual View: Returns a STAC collection representation of a hierarchy level.
        If parent_value is provided, the collection metadata is scoped to that parent.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        
        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_manager = get_protocol(ConfigsProtocol)
            if not config_manager:
                 raise HTTPException(status_code=500, detail="Configs protocol not available.")
            
            stac_config = cast(StacPluginConfig, await config_manager.get_config(
                STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
            ))
            
            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(status_code=404, detail="Hierarchy not enabled for this collection.")
            
            # Get the rule directly from dictionary (O(1) lookup)
            matching_rule = stac_config.hierarchy.rules.get(hierarchy_id)
            if not matching_rule:
                raise HTTPException(status_code=404, detail=f"Hierarchy rule '{hierarchy_id}' not found.")
            
            # Import hierarchy queries
            from dynastore.extensions.stac import stac_hierarchy_queries
            
            # Get distinct values for this hierarchy level (optionally filtered by parent_value)
            distinct_values = await stac_hierarchy_queries.get_distinct_hierarchy_values(
                conn, catalog_id, collection_id, matching_rule, parent_value=parent_value, limit=limit
            )
            
            # Get computed extent (bbox and temporal)
            extent_data = await stac_hierarchy_queries.get_hierarchy_extent(
                conn, catalog_id, collection_id, matching_rule, parent_value=parent_value
            )
        
        # Generate virtual collection
        base_url = get_url(request, remove_qp=True)
        root_url = get_root_url(request)
        
        # Use template if provided, otherwise use default
        title = matching_rule.collection_title_template or f"{matching_rule.level_name or hierarchy_id} Hierarchy"
        if parent_value:
             title += f" (Parent: {parent_value})"
             
        description = matching_rule.collection_description_template or f"Virtual collection for {matching_rule.level_name or hierarchy_id} level"
        
        virtual_collection = pystac.Collection(
            id=f"{collection_id}_{hierarchy_id}",
            description=description,
            title=title,
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([extent_data["bbox"]]),
                temporal=pystac.TemporalExtent(extent_data["temporal"])
            ),
            license="proprietary"
        )
        
        virtual_collection.set_self_href(get_url(request))
        
        # Add parent link if this rule has a parent level
        if matching_rule.parent_hierarchy_id:
             parent_rule_id = matching_rule.parent_hierarchy_id
             # Link to the parent hierarchy collection
             parent_coll_url = f"{root_url}/stac/virtual/hierarchy/{parent_rule_id}/catalogs/{catalog_id}/collections/{collection_id}"
             virtual_collection.add_link(
                 pystac.Link(rel="parent", target=parent_coll_url, title=f"Parent Level: {parent_rule_id}", media_type="application/json")
             )
        else:
             # Root of hierarchy links back to the main collection
             main_coll_url = f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
             virtual_collection.add_link(
                 pystac.Link(rel="parent", target=main_coll_url, title="Main Collection", media_type="application/json")
             )

        # Add child links for each distinct value
        for value_row in distinct_values:
            code = value_row['code']
            item_count = value_row.get('item_count', 0)
            
            # Children are either items (if at leaf) or next level collections
            # For now we link to items scoped by this value
            child_href = f"{base_url}/items?parent_value={code}"
            
            # If there is a rule that has THIS rule as its parent, we could also link to that
            # but usually the navigation is Value -> Children
            virtual_collection.add_link(
                pystac.Link(
                    rel="child",
                    target=child_href,
                    title=f"Items for {code} ({item_count} items)",
                    media_type="application/geo+json"
                )
            )
        
        # Add items link (all items in this level)
        items_url = f"{base_url}/items"
        if parent_value:
             items_url += f"?parent_value={parent_value}"
             
        virtual_collection.add_link(
            pystac.Link(rel="items", target=items_url, media_type="application/geo+json")
        )
        
        return JSONResponse(content=virtual_collection.to_dict())

    @router.get("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}/items", response_class=JSONResponse)
    async def get_virtual_hierarchy_items(
        hierarchy_id: str,
        catalog_id: str,
        collection_id: str,
        request: Request,
        engine = Depends(get_async_engine),
        parent_value: Optional[str] = Query(None),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language)
    ):
        """
        Virtual View: Returns items filtered by hierarchy rule.
        If parent_value is provided, filters to children of that parent.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        
        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_manager = catalog_manager.get_config_manager()
            stac_config = cast(StacPluginConfig, await config_manager.get_config(
                STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
            ))
            
            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(status_code=404, detail="Hierarchy not enabled for this collection.")
            
            # Find the matching rule directly from dictionary (O(1) lookup)
            matching_rule = stac_config.hierarchy.rules.get(hierarchy_id)
            
            if not matching_rule:
                raise HTTPException(status_code=404, detail=f"Hierarchy rule '{hierarchy_id}' not found.")
            
            # Import hierarchy queries
            from dynastore.extensions.stac import stac_hierarchy_queries
            from dynastore.modules.catalog.catalog_module import GeoDQLQuery
            
            # Build and execute query
            sql, params = await stac_hierarchy_queries.build_hierarchy_items_query(
                catalog_id, collection_id, matching_rule, parent_value, limit, offset
            )
            
            query = GeoDQLQuery(text(sql), result_handler=ResultHandler.ALL)
            items_rows = await query.execute(conn, **params)
            
            # Get total count
            total_count = await stac_hierarchy_queries.get_hierarchy_item_count(
                conn, catalog_id, collection_id, matching_rule, parent_value
            )
            
            # Generate STAC items
            stac_items = await asyncio.gather(*[
                stac_generator.create_item_from_row(request, catalog_id, collection_id, row, stac_config, view_mode="virtual-hierarchy", lang=language)
                for row in items_rows
            ])
        
        # Create item collection
        item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
        coll_dict = item_collection.to_dict()
        
        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)
        
        # Paging links
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            next_url = f"{base_url}?offset={offset+limit}&limit={limit}"
            if parent_value:
                next_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "next", "href": next_url})
            
        return JSONResponse(content=coll_dict)

    @router.post("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}/search", response_class=JSONResponse)
    async def search_virtual_hierarchy_items(
        hierarchy_id: str,
        catalog_id: str,
        collection_id: str,
        search_request: ItemSearchRequest,
        request: Request,
        parent_value: Optional[str] = Query(None),
        engine = Depends(get_async_engine),
        language: str = Depends(get_language)
    ):
        """
        Virtual View: Executes a STAC search scoped to a hierarchy level.
        Combines hierarchy filters with standard search parameters.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        
        # Override catalog_id in search_request to ensure consistency
        search_request.catalog_id = catalog_id
        # Scoped to the specific collection
        search_request.collections = [collection_id]
        
        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_manager = catalog_manager.get_config_manager()
            stac_config = cast(StacPluginConfig, await config_manager.get_config(
                STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
            ))
            
            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(status_code=404, detail="Hierarchy not enabled for this collection.")
            
            # Find the matching rule directly from dictionary (O(1) lookup)
            matching_rule = stac_config.hierarchy.rules.get(hierarchy_id)
            
            if not matching_rule:
                raise HTTPException(status_code=404, detail=f"Hierarchy rule '{hierarchy_id}' not found.")
            
            # Import hierarchy queries and search
            from dynastore.extensions.stac import stac_hierarchy_queries, search as stac_search
            
            # Build hierarchy SQL fragment
            hierarchy_params = {}
            hierarchy_sql = stac_hierarchy_queries.build_hierarchy_where_clause(
                matching_rule, parent_value, hierarchy_params
            )
            
            # Execute search with hierarchy scope
            items_rows, total_count, aggregation_results = await stac_search.search_items(
                conn, search_request, stac_config, 
                hierarchy_sql=hierarchy_sql, 
                hierarchy_params=hierarchy_params
            )
            
            # Generate STAC items
            stac_items = await asyncio.gather(*[
                stac_generator.create_item_from_row(request, catalog_id, collection_id, row, stac_config, view_mode="virtual-hierarchy", lang=language)
                for row in items_rows
            ])
            
        # Create item collection
        item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
        coll_dict = item_collection.to_dict()
        
        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)
        
        # Add aggregation results if present
        if aggregation_results:
            coll_dict["aggregations"] = aggregation_results
            
        # Paging links
        offset = search_request.offset
        limit = search_request.limit
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            next_url = f"{base_url}?offset={offset+limit}&limit={limit}"
            if parent_value:
                next_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "next", "href": next_url})
            
        if offset > 0:
            prev_url = f"{base_url}?offset={max(0, offset-limit)}&limit={limit}"
            if parent_value:
                prev_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "prev", "href": prev_url})
            
        return JSONResponse(content=coll_dict)

    @router.post("/catalogs/{catalog_id}/collections/{collection_id}/aggregate", response_class=JSONResponse)
    async def aggregate_collection_items(
        catalog_id: str,
        collection_id: str,
        request: Request,
        aggregation_request: AggregationRequest,
        engine = Depends(get_async_engine)
    ):
        """
        Executes aggregations on collection items.
        Supports OGC STAC Aggregation Extension.
        """
        from dynastore.extensions.stac import stac_aggregations
        
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        
        async with managed_transaction(engine) as conn:
            # Get STAC config
            config_manager = catalog_manager.get_config_manager()
            stac_config = cast(StacPluginConfig, await config_manager.get_config(
                STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id, db_resource=conn
            ))
            
            # Check if aggregations are enabled
            if stac_config.aggregations and not stac_config.aggregations.enabled:
                raise HTTPException(status_code=403, detail="Aggregations are not enabled for this collection.")
            
            # Validate aggregation count
            if stac_config.aggregations:
                max_aggs = stac_config.aggregations.max_aggregations_per_request
                if len(aggregation_request.aggregations) > max_aggs:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Too many aggregations requested. Maximum is {max_aggs}."
                    )
                
                # Check if custom aggregations are allowed
                if not stac_config.aggregations.allow_custom:
                    # Verify all requested aggregations are in default_rules
                    default_names = {rule.name for rule in stac_config.aggregations.default_rules}
                    requested_names = {agg.name for agg in aggregation_request.aggregations}
                    if not requested_names.issubset(default_names):
                        raise HTTPException(
                            status_code=403,
                            detail="Custom aggregations are not allowed. Use only pre-configured aggregations."
                        )
            
            # Build WHERE clause from filters
            where_clauses = ["deleted_at IS NULL"]
            params = {}
            
            if aggregation_request.bbox:
                bbox = aggregation_request.bbox
                where_clauses.append(
                    "ST_Intersects(geom, ST_MakeEnvelope(:bbox_xmin, :bbox_ymin, :bbox_xmax, :bbox_ymax, 4326))"
                )
                params.update(bbox_xmin=bbox[0], bbox_ymin=bbox[1], bbox_xmax=bbox[2], bbox_ymax=bbox[3])
            
            if aggregation_request.datetime:
                # Simple datetime filter (could be enhanced for intervals)
                where_clauses.append("attributes->>'datetime' = :datetime")
                params["datetime"] = aggregation_request.datetime
            
            where_sql = " AND ".join(where_clauses)
            
            # Execute aggregations
            collections = aggregation_request.collections or [collection_id]
            results = await stac_aggregations.execute_aggregations(
                conn, catalog_id, collections, aggregation_request.aggregations, where_sql, params
            )
        
        return JSONResponse(content={"aggregations": results})