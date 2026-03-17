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

# dynastore/extensions/maps/maps_service.py

import logging
import asyncio
from typing import Optional, Any
from concurrent.futures import ProcessPoolExecutor
from fastapi import FastAPI, APIRouter, Depends, HTTPException, Response, Query, Request, Path
from sqlalchemy.ext.asyncio import AsyncConnection
from contextlib import asynccontextmanager
from pyproj import CRS

from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.maps.renderer import render_map_image
import dynastore.modules.catalog.catalog_module as catalog_manager
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.modules.db_config import shared_queries
from dynastore.tools.ogc_common import parse_subset_parameter
from . import maps_db
from .maps_models import MapsLandingPage, DatasetMaps, MapContent, Link
from .maps_config import MapsConfig
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_web_page
import os

# Imports for Tiling Support
from dynastore.modules.tiles.tiles_models import TileMatrixSetList, TileMatrixSet, TileMatrixSetRef, Link as TileLink
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

logger = logging.getLogger(__name__)

# --- Helpers ---

async def _get_style_to_render(conn: AsyncConnection, dataset: str, collection_id: Optional[str], style_name: Optional[str]) -> Optional[Any]:
    """
    Fetches a style record and finds the first compatible stylesheet (SLD or MapboxGL).
    Returns the stylesheet content object, or None if no style is requested.
    Raises HTTPException if the style is not found.
    """
    if not style_name or not collection_id:
        return None

    from dynastore.models.protocols import StylesProtocol
    from dynastore.tools.discovery import get_protocol
    styles_ext = get_protocol(StylesProtocol)
    if not styles_ext:
        return None # Styles extension is not enabled

    from dynastore.modules.styles import db as styles_db
    from dynastore.modules.styles.models import StyleFormatEnum

    style_record = await styles_db.get_style_by_id_and_collection(conn, dataset, collection_id, style_name)
    if not style_record:
        raise HTTPException(status_code=404, detail=f"Style with name '{style_name}' not found for collection '{collection_id}'.")

    # Find a compatible stylesheet from the list.
    # The renderer supports SLD and MapboxGL, so we look for those.
    for ss in style_record.stylesheets:
        if ss.content.format in [StyleFormatEnum.SLD_1_1, StyleFormatEnum.MAPBOX_GL]:
            return ss # Return the first compatible StyleSheet object
    
    return None # No compatible stylesheet format found in the style record

async def _validate_collections_helper(conn, dataset, requested_collections):
    """Shared helper to check logical and physical existence of collections."""
    collection_metadata_coroutines = []
    for coll_id in requested_collections:
        collection_metadata_coroutines.append(catalog_manager.get_collection(catalog_id=dataset, collection_id=coll_id))
    
    collection_metadata_results = await asyncio.gather(*collection_metadata_coroutines)
    
    physical_table_checks = []
    for i, coll_id in enumerate(requested_collections):
        if collection_metadata_results[i]:
            physical_table_checks.append(shared_queries.table_exists_query.execute(conn, schema=dataset, table=coll_id))
        else:
            physical_table_checks.append(asyncio.sleep(0, result=False))
    
    physical_table_results = await asyncio.gather(*physical_table_checks)

    valid_collections = []
    for i, coll_id in enumerate(requested_collections):
        if collection_metadata_results[i] and physical_table_results[i]:
            valid_collections.append(coll_id)
    return valid_collections

def _return_empty_tile(width, height):
    # Create a transparent 1x1 pixel or full size empty PNG
    # Minimal 1x1 transparent PNG signature
    empty_png = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82'
    # To generate a full-size empty tile, a library like Pillow would be better,
    # but for now, we return a minimal valid PNG to avoid client errors.
    # from PIL import Image
    # img = Image.new('RGBA', (width, height), (255, 255, 255, 0))
    # buffer = io.BytesIO()
    # img.save(buffer, format="PNG")
    # return Response(content=buffer.getvalue(), media_type="image/png")
    return Response(content=empty_png, media_type="image/png")

from .policies import register_maps_policies
class MapsService(ExtensionProtocol):
    priority: int = 100
    """Provides OGC API - Maps (WMS-like) functionality with filtering and Tiling."""
    router:APIRouter = APIRouter(tags=["OGC API - Maps (WMS)"], prefix="/maps")
    process_pool: Optional[ProcessPoolExecutor] = None

    def configure_app(self, app: FastAPI):
        """Early configuration: inject Maps public-access policy into global registry."""
        register_maps_policies()
        logger.info("MapsService: Policies registered.")

        # Register @expose_web_page methods via WebModuleProtocol
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import WebModuleProtocol
        web = get_protocol(WebModuleProtocol)
        if web:
            web.scan_and_register_providers(self)
            logger.info("MapsService: Web pages registered via WebModuleProtocol.")

    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Maps Service startup: creating process pool...")
        MapsService.process_pool = ProcessPoolExecutor()
        app.state.maps_config = MapsConfig()
        yield
        logger.info("Maps Service shutdown: closing process pool.")
        if MapsService.process_pool:
            MapsService.process_pool.shutdown(wait=True)

    @router.get("/", response_model=MapsLandingPage)
    async def get_maps_landing_page(request: Request):
        # Correctly discover catalogs from the catalog_module
        catalogs = await catalog_manager.list_catalogs(limit=1000)
        links = [Link(href=str(request.url), rel="self", type="application/json", title="this document")]
        for cat in catalogs:
            links.append(Link(
                href=str(request.url_for('get_dataset_maps', dataset=cat.id)),
                rel="dataset", type="application/json", title=f"Maps for dataset '{cat.id}'"
            ))
        return MapsLandingPage(links=links)

    @expose_web_page(
        page_id="map_viewer",
        title="Map Viewer",
        icon="fa-map",
        description="Visualize geospatial data on an interactive map.",
    )
    async def provide_map_viewer(self, request: Request):
        return await self._serve_page_template("map_viewer.html")

    async def _serve_page_template(self, filename: str):
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
             return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
             return Response(content=f.read(), media_type="text/html")

    @router.get("/{dataset}", response_model=DatasetMaps)
    async def get_dataset_maps(dataset: str, request: Request):
        if not await catalog_manager.get_catalog(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")
        
        collections = await catalog_manager.list_collections(dataset, limit=1000)
        maps = []
        for coll in collections:
            map_links = [
                Link(href=f"{request.url}/map?collections={coll.id}&bbox=-180,-90,180,90&crs=EPSG:4326", rel="item", type="image/png"),
                Link(href=f"{request.url}/map/tiles", rel="http://www.opengis.net/def/rel/ogc/1.0/tilesets-map", type="application/json", title="Map Tilesets")
            ]
            maps.append(MapContent(title=coll.title, links=map_links))
        
        links = [Link(href=str(request.url), rel="self"), Link(href=str(request.url_for('get_maps_landing_page')), rel="up")]
        return DatasetMaps(title=f"Maps for '{dataset}'", maps=maps, links=links)

    # --- Tiling Endpoints (Requirements Class "Map Tilesets") ---

    @router.get("/{dataset}/map/tiles", response_model=TileMatrixSetList, summary="Retrieve available Map Tile Matrix Sets")
    async def get_map_tilesets(dataset: str, request: Request):
        """List all supported Tile Matrix Sets for rendering raster map tiles."""
        if not await catalog_manager.get_catalog(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        tms_refs = []
        # 1. Built-in TMS
        for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items():
            tms_refs.append(TileMatrixSetRef(
                id=tms_id,
                title=tms_def.title,
                links=[
                    TileLink(
                        href=str(request.url_for("get_map_tileset", dataset=dataset, tileMatrixSetId=tms_id)), 
                        rel="self", 
                        type="application/json",
                        title=tms_def.title)
                ]
            ))
        
        # 2. Custom TMS from DB
        custom_tms_list = await tms_manager.list_custom_tms(catalog_id=dataset)
        for tms in custom_tms_list:
            if not any(ref.id == tms.id for ref in tms_refs):
                tms_refs.append(TileMatrixSetRef(
                    id=tms.id,
                    title=tms.title,
                    links=[TileLink(
                        href=str(request.url_for("get_map_tileset", dataset=dataset, tileMatrixSetId=tms.id)), 
                        rel="self", 
                        type="application/json",
                        title=tms.title)]
                ))
        return TileMatrixSetList(tileMatrixSets=tms_refs)

    @router.get("/{dataset}/map/tiles/{tileMatrixSetId}", response_model=TileMatrixSet, summary="Retrieve a Map Tile Matrix Set definition")
    async def get_map_tileset(dataset: str, tileMatrixSetId: str = Path(..., description="The Identifier of the Tile Matrix Set")):
        """Return the full definition of a specific Tile Matrix Set."""
        tms = await tms_manager.get_custom_tms(catalog_id=dataset, tms_id=tileMatrixSetId)
        if not tms:
            tms = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms:
                raise HTTPException(status_code=404, detail=f"TileMatrixSet '{tileMatrixSetId}' not found.")
        return tms

    @router.get("/{dataset}/map/tiles/{tileMatrixSetId}/{z}/{x}/{y}", summary="Get Rendered Map Tile")
    async def get_map_tile(
        request: Request, dataset: str, tileMatrixSetId: str, z: str, x: int, y: int,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(..., description="Comma-separated list of collection IDs."),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        subset: Optional[str] = Query(None, description="Custom dimension filter."),
        bgcolor: Optional[str] = Query(None),
        transparent: bool = Query(True),
        style: Optional[str] = Query(None)
    ):
        """
        Generates a raster map tile (PNG) for the specific Z/X/Y.
        """
        # 1. Fetch TMS Definition
        tms_def = await tms_manager.get_custom_tms(catalog_id=dataset, tms_id=tileMatrixSetId)
        if not tms_def:
            tms_def = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms_def:
                raise HTTPException(status_code=404, detail=f"TileMatrixSet {tileMatrixSetId} not supported.")

        # 2. Validate Matrix (Zoom Level)
        matrix_def = next((m for m in tms_def.tileMatrices if m.id == str(z)), None)
        if not matrix_def:
            raise HTTPException(status_code=400, detail=f"Zoom level '{z}' not found in TMS '{tileMatrixSetId}'.")

        # 3. Validate Coordinates
        if not (0 <= x < matrix_def.matrixWidth and 0 <= y < matrix_def.matrixHeight):
            raise HTTPException(status_code=400, detail="Tile coordinates out of bounds.")

        # 4. Resolve CRS and SRID
        try:
            target_srid = await tms_manager.resolve_srid(conn=conn, crs_str=tms_def.crs, catalog_id=dataset)
        except Exception as e:
            logger.error(f"CRS Error: {e}")
            raise HTTPException(status_code=500, detail=f"Could not process CRS '{tms_def.crs}' in TMS '{tileMatrixSetId}'.")

        # 5. Calculate Bounding Box for the Tile
        # OGC Tiles usually assume TopLeft origin for the matrix
        pixel_span_x = matrix_def.tileWidth * matrix_def.cellSize
        pixel_span_y = matrix_def.tileHeight * matrix_def.cellSize
        
        tile_min_x = matrix_def.pointOfOrigin[0] + (x * pixel_span_x)
        tile_max_y = matrix_def.pointOfOrigin[1] - (y * pixel_span_y)
        tile_max_x = tile_min_x + pixel_span_x
        tile_min_y = tile_max_y - pixel_span_y
        
        bbox_list = [tile_min_x, tile_min_y, tile_max_x, tile_max_y]

        # 6. Validate Collections
        requested_collections = [c.strip() for c in collections.split(',')]
        # Reuse validation logic (check metadata + table existence)
        valid_collections = await _validate_collections_helper(conn, dataset, requested_collections)
        if not valid_collections:
             # Return transparent empty tile if no valid data source
             return _return_empty_tile(matrix_def.tileWidth, matrix_def.tileHeight)

        subset_params = parse_subset_parameter(subset)

        # 7. Fetch Features (Optimized for Render)
        # Note: We pass the Tile Width/Height and the TMS SRID (target_srid) as the BBOX SRID
        try:
            layer_config, layers_data = await asyncio.gather(
                catalog_manager.get_collection_config(dataset, valid_collections[0]),
                maps_db.get_features_for_rendering(
                    conn=conn, 
                    schema=dataset, 
                    collections=valid_collections, 
                    bbox=bbox_list, 
                    crs=tms_def.crs,
                    width=matrix_def.tileWidth, 
                    height=matrix_def.tileHeight,
                    bbox_srid=target_srid, # Vital: The computed BBOX is in the TMS CRS
                    datetime_str=datetime, 
                    subset_params=subset_params
                )
            )
        except ValueError as e:
            logger.error(f"Render Fetch Error: {e}")
            raise HTTPException(status_code=400, detail=str(e))

        # 8. Render Image
        style_to_render = await _get_style_to_render(
            conn, dataset, valid_collections[0] if valid_collections else None, style
        )
        
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                MapsService.process_pool,
                render_map_image,
                matrix_def.tileWidth, matrix_def.tileHeight, 
                bbox_list, tms_def.crs, 
                layer_config.geometry_storage.target_srid, # Source SRID
                layers_data, style_to_render,
                transparent, bgcolor
            )
            return Response(content=image_bytes, media_type="image/png")
        except Exception as e:
            logger.error(f"Tile Render Failed: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Rendering failed.")

    # --- Existing Map Endpoint ---

    @router.get("/{dataset}/map")
    async def get_map(
        dataset: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(..., description="Comma-separated list of collections to render."),
        bbox: str = Query(..., description="Bounding box in CRS coordinates."),
        bbox_crs: str = Query(None, description="CRS of the BBOX (defaults to OGC:CRS84)."),
        crs: str = Query("EPSG:3857", description="Coordinate Reference System."),
        width: int = Query(768, description="Width of the output image."),
        height: int = Query(768, description="Height of the output image."),
        style: Optional[str] = Query(None, description="Name of the style to apply."),
        bgcolor: Optional[str] = Query(None, description="Background color of the map."),
        transparent: bool = Query(True, description="Whether the map background should be transparent."),
        datetime: Optional[str] = Query(None, description="Temporal filter (timestamp or interval)."),
        subset: Optional[str] = Query(None, description="Custom dimension filter.")
    ):
        # ... (Existing validation logic) ...
        if not await catalog_manager.get_catalog(dataset):
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found.")

        requested_collections = [c.strip() for c in collections.split(',')]
        valid_collections = await _validate_collections_helper(conn, dataset, requested_collections)

        if not valid_collections:
            raise HTTPException(status_code=404, detail="One or more collections not found.")

        # Handle BBOX CRS (Req 18)
        # If bbox_crs is provided, extract SRID. If NOT provided, standard says default is CRS84 (4326).
        bbox_srid = 4326
        if bbox_crs:
            try:
                # Simple parsing for [EPSG:XXXX] or URIs
                import re
                match = re.search(r'(\d+)$', bbox_crs)
                if match:
                    bbox_srid = int(match.group(1))
            except:
                pass # Fallback or error handling

        try:
            bbox_list = [float(coord) for coord in bbox.split(',')]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid BBOX format.")

        # Fetch Data with Updated DB Signature
        try:
            layer_config, layers_data = await asyncio.gather(
                catalog_manager.get_collection_config(dataset, valid_collections[0]),
                maps_db.get_features_for_rendering(
                    conn=conn, 
                    schema=dataset, 
                    collections=valid_collections, 
                    bbox=bbox_list, 
                    crs=crs,
                    width=width, 
                    height=height, 
                    bbox_srid=bbox_srid,
                    datetime_str=datetime, 
                    subset_params=parse_subset_parameter(subset)
                )
            )
        except ValueError as e:
            logger.error(f"Data Error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        
        # Fetch style to render
        style_to_render = await _get_style_to_render(
            conn, dataset, valid_collections[0] if valid_collections else None, style
        )

        # Render
        try:
            loop = asyncio.get_running_loop()
            image_bytes = await loop.run_in_executor(
                MapsService.process_pool,
                render_map_image,
                width, height, bbox_list, crs, 
                layer_config.geometry_storage.target_srid, 
                layers_data, style_to_render,
                transparent, bgcolor
            )
            return Response(content=image_bytes, media_type="image/png")
        except Exception as e:
            logger.error(f"Render Error: {e}")
            raise HTTPException(status_code=500, detail="Failed to render map.")