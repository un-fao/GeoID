#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    you may obtain a copy of the License at
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

# dynastore/extensions/styles/styles_service.py

import json as _json
import logging
import uuid
from typing import List, Optional

from fastapi import (APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request, Response)
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction
from starlette import status
from sqlalchemy import text
from sqlalchemy import text, DDL
from dynastore.extensions import protocols
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.url import get_root_url
from dynastore.modules.catalog import catalog_module
from dynastore.modules.styles import db as styles_db
from dynastore.modules.styles.encodings import (
    STYLE_FORMAT_TO_MEDIA_TYPE,
    f_param_to_media_type,
    normalize_accept_to_media_type,
)
from dynastore.modules.styles.models import (
    MapboxContent, SLDContent, Style, StyleCreate, StyleUpdate,
)
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


# OGC API - Styles Part 1 conformance URIs declared by this extension.
# The StylesService class attribute `conformance_uris` is picked up by the
# global /conformance aggregator (extensions/tools/conformance.py).
OGC_API_STYLES_URIS = [
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/style-info",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/mapbox-style",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-10",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-11",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/json",
]


def _pick_stylesheet_by_media_type(style: Style, media_type: str):
    """Return the StyleSheet matching ``media_type``, or ``None``."""
    for sheet in style.stylesheets:
        fmt = getattr(sheet.content, "format", None)
        fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
        if STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str) == media_type:
            return sheet
    return None


def _stylesheet_to_bytes(sheet) -> bytes:
    """Serialise a StyleSheet's content to bytes in its wire format."""
    content = sheet.content
    if isinstance(content, SLDContent):
        return content.sld_body.encode("utf-8")
    if isinstance(content, MapboxContent):
        # MapboxContent allows extra fields; dump the full content.
        return _json.dumps(
            content.model_dump(by_alias=True, exclude_none=True),
        ).encode("utf-8")
    # Defensive: unknown content type serialises via model_dump.
    return _json.dumps(
        content.model_dump() if hasattr(content, "model_dump") else dict(content),
    ).encode("utf-8")


from dynastore.models.protocols import StylesProtocol
class StylesService(protocols.ExtensionProtocol, StylesProtocol):
    priority: int = 100
    """
    Provides an OGC API - Styles compliant service.
    """
    router: APIRouter = APIRouter(prefix="/styles", tags=["OGC API - Styles"])

    # ConformanceContributor structural attribute — aggregated into
    # /conformance by extensions/tools/conformance.py.
    conformance_uris = OGC_API_STYLES_URIS

    def __init__(self, app: Optional[FastAPI] = None):
        app = app

    @router.post("/catalogs/{catalog_id}/collections/{collection_id}/styles", response_model=Style, status_code=status.HTTP_201_CREATED)
    async def create_style_for_collection(
        catalog_id: str,
        collection_id: str,
        style: StyleCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")

        # Ensure the partition for this catalog_id exists in the 'styles' table.
        # The 'styles' table is assumed to be in the 'public' schema and partitioned by 'catalog_id' (LIST strategy).
        from dynastore.modules.db_config.partition_tools import ensure_partition_exists
        try:
            await ensure_partition_exists(
                conn,
                table_name="styles",
                schema="styles",
                strategy="LIST",
                partition_value=catalog_id
            )
        except Exception as e:
            logger.error(f"Failed to ensure partition for catalog '{catalog_id}' in 'styles' table: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error: Could not prepare database for catalog '{catalog_id}'. Please ensure the catalog is properly initialized or contact support.")

        try:
            return await styles_db.create_style(conn, catalog_id, collection_id, style)
        except IntegrityError as e:
            # asyncpg specific check for unique violation on (catalog_id, collection_id, style_id)
            if e.orig and getattr(e.orig, "sqlstate", None) == '23505': # unique_violation
                raise HTTPException(status_code=409, detail=f"Style with ID '{style.style_id}' already exists for this collection.")
            raise e

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/styles", response_model=List[Style], summary="List styles for a collection")
    async def list_styles(
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
        return await styles_db.list_styles_for_collection(conn, catalog_id, collection_id, limit, offset)

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}", response_model=Style, summary="Get a specific style by its ID")
    async def get_style(
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        style = await styles_db.get_style_by_id(conn, catalog_id, style_id)
        if not style:
            raise HTTPException(status_code=404, detail="Style not found.")
        return style

    @router.put("/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}", response_model=Style, summary="Update a style")
    async def update_style(
        catalog_id: str,
        collection_id: str,
        style_id: str,
        style_update: StyleUpdate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        updated = await styles_db.update_style(conn, catalog_id, style_id, style_update)
        if not updated:
            raise HTTPException(status_code=404, detail="Style not found.")
        return updated

    @router.delete("/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a style")
    async def delete_style(
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # To delete, we need the internal id (UUID)
        style_to_delete = await styles_db.get_style_by_id(conn, catalog_id, style_id)
        if not style_to_delete:
            raise HTTPException(status_code=404, detail="Style not found.")
        
        deleted = await styles_db.delete_style(conn, catalog_id, style_to_delete.id)
        if not deleted:
            # This case should be rare if the previous check passed, but it's good practice
            raise HTTPException(status_code=404, detail="Style found but could not be deleted.")

    # --- Content-negotiated stylesheet endpoint ---
    #
    # Separate path from `get_style` (which returns the Style metadata
    # JSON). Per OGC API - Styles Part 1 the stylesheet body itself lives
    # at ``/styles/{style_id}``; we expose it at ``/stylesheet`` so the
    # existing metadata endpoint stays backward-compatible for callers
    # already relying on ``get_style``.

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/stylesheet",
        summary="Get a style's stylesheet body (content-negotiated by Accept / ?f=)",
    )
    async def get_stylesheet(
        request: Request,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        f: Optional[str] = Query(None, description="Format shorthand (mapbox|sld11|sld10|qml|flat|html|json)"),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id,
        )
        if style is None:
            raise HTTPException(status_code=404, detail="Style not found.")

        available_media_types = []
        for sheet in style.stylesheets:
            fmt = getattr(sheet.content, "format", None)
            fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
            mt = STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
            if mt is not None:
                available_media_types.append(mt)

        if not available_media_types:
            raise HTTPException(
                status_code=404, detail="Style has no stylesheet in a supported encoding.",
            )

        # ?f= wins over Accept.
        target = f_param_to_media_type(f) if f else None
        if target is None:
            target = normalize_accept_to_media_type(
                request.headers.get("accept", ""),
                available_media_types,
            )
        if target is None:
            raise HTTPException(
                status_code=406,
                detail=f"No acceptable encoding. Available: {available_media_types}",
            )
        if target not in available_media_types:
            # Mismatch (e.g. ?f=sld10 but only SLD_1.1 stored): 406.
            raise HTTPException(
                status_code=406,
                detail=f"Requested encoding {target!r} not stored for this style.",
            )

        sheet = _pick_stylesheet_by_media_type(style, target)
        if sheet is None:
            raise HTTPException(status_code=406, detail="Encoding not stored.")
        return Response(
            content=_stylesheet_to_bytes(sheet),
            media_type=target,
        )

    # --- Style metadata endpoint (OGC API - Styles style-info) ---

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/metadata",
        summary="Style metadata with stylesheet links per encoding",
    )
    async def get_style_metadata(
        request: Request,
        catalog_id: str,
        collection_id: str,
        style_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        style = await styles_db.get_style_by_id_and_collection(
            conn, catalog_id, collection_id, style_id,
        )
        if style is None:
            raise HTTPException(status_code=404, detail="Style not found.")

        root = get_root_url(request).rstrip("/")
        base_href = (
            f"{root}/styles/catalogs/{catalog_id}"
            f"/collections/{collection_id}/styles/{style_id}"
        )
        stylesheet_links = []
        for sheet in style.stylesheets:
            fmt = getattr(sheet.content, "format", None)
            fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
            mt = STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
            if mt is None:
                continue  # unknown encoding — not advertised
            stylesheet_links.append({
                "rel": "stylesheet",
                "type": mt,
                "href": f"{base_href}/stylesheet",
                "title": f"{style.title or style.style_id} ({mt})",
            })
        links = list(style.links or [])
        links.extend(stylesheet_links)
        return JSONResponse(content={
            "id": style.style_id,
            "title": style.title,
            "description": style.description,
            "keywords": style.keywords,
            "scope": "style",
            "links": links,
        })

    # --- Flat styles list (root discovery) ---

    @router.get(
        "/",
        summary="List all styles across catalogs (discovery convenience)",
    )
    async def list_all_styles(
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        # Reuses the existing per-collection helper by querying across all
        # catalogs — db.py may not expose a list_all(); if not, this returns
        # an empty discovery result rather than failing.
        try:
            # Prefer a dedicated helper if present.
            all_fn = getattr(styles_db, "list_all_styles", None)
            if callable(all_fn):
                styles = await all_fn(conn, limit=limit, offset=offset)
            else:
                styles = []
        except Exception:
            styles = []
        return {
            "styles": [
                {"id": s.style_id, "title": s.title, "description": s.description}
                for s in styles
                if s is not None
            ],
        }
