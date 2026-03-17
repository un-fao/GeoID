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

import logging
import uuid
from typing import List, Optional

from fastapi import (APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request)
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
from dynastore.modules.styles.models import Style, StyleCreate, StyleUpdate
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


from dynastore.models.protocols import StylesProtocol
class StylesService(protocols.ExtensionProtocol, StylesProtocol):
    priority: int = 100
    """
    Provides an OGC API - Styles compliant service.
    """
    router: APIRouter = APIRouter(prefix="/styles", tags=["OGC API - Styles"])

    def __init__(self, app: FastAPI = None):
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
            if e.orig and hasattr(e.orig, "sqlstate") and e.orig.sqlstate == '23505': # unique_violation
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
