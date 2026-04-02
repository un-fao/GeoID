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

"""OGC API - Records Part 1 extension for DynaStore.

Serves RECORDS-type collections as OGC API - Records catalogues.
Records are stored as ``Feature(geometry=None, properties={...})`` using
the standard sidecar pipeline (AttributesSidecar).  The GeometrySidecar
is skipped for RECORDS collections.

Delegates to ``CatalogsProtocol`` / ``ItemsProtocol`` for all CRUD —
no new storage layer is introduced.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.conformance import register_conformance_uris
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols import CatalogsProtocol, ItemsProtocol
from dynastore.models.shared_models import Link
from dynastore.modules.catalog.sidecars.base import ConsumerType
from dynastore.tools.discovery import get_protocol

from . import records_generator as gen
from . import records_models as rm
from .policies import register_records_policies

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OGC API - Records conformance URIs (OGC 20-004)
# ---------------------------------------------------------------------------

OGC_API_RECORDS_URIS = [
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-core",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-collection",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/sorting",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class RecordsService(ExtensionProtocol):
    """OGC API - Records Part 1 extension.

    Priority 150 — after STAC (100) and Features (100), before
    Dimensions (200).
    """

    priority: int = 150
    router: APIRouter

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/records", tags=["OGC API - Records"])
        register_conformance_uris(OGC_API_RECORDS_URIS)
        self._catalogs_protocol: Optional[CatalogsProtocol] = None
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        register_conformance_uris(OGC_API_RECORDS_URIS)
        register_records_policies()
        logger.info("RecordsService: conformance classes and policies registered.")
        yield

    # ------------------------------------------------------------------
    # Protocol helpers
    # ------------------------------------------------------------------

    async def _get_catalogs_service(self) -> CatalogsProtocol:
        if self._catalogs_protocol is None:
            svc = get_protocol(CatalogsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Catalogs service not available."
                )
            self._catalogs_protocol = svc
        return cast(CatalogsProtocol, self._catalogs_protocol)

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        # Landing page & conformance
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            response_model=rm.LandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=rm.Conformance,
        )

        # Collections (RECORDS-type only)
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections,
            methods=["GET"],
            response_model=rm.RecordsCatalogCollections,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection,
            methods=["GET"],
            response_model=rm.RecordsCatalogCollection,
        )

        # Record items
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.get_records,
            methods=["GET"],
            response_model=rm.RecordCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.add_records,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{record_id}",
            self.get_record,
            methods=["GET"],
            response_model=rm.Record,
        )

    # ------------------------------------------------------------------
    # Landing page & conformance
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request) -> rm.LandingPage:
        root_url = get_root_url(request)
        return rm.LandingPage(
            title="DynaStore OGC API - Records",
            description="Access to catalog records via OGC API - Records",
            links=[
                Link(href=f"{root_url}/records/", rel="self", type="application/json", title="This document"),
                Link(href=f"{root_url}/records/conformance", rel="conformance", type="application/json", title="Conformance classes"),
                Link(href=f"{root_url}/api", rel="service-doc", type="application/json", title="API documentation"),
            ],
        )

    async def get_conformance(self, request: Request) -> rm.Conformance:
        return rm.Conformance(conformsTo=OGC_API_RECORDS_URIS)

    # ------------------------------------------------------------------
    # Collections (filtered to RECORDS type)
    # ------------------------------------------------------------------

    async def list_collections(
        self,
        catalog_id: str,
        request: Request,
        language: str = Depends(get_language),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> rm.RecordsCatalogCollections:
        catalogs_svc = await self._get_catalogs_service()

        all_collections = await catalogs_svc.list_collections(
            catalog_id, lang=language, limit=limit, offset=offset,
        )
        if all_collections is None:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        root_url = get_root_url(request)

        # Filter to RECORDS-type collections
        records_collections = []
        for coll in all_collections:
            if await self._is_records_collection(catalog_id, coll):
                localized, _ = coll.localize(language) if hasattr(coll, "localize") else (coll, language)
                records_collections.append(
                    gen.collection_to_records_collection(localized, catalog_id, root_url)
                )

        links = [
            Link(href=str(request.url), rel="self", type="application/json"),
        ]
        return rm.RecordsCatalogCollections(
            collections=records_collections,
            links=links,
            numberMatched=len(records_collections),
            numberReturned=len(records_collections),
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ) -> rm.RecordsCatalogCollection:
        catalogs_svc = await self._get_catalogs_service()

        coll = await catalogs_svc.get_collection(catalog_id, collection_id, lang=language)
        if not coll:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        if not await self._is_records_collection(catalog_id, coll):
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' is not a records collection.")

        root_url = get_root_url(request)
        localized, _ = coll.localize(language) if hasattr(coll, "localize") else (coll, language)
        return gen.collection_to_records_collection(localized, catalog_id, root_url)

    # ------------------------------------------------------------------
    # Record items
    # ------------------------------------------------------------------

    async def get_records(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(10, ge=1, le=1000, description="Maximum number of records to return."),
        offset: int = Query(0, ge=0, description="Offset of the first record to return."),
        filter: Optional[str] = Query(None, description="CQL2-TEXT filter expression."),
        sortby: Optional[str] = Query(None, description="Sort order (e.g., '-title,+created')."),
        q: Optional[str] = Query(None, description="Free-text search query."),
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()

        collection_meta = await catalogs_svc.get_collection(catalog_id, collection_id, lang="en")
        if not collection_meta:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        from dynastore.extensions.features.features_service import parse_ogc_query_request

        request_obj = parse_ogc_query_request(
            bbox=None,
            datetime_param=None,
            sortby=sortby,
            filter=filter,
            limit=limit,
            offset=offset,
            include_total_count=True,
        )

        # Free-text search — add as CQL2 LIKE filter on title if present
        if q:
            # Escape SQL special characters to prevent injection
            safe_q = q.replace("'", "''").replace("%", r"\%").replace("_", r"\_")
            ilike_expr = f"title ILIKE '%{safe_q}%' ESCAPE '\\'"
            if request_obj.cql_filter:
                request_obj.cql_filter = f"({request_obj.cql_filter}) AND {ilike_expr}"
            else:
                request_obj.cql_filter = ilike_expr

        items_protocol = cast(ItemsProtocol, catalogs_svc)
        try:
            query_response = await items_protocol.stream_items(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request_obj,
                db_resource=conn,
                consumer=ConsumerType.OGC_RECORDS,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        count = query_response.total_count or 0
        root_url = get_root_url(request)

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, db_resource=conn,
        )

        records: List[rm.Record] = []
        async for feature in query_response:
            records.append(gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config))

        # Pagination links
        base_url = str(request.url).split("?")[0]
        query_params = dict(request.query_params)
        links = [
            Link(href=str(request.url), rel="self", type="application/geo+json"),
        ]
        if offset > 0:
            prev_params = query_params.copy()
            prev_params["offset"] = str(max(0, offset - limit))
            links.append(
                Link(
                    href=f"{base_url}?{'&'.join(f'{k}={v}' for k, v in prev_params.items())}",
                    rel="prev",
                    type="application/geo+json",
                )
            )
        if offset + limit < count:
            next_params = query_params.copy()
            next_params["offset"] = str(offset + limit)
            links.append(
                Link(
                    href=f"{base_url}?{'&'.join(f'{k}={v}' for k, v in next_params.items())}",
                    rel="next",
                    type="application/geo+json",
                )
            )

        result = rm.RecordCollection(
            type="FeatureCollection",
            features=records,
            links=links,
            numberMatched=count,
            numberReturned=len(records),
        )
        return JSONResponse(
            content=result.model_dump(exclude_none=True),
            media_type="application/geo+json",
        )

    async def get_record(
        self,
        catalog_id: str,
        collection_id: str,
        record_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> rm.Record:
        catalogs_svc = await self._get_catalogs_service()
        items_protocol = cast(ItemsProtocol, catalogs_svc)

        feature = await items_protocol.get_item(
            catalog_id, collection_id, record_id, db_resource=conn,
        )
        if not feature:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found.")

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, db_resource=conn,
        )
        root_url = get_root_url(request)
        return gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config)

    async def add_records(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Response:
        """Create/upsert records into a RECORDS-type collection."""
        catalogs_svc = await self._get_catalogs_service()

        body = await request.json()

        # Accept single record or array
        if isinstance(body, list):
            items = body
        elif isinstance(body, dict) and body.get("type") == "FeatureCollection":
            items = body.get("features", [])
        elif isinstance(body, dict):
            items = [body]
        else:
            raise HTTPException(status_code=400, detail="Invalid request body.")

        # Ensure geometry is null for records
        for item in items:
            if isinstance(item, dict):
                item["geometry"] = None

        result = await catalogs_svc.upsert(
            catalog_id=catalog_id,
            collection_id=collection_id,
            items=items,
            db_resource=conn,
        )

        root_url = get_root_url(request)
        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, db_resource=conn,
        )

        if isinstance(result, list) and len(result) == 1:
            record = gen.db_row_to_record(result[0], catalog_id, collection_id, root_url, layer_config)
            return JSONResponse(
                content=record.model_dump(exclude_none=True),
                status_code=status.HTTP_201_CREATED,
            )

        records = []
        if isinstance(result, list):
            for feat in result:
                records.append(gen.db_row_to_record(feat, catalog_id, collection_id, root_url, layer_config))

        collection = rm.RecordCollection(
            type="FeatureCollection",
            features=records,
            numberReturned=len(records),
        )
        return JSONResponse(
            content=collection.model_dump(exclude_none=True),
            status_code=status.HTTP_201_CREATED,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _is_records_collection(
        self, catalog_id: str, collection: Any
    ) -> bool:
        """Check if a collection is of RECORDS type via its driver config."""
        from dynastore.modules.storage.driver_config import get_pg_collection_config

        coll_id = collection.id if hasattr(collection, "id") else collection.get("id")
        if not coll_id:
            return False
        try:
            config = await get_pg_collection_config(catalog_id, coll_id)
            return getattr(config, "collection_type", "VECTOR") == "RECORDS"
        except Exception:
            return False
