#    Copyright 2026 FAO
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

"""OGC API - 3D GeoVolumes extension for DynaStore.

Implements the OGCServiceMixin architecture for 3D GeoVolumes support,
including CityJSON content negotiation and spatial query (OGC 22-029).
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_static, expose_web_page
from dynastore.extensions.geovolumes.geovolumes_models import (
    ContentExtent,
    ContentLink,
    ThreeDContainer,
    ThreeDContainerList,
    _bbox_intersects,
    _parse_bbox,
)

import cjio as _cjio_scope_gate  # noqa: F401  # SCOPE gate: extension_geovolumes requires cjio
import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_geovolumes requires pyproj
_ = (_cjio_scope_gate, _pyproj_scope_gate)  # silence pyright "unused" — load-bearing for SCOPE filtering

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - 3D GeoVolumes conformance URIs
# ---------------------------------------------------------------------------

OGC_API_GEOVOLUMES_URIS = [
    "http://www.opengis.net/spec/ogcapi-geovolumes-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-geovolumes-1/1.0/conf/spatialquery",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class GeoVolumesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - 3D GeoVolumes extension.

    Priority 175 — after Coverages (160), EDR (165), DGGS (170); before Joins (180).
    """

    priority: int = 175
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_GEOVOLUMES_URIS
    prefix = "/geovolumes"
    protocol_title = "DynaStore OGC API - 3D GeoVolumes"
    protocol_description = "Access to 3D geospatial volumes and CityJSON data via OGC API - 3D GeoVolumes"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/geovolumes", tags=["OGC API - 3D GeoVolumes"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("GeoVolumesService: policies registered.")
        yield

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_3d_collections,
            methods=["GET"],
            summary="List 3D container collections",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_3d_collection,
            methods=["GET"],
            summary="Get a single 3D container",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/cityjsonseq",
            self.stream_cityjsonseq,
            methods=["GET"],
            summary="Stream CityJSONSeq for a 3D container collection",
        )

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    async def list_3d_collections(
        self,
        catalog_id: str,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        bbox: Optional[str] = Query(None),
    ) -> Dict[str, Any]:
        """List collections flagged as 3D containers.

        A collection is 3D iff its extras carry ``cityjson:version`` or
        the explicit marker ``geovolumes:enabled``. The optional ``bbox``
        parameter (4 or 6 comma-separated floats) filters by spatial extent.
        """
        parsed_bbox: Optional[Tuple] = None
        if bbox is not None:
            try:
                parsed_bbox = _parse_bbox(bbox)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        catalogs_svc = await self._get_catalogs_service()
        all_collections = await catalogs_svc.list_collections(
            catalog_id, limit=limit, offset=offset
        )

        containers: List[ThreeDContainer] = []
        for coll in (all_collections or []):
            if not _is_3d_collection(coll):
                continue
            container = _build_3d_container(coll, catalog_id)
            if parsed_bbox is not None:
                if not _bbox_intersects(container.contentExtent.bbox, parsed_bbox):
                    continue
            containers.append(container)

        return ThreeDContainerList(
            collections=containers,
            links=[],
        ).model_dump(exclude_none=True)

    async def get_3d_collection(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Dict[str, Any]:
        """Return a ThreeDContainer for the given collection.

        Returns 404 if the collection does not exist or is not 3D.
        """
        catalogs_svc = await self._get_catalogs_service()
        coll = await catalogs_svc.get_collection(catalog_id, collection_id)
        if coll is None:
            raise HTTPException(status_code=404, detail="Collection not found.")
        if not _is_3d_collection(coll):
            raise HTTPException(
                status_code=404,
                detail="Collection is not a 3D GeoVolumes container.",
            )
        container = _build_3d_container(coll, catalog_id)
        return container.model_dump(exclude_none=True)

    async def stream_cityjsonseq(
        self,
        catalog_id: str,
        collection_id: str,
        limit: int = Query(10000, ge=1, le=100000),
    ) -> StreamingResponse:
        """Stream a CityJSONSeq response for all items in a 3D collection.

        Line 1 is a CityJSONSeq header reconstructed from collection extras.
        Subsequent lines are individual CityJSONFeature objects (NDJSON).
        Media type: ``application/city+json``.
        """
        catalogs_svc = await self._get_catalogs_service()
        coll = await catalogs_svc.get_collection(catalog_id, collection_id)
        if coll is None:
            raise HTTPException(status_code=404, detail="Collection not found.")
        if not _is_3d_collection(coll):
            raise HTTPException(
                status_code=404,
                detail="Collection is not a 3D GeoVolumes container.",
            )

        extras = _get_extras(coll)
        header = _build_cityjsonseq_header(extras)

        from dynastore.models.query_builder import QueryRequest

        features = await catalogs_svc.search_items(
            catalog_id, collection_id, QueryRequest(limit=limit)
        )

        async def _generate():
            yield json.dumps(header) + "\n"
            for feat in (features or []):
                cityjson = _extract_cityjson(feat)
                if cityjson is not None:
                    yield json.dumps(cityjson) + "\n"

        return StreamingResponse(
            _generate(),
            media_type="application/city+json",
        )

    # ------------------------------------------------------------------
    # Web page contributions (globe browser)
    # ------------------------------------------------------------------

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    @expose_static("geovolumes")
    def provide_static_files(self) -> list:
        """Exposes the static directory for the GeoVolumes globe browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        if os.path.isdir(static_dir):
            for root, _, filenames in os.walk(static_dir):
                for filename in filenames:
                    files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="geovolumes_browser",
        title={"en": "3D GeoVolumes", "fr": "GéoVolumes 3D", "es": "GeoVolúmenes 3D"},
        icon="fa-cube",
        description={
            "en": "Browse 3D building volumes and CityJSON data on a globe.",
            "fr": "Explorer des volumes de bâtiments 3D et des données CityJSON sur un globe.",
            "es": "Explorar volúmenes de edificios 3D y datos CityJSON en un globo.",
        },
    )
    async def provide_geovolumes_browser(self, request: Request):
        return await self._serve_page_template("geovolumes_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION

        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(
                content=f.read().replace("{{VERSION}}", VERSION),
                media_type="text/html",
            )


# ---------------------------------------------------------------------------
# Collection classification and decoration helpers
# ---------------------------------------------------------------------------


def _get_extras(coll: Any) -> Dict[str, Any]:
    """Extract the extras dict from a Collection, tolerating varied shapes."""
    raw_extra = getattr(coll, "model_extra", None) or {}
    extras = raw_extra.get("extras") or {}
    if not extras and isinstance(raw_extra, dict):
        # Flat extras stored directly on model_extra
        extras = {k: v for k, v in raw_extra.items() if ":" in k}
    return extras


def _is_3d_collection(coll: Any) -> bool:
    """Return True iff the collection carries 3D GeoVolumes provenance."""
    extras = _get_extras(coll)
    return bool(extras.get("cityjson:version") or extras.get("geovolumes:enabled"))


def _collection_bbox_3d(coll: Any, extras: Dict[str, Any]) -> List[float]:
    """Build a 6-element 3D bbox from collection extent + z-range extras."""
    bbox_2d: List[float] = []
    extent = getattr(coll, "extent", None)
    if extent is not None:
        spatial = getattr(extent, "spatial", None)
        if spatial is not None:
            raw = getattr(spatial, "bbox", None) or []
            if raw:
                first = raw[0] if isinstance(raw[0], (list, tuple)) else raw
                bbox_2d = list(first)

    if len(bbox_2d) >= 4:
        minx, miny, maxx, maxy = bbox_2d[0], bbox_2d[1], bbox_2d[2], bbox_2d[3]
    else:
        minx, miny, maxx, maxy = 0.0, 0.0, 0.0, 0.0

    zrange = extras.get("geovolumes:zrange") or {}
    zmin = float(zrange.get("zmin", 0.0))
    zmax = float(zrange.get("zmax", 0.0))
    return [minx, miny, zmin, maxx, maxy, zmax]


def _resolve_3dtiles_href(coll: Any) -> Optional[str]:
    """Return the href of the first asset with role '3dtiles', or None."""
    assets: Dict[str, Any] = getattr(coll, "assets", None) or {}
    for asset in assets.values():
        if not isinstance(asset, dict):
            continue
        roles = asset.get("roles") or []
        if "3dtiles" in roles:
            return asset.get("href")
    return None


def _build_cityjsonseq_link(catalog_id: str, collection_id: str) -> ContentLink:
    """Build the alternate CityJSONSeq content link for the given collection."""
    return ContentLink(
        rel="alternate",
        href=f"/geovolumes/catalogs/{catalog_id}/collections/{collection_id}/cityjsonseq",
        type="application/city+json",
        title="CityJSONSeq stream",
    )


def _build_3d_container(coll: Any, catalog_id: str) -> ThreeDContainer:
    """Build a ThreeDContainer wire model from a Collection."""
    extras = _get_extras(coll)
    bbox_3d = _collection_bbox_3d(coll, extras)
    content: List[ContentLink] = []

    tileset_href = _resolve_3dtiles_href(coll)
    if tileset_href:
        content.append(
            ContentLink(
                rel="http://www.opengis.net/def/rel/ogc/1.0/3dtiles",
                href=tileset_href,
                type="application/json+3dtiles",
                title="3D Tiles tileset",
            )
        )

    content.append(_build_cityjsonseq_link(catalog_id, coll.id))

    return ThreeDContainer(
        id=coll.id,
        title=getattr(coll, "title", None),
        collectionType="3dcontainer",
        contentExtent=ContentExtent(bbox=bbox_3d),
        content=content,
        links=None,
        children=None,
    )


def _build_cityjsonseq_header(extras: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct a CityJSONSeq header dict from collection extras."""
    transform = extras.get("cityjson:transform") or {
        "scale": [1.0, 1.0, 1.0],
        "translate": [0.0, 0.0, 0.0],
    }
    header: Dict[str, Any] = {
        "type": "CityJSONSeq",
        "version": extras.get("cityjson:version", "2.0"),
        "transform": transform,
    }
    ref_sys = extras.get("cityjson:referenceSystem")
    if ref_sys:
        header["metadata"] = {"referenceSystem": ref_sys}
    return header


def _extract_cityjson(feat: Any) -> Optional[Dict[str, Any]]:
    """Extract the stored CityJSONFeature dict from an item, or None."""
    # Try model_extra path (extras container)
    model_extra = getattr(feat, "model_extra", None) or {}
    extras = model_extra.get("extras") or {}
    cityjson = extras.get("cityjson")
    if cityjson is not None:
        return cityjson
    # Flat model_extra path
    cityjson = model_extra.get("cityjson")
    if cityjson is not None:
        return cityjson
    # Plain dict from model_dump
    if isinstance(feat, dict):
        return feat.get("extras", {}).get("cityjson") or feat.get("cityjson")
    return None
