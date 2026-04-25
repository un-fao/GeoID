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

"""OGC API - Discrete Global Grid Systems (DGGS) Part 1 extension for DynaStore.

Implements the following endpoints:

    GET /dggs/                                                  landing page
    GET /dggs/conformance                                       conformance
    GET /dggs/dggs-list                                         list DGGRS
    GET /dggs/dggs-list/{dggsId}                               DGGRS metadata
    GET /dggs/collections                                       DGGS-enabled collections
    GET /dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs
        Aggregate collection features into H3 zones
    GET /dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs/{zoneId}
        Features aggregated within a specific H3 zone
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Set

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, status

from dynastore.extensions.dggs.config import DGGSConfig
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.shared_models import Link
from dynastore.modules.dggs.aggregator import aggregate_features
from dynastore.modules.dggs.h3_indexer import (
    H3_MAX_RESOLUTION,
    H3_MIN_RESOLUTION,
    bbox_to_cells,
    get_resolution,
    is_valid_cell,
    parse_bbox,
)
from dynastore.modules.dggs.models import (
    DGGRSInfo,
    DGGRSList,
    DGGSFeatureCollection,
)
from dynastore.modules.dggs.zone_query import (
    build_global_query,
    build_query_for_bbox,
    build_query_for_zone,
    build_query_for_zone_indexed,
)

logger = logging.getLogger(__name__)


def _parse_parameter_names(parameter_name: Optional[str]) -> Optional[Set[str]]:
    """Parse a comma-separated parameter-name query string into a set, or None."""
    if not parameter_name:
        return None
    return {p.strip() for p in parameter_name.split(",") if p.strip()}


OGC_API_DGGS_URIS = [
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/core",
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/data-retrieval",
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/zone-query",
]

_H3_DGGRS = DGGRSInfo(
    id="H3",
    title="Uber H3 Hierarchical Hexagonal Grid",
    description=(
        "The H3 Discrete Global Grid Reference System uses a hierarchical hexagonal "
        "tessellation of the sphere at 16 resolutions (0–15). It is widely used for "
        "planetary-scale geospatial aggregations (agriculture, climate, food security)."
    ),
    uri="https://h3geo.org/",
    maxRefinementLevel=H3_MAX_RESOLUTION,
    defaultRefinementLevel=5,
    links=[],
)

_SUPPORTED_DGGRS: dict = {"H3": _H3_DGGRS}


class DGGSService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - DGGS Part 1 extension.

    Priority 170 — after Coverages (160), before Dimensions (200).
    """

    priority: int = 170
    router: APIRouter

    conformance_uris = OGC_API_DGGS_URIS
    prefix = "/dggs"
    protocol_title = "DynaStore OGC API - DGGS"
    protocol_description = (
        "Access to collection data aggregated into Discrete Global Grid Systems (DGGS)"
    )

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/dggs", tags=["OGC API - DGGS"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.register_policies()
        logger.info("DGGSService: policies registered.")
        yield

    def register_policies(self):
        register_ogc_public_access_policy("dggs")

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/dggs-list",
            self.get_dggrs_list,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/dggs-list/{dggsId}",
            self.get_dggrs,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/collections",
            self.get_collections,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/dggs",
            self.get_dggs_data,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/dggs/{zoneId}",
            self.get_dggs_zone,
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # Landing page & conformance
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # DGGRS discovery endpoints
    # ------------------------------------------------------------------

    async def get_dggrs_list(self, request: Request) -> DGGRSList:
        """List all supported DGGRS."""
        root_url = get_root_url(request)
        dggrs = []
        for dggrs_id, info in _SUPPORTED_DGGRS.items():
            enriched = info.model_copy(
                update={
                    "links": [
                        Link(
                            href=f"{root_url}/dggs/dggs-list/{dggrs_id}",
                            rel="self",
                            type="application/json",
                            title=f"{dggrs_id} DGGRS definition",  # type: ignore[arg-type]
                        )
                    ]
                }
            )
            dggrs.append(enriched)
        return DGGRSList(
            dggrs=dggrs,
            links=[
                Link(
                    href=f"{root_url}/dggs/dggs-list",
                    rel="self",
                    type="application/json",
                    title="List of supported DGGRS",  # type: ignore[arg-type]
                )
            ],
        )

    async def get_dggrs(self, dggsId: str, request: Request) -> DGGRSInfo:
        """Return metadata for a specific DGGRS."""
        info = _SUPPORTED_DGGRS.get(dggsId.upper())
        if info is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"DGGRS '{dggsId}' not supported. Supported: {list(_SUPPORTED_DGGRS)}",
            )
        return info

    # ------------------------------------------------------------------
    # Collections listing
    # ------------------------------------------------------------------

    async def get_collections(self, request: Request) -> dict:
        """List all collections accessible via DGGS endpoints.

        Returns the same collection list as the STAC/Features extension but
        scoped to the /dggs namespace, so clients can discover which collections
        support DGGS data retrieval.
        """
        catalogs_svc = await self._get_catalogs_service()
        try:
            catalogs = await catalogs_svc.list_catalogs()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list catalogs: {exc}",
            )

        root_url = get_root_url(request)
        result = []
        for catalog in catalogs or []:
            catalog_id = getattr(catalog, "id", None) or catalog.get("id", "")
            try:
                collections = await catalogs_svc.list_collections(catalog_id)
            except Exception:
                continue
            for col in collections or []:
                col_id = getattr(col, "id", None) or col.get("id", "")
                result.append(
                    {
                        "id": col_id,
                        "catalog": catalog_id,
                        "links": [
                            {
                                "href": (
                                    f"{root_url}/dggs/catalogs/{catalog_id}"
                                    f"/collections/{col_id}/dggs"
                                ),
                                "rel": "dggs",
                                "type": "application/geo+json",
                                "title": f"DGGS data for {col_id}",
                            }
                        ],
                    }
                )

        return {"collections": result, "numberMatched": len(result)}

    # ------------------------------------------------------------------
    # DGGS data retrieval endpoints
    # ------------------------------------------------------------------

    async def get_dggs_data(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        zone_level: Optional[int] = Query(
            None,
            alias="zone-level",
            ge=H3_MIN_RESOLUTION,
            le=H3_MAX_RESOLUTION,
            description="H3 resolution level (0-15). Defaults to DGGSConfig.default_resolution.",
        ),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box filter: xmin,ymin,xmax,ymax (WGS-84)",
        ),
        datetime: Optional[str] = Query(
            None,
            description="Temporal filter (RFC 3339 date or date-time, or open interval)",
        ),
        parameter_name: Optional[str] = Query(
            None,
            alias="parameter-name",
            description="Comma-separated property names to include in aggregation",
        ),
        dggs_id: str = Query(
            "H3",
            alias="dggs-id",
            description="DGGRS identifier (currently only 'H3' is supported)",
        ),
    ) -> DGGSFeatureCollection:
        """Retrieve collection features aggregated into H3 DGGS zones.

        Queries PostGIS for features in the requested bbox/datetime, then
        aggregates them on-the-fly into H3 hexagons at the requested resolution.
        """
        dggs_id = dggs_id.upper()
        if dggs_id not in _SUPPORTED_DGGRS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported DGGRS '{dggs_id}'. Supported: {list(_SUPPORTED_DGGRS)}",
            )

        config = await self._get_dggs_config(catalog_id, collection_id)
        resolution = zone_level if zone_level is not None else config.default_resolution
        resolution = min(resolution, config.max_resolution)

        param_names = _parse_parameter_names(parameter_name)

        bbox_tuple = None
        if bbox:
            try:
                bbox_tuple = parse_bbox(bbox)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
                )

        if bbox_tuple:
            query = build_query_for_bbox(
                *bbox_tuple,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        else:
            query = build_global_query(
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )

        features = await self._fetch_features(catalog_id, collection_id, query)
        return aggregate_features(features, resolution, param_names, dggs_id)

    async def get_dggs_zone(
        self,
        catalog_id: str,
        collection_id: str,
        zoneId: str,
        request: Request,
        datetime: Optional[str] = Query(None),
        parameter_name: Optional[str] = Query(
            None,
            alias="parameter-name",
        ),
        dggs_id: str = Query(
            "H3",
            alias="dggs-id",
            description="DGGRS identifier (currently only 'H3' is supported)",
        ),
    ) -> DGGSFeatureCollection:
        """Retrieve data aggregated within a specific H3 zone.

        The zoneId must be a valid H3 cell index. Features intersecting the
        zone's bounding box are fetched from PostGIS and aggregated into the
        single requested cell.

        Note: features whose centroid falls inside the bbox but outside the
        exact H3 hexagon boundary will map to a neighbouring cell and be
        excluded from the result. This is expected behaviour for centroid-based
        aggregation at high resolutions.
        """
        dggs_id = dggs_id.upper()
        if dggs_id not in _SUPPORTED_DGGRS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported DGGRS '{dggs_id}'. Supported: {list(_SUPPORTED_DGGRS)}",
            )

        if not is_valid_cell(zoneId):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid H3 zone ID: {zoneId!r}",
            )

        resolution = get_resolution(zoneId)
        config = await self._get_dggs_config(catalog_id, collection_id)

        if await self._has_h3_field(catalog_id, collection_id, resolution):
            # Preferred: exact B-tree EQ on pre-computed sidecar column.
            query = build_query_for_zone_indexed(
                zoneId,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        else:
            # Fallback: GIST bbox scan (may overselect near hexagon edges).
            query = build_query_for_zone(
                zoneId,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        features = await self._fetch_features(catalog_id, collection_id, query)

        param_names = _parse_parameter_names(parameter_name)
        all_zones = aggregate_features(features, resolution, param_names, dggs_id=dggs_id)

        # Keep only the requested zone
        matching = [f for f in all_zones.features if f.id == zoneId]
        return all_zones.model_copy(
            update={
                "features": matching,
                "numberMatched": len(matching),
                "numberReturned": len(matching),
            }
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _has_h3_field(
        self,
        catalog_id: str,
        collection_id: str,
        resolution: int,
    ) -> bool:
        """Return True if the collection's geometry sidecar has ``h3_res{resolution}`` pre-computed.

        When True, :func:`build_query_for_zone_indexed` can be used instead of the
        bbox fallback, giving exact B-tree indexed lookups.
        """
        try:
            from dynastore.models.protocols import ItemsProtocol
            from dynastore.tools.discovery import get_protocol

            items_svc = get_protocol(ItemsProtocol)
            if not items_svc:
                return False
            fields = await items_svc.get_collection_fields(catalog_id, collection_id)
            return f"h3_res{resolution}" in (fields or {})
        except Exception:
            return False

    async def _get_dggs_config(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> DGGSConfig:
        try:
            configs_svc = await self._get_configs_service()
            return await configs_svc.get_config(DGGSConfig, catalog_id, collection_id)
        except Exception:
            return DGGSConfig()

    async def _fetch_features(
        self,
        catalog_id: str,
        collection_id: str,
        query,
    ) -> list:
        catalogs_svc = await self._get_catalogs_service()
        try:
            return await catalogs_svc.search_items(catalog_id, collection_id, query)
        except Exception as exc:
            logger.exception("DGGS query failed for collection '%s/%s'", catalog_id, collection_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to query collection '{collection_id}': {exc}",
            ) from exc
