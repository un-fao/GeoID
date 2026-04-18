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

"""OGC API - Coverages extension for DynaStore.

Demonstrates the OGCServiceMixin architecture: a new OGC protocol
extension requires only a service class with routes, conformance URIs,
and protocol-specific response models. Zero core changes needed.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, cast

from fastapi import APIRouter, FastAPI, HTTPException, Request

from dynastore.extensions.coverages.config import CoveragesConfig
from dynastore.extensions.coverages.links import build_coverage_links
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy
from dynastore.extensions.tools.url import get_root_url
from dynastore.modules.coverages.domainset import build_domainset
from dynastore.modules.coverages.rangetype import build_rangetype

from . import coverages_models as cm


def _extract_domainset(item: Optional[dict]) -> dict:
    ds = build_domainset(item) if item is not None else None
    if ds is None:
        raise HTTPException(status_code=404, detail="No coverage item found.")
    return ds


def _extract_rangetype(item: Optional[dict]) -> dict:
    rt = build_rangetype(item) if item is not None else None
    if rt is None:
        raise HTTPException(status_code=404, detail="No coverage item found.")
    return rt


def _build_metadata_response(
    *,
    item: dict,
    base_url: str,
    catalog_id: str,
    collection_id: str,
    default_style_id: Optional[str],
) -> dict:
    """Assemble the /coverage/metadata payload for a given STAC-ish item dict."""
    return {
        "title": item.get("id"),
        "extent": {"spatial": {"bbox": [item.get("bbox", [])]}},
        "domainset": build_domainset(item),
        "rangetype": build_rangetype(item),
        "links": build_coverage_links(
            base_url=base_url,
            catalog_id=catalog_id,
            collection_id=collection_id,
            default_style_id=default_style_id,
        ),
    }

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - Coverages conformance URIs (OGC 19-087r6)
# ---------------------------------------------------------------------------

OGC_API_COVERAGES_URIS = [
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-subset",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-bbox",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-datetime",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geotiff",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/netcdf",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coveragejson",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class CoveragesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - Coverages extension.

    Priority 160 — after Records (150), before Dimensions (200).
    """

    priority: int = 160
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_COVERAGES_URIS
    prefix = "/coverages"
    protocol_title = "DynaStore OGC API - Coverages"
    protocol_description = "Access to coverage data via OGC API - Coverages"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/coverages", tags=["OGC API - Coverages"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.register_policies()
        logger.info("CoveragesService: policies registered.")
        yield

    def register_policies(self):
        register_ogc_public_access_policy("coverages")

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            response_model=cm.CoveragesLandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=cm.Conformance,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage",
            self.get_coverage,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/metadata",
            self.get_coverage_metadata,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/domainset",
            self.get_coverage_domainset,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/rangetype",
            self.get_coverage_rangetype,
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # Landing page & conformance (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request) -> cm.Conformance:
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Coverage endpoints (stubs — to be implemented per data model)
    # ------------------------------------------------------------------

    async def get_coverage(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ):
        """Return coverage data as CoverageJSON.

        Supports optional bbox and datetime query parameters for subsetting.
        """
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation
        from dynastore.models.query_builder import QueryRequest

        bbox_str = request.query_params.get("bbox")
        datetime_str = request.query_params.get("datetime")
        limit = int(request.query_params.get("limit", "1000"))

        cql_parts: list[str] = []
        if bbox_str:
            try:
                bbox_nums = [float(v) for v in bbox_str.split(",")]
            except ValueError:
                raise HTTPException(400, detail="Invalid bbox format")
            if len(bbox_nums) != 4:
                raise HTTPException(400, detail="bbox must have 4 values")
            cql_parts.append(
                f"S_INTERSECTS(geometry,BBOX({bbox_nums[0]},{bbox_nums[1]},{bbox_nums[2]},{bbox_nums[3]}))"
            )
        if datetime_str:
            cql_parts.append(f"T_INTERSECTS(datetime,INTERVAL('{datetime_str}'))")

        qr = QueryRequest(limit=limit, cql_filter=" AND ".join(cql_parts) or None)

        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
        except Exception:
            raise HTTPException(404, detail=f"Collection '{collection_id}' not found")

        features = []
        iterator = await driver.read_entities(
            catalog_id, collection_id, request=qr, limit=limit,
        )
        async for feature in iterator:
            features.append(feature.model_dump(by_alias=True, exclude_none=True))

        # Build CoverageJSON response
        coverage_json = {
            "type": "Coverage",
            "domain": await self._build_domain(catalog_id, collection_id),
            "ranges": {},
            "parameters": {},
        }

        # Extract parameter values from features
        if features:
            props_keys = set()
            for f in features:
                props = f.get("properties", {})
                for k, v in props.items():
                    if isinstance(v, (int, float)):
                        props_keys.add(k)

            for key in sorted(props_keys):
                values = [
                    f.get("properties", {}).get(key)
                    for f in features
                ]
                coverage_json["parameters"][key] = {
                    "type": "Parameter",
                    "observedProperty": {"label": {"en": key}},
                }
                coverage_json["ranges"][key] = {
                    "type": "NdArray",
                    "dataType": "float",
                    "values": values,
                }

        from fastapi.responses import JSONResponse

        return JSONResponse(
            content=coverage_json,
            media_type="application/prs.coverage+json",
        )

    async def get_coverage_domainset(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages DomainSet derived from the first item."""
        item = await self._get_first_coverage_item(catalog_id, collection_id)
        return _extract_domainset(item)

    async def get_coverage_rangetype(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages RangeType derived from the first item."""
        item = await self._get_first_coverage_item(catalog_id, collection_id)
        return _extract_rangetype(item)

    async def _get_coverages_config(
        self, catalog_id: Optional[str] = None, collection_id: Optional[str] = None,
    ) -> CoveragesConfig:
        """Fetch CoveragesConfig via the platform configs service with waterfall.

        Falls back to a default-constructed CoveragesConfig if the configs service
        is unavailable (keeps handlers resilient in test / stub contexts).
        """
        try:
            configs_svc = await self._get_configs_service()
            return await configs_svc.get_config(
                CoveragesConfig, catalog_id, collection_id,
            )
        except Exception:  # pragma: no cover - defensive fallback
            return CoveragesConfig()

    async def _get_first_coverage_item(
        self, catalog_id: str, collection_id: str,
    ) -> Optional[dict]:
        """Return the first item in a collection as a plain dict, or None."""
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.query_builder import QueryRequest

        catalogs = cast(CatalogsProtocol, await self._get_catalogs_service())
        try:
            features = await catalogs.search_items(
                catalog_id, collection_id, QueryRequest(limit=1),
            )
        except Exception:
            return None
        if not features:
            return None
        first = features[0]
        # Feature is a pydantic model — coerce to the same dict shape the
        # domainset/rangetype helpers expect.
        if hasattr(first, "model_dump"):
            return first.model_dump(by_alias=True, exclude_none=True)
        return dict(first)

    async def get_coverage_metadata(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        item = await self._get_first_coverage_item(catalog_id, collection_id)
        if item is None:
            raise HTTPException(status_code=404, detail="No coverage item found.")
        cfg = await self._get_coverages_config(catalog_id, collection_id)
        return _build_metadata_response(
            item=item,
            base_url=get_root_url(request).rstrip("/"),
            catalog_id=catalog_id,
            collection_id=collection_id,
            default_style_id=getattr(cfg, "default_style_id", None),
        )

    async def _build_domain(self, catalog_id: str, collection_id: str) -> dict:
        """Build a CoverageJSON domain dict from collection extent."""
        catalogs = await self._get_catalogs_service()
        collection = await catalogs.get_collection(catalog_id, collection_id)
        domain = {"type": "Domain", "domainType": "Grid", "axes": {}}

        if collection and collection.extent:
            extent_dict = collection.extent.model_dump(exclude_none=True)
            spatial = extent_dict.get("spatial") or {}
            if spatial.get("bbox"):
                bbox = spatial["bbox"]
                first_bbox = bbox[0] if isinstance(bbox[0], list) else bbox
                domain["axes"]["x"] = {"values": [first_bbox[0], first_bbox[2]]}
                domain["axes"]["y"] = {"values": [first_bbox[1], first_bbox[3]]}

        return domain
