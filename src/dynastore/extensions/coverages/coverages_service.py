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
from typing import Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import register_ogc_public_access_policy

from . import coverages_models as cm

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - Coverages conformance URIs (OGC 19-087r6)
# ---------------------------------------------------------------------------

OGC_API_COVERAGES_URIS = [
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/json",
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
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/domainset",
            self.get_domain_set,
            methods=["GET"],
            response_model=cm.DomainSet,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/rangetype",
            self.get_range_type,
            methods=["GET"],
            response_model=cm.RangeType,
        )

    # ------------------------------------------------------------------
    # Landing page & conformance (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request) -> cm.CoveragesLandingPage:
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

        qr = QueryRequest(limit=limit)
        if bbox_str:
            try:
                qr.bbox = [float(v) for v in bbox_str.split(",")]
            except ValueError:
                raise HTTPException(400, detail="Invalid bbox format")
        if datetime_str:
            qr.datetime = datetime_str

        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
        except Exception:
            raise HTTPException(404, detail=f"Collection '{collection_id}' not found")

        features = []
        async for feature in driver.read_entities(
            catalog_id, collection_id, request=qr, limit=limit,
        ):
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

    async def get_domain_set(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> cm.DomainSet:
        """Return the domain (spatial/temporal extent) of the coverage."""
        catalogs = await self._get_catalogs_service()
        collection = await catalogs.get_collection(catalog_id, collection_id)
        if not collection:
            raise HTTPException(404, detail=f"Collection '{collection_id}' not found")

        extent = collection.extent
        general_grid = {}

        if extent:
            extent_dict = extent.model_dump(exclude_none=True) if hasattr(extent, "model_dump") else extent
            spatial = extent_dict.get("spatial", {})
            temporal = extent_dict.get("temporal", {})

            if spatial.get("bbox"):
                bbox = spatial["bbox"]
                first_bbox = bbox[0] if isinstance(bbox[0], list) else bbox
                general_grid["axis"] = [
                    {
                        "type": "RegularAxis",
                        "axisLabel": "x",
                        "lowerBound": first_bbox[0],
                        "upperBound": first_bbox[2],
                    },
                    {
                        "type": "RegularAxis",
                        "axisLabel": "y",
                        "lowerBound": first_bbox[1],
                        "upperBound": first_bbox[3],
                    },
                ]
                general_grid["srsName"] = spatial.get("crs", "http://www.opengis.net/def/crs/OGC/1.3/CRS84")

            if temporal.get("interval"):
                intervals = temporal["interval"]
                if intervals:
                    first_interval = intervals[0] if isinstance(intervals[0], list) else intervals
                    time_axis = {
                        "type": "RegularAxis",
                        "axisLabel": "t",
                        "lowerBound": first_interval[0] if first_interval[0] else None,
                        "upperBound": first_interval[1] if len(first_interval) > 1 and first_interval[1] else None,
                    }
                    axes = general_grid.get("axis", [])
                    axes.append(time_axis)
                    general_grid["axis"] = axes

        return cm.DomainSet(
            type="DomainSet",
            generalGrid=general_grid if general_grid else None,
        )

    async def get_range_type(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> cm.RangeType:
        """Return the range type (data fields) of the coverage."""
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation
        from dynastore.models.protocols.storage_driver import Capability

        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
        except Exception:
            raise HTTPException(404, detail=f"Collection '{collection_id}' not found")

        if Capability.INTROSPECTION not in driver.capabilities:
            raise HTTPException(
                501,
                detail=f"Driver '{type(driver).__name__}' does not support field introspection",
            )

        fields = await driver.introspect_schema(catalog_id, collection_id)

        # Map FieldDefinition to OGC range type fields
        range_fields = []
        for fd in fields:
            field_entry = {
                "type": "Quantity",
                "id": fd.name,
                "name": fd.name,
                "definition": f"https://dynastore.fao.org/def/{fd.data_type}",
                "encodingInfo": {"dataType": f"http://www.opengis.net/def/dataType/OGC/0/{fd.data_type}"},
            }
            if fd.description:
                desc = fd.description
                if hasattr(desc, "model_dump"):
                    desc = desc.model_dump(exclude_none=True)
                field_entry["description"] = desc
            range_fields.append(field_entry)

        return cm.RangeType(type="DataRecord", field=range_fields)

    async def _build_domain(self, catalog_id: str, collection_id: str) -> dict:
        """Build a CoverageJSON domain dict from collection extent."""
        catalogs = await self._get_catalogs_service()
        collection = await catalogs.get_collection(catalog_id, collection_id)
        domain = {"type": "Domain", "domainType": "Grid", "axes": {}}

        if collection and collection.extent:
            extent_dict = collection.extent.model_dump(exclude_none=True) if hasattr(collection.extent, "model_dump") else collection.extent
            spatial = extent_dict.get("spatial", {})
            if spatial.get("bbox"):
                bbox = spatial["bbox"]
                first_bbox = bbox[0] if isinstance(bbox[0], list) else bbox
                domain["axes"]["x"] = {"values": [first_bbox[0], first_bbox[2]]}
                domain["axes"]["y"] = {"values": [first_bbox[1], first_bbox[3]]}

        return domain
