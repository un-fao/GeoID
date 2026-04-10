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
        self._register_ogc_conformance()
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
        raise HTTPException(
            status_code=501,
            detail="Coverage data retrieval not yet implemented.",
        )

    async def get_domain_set(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> cm.DomainSet:
        raise HTTPException(
            status_code=501,
            detail="Domain set retrieval not yet implemented.",
        )

    async def get_range_type(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> cm.RangeType:
        raise HTTPException(
            status_code=501,
            detail="Range type retrieval not yet implemented.",
        )
